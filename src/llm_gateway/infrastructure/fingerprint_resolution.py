"""Bounded off-loop Fingerprint material delivery, without retries or caching.

The source validates exact member identity/version/profile and 32 raw bytes.
Its permit follows material ownership, including abandoned OS reads, until the
buffer is closed. Closing this resolver abandons pending deliveries; already
handed-off material remains operation-owned and must be closed by that caller.
"""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from typing import Protocol

from llm_gateway.adapters.fingerprint_material import FingerprintMaterial, FingerprintMaterialUnavailable
from llm_gateway.application.concurrency import ConcurrencyPool
from llm_gateway.application.fingerprint_keys import FingerprintSourceCapacity
from llm_gateway.domain.fingerprint_keys import FingerprintKeyMember


class BlockingFingerprintSource(Protocol):
    def resolve(self, member: FingerprintKeyMember) -> FingerprintMaterial:
        """One fresh exact-version read; return validated operation-owned material."""
        ...


class _HeldMaterial:
    def __init__(self, material, permit):
        self._material, self._permit = material, permit
        self._closed = False

    def digest(self, message: bytes) -> bytes:
        if self._closed:
            raise FingerprintMaterialUnavailable()
        return self._material.digest(message)

    def close(self):
        if not self._closed:
            self._closed = True
            try:
                self._material.close()
            finally:
                self._permit.release()

    def __repr__(self):
        return "<FingerprintMaterial redacted>"

    def __reduce_ex__(self, protocol):
        raise TypeError("Fingerprint material cannot be serialized")


class _Delivery:
    def __init__(self):
        self.lock = Lock()
        self.material = None
        self.abandoned = False

    def produce(self, source, member, permit):
        material = _HeldMaterial(source.resolve(member), permit)
        with self.lock:
            if self.abandoned:
                material.close()
            else:
                self.material = material

    def take(self):
        with self.lock:
            if self.abandoned or self.material is None:
                raise FingerprintMaterialUnavailable()
            material, self.material = self.material, None
            return material

    def abandon(self):
        with self.lock:
            self.abandoned = True
            if self.material is not None:
                self.material.close()
                self.material = None


class AsyncFingerprintResolver:
    def __init__(self, source: BlockingFingerprintSource):
        self._source = source
        self._pool = ConcurrencyPool(32)
        self._workers = ThreadPoolExecutor(max_workers=32, thread_name_prefix="gateway-fingerprint")
        self._lock = Lock()
        self._pending = {}
        self._closed = False

    @property
    def in_use(self):
        return self._pool.in_use

    async def resolve(self, member: FingerprintKeyMember):
        delivery = _Delivery()
        with self._lock:
            if self._closed:
                raise FingerprintMaterialUnavailable()
            permit = self._pool.try_acquire()
            if permit is None:
                raise FingerprintSourceCapacity()
            try:
                future = self._workers.submit(delivery.produce, self._source, member, permit)
            except RuntimeError:
                permit.release()
                raise FingerprintMaterialUnavailable() from None
            self._pending[delivery] = future

            def completed(future):
                # Successful material owns its permit until close, not just read completion.
                if future.cancelled() or future.exception() is not None:
                    permit.release()
            future.add_done_callback(completed)
        try:
            try:
                await asyncio.wrap_future(future)
            except Exception:
                pass
            else:
                return delivery.take()
            # Raise outside the handler so backend exception text is not chained.
            raise FingerprintMaterialUnavailable()
        finally:
            delivery.abandon()
            with self._lock:
                self._pending.pop(delivery, None)

    def close(self):
        with self._lock:
            self._closed = True
            for delivery, future in self._pending.items():
                delivery.abandon()
                future.cancel()
        self._workers.shutdown(wait=False, cancel_futures=True)
