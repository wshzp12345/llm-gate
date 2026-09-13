"""Bounded off-loop delivery of local SecretSource leases.

Cancellation abandons ownership, not the OS read. A late worker result is closed
in that worker; its capacity is retained until the actual read has finished.
The Invocation's outer monotonic deadline bounds the awaiting caller.
"""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

from llm_gateway.adapters.provider_credentials import (
    CredentialUnavailable, ProviderCredentialLease, ProviderSecretSource,
)
from llm_gateway.application.concurrency import ConcurrencyPool


class _Delivery:
    def __init__(self):
        self.lock = Lock()
        self.lease = None
        self.abandoned = False

    def produce(self, source, secret_ref):
        lease = source.resolve(secret_ref)
        with self.lock:
            if self.abandoned:
                lease.close()
            else:
                self.lease = lease

    def take(self):
        with self.lock:
            if self.abandoned or self.lease is None:
                raise CredentialUnavailable()
            lease, self.lease = self.lease, None
            return lease

    def abandon(self):
        with self.lock:
            self.abandoned = True
            if self.lease is not None:
                self.lease.close()
                self.lease = None


class AsyncCredentialResolver:
    def __init__(self, source: ProviderSecretSource, *, max_concurrent_reads: int):
        self._pool = ConcurrencyPool(max_concurrent_reads)
        self._workers = ThreadPoolExecutor(max_workers=max_concurrent_reads, thread_name_prefix="gateway-secret")
        self._source = source
        self._lock = Lock()
        self._pending = {}
        self._closed = False

    async def resolve(self, secret_ref: str) -> ProviderCredentialLease:
        delivery = _Delivery()
        with self._lock:
            permit = None if self._closed else self._pool.try_acquire()
            if permit is None:
                raise CredentialUnavailable()
            try:
                future = self._workers.submit(delivery.produce, self._source, secret_ref)
            except RuntimeError:
                permit.release()
                raise CredentialUnavailable() from None
            self._pending[delivery] = future
            # Releases capacity for success, error, or cancellation before start.
            future.add_done_callback(lambda _: permit.release())
        try:
            try:
                await asyncio.wrap_future(future)
            except Exception:
                pass
            else:
                return delivery.take()
            raise CredentialUnavailable()
        finally:
            # On cancellation a running thread can still finish after this.
            delivery.abandon()
            with self._lock:
                self._pending.pop(delivery, None)

    def close(self) -> None:
        with self._lock:
            self._closed = True
            for delivery, future in self._pending.items():
                delivery.abandon()
                future.cancel()
        self._workers.shutdown(wait=False, cancel_futures=True)
