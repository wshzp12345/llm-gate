"""Event-loop-owned Fingerprint access; transaction effects remain caller-owned.

One instance is shared by the process composition. Source implementations must
close abandoned results on cancellation and may not cache material. The database
port is mandatory; local checks never substitute for its transaction fence.
"""

import asyncio
import math
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Protocol

from llm_gateway.application.concurrency import ConcurrencyPool
from llm_gateway.domain.fingerprint_keys import FingerprintKeyMember, FingerprintKeyRing
from llm_gateway.domain.invocation import InvocationPersistenceUnavailable
from llm_gateway.domain.routing_seed import routing_seed_message


class FingerprintVersionInvalidated(Exception):
    """The exact database key version is permanently unusable."""


class FingerprintSourceCapacity(Exception):
    """Local reads still own capacity; this is not a key-material fault."""


class FingerprintMaterialPort(Protocol):
    def digest(self, message: bytes) -> bytes: ...
    def close(self) -> None: ...


class FingerprintSourcePort(Protocol):
    async def resolve(self, member: FingerprintKeyMember) -> FingerprintMaterialPort:
        """Resolve once; validate exact identity/profile/32 raw bytes, no fallback."""
        ...


class FingerprintProtectionPort(Protocol):
    async def check_ring(self, ring: FingerprintKeyRing) -> None:
        """Reject omitted unexpired non-invalidated versions using database time."""
        ...

    async def generation(self, member: FingerprintKeyMember) -> int:
        """Read current generation, raising FingerprintVersionInvalidated if set.

        This read is NOT the transaction guard for comparison/admission/retention.
        """
        ...


@dataclass(frozen=True)
class FingerprintFence:
    key_id: str
    key_version: str
    invalidation_generation: int
    observed_generation: int


class FingerprintLease:
    __slots__ = ("_owner", "_material", "_fence", "_historical", "_closed")

    def __init__(self, owner, material, fence, *, historical):
        self._owner, self._material, self._fence = owner, material, fence
        self._historical, self._closed = historical, False

    @property
    def fence(self) -> FingerprintFence:
        return self._fence

    def digest(self, domain_separated_message: bytes, *, purpose: str) -> bytes:
        # Canonical projections/domain separation are a separate implementation.
        if (self._closed or purpose not in ("request", "execution", "routing_seed")
                or self._historical and purpose != "request"):
            raise InvocationPersistenceUnavailable()
        self._owner.check_observed_fence(self._fence)
        return self._material.digest(domain_separated_message)

    def close(self):
        if not self._closed:
            self._closed = True
            self._material.close()

    def routing_seed(self, call_id, routing_policy: str, configuration_revision: str) -> str:
        """Derive using this same active operation's material, never a new read."""
        message = routing_seed_message(call_id, routing_policy, configuration_revision)
        return self.digest(message, purpose="routing_seed").hex()

    def __repr__(self):
        return "<FingerprintLease redacted>"

    def __reduce_ex__(self, protocol):
        raise TypeError("Fingerprint leases cannot be serialized")


class FingerprintKeys:
    def __init__(self, ring: FingerprintKeyRing, source: FingerprintSourcePort,
                 protection: FingerprintProtectionPort):
        self._ring, self._source, self._protection = ring, source, protection
        self._pool = ConcurrencyPool(32)
        self._fault = True
        self._fault_generation = 0
        self._observed = {member.identity: 0 for member in ring.members}
        self._invalidated = set()
        self._ring_invalidated = False
        self._effect_lock = asyncio.Lock()
        self._effect_owner = None
        self._pending_observations = {}

    @property
    def ready(self) -> bool:
        return not self._fault

    @property
    def in_use(self) -> int:
        return self._pool.in_use

    def check_observed_fence(self, fence: FingerprintFence) -> None:
        if (fence.key_id, fence.key_version) in self._pending_observations or self._ring_invalidated:
            raise InvocationPersistenceUnavailable()
        self._check_committed_fence(fence)

    def _check_committed_fence(self, fence: FingerprintFence) -> None:
        identity = fence.key_id, fence.key_version
        if (identity in self._invalidated
                or self._observed.get(identity) != fence.observed_generation):
            raise InvocationPersistenceUnavailable()

    async def _failed(self, member, *, material_failure, invalidated=False, deadline=None):
        if member.role == "active":
            self._fault = True
            self._fault_generation += 1
        if not material_failure and not invalidated:
            return
        if not self._effect_lock.locked():
            self._observe(member.identity, invalidated)
            return
        # The current transaction and this overlapping observation are serialized
        # in that order. Reject new work immediately, but do not complete the
        # observation until the earlier transaction commits or rolls back.
        previous = self._pending_observations.get(member.identity)
        future = previous[1] if previous else asyncio.get_running_loop().create_future()
        self._pending_observations[member.identity] = (invalidated or bool(previous and previous[0]), future)
        # Coalesced to at most one pending observation per ring member. Cancelling
        # the access cannot cancel the process-owned observation or lose its fence.
        try:
            async with asyncio.timeout_at(deadline):
                await asyncio.shield(future)
        except TimeoutError:
            # A timed-out access leaves its process-owned observation queued.
            pass

    def _observe(self, identity, invalidated):
        self._observed[identity] += 1
        if invalidated:
            self._invalidated.add(identity)

    @asynccontextmanager
    async def security_scope(self, fence: FingerprintFence):
        """Serialize one database effect through COMMIT, never through Provider I/O.

        The caller supplies its deadline. Do not resolve another key or nest a
        scope inside this region. The yielded checker is valid only in this task
        and region, for this exact fence; DB generation checks remain mandatory.
        """
        if self._effect_owner is asyncio.current_task():
            raise ValueError("Fingerprint security scopes cannot be nested")
        self.check_observed_fence(fence)
        async with self._effect_lock:
            self.check_observed_fence(fence)
            owner = asyncio.current_task()
            self._effect_owner = owner
            open_scope = True
            def check(actual):
                if not open_scope or asyncio.current_task() is not owner or actual != fence:
                    raise InvocationPersistenceUnavailable()
                self._check_committed_fence(actual)
            try:
                yield check
            finally:
                open_scope = False
                self._effect_owner = None
                pending, self._pending_observations = self._pending_observations, {}
                for identity, (invalidated, future) in pending.items():
                    self._observe(identity, invalidated)
                    future.set_result(None)

    @asynccontextmanager
    async def _access(self, member, *, deadline, historical, probe=False):
        if self._effect_owner is asyncio.current_task():
            raise ValueError("Resolve Fingerprint material before the security transaction")
        if type(deadline) not in (int, float) or not math.isfinite(deadline):
            raise ValueError("Finite monotonic deadline required")
        loop = asyncio.get_running_loop()
        if deadline <= loop.time() or member.identity in self._invalidated:
            raise InvocationPersistenceUnavailable()
        if not historical and not probe and self._fault:
            raise InvocationPersistenceUnavailable()
        permit = self._pool.try_acquire()
        if permit is None:
            raise InvocationPersistenceUnavailable()
        material = lease = None
        source_started = False
        observed = self._observed[member.identity]
        resolution_deadline = min(deadline, loop.time() + 1)
        try:
            try:
                async with asyncio.timeout_at(resolution_deadline):
                    generation = await self._protection.generation(member)
                    if type(generation) is not int or generation < 0:
                        raise InvocationPersistenceUnavailable()
                    source_started = True
                    material = await self._source.resolve(member)
                fence = FingerprintFence(*member.identity, generation, observed)
                self.check_observed_fence(fence)
            except asyncio.CancelledError:
                raise
            except FingerprintSourceCapacity:
                raise InvocationPersistenceUnavailable() from None
            except FingerprintVersionInvalidated:
                await self._failed(member, material_failure=False, invalidated=True, deadline=resolution_deadline)
                raise InvocationPersistenceUnavailable() from None
            except Exception:
                await self._failed(member, material_failure=source_started, deadline=resolution_deadline)
                raise InvocationPersistenceUnavailable() from None
            lease = FingerprintLease(self, material, fence, historical=historical)
            # Caller exceptions must NOT be classified as SecretSource failures.
            yield lease
        finally:
            try:
                if lease is not None:
                    lease.close()
                elif material is not None:
                    material.close()
            finally:
                permit.release()

    def active(self, *, deadline: float):
        return self._access(self._ring.active, deadline=deadline, historical=False)

    def historical(self, key_id: str, key_version: str, *, deadline: float):
        try:
            member = self._ring.member(key_id, key_version)
        except ValueError:
            raise InvocationPersistenceUnavailable() from None
        return self._access(member, deadline=deadline, historical=True)

    async def validate_active(self) -> bool:
        """Startup/recovery probe; never retain bytes or overwrite a newer fault."""
        if self._ring_invalidated:
            return False
        generation = self._fault_generation
        deadline = asyncio.get_running_loop().time() + 1
        try:
            async with asyncio.timeout_at(deadline):
                await self._protection.check_ring(self._ring)
        except asyncio.CancelledError:
            raise
        except FingerprintVersionInvalidated:
            # An invalidated member cannot re-enter this immutable ring after
            # source recovery or restoration of an older database snapshot.
            self._ring_invalidated = True
            await self._failed(self._ring.active, material_failure=False)
            return False
        except Exception:
            await self._failed(self._ring.active, material_failure=False)
            return False
        try:
            async with self._access(self._ring.active, deadline=deadline,
                                   historical=False, probe=True):
                pass
        except asyncio.CancelledError:
            raise
        except Exception:
            # Capacity exhaustion must leave readiness and fault generation alone.
            # Source/DB failures in _access have already classified their effect.
            return False
        if generation == self._fault_generation:
            self._fault = False
            return True
        return False

    async def recover(self, stop: asyncio.Event) -> None:
        """Isolated five-second recovery loop; caller owns task lifecycle."""
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=5)
            except TimeoutError:
                if self._fault:
                    await self.validate_active()
