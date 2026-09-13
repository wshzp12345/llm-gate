"""Process-wide Provider/Binding counts with Invocation-locked policy limits.

Counts are keyed by resource identity, not Revision or Alias, and never reset
on publication. This stage neither owns Invocation permits nor authorizes I/O.
"""

from collections.abc import Awaitable, Callable, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from threading import Lock

from llm_gateway.domain.routing_eligibility import StaticRoutingReason


@dataclass(frozen=True)
class AttemptCapacityLimits:
    binding: str
    provider: str
    provider_max_concurrency: int

    def __post_init__(self):
        if any(not isinstance(value, str) or not value for value in (self.binding, self.provider)):
            raise ValueError("Binding and Provider identities required")
        if type(self.provider_max_concurrency) is not int or self.provider_max_concurrency < 1:
            raise ValueError("Published positive Provider concurrency required")


class AttemptCapacityLease:
    def __init__(self, owner, provider, binding):
        self._owner = owner
        self._provider = provider
        self._binding = binding
        self._entered = False

    def release(self):
        with self._owner._lock:
            if self not in self._owner._leases:
                return
            self._owner._leases.remove(self)
            for counts, key in ((self._owner._providers, self._provider), (self._owner._bindings, self._binding)):
                counts[key] -= 1
                if counts[key] == 0:
                    del counts[key]

    def __enter__(self):
        with self._owner._lock:
            if self not in self._owner._leases or self._entered:
                raise RuntimeError("Attempt capacity lease cannot be reused")
            self._entered = True
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.release()
        return False


class AttemptCapacity:
    def __init__(self):
        self._lock = Lock()
        self._providers = {}
        self._bindings = {}
        self._leases = set()
        self._peak = self._provider_rejected = self._binding_rejected = 0

    def metrics(self):
        with self._lock:
            return {"attempt.in_use": len(self._leases), "attempt.peak": self._peak,
                "attempt.active_providers": len(self._providers), "attempt.active_bindings": len(self._bindings),
                "attempt.provider_rejected": self._provider_rejected, "attempt.binding_rejected": self._binding_rejected}

    def available(self, limits: AttemptCapacityLimits) -> bool:
        """Advisory initial eligibility; neither reserve nor create counters."""
        with self._lock:
            return (self._providers.get(limits.provider, 0) < limits.provider_max_concurrency
                    and self._bindings.get(limits.binding, 0) < 20)

    def try_acquire(self, limits: AttemptCapacityLimits) -> AttemptCapacityLease | None:
        with self._lock:
            provider_used = self._providers.get(limits.provider, 0)
            binding_used = self._bindings.get(limits.binding, 0)
            if provider_used >= limits.provider_max_concurrency or binding_used >= 20:
                if provider_used >= limits.provider_max_concurrency:
                    self._provider_rejected += 1
                else:
                    self._binding_rejected += 1
                return None
            lease = AttemptCapacityLease(self, limits.provider, limits.binding)
            self._providers[limits.provider] = provider_used + 1
            self._bindings[limits.binding] = binding_used + 1
            self._leases.add(lease)
            self._peak = max(self._peak, len(self._leases))
            return lease


class AttemptCapacityStage:
    def __init__(self, *, capacity: AttemptCapacity, limits: Mapping[str, AttemptCapacityLimits],
                 reject: Callable[[str, tuple[StaticRoutingReason, ...]], Awaitable[None]]):
        if any(key != value.binding for key, value in limits.items()):
            raise ValueError("Capacity Binding identity mismatch")
        self._capacity = capacity
        self._limits = dict(limits)
        self._reject = reject

    @asynccontextmanager
    async def acquire(self, binding_id: str):
        permit = self._capacity.try_acquire(self._limits[binding_id])
        if permit is None:
            await self._reject(binding_id, (StaticRoutingReason("concurrency_exhausted"),))
            yield False
            return
        with permit:
            # Caller still must check QPS/other gates and commit allowed Evidence.
            yield True
