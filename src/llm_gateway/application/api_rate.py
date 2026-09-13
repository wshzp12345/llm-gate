"""Atomic instance/tenant API token accounting, separate from Provider work.

The first-phase 100/50 QPS defaults use one-second burst capacities of 100/50.
The owner is shared across requests, not constructed per Invocation or tenant.
Replay can share this counter without acquiring Model Invocation permits; its
authorization/read-concurrency and artifact access still need their own gates.
"""

from dataclasses import dataclass
from threading import Lock
from time import monotonic_ns

from llm_gateway.domain.invocation import AuthorizationContext


_TOKEN = 1_000_000_000


@dataclass
class _Bucket:
    credit: int
    at: int

    def available(self, now, rate):
        return min(rate * _TOKEN, self.credit + (now - self.at) * rate)


class ApiRate:
    def __init__(self, *, clock=monotonic_ns):
        self._clock = clock
        self._lock = Lock()
        self._instance = None
        self._tenants = {}

    def try_consume(self, authorization: AuthorizationContext) -> bool:
        if not isinstance(authorization, AuthorizationContext):
            raise ValueError("Trusted Authorization Context required")
        with self._lock:
            now = self._clock()
            if type(now) is not int or now < 0 or self._instance is not None and now < self._instance.at:
                raise ValueError("Nondecreasing monotonic nanoseconds required")
            if self._instance is None:
                self._instance = _Bucket(100 * _TOKEN, now)
            # Full idle tenant buckets are equivalent to absent buckets. Only
            # successful admissions create entries; rejected novel tenants
            # cannot grow memory. The instance rate bounds recent admissions.
            for tenant, bucket in tuple(self._tenants.items()):
                if bucket.available(now, 50) == 50 * _TOKEN:
                    del self._tenants[tenant]
            instance_credit = self._instance.available(now, 100)
            tenant = authorization.tenant_id
            bucket = self._tenants.get(tenant)
            tenant_credit = bucket.available(now, 50) if bucket is not None else 50 * _TOKEN
            self._instance = _Bucket(instance_credit, now)
            if min(instance_credit, tenant_credit) < _TOKEN:
                return False
            self._instance.credit -= _TOKEN
            self._tenants[tenant] = _Bucket(tenant_credit - _TOKEN, now)
            return True
