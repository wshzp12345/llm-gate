"""Provider token buckets supplement, but never replace, Attempt capacity.

Process-owned buckets are keyed only by Provider resource identity. Tokens are
spent at gate admission, not refunded on downstream failure, and never reserve
future capacity. Configuration changes preserve credit and cannot mint a burst.
"""

from collections.abc import Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from threading import Lock
from time import monotonic_ns

from llm_gateway.application.attempt_capacity import AttemptCapacityLimits, AttemptCapacityStage
from llm_gateway.domain.routing_eligibility import StaticRoutingReason


_TOKEN = 1_000_000_000


@dataclass(frozen=True)
class ProviderRateLimits:
    capacity: AttemptCapacityLimits
    qps: int
    burst: int

    def __post_init__(self):
        if not isinstance(self.capacity, AttemptCapacityLimits):
            raise ValueError("Attempt capacity policy required")
        if any(type(value) is not int or value < 1 for value in (self.qps, self.burst)):
            raise ValueError("Published positive Provider QPS and burst required")


@dataclass
class _Bucket:
    credit: int
    at: int
    qps: int
    burst: int


class ProviderRate:
    def __init__(self, *, clock=monotonic_ns):
        self._clock = clock
        self._lock = Lock()
        self._buckets = {}

    def available(self, limits: ProviderRateLimits) -> bool:
        """Read current eligibility without spending, refilling or changing policy.

        A later Attempt must still try_consume; this is not a token reservation.
        """
        with self._lock:
            now = self._clock()
            if type(now) is not int or now < 0:
                raise ValueError("Nonnegative monotonic nanoseconds required")
            bucket = self._buckets.get(limits.capacity.provider)
            if bucket is None:
                return True
            if now < bucket.at:
                raise ValueError("Provider rate clock regressed")
            credit = min(min(bucket.burst, limits.burst) * _TOKEN,
                bucket.credit + (now - bucket.at) * min(bucket.qps, limits.qps))
            return credit >= _TOKEN

    def try_consume(self, limits: ProviderRateLimits) -> bool:
        with self._lock:
            now = self._clock()
            if type(now) is not int or now < 0:
                raise ValueError("Nonnegative monotonic nanoseconds required")
            provider = limits.capacity.provider
            bucket = self._buckets.get(provider)
            if bucket is None:
                bucket = _Bucket(limits.burst * _TOKEN, now, limits.qps, limits.burst)
                self._buckets[provider] = bucket
            if now < bucket.at:
                raise ValueError("Provider rate clock regressed")
            # At a policy boundary, do not retroactively grant a faster refill
            # under either the previous or newly presented locked policy.
            bucket.credit = min(min(bucket.burst, limits.burst) * _TOKEN,
                bucket.credit + (now - bucket.at) * min(bucket.qps, limits.qps))
            bucket.at, bucket.qps, bucket.burst = now, limits.qps, limits.burst
            if bucket.credit < _TOKEN:
                return False
            bucket.credit -= _TOKEN
            return True


class ProviderRateStage:
    def __init__(self, *, capacity, rate: ProviderRate, limits: Mapping[str, ProviderRateLimits], reject):
        if any(key != value.capacity.binding for key, value in limits.items()):
            raise ValueError("Rate Binding identity mismatch")
        policies = {}
        for value in limits.values():
            policy = (value.capacity.provider_max_concurrency, value.qps, value.burst)
            previous = policies.setdefault(value.capacity.provider, policy)
            if previous != policy:
                raise ValueError("One locked Provider must have one rate policy")
        self._capacity = AttemptCapacityStage(capacity=capacity,
            limits={key: value.capacity for key, value in limits.items()}, reject=reject)
        self._rate, self._limits, self._reject = rate, dict(limits), reject

    @asynccontextmanager
    async def acquire(self, binding_id):
        async with self._capacity.acquire(binding_id) as available:
            if not available:
                yield False
                return
            if not self._rate.try_consume(self._limits[binding_id]):
                await self._reject(binding_id, (StaticRoutingReason("qps_exhausted"),))
                yield False
                return
            # Other gates and the durable allowed/Attempt-start checkpoints
            # remain mandatory. No queue, sleep, retry or Retry-After is added.
            yield True
