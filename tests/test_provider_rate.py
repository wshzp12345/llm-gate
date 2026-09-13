import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace

import pytest

from llm_gateway.adapters.provider_rate_projection import project_provider_rate
from llm_gateway.application.attempt_capacity import AttemptCapacity, AttemptCapacityLimits
from llm_gateway.application.provider_rate import ProviderRate, ProviderRateLimits, ProviderRateStage
from tests.test_text_completion_projection import snapshot


LIMIT = ProviderRateLimits(AttemptCapacityLimits("a", "provider", 2), 2, 2)


def test_burst_refill_exact_boundary_and_idle_cap():
    now = [0]
    rate = ProviderRate(clock=lambda: now[0])
    assert rate.try_consume(LIMIT) and rate.try_consume(LIMIT)
    assert not rate.try_consume(LIMIT)
    now[0] = 499_999_999
    assert not rate.try_consume(LIMIT)
    now[0] += 1
    assert rate.try_consume(LIMIT)
    assert not rate.try_consume(LIMIT)
    now[0] = 1_000_000_000_000
    assert rate.try_consume(LIMIT) and rate.try_consume(LIMIT)
    assert not rate.try_consume(LIMIT)


def test_binding_sharing_provider_isolation_and_atomic_competing_consumers():
    rate = ProviderRate(clock=lambda: 0)
    def consume(n):
        return rate.try_consume(replace(LIMIT, capacity=replace(LIMIT.capacity, binding=f"b{n}")))
    with ThreadPoolExecutor(max_workers=8) as workers:
        assert sum(workers.map(consume, range(40))) == 2
    assert rate.try_consume(replace(LIMIT, capacity=replace(LIMIT.capacity, provider="other")))


def test_policy_changes_preserve_credit_without_reset_or_retroactive_fast_refill():
    now = [0]
    rate = ProviderRate(clock=lambda: now[0])
    assert rate.try_consume(LIMIT) and rate.try_consume(LIMIT)
    higher = replace(LIMIT, qps=100, burst=100)
    assert not rate.try_consume(higher)
    now[0] = 10_000_000
    assert not rate.try_consume(LIMIT)  # Tightening cannot credit the old faster rate.
    now[0] = 500_000_000
    assert rate.try_consume(LIMIT)
    assert not rate.try_consume(LIMIT)


def test_larger_burst_after_idle_does_not_retroactively_expand_old_capacity():
    now = [0]
    rate = ProviderRate(clock=lambda: now[0])
    assert rate.try_consume(LIMIT) and rate.try_consume(LIMIT)
    now[0] = 100_000_000_000
    higher = replace(LIMIT, burst=100)
    assert rate.try_consume(higher) and rate.try_consume(higher)
    assert not rate.try_consume(higher)
    now[0] += 500_000_000
    assert rate.try_consume(higher)


@pytest.mark.parametrize("value", [0, -1, True, 1.5])
def test_invalid_published_rates_are_not_defaulted(value):
    with pytest.raises(ValueError):
        replace(LIMIT, qps=value)
    with pytest.raises(ValueError):
        replace(LIMIT, burst=value)


@pytest.mark.parametrize("invalid", [-1, True, 1.5])
def test_invalid_clock_does_not_create_bucket(invalid):
    rate = ProviderRate(clock=lambda: invalid)
    with pytest.raises(ValueError):
        rate.try_consume(LIMIT)
    assert not rate._buckets


def test_clock_regression_does_not_mutate_credit():
    now = [100]
    rate = ProviderRate(clock=lambda: now[0])
    assert rate.try_consume(LIMIT)
    now[0] = 99
    with pytest.raises(ValueError):
        rate.try_consume(LIMIT)
    now[0] = 100
    assert rate.try_consume(LIMIT)
    assert not rate.try_consume(LIMIT)


@pytest.mark.parametrize("failure", [RuntimeError, asyncio.CancelledError])
def test_concurrency_precedes_qps_and_downstream_failure_releases_capacity_not_tokens(failure):
    async def run():
        rate, capacity = ProviderRate(clock=lambda: 0), AttemptCapacity()
        limit = replace(LIMIT, burst=1, capacity=replace(LIMIT.capacity, provider_max_concurrency=1))
        rejected = []
        async def reject(binding, reasons):
            rejected.append(reasons[0].code)
        stage = ProviderRateStage(rate=rate, capacity=capacity, limits={"a": limit}, reject=reject)
        with capacity.try_acquire(limit.capacity):
            async with stage.acquire("a") as allowed:
                assert not allowed and not rate._buckets
        with pytest.raises(failure):
            async with stage.acquire("a") as allowed:
                assert allowed and capacity._providers
                raise failure()
        async with stage.acquire("a") as allowed:
            assert not allowed
        assert rejected == ["concurrency_exhausted", "qps_exhausted"]
        assert not capacity._leases and not capacity._providers
    asyncio.run(run())


def test_qps_rejection_checkpoint_failure_propagates_and_releases_capacity():
    async def run():
        rate, capacity = ProviderRate(clock=lambda: 0), AttemptCapacity()
        assert rate.try_consume(LIMIT) and rate.try_consume(LIMIT)
        async def reject(*args):
            raise RuntimeError("checkpoint failed")
        stage = ProviderRateStage(rate=rate, capacity=capacity, limits={"a": LIMIT}, reject=reject)
        with pytest.raises(RuntimeError):
            async with stage.acquire("a"):
                pytest.fail("No successful skip before evidence commits")
        assert not capacity._leases
    asyncio.run(run())


def test_projection_requires_explicit_provider_fields_and_consistent_binding_policy():
    content = {"provider_model_bindings": {"a": {"provider": "provider"}},
        "providers": {"provider": {"rate_limit": {"max_concurrency": 2, "qps": 2, "burst": 2}}}}
    loaded = replace(snapshot(), snapshot_json=json.dumps(content).encode())
    assert project_provider_rate(loaded, ("a",)) == {"a": LIMIT}
    del content["providers"]["provider"]["rate_limit"]["qps"]
    with pytest.raises(KeyError):
        project_provider_rate(replace(loaded, snapshot_json=json.dumps(content).encode()), ("a",))
    with pytest.raises(ValueError):
        ProviderRateStage(capacity=AttemptCapacity(), rate=ProviderRate(), reject=None,
            limits={"a": LIMIT, "b": replace(LIMIT, capacity=replace(LIMIT.capacity, binding="b"), qps=3)})
