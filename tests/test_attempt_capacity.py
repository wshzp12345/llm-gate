import asyncio
import copy
import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace

import pytest

from llm_gateway.adapters.capacity_projection import project_attempt_capacity
from llm_gateway.application.attempt_capacity import AttemptCapacity, AttemptCapacityLimits, AttemptCapacityStage
from llm_gateway.domain.routing_eligibility import StaticRoutingReason
from tests.test_text_completion_projection import snapshot


LIMIT = AttemptCapacityLimits("a", "provider", 2)


def test_provider_capacity_is_shared_by_bindings_without_partial_acquisition():
    capacity = AttemptCapacity()
    with capacity.try_acquire(LIMIT), capacity.try_acquire(replace(LIMIT, binding="b")):
        assert capacity.try_acquire(replace(LIMIT, binding="c")) is None
        assert "c" not in capacity._bindings
        with capacity.try_acquire(AttemptCapacityLimits("d", "other", 1)):
            pass
    assert not capacity._providers and not capacity._bindings


def test_binding_default_caps_large_published_provider_limit():
    capacity = AttemptCapacity()
    limits = replace(LIMIT, provider_max_concurrency=100)
    leases = [capacity.try_acquire(limits) for _ in range(20)]
    assert all(leases)
    assert capacity.try_acquire(limits) is None
    with capacity.try_acquire(replace(limits, binding="b")):
        pass
    for lease in leases:
        lease.release()
    assert not capacity._leases


def test_new_policy_uses_existing_counts_without_reset_or_revoking_held_permits():
    capacity = AttemptCapacity()
    first = capacity.try_acquire(LIMIT)
    tightened = replace(LIMIT, provider_max_concurrency=1)
    assert capacity.try_acquire(tightened) is None
    with first:
        with capacity.try_acquire(replace(LIMIT, binding="b")):
            assert capacity._providers["provider"] == 2
    with capacity.try_acquire(tightened):
        pass


def test_lease_release_is_identity_checked_idempotent_and_non_reentrant():
    capacity = AttemptCapacity()
    lease = capacity.try_acquire(LIMIT)
    copy.copy(lease).release()
    assert capacity._providers["provider"] == 1
    with lease:
        with pytest.raises(RuntimeError):
            with lease:
                pass
    lease.release()
    with pytest.raises(RuntimeError):
        with lease:
            pass
    assert not capacity._providers


def test_concurrent_acquisition_is_atomic():
    capacity = AttemptCapacity()
    with ThreadPoolExecutor(max_workers=8) as workers:
        leases = list(workers.map(lambda n: capacity.try_acquire(AttemptCapacityLimits(f"b{n}", "provider", 7)), range(40)))
    assert sum(lease is not None for lease in leases) == 7
    for lease in leases:
        if lease is not None:
            lease.release()
    assert not capacity._providers and not capacity._bindings


@pytest.mark.parametrize("limit", [0, -1, True, 1.5])
def test_invalid_published_limit_is_not_defaulted(limit):
    with pytest.raises(ValueError):
        replace(LIMIT, provider_max_concurrency=limit)


def test_projection_uses_snapshot_provider_limit_not_an_environment_default():
    content = {"provider_model_bindings": {"a": {"provider": "provider"}},
               "providers": {"provider": {"rate_limit": {"max_concurrency": 7}}}}
    loaded = replace(snapshot(), snapshot_json=json.dumps(content).encode())
    assert project_attempt_capacity(loaded, ("a",)) == {"a": AttemptCapacityLimits("a", "provider", 7)}
    with pytest.raises(ValueError):
        project_attempt_capacity(loaded, ("a", "a"))
    del content["providers"]["provider"]["rate_limit"]["max_concurrency"]
    with pytest.raises(KeyError):
        project_attempt_capacity(replace(loaded, snapshot_json=json.dumps(content).encode()), ("a",))


@pytest.mark.parametrize("failure", [RuntimeError, asyncio.CancelledError])
def test_stage_holds_capacity_and_releases_on_downstream_failure(failure):
    async def run():
        capacity = AttemptCapacity()
        rejected = []
        async def reject(binding, reasons):
            rejected.append((binding, reasons))
        stage = AttemptCapacityStage(capacity=capacity, limits={"a": replace(LIMIT, provider_max_concurrency=1)}, reject=reject)
        with pytest.raises(failure):
            async with stage.acquire("a") as allowed:
                assert allowed
                async with stage.acquire("a") as second:
                    assert not second
                raise failure()
        assert rejected == [("a", (StaticRoutingReason("concurrency_exhausted"),))]
        assert not capacity._providers
        async with stage.acquire("a") as allowed:
            assert allowed
    asyncio.run(run())


def test_rejection_checkpoint_failure_is_not_a_successful_skip():
    async def run():
        capacity = AttemptCapacity()
        limits = replace(LIMIT, provider_max_concurrency=1)
        async def reject(binding, reasons):
            raise RuntimeError("checkpoint failed")
        stage = AttemptCapacityStage(capacity=capacity, limits={"a": limits}, reject=reject)
        with capacity.try_acquire(limits):
            with pytest.raises(RuntimeError):
                async with stage.acquire("a"):
                    pytest.fail("Rejection must be durable before skip")
        assert not capacity._providers
    asyncio.run(run())
