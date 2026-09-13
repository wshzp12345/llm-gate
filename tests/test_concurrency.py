import asyncio
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, Event

import pytest

from llm_gateway.application.concurrency import ConcurrencyPool


@pytest.mark.parametrize("limit", [0, -1, True, 1.5, "2", None])
def test_invalid_limits_are_rejected(limit):
    with pytest.raises(ValueError):
        ConcurrencyPool(limit)


def test_full_pool_refuses_without_queueing_and_recovers_after_release():
    pool = ConcurrencyPool(1)
    lease = pool.try_acquire()
    assert lease is not None and pool.in_use == 1
    assert pool.try_acquire() is None
    lease.release()
    lease.release()
    assert pool.in_use == 0
    replacement = pool.try_acquire()
    assert replacement is not None
    assert pool.try_acquire() is None
    replacement.release()


def test_copied_lease_cannot_release_or_claim_original_capacity():
    from copy import copy
    pool = ConcurrencyPool(1)
    original = pool.try_acquire()
    duplicate = copy(original)
    duplicate.release()
    assert pool.in_use == 1
    with pytest.raises(RuntimeError, match="cannot be reused"):
        with duplicate:
            pass
    original.release()
    assert pool.in_use == 0


def test_context_exception_is_not_swallowed_and_releases_capacity():
    pool = ConcurrencyPool(1)
    with pytest.raises(RuntimeError, match="operation failed"):
        with pool.try_acquire():
            raise RuntimeError("operation failed")
    assert pool.in_use == 0


def test_released_and_nested_lease_use_is_rejected():
    pool = ConcurrencyPool(1)
    lease = pool.try_acquire()
    with lease:
        with pytest.raises(RuntimeError, match="cannot be reused"):
            with lease:
                pass
        assert pool.in_use == 1
    with pytest.raises(RuntimeError, match="cannot be reused"):
        with lease:
            pass
    assert pool.in_use == 0


def test_async_cancellation_releases_permit_without_sleep():
    async def scenario():
        pool = ConcurrencyPool(1)
        started, finish = asyncio.Event(), asyncio.Event()

        async def operation():
            with pool.try_acquire():
                started.set()
                await finish.wait()

        task = asyncio.create_task(operation())
        await started.wait()
        assert pool.try_acquire() is None
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert pool.in_use == 0
    asyncio.run(scenario())


def test_saturated_candidate_does_not_release_outer_invocation_permit():
    instance, provider_a, provider_b = ConcurrencyPool(1), ConcurrencyPool(1), ConcurrencyPool(1)
    occupied = provider_a.try_acquire()
    with instance.try_acquire():
        assert provider_a.try_acquire() is None
        assert instance.in_use == 1
        with provider_b.try_acquire():
            assert instance.in_use == 1 and provider_b.in_use == 1
        assert instance.in_use == 1 and provider_b.in_use == 0
    occupied.release()
    assert instance.in_use == provider_a.in_use == provider_b.in_use == 0


def test_parallel_acquisition_never_exceeds_limit():
    pool = ConcurrencyPool(3)
    acquired = Barrier(9, timeout=5)
    finish = Event()

    def operation():
        lease = pool.try_acquire()
        try:
            acquired.wait()
            assert finish.wait(5)
            return lease is not None
        finally:
            if lease is not None:
                lease.release()

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(operation) for _ in range(8)]
        try:
            acquired.wait()
            assert pool.in_use == 3
        finally:
            finish.set()
        assert sum(future.result() for future in futures) == 3
    assert pool.in_use == 0
