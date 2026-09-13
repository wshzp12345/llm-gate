import asyncio
from dataclasses import replace

import pytest

from llm_gateway.domain.invocation import InvocationPersistenceUnavailable
from tests.test_fingerprint_leases import ACTIVE, setup, deadline


def test_earlier_effect_finishes_before_overlapping_fault_and_later_effects_fail():
    async def run():
        keys, source, _ = setup()
        assert await keys.validate_active()
        async with keys.active(deadline=deadline()) as lease:
            async with keys.security_scope(lease.fence) as check:
                source.fail.add(ACTIVE.identity)
                async def fails():
                    async with keys.active(deadline=deadline()):
                        pass
                failure = asyncio.create_task(fails())
                await asyncio.sleep(0)
                assert not keys.ready and not failure.done()
                with pytest.raises(InvocationPersistenceUnavailable):
                    keys.check_observed_fence(lease.fence)
                check(lease.fence)  # current serialized effect can reach its COMMIT
                async def later_effect():
                    with pytest.raises(InvocationPersistenceUnavailable):
                        async with keys.security_scope(lease.fence):
                            pass
                await asyncio.create_task(later_effect())
            with pytest.raises(InvocationPersistenceUnavailable):
                await failure
            with pytest.raises(InvocationPersistenceUnavailable):
                check(lease.fence)  # cannot retain the transaction checker
            with pytest.raises(InvocationPersistenceUnavailable):
                async with keys.security_scope(lease.fence):
                    pass
        assert not keys._pending_observations and keys.in_use == 0
    asyncio.run(run())


def test_cancelling_fault_waiter_does_not_lose_observation_and_rollback_releases_it():
    async def run():
        keys, source, _ = setup()
        assert await keys.validate_active()
        async with keys.active(deadline=deadline()) as lease:
            with pytest.raises(RuntimeError):
                async with keys.security_scope(lease.fence):
                    source.fail.add(ACTIVE.identity)
                    async def fails():
                        async with keys.active(deadline=deadline()):
                            pass
                    task = asyncio.create_task(fails())
                    await asyncio.sleep(0)
                    task.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await task
                    assert keys._pending_observations
                    raise RuntimeError("transaction rolled back")
            assert not keys._pending_observations
            with pytest.raises(InvocationPersistenceUnavailable):
                keys.check_observed_fence(lease.fence)
        source.fail.clear()
        assert await keys.validate_active()
    asyncio.run(run())


def test_checker_cannot_switch_identity_or_task():
    async def run():
        keys, _, _ = setup()
        assert await keys.validate_active()
        async with keys.active(deadline=deadline()) as lease:
            async with keys.security_scope(lease.fence) as check:
                with pytest.raises(ValueError):
                    async with keys.security_scope(lease.fence):
                        pass
                with pytest.raises(ValueError):
                    async with keys.active(deadline=deadline()):
                        pass
                with pytest.raises(InvocationPersistenceUnavailable):
                    check(replace(lease.fence, key_version="other"))
                async def foreign():
                    check(lease.fence)
                with pytest.raises(InvocationPersistenceUnavailable):
                    await asyncio.create_task(foreign())
                check(lease.fence)
    asyncio.run(run())


def test_key_resolution_budget_does_not_wait_for_longer_transaction():
    async def run():
        keys, source, _ = setup()
        assert await keys.validate_active()
        async with keys.active(deadline=deadline()) as lease:
            async with keys.security_scope(lease.fence) as check:
                source.fail.add(ACTIVE.identity)
                async def access():
                    with pytest.raises(InvocationPersistenceUnavailable):
                        async with keys.active(deadline=asyncio.get_running_loop().time()+.01):
                            pass
                await asyncio.wait_for(asyncio.create_task(access()), .5)
                assert keys._pending_observations
                check(lease.fence)
            assert not keys._pending_observations
            with pytest.raises(InvocationPersistenceUnavailable):
                keys.check_observed_fence(lease.fence)
    asyncio.run(run())
