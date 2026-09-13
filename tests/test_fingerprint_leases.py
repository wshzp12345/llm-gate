import asyncio
import pickle

import pytest

from llm_gateway.adapters.fingerprint_material import FingerprintMaterial, FingerprintMaterialMetadata
from llm_gateway.application.fingerprint_keys import FingerprintKeys, FingerprintVersionInvalidated
from llm_gateway.domain.fingerprint_keys import FingerprintKeyMember, FingerprintKeyRing
from llm_gateway.domain.invocation import InvocationPersistenceUnavailable


ACTIVE = FingerprintKeyMember("fp", "v2", "active", "ref2")
OLD = FingerprintKeyMember("fp", "v1", "verification_only", "ref1")
RING = FingerprintKeyRing((ACTIVE, OLD))


class Source:
    def __init__(self):
        self.calls = []
        self.materials = []
        self.fail = set()
        self.wait = None

    async def resolve(self, member):
        self.calls.append(member)
        if self.wait:
            await self.wait.wait()
        if member.identity in self.fail:
            raise RuntimeError("secret source private detail")
        material = FingerprintMaterial(member, FingerprintMaterialMetadata(
            member.key_id, member.key_version, member.secret_ref), b"k" * 32)
        self.materials.append(material)
        return material


class Protection:
    def __init__(self):
        self.invalid = set()
        self.rings = []

    async def check_ring(self, ring):
        self.rings.append(ring)

    async def generation(self, member):
        if member.identity in self.invalid:
            raise FingerprintVersionInvalidated()
        return 7


def setup():
    source, protection = Source(), Protection()
    return FingerprintKeys(RING, source, protection), source, protection


def deadline():
    return asyncio.get_running_loop().time() + 10


def test_startup_gate_one_material_per_execution_and_cleanup():
    async def run():
        keys, source, protection = setup()
        assert not keys.ready
        with pytest.raises(InvocationPersistenceUnavailable):
            async with keys.active(deadline=deadline()):
                pytest.fail("admitted before startup")
        assert source.calls == []
        assert await keys.validate_active()
        assert protection.rings == [RING]
        assert source.materials[0]._closed
        async with keys.active(deadline=deadline()) as lease:
            assert keys.in_use == 1
            assert lease.fence.invalidation_generation == 7
            for purpose in ("request", "execution", "routing_seed"):
                assert len(lease.digest(purpose.encode(), purpose=purpose)) == 32
            assert len(source.calls) == 2
            with pytest.raises(TypeError):
                pickle.dumps(lease)
        assert keys.in_use == 0 and source.materials[-1]._closed
        with pytest.raises(InvocationPersistenceUnavailable):
            lease.digest(b"x", purpose="request")
    asyncio.run(run())


def test_historical_lookup_is_exact_and_never_derives_execution_identity():
    async def run():
        keys, source, _ = setup()
        assert await keys.validate_active()
        source.fail.add(OLD.identity)
        with pytest.raises(InvocationPersistenceUnavailable):
            async with keys.historical(*OLD.identity, deadline=deadline()):
                pass
        assert keys.ready and source.calls == [ACTIVE, OLD]
        with pytest.raises(InvocationPersistenceUnavailable):
            keys.historical("fp", "missing", deadline=deadline())
        source.fail.clear()
        for member in (OLD, ACTIVE):
            async with keys.historical(*member.identity, deadline=deadline()) as lease:
                assert lease.digest(b"request", purpose="request")
                for purpose in ("execution", "routing_seed"):
                    with pytest.raises(InvocationPersistenceUnavailable):
                        lease.digest(b"x", purpose=purpose)
    asyncio.run(run())


def test_capacity_32_is_immediate_and_does_not_change_readiness():
    async def run():
        keys, source, _ = setup()
        assert await keys.validate_active()
        contexts = [keys.active(deadline=deadline()) for _ in range(32)]
        try:
            for context in contexts:
                await context.__aenter__()
            assert keys.in_use == 32
            with pytest.raises(InvocationPersistenceUnavailable):
                async with keys.active(deadline=deadline()):
                    pass
            assert not await keys.validate_active()
            assert keys.ready and keys.in_use == 32 and len(source.calls) == 33
        finally:
            for context in contexts:
                await context.__aexit__(None, None, None)
        assert keys.in_use == 0
    asyncio.run(run())


def test_active_failure_fences_older_lease_and_requires_probe_recovery():
    async def run():
        keys, source, _ = setup()
        assert await keys.validate_active()
        async with keys.active(deadline=deadline()) as old_lease:
            source.fail.add(ACTIVE.identity)
            with pytest.raises(InvocationPersistenceUnavailable) as error:
                async with keys.active(deadline=deadline()):
                    pass
            assert "private" not in str(error.value)
            assert not keys.ready
            with pytest.raises(InvocationPersistenceUnavailable):
                old_lease.digest(b"x", purpose="request")
            source.fail.clear()
            with pytest.raises(InvocationPersistenceUnavailable):
                async with keys.active(deadline=deadline()):
                    pass
            assert await keys.validate_active()
            with pytest.raises(InvocationPersistenceUnavailable):
                keys.check_observed_fence(old_lease.fence)
    asyncio.run(run())


def test_cancellation_and_caller_failure_release_without_material_fault():
    async def run():
        keys, source, _ = setup()
        assert await keys.validate_active()
        source.wait = asyncio.Event()
        async def operation():
            async with keys.active(deadline=deadline()):
                pass
        task = asyncio.create_task(operation())
        await asyncio.sleep(0)
        assert keys.in_use == 1
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert keys.ready and keys.in_use == 0
        source.wait = None
        with pytest.raises(RuntimeError):
            async with keys.active(deadline=deadline()):
                raise RuntimeError("caller failed")
        assert keys.ready and keys.in_use == 0 and source.materials[-1]._closed
    asyncio.run(run())


def test_timeout_uses_remaining_request_budget_and_no_retry():
    async def run():
        keys, source, _ = setup()
        assert await keys.validate_active()
        source.wait = asyncio.Event()
        with pytest.raises(InvocationPersistenceUnavailable):
            async with keys.active(deadline=asyncio.get_running_loop().time() + .01):
                pass
        assert source.calls == [ACTIVE, ACTIVE]
        assert not keys.ready and keys.in_use == 0
    asyncio.run(run())


def test_database_invalidation_cannot_recover_with_same_version():
    async def run():
        keys, source, protection = setup()
        assert await keys.validate_active()
        protection.invalid.add(ACTIVE.identity)
        with pytest.raises(InvocationPersistenceUnavailable):
            async with keys.active(deadline=deadline()):
                pass
        protection.invalid.clear()
        assert not await keys.validate_active()
        assert not keys.ready and source.calls == [ACTIVE]
    asyncio.run(run())


def test_older_successful_probe_cannot_clear_newer_source_fault():
    async def run():
        keys, source, _ = setup()
        assert await keys.validate_active()
        started, release = asyncio.Event(), asyncio.Event()
        original = source.resolve
        async def resolve(member):
            if asyncio.current_task().get_name() == "older-probe":
                started.set()
                await release.wait()
            return await original(member)
        source.resolve = resolve
        task = asyncio.create_task(keys.validate_active(), name="older-probe")
        await started.wait()
        source.fail.add(ACTIVE.identity)
        with pytest.raises(InvocationPersistenceUnavailable):
            async with keys.active(deadline=deadline()):
                pass
        source.fail.clear()
        release.set()
        assert not await task
        assert not keys.ready and keys.in_use == 0
        assert all(material._closed for material in source.materials)
        assert await keys.validate_active()
    asyncio.run(run())


def test_ring_protection_failure_blocks_probe_without_resolving_material():
    async def run():
        keys, source, protection = setup()
        async def reject(ring):
            raise RuntimeError("protected version omitted")
        protection.check_ring = reject
        assert not await keys.validate_active()
        assert not keys.ready and source.calls == [] and keys.in_use == 0
        stop = asyncio.Event()
        stop.set()
        await keys.recover(stop)
        assert source.calls == []
    asyncio.run(run())


def test_observed_invalidated_ring_cannot_recover_after_database_restore():
    async def run():
        keys, source, protection = setup()
        original = protection.check_ring
        async def invalidated(ring):
            raise FingerprintVersionInvalidated()
        protection.check_ring = invalidated
        assert not await keys.validate_active()
        protection.check_ring = original
        assert not await keys.validate_active()
        assert not keys.ready and source.calls == []
    asyncio.run(run())
