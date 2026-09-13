import asyncio
from threading import Event

import pytest

from llm_gateway.adapters.fingerprint_material import FingerprintMaterial, FingerprintMaterialMetadata, FingerprintMaterialUnavailable
from llm_gateway.application.fingerprint_keys import FingerprintKeys, FingerprintSourceCapacity
from llm_gateway.domain.invocation import InvocationPersistenceUnavailable
from llm_gateway.infrastructure.fingerprint_resolution import AsyncFingerprintResolver
from tests.test_fingerprint_leases import ACTIVE, RING, Protection, deadline


class Source:
    def __init__(self):
        self.materials = []

    def resolve(self, member):
        material = FingerprintMaterial(member, FingerprintMaterialMetadata(
            member.key_id, member.key_version, member.secret_ref), b"k" * 32)
        self.materials.append(material)
        return material


def test_delivered_material_keeps_capacity_until_closed_and_shutdown_does_not_revoke_it():
    async def run():
        source = Source()
        resolver = AsyncFingerprintResolver(source)
        material = None
        try:
            material = await resolver.resolve(ACTIVE)
            assert resolver.in_use == 1
            resolver.close()
            assert len(material.digest(b"input")) == 32
            with pytest.raises(FingerprintMaterialUnavailable):
                await resolver.resolve(ACTIVE)
        finally:
            if material:
                material.close()
                material.close()
            resolver.close()
        assert resolver.in_use == 0 and source.materials[0]._closed
    asyncio.run(run())


def test_32_live_materials_prevent_additional_read_without_faulting_application():
    async def run():
        resolver = AsyncFingerprintResolver(Source())
        keys = FingerprintKeys(RING, resolver, Protection())
        materials = []
        try:
            assert await keys.validate_active()
            for _ in range(32):
                materials.append(await resolver.resolve(ACTIVE))
            with pytest.raises(FingerprintSourceCapacity):
                await resolver.resolve(ACTIVE)
            with pytest.raises(InvocationPersistenceUnavailable):
                async with keys.active(deadline=deadline()):
                    pass
            assert keys.ready and keys.in_use == 0 and resolver.in_use == 32
            assert not await keys.validate_active()
            assert keys.ready
        finally:
            for material in materials:
                material.close()
            resolver.close()
    asyncio.run(run())


@pytest.mark.parametrize("stop", ["cancel", "shutdown", "timeout"])
def test_late_material_is_closed_and_abandoned_worker_retains_permit(stop):
    async def run():
        loop = asyncio.get_running_loop()
        started = loop.create_future()
        release = Event()
        source = Source()
        class BlockedSource:
            def resolve(self, member):
                loop.call_soon_threadsafe(started.set_result, None)
                assert release.wait(5)
                return source.resolve(member)
        resolver = AsyncFingerprintResolver(BlockedSource())
        async def resolve():
            if stop == "timeout":
                async with asyncio.timeout(.05):
                    return await resolver.resolve(ACTIVE)
            return await resolver.resolve(ACTIVE)
        task = asyncio.create_task(resolve())
        try:
            await asyncio.wait_for(started, 2)
            worker = next(iter(resolver._pending.values()))
            if stop == "shutdown":
                resolver.close()
            if stop != "timeout":
                task.cancel()
            with pytest.raises(TimeoutError if stop == "timeout" else asyncio.CancelledError):
                await task
            assert resolver.in_use == 1
            release.set()
            await asyncio.wait_for(asyncio.wrap_future(worker), 2)
            assert resolver.in_use == 0 and source.materials[0]._closed
        finally:
            release.set()
            resolver.close()
    asyncio.run(run())


def test_backend_failure_has_no_sensitive_exception_context_and_releases_capacity():
    class BrokenSource:
        def resolve(self, member):
            raise RuntimeError("private backend detail")
    async def run():
        resolver = AsyncFingerprintResolver(BrokenSource())
        try:
            with pytest.raises(FingerprintMaterialUnavailable) as error:
                await resolver.resolve(ACTIVE)
            assert error.value.__context__ is None
            assert resolver.in_use == 0
        finally:
            resolver.close()
    asyncio.run(run())
