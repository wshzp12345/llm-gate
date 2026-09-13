import asyncio
from threading import Event

import pytest

from llm_gateway.adapters.provider_credentials import CredentialUnavailable
from llm_gateway.infrastructure.credential_resolution import AsyncCredentialResolver
from tests.test_development_secrets import environment_source, record


def test_async_delivery_and_shutdown():
    async def run():
        resolver = AsyncCredentialResolver(environment_source({"EXPLICIT_RECORD": record()}), max_concurrent_reads=1)
        try:
            with await resolver.resolve("provider-key") as lease:
                assert lease.bearer_value() == "fake-test-token"
        finally:
            resolver.close()
        with pytest.raises(CredentialUnavailable):
            await resolver.resolve("provider-key")
    asyncio.run(run())


def test_source_failure_is_safe_and_returns_capacity():
    async def run():
        env = {}
        resolver = AsyncCredentialResolver(environment_source(env), max_concurrent_reads=1)
        try:
            with pytest.raises(CredentialUnavailable) as error:
                await resolver.resolve("provider-key")
            assert error.value.__context__ is None
            env["EXPLICIT_RECORD"] = record()
            with await resolver.resolve("provider-key"):
                pass
        finally:
            resolver.close()
    asyncio.run(run())


@pytest.mark.parametrize("stop", ["cancel", "shutdown"])
def test_abandoned_read_keeps_capacity_until_worker_finishes_and_closes_late_lease(stop):
    async def run():
        loop = asyncio.get_running_loop()
        started = loop.create_future()
        release = Event()
        leases = []

        class BlockedSource:
            def resolve(self, secret_ref):
                loop.call_soon_threadsafe(started.set_result, None)
                assert release.wait(5), "test worker was not released"
                lease = environment_source({"EXPLICIT_RECORD": record()}).resolve(secret_ref)
                leases.append(lease)
                return lease

        resolver = AsyncCredentialResolver(BlockedSource(), max_concurrent_reads=1)
        task = asyncio.create_task(resolver.resolve("provider-key"))
        try:
            await asyncio.wait_for(started, 2)
            worker = next(iter(resolver._pending.values()))
            if stop == "shutdown":
                resolver.close()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            with pytest.raises(CredentialUnavailable):
                await resolver.resolve("provider-key")
            release.set()
            await asyncio.wait_for(asyncio.wrap_future(worker), 2)
            assert len(leases) == 1 and not leases[0]._material
            with pytest.raises(CredentialUnavailable):
                leases[0].bearer_value()
        finally:
            release.set()
            resolver.close()
    asyncio.run(run())
