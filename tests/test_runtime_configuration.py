import asyncio
from dataclasses import replace

import pytest

from llm_gateway.application.runtime_configuration import RuntimeConfiguration
from llm_gateway.adapters.provider_transport_registry import ProviderTransportRegistry, ProviderTransportSnapshots, project_provider_transports
from llm_gateway.adapters.egress_pool import ProviderTransportRetired
from llm_gateway.domain.configuration import ConfigurationPersistenceUnavailable
from tests.test_provider_transport_registry import Client
from tests.test_active_configuration import loader, UnitOfWork, stored


class Loader:
    def __init__(self, selected):
        self.selected = selected
        self.calls = 0
    async def load(self, *, timeout_seconds):
        self.calls += 1
        if isinstance(self.selected, BaseException):
            raise self.selected
        return self.selected


async def initial():
    return await loader(UnitOfWork(stored())).load(timeout_seconds=3)


def test_fresh_failure_fences_old_pools_and_same_revision_can_recover():
    async def run():
        selected = await initial()
        source = Loader(selected)
        registry = ProviderTransportRegistry(factory=lambda plan: Client())
        runtime = RuntimeConfiguration(loader=source, target=ProviderTransportSnapshots(registry), timeout_seconds=3)
        assert runtime.current is None
        assert await runtime.refresh() is selected
        plan = project_provider_transports(selected)["provider-a"]
        old = registry.acquire(plan)
        source.selected = ConfigurationPersistenceUnavailable()
        with pytest.raises(ConfigurationPersistenceUnavailable):
            await runtime.refresh()
        assert runtime.current is None and not old.accepting
        with pytest.raises(ProviderTransportRetired):
            registry.acquire(plan)
        source.selected = selected
        assert await runtime.refresh() is selected
        assert old.closed and registry.acquire(plan) is not old
        await registry.aclose()
    asyncio.run(run())


def test_empty_configuration_fences_previous_state_without_last_known_good_fallback():
    async def run():
        source = Loader(await initial())
        registry = ProviderTransportRegistry(factory=lambda plan: Client())
        runtime = RuntimeConfiguration(loader=source, target=ProviderTransportSnapshots(registry), timeout_seconds=3)
        await runtime.refresh()
        plan = project_provider_transports(source.selected)["provider-a"]
        old = registry.acquire(plan)
        source.selected = None
        assert await runtime.refresh() is None and runtime.current is None and old.closed
        await registry.aclose()
    asyncio.run(run())


def test_publication_fences_an_already_running_old_read_before_reloading_current_authority():
    async def run():
        old = await initial()
        new = replace(old, revision="2")
        entered, release = asyncio.Event(), asyncio.Event()
        class DelayedLoader:
            calls = 0
            async def load(self, **kwargs):
                self.calls += 1
                if self.calls == 1:
                    entered.set()
                    await release.wait()
                    return old
                return new
        class Target:
            installed = []
            async def install(self, snapshot):
                self.installed.append(snapshot.revision)
            def suspend(self):
                pass
            async def reap(self):
                pass
        target = Target()
        runtime = RuntimeConfiguration(loader=DelayedLoader(), target=target, timeout_seconds=3)
        background = asyncio.create_task(runtime.refresh())
        await entered.wait()
        async def publishing():
            await runtime.after_publication()
        publication = asyncio.create_task(publishing())
        # Let the publication execute its synchronous generation fence and wait
        # on the refresh lock; no wall-clock delay is used.
        await asyncio.sleep(0)
        assert runtime.current is None
        release.set()
        assert await background is None
        await publication
        assert target.installed == ["2"] and runtime.current is new
    asyncio.run(run())


def test_cancelled_refresh_does_not_publish_or_keep_stale_state():
    async def run():
        registry = ProviderTransportRegistry(factory=lambda plan: Client())
        source = Loader(await initial())
        runtime = RuntimeConfiguration(loader=source, target=ProviderTransportSnapshots(registry), timeout_seconds=3)
        await runtime.refresh()
        source.selected = asyncio.CancelledError()
        with pytest.raises(asyncio.CancelledError):
            await runtime.refresh()
        assert runtime.current is None
        await registry.aclose()
    asyncio.run(run())


def test_watcher_retries_failed_fresh_observation_and_stops_without_more_reads():
    async def run():
        selected, stop = await initial(), asyncio.Event()
        registry = ProviderTransportRegistry(factory=lambda plan: Client())
        class RecoveringLoader:
            calls = 0
            async def load(self, **kwargs):
                self.calls += 1
                if self.calls == 1:
                    raise ConfigurationPersistenceUnavailable()
                stop.set()
                return selected
        source = RecoveringLoader()
        runtime = RuntimeConfiguration(loader=source, target=ProviderTransportSnapshots(registry), timeout_seconds=3)
        async with asyncio.timeout(1):
            await runtime.watch(stop=stop, interval_seconds=0.001)
        assert source.calls == 2 and runtime.current is selected
        await runtime.watch(stop=stop, interval_seconds=0.001)
        assert source.calls == 2
        await registry.aclose()
    asyncio.run(run())
