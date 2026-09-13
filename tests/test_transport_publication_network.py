import asyncio

import pytest

from llm_gateway.adapters.egress_pool import ProviderTransportRetired
from llm_gateway.adapters.provider_transport_registry import ProviderTransportFactory, ProviderTransportRegistry
from tests.config_fixtures import bundle_submission
from tests.test_egress_network_local import tls_contexts
from tests.test_provider_transport_registry import plans


@pytest.mark.parametrize("cancel_active", [False, True])
def test_real_tls_publication_closes_idle_drains_active_and_never_migrates(tmp_path, cancel_active):
    async def run():
        server_context, client_context = tls_contexts(tmp_path, "provider.invalid")
        active_started, release = asyncio.Event(), asyncio.Event()
        handlers, requests, closed_paths = set(), [], []
        async def handle(reader, writer):
            task, paths = asyncio.current_task(), []
            handlers.add(task)
            try:
                while True:
                    try:
                        data = await reader.readuntil(b"\r\n\r\n")
                    except asyncio.IncompleteReadError:
                        break
                    path = data.split(b" ")[1]
                    paths.append(path)
                    requests.append(path)
                    if path == b"/active":
                        active_started.set()
                        await release.wait()
                    writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                    await writer.drain()
            except (ConnectionError, OSError):
                pass
            finally:
                closed_paths.extend(paths)
                writer.close()
                try:
                    await writer.wait_closed()
                except (ConnectionError, OSError):
                    pass
                handlers.discard(task)
        server = await asyncio.start_server(handle, "127.0.0.1", 0, ssl=server_context)
        port = server.sockets[0].getsockname()[1]
        class Resolver:
            calls = 0
            async def resolve(self, host, destination_port):
                assert (host, destination_port) == ("provider.invalid", port)
                self.calls += 1
                return ("127.0.0.1",)
        resolver = Resolver()
        registry = ProviderTransportRegistry(factory=ProviderTransportFactory(resolver=resolver, system_context=client_context))
        active = None
        try:
            async with asyncio.timeout(5):
                body = bundle_submission()
                provider = body["bundle"]["providers"]["provider-a"]
                root = f"https://provider.invalid:{port}"
                provider["endpoint"]["base_url"] = root + "/v1"
                provider["egress"]["allowed_networks"] = ["127.0.0.1/32"]
                first = plans(body)
                await registry.install("1", first)
                old = registry.acquire(first["provider-a"])
                assert resolver.calls == 0
                active = asyncio.create_task(old.get(root + "/active"))
                await active_started.wait()
                assert (await old.get(root + "/idle")).content == b"ok"
                assert resolver.calls == 2
                provider["transport"]["idle_timeout_seconds"] = 60
                second = plans(body)
                await registry.install("2", second)
                assert not active.done() and not old.is_closed
                assert resolver.calls == 2  # Publication created no replacement connection.
                with pytest.raises(ProviderTransportRetired):
                    await old.get(root + "/stale")
                new = registry.acquire(second["provider-a"])
                assert (await new.get(root + "/new")).content == b"ok"
                assert resolver.calls == 3 and requests == [b"/active", b"/idle", b"/new"]
                if cancel_active:
                    active.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await active
                else:
                    release.set()
                    assert (await active).content == b"ok"
                await registry.reap()
                assert old.is_closed and not new.is_closed
        finally:
            release.set()
            if active is not None and not active.done():
                active.cancel()
                await asyncio.gather(active, return_exceptions=True)
            await registry.aclose()
            server.close()
            await server.wait_closed()
            if handlers:
                await asyncio.wait_for(asyncio.gather(*handlers), 3)
        assert sorted(closed_paths) == [b"/active", b"/idle", b"/new"]
    asyncio.run(run(), loop_factory=asyncio.SelectorEventLoop)
