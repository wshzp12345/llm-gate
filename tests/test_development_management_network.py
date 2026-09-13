import asyncio
import socket
from uuid import uuid4

import httpx
import pytest

from llm_gateway.infrastructure.development_management import create_app, create_server
from llm_gateway.infrastructure.migrate import verify_schema
from tests.test_configuration import database
from tests.test_development_policy import local_bundle
from llm_gateway.adapters.provider_transport_registry import ProviderTransportFactory
from llm_gateway.domain.configuration import ConfigurationPersistenceUnavailable


@pytest.mark.postgres
@pytest.mark.parametrize("refresh_failure", [False, True])
def test_real_management_http_and_graceful_shutdown(database, monkeypatch, refresh_failure):
    def no_provider_initialization(self, plan):
        pytest.fail("Management startup/publication must not initialize a Provider client")
    monkeypatch.setattr(ProviderTransportFactory, "__call__", no_provider_initialization)
    async def scenario():
        await verify_schema(database, expected_version="0018_invocation_correlation.sql", timeout_seconds=5)
        app = create_app(database)
        server = create_server(app, 0)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            listener.bind(("127.0.0.1", 0))
            listener.listen(32)
            listener.setblocking(False)
            port = listener.getsockname()[1]
            task = asyncio.create_task(server.serve(sockets=[listener]))
            try:
                async with asyncio.timeout(10):
                    while not server.started:
                        if task.done():
                            await task
                            pytest.fail("Listener exited before startup")
                        await asyncio.sleep(0.01)
                    async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}", trust_env=False, timeout=5) as client:
                        assert (await client.get("/healthz")).status_code == 200
                        assert (await client.get("/readyz")).status_code == 503
                        assert app.state.runtime_configuration.current is None
                        active = await client.get("/gateway/v1/config/active")
                        assert active.status_code == 200 and active.json() == {"active": None}
                        assert active.headers["cache-control"] == "no-store"
                        body = local_bundle()
                        validated = await client.post("/gateway/v1/config/validate", json=body)
                        assert validated.status_code == 200 and validated.json()["valid"]
                        created = await client.post("/gateway/v1/config/revisions", json=body,
                                                    headers={"X-Gateway-Command-Id": str(uuid4())})
                        assert created.status_code == 201, created.text
                        candidate = created.json()["revision"]
                        assert (await client.get("/gateway/v1/config/active")).json() == {"active": None}
                        if refresh_failure:
                            original = app.state.runtime_configuration._target.install
                            failed = False
                            async def fail_once(snapshot):
                                nonlocal failed
                                if not failed:
                                    failed = True
                                    raise ConfigurationPersistenceUnavailable()
                                await original(snapshot)
                            monkeypatch.setattr(app.state.runtime_configuration._target, "install", fail_once)
                        publish_path = f"/gateway/v1/config/revisions/{candidate['revision']}/publish"
                        publish_body = {"expected_active_revision": "0", "candidate_snapshot_digest": candidate["snapshot_digest"], "description": "network test"}
                        publish_headers = {"X-Gateway-Command-Id": str(uuid4())}
                        published = await client.post(publish_path, json=publish_body, headers=publish_headers)
                        if refresh_failure:
                            assert published.status_code == 503
                            assert (await client.get("/gateway/v1/config/active")).json()["active"]["revision"] == candidate["revision"]
                            # Retry the exact indexed command: publication is not
                            # repeated, but local runtime installation recovers.
                            published = await client.post(publish_path, json=publish_body, headers=publish_headers)
                        assert published.status_code == 200
                        assert app.state.runtime_configuration.current.revision == candidate["revision"]
                        assert not app.state.provider_transports._live
                        assert (await client.get("/gateway/v1/config/active")).json() == published.json()
                        assert (await client.get("/readyz")).status_code == 503
                        assert (await client.post("/v1/chat/completions", json={})).status_code == 404
            finally:
                server.should_exit = True
                try:
                    await asyncio.wait_for(task, 5)
                finally:
                    if not task.done():
                        task.cancel()
                        await asyncio.gather(task, return_exceptions=True)
        assert task.done()
        assert app.state.provider_transports._closed
        restarted = create_app(database)
        async with restarted.router.lifespan_context(restarted):
            assert restarted.state.runtime_configuration.current.revision == candidate["revision"]
            assert not restarted.state.provider_transports._live
        assert restarted.state.provider_transports._closed
    asyncio.run(scenario(), loop_factory=asyncio.SelectorEventLoop)
