import asyncio
from pathlib import Path

import httpx
import pytest

from llm_gateway.infrastructure import development_management as management
from llm_gateway.infrastructure.development_environment import DevelopmentEnvironment
from llm_gateway.domain.configuration import ConfigurationPersistenceUnavailable
from tests.test_configuration_http import UnitOfWork


def test_management_wiring_keeps_unimplemented_routes_closed(monkeypatch):
    uow = UnitOfWork()
    monkeypatch.setattr(management, "PostgresConfigurationUnitOfWork", lambda dsn: uow)

    async def scenario():
        app = management.create_app("synthetic")
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://local.test") as client:
            assert (await client.get("/healthz")).status_code == 200
            assert (await client.get("/readyz")).status_code == 503
            active = await client.get("/gateway/v1/config/active")
            assert active.json() == {"active": None}
            assert (await client.post("/v1/chat/completions", json={})).status_code == 404
            for path in ("/gateway/v1/config/revisions", "/gateway/v1/config/validate"):
                assert (await client.post(path, json={})).status_code == 400
            assert uow.reads == 1
    asyncio.run(scenario())


def test_server_is_loopback_only_and_does_not_trust_proxy_headers():
    config = management.create_server(management.create_app("synthetic")).config
    assert config.host == "127.0.0.1" and config.port == 8001
    assert config.proxy_headers is False and config.access_log is False
    assert config.server_header is False


@pytest.mark.parametrize("port", [-1, 65536, True, "8001"])
def test_invalid_server_port_is_rejected(port):
    with pytest.raises(ValueError):
        management.create_server(None, port)


@pytest.mark.parametrize("schema_ok", [True, False])
def test_schema_verification_precedes_listener_and_never_migrates(monkeypatch, schema_ok):
    events = []
    monkeypatch.setattr(management, "load_development_environment", lambda path:
                        DevelopmentEnvironment("postgresql://localhost/fixture", "sensitive-key"))

    async def verify(dsn, **kwargs):
        assert "search_path=llm_gateway" in dsn
        assert "sensitive-key" not in dsn
        events.append("verify")
        if not schema_ok:
            raise ConfigurationPersistenceUnavailable()

    class Server:
        async def serve(self):
            events.append("serve")

    def app(dsn):
        events.append("app")
        return object()

    monkeypatch.setattr(management, "verify_schema", verify)
    monkeypatch.setattr(management, "create_app", app)
    monkeypatch.setattr(management, "create_server", lambda app, port: Server())
    if schema_ok:
        asyncio.run(management.serve(Path("unused")))
        assert events == ["verify", "app", "serve"]
    else:
        with pytest.raises(ConfigurationPersistenceUnavailable):
            asyncio.run(management.serve(Path("unused")))
        assert events == ["verify"]
