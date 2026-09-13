import json
from pathlib import Path

import pytest

from llm_gateway.infrastructure.gateway_bootstrap import load_bootstrap
from llm_gateway.infrastructure.gateway import authorize_dev, DEV_AUTHORIZATION, create_servers
from llm_gateway.infrastructure.development_init import initialize
from tests.test_configuration import run


def bootstrap(tmp_path):
    value = json.loads(Path("deploy/bootstrap.dev.json").read_text())
    value.pop("telemetry", None)
    value["database_url_file"] = str(tmp_path / "database")
    value["fingerprint_keys"][0]["file"] = str(tmp_path / "fingerprint")
    value["provider_secret_files"]["deepseek-api-key"] = str(tmp_path / "provider")
    return value


def write(tmp_path, value):
    path = tmp_path / "bootstrap.json"
    path.write_text(json.dumps(value))
    return path


def test_defaults_are_exact_dev_and_listeners_are_separate(tmp_path):
    value = bootstrap(tmp_path)
    del value["startup_profile"], value["authorization"]
    config = load_bootstrap(write(tmp_path, value))
    assert config.startup_profile == "dev" and config.authorization.mode == "dev_bypass"
    assert config.telemetry is None
    assert run(authorize_dev(object())) is DEV_AUTHORIZATION
    assert DEV_AUTHORIZATION.subject == DEV_AUTHORIZATION.tenant_id == "dev"
    assert DEV_AUTHORIZATION.expires_at.isoformat() == "9999-12-31T23:59:59+00:00"
    assert DEV_AUTHORIZATION.scopes == frozenset({"dev:*"})


@pytest.mark.parametrize("field,value", [("startup_profile", "production"), ("startup_profile", "typo"),
    ("authorization", {"mode": "local_jwt"}), ("authorization", {"mode": "dev_bypass", "issuer": "x"}),
    ("management_port", 8000), ("model_port", True), ("database_url_file", "relative"),
    ("expected_schema_version", "old"), ("unknown", True)])
def test_invalid_bootstrap_fails_closed_without_io(tmp_path, field, value):
    body = bootstrap(tmp_path)
    body[field] = value
    with pytest.raises(ValueError):
        load_bootstrap(write(tmp_path, body))


def test_duplicate_fields_and_key_roles_are_invalid(tmp_path):
    path = tmp_path / "duplicate.json"
    path.write_bytes(b'{"startup_profile":"dev","startup_profile":"production"}')
    with pytest.raises(Exception):
        load_bootstrap(path)
    value = bootstrap(tmp_path)
    value["fingerprint_keys"][0]["role"] = "verification_only"
    with pytest.raises(ValueError):
        load_bootstrap(write(tmp_path, value))


def test_dev_initialization_is_private_explicit_and_never_overwrites(tmp_path):
    initialize(tmp_path)
    original = (tmp_path / "fingerprint-key").read_bytes()
    assert len(original) == 32
    record = json.loads((tmp_path / "provider.json").read_bytes())
    assert record["revoked"] is True  # No fake usable upstream credential.
    with pytest.raises(ValueError):
        initialize(tmp_path)
    assert (tmp_path / "fingerprint-key").read_bytes() == original


def test_servers_disable_proxy_trust_and_expose_only_selected_ports(tmp_path):
    from types import SimpleNamespace
    config = load_bootstrap(write(tmp_path, bootstrap(tmp_path)))
    servers = create_servers(SimpleNamespace(config=config, model_app=object(), management_app=object()))
    assert [item.config.port for item in servers] == [8000, 8001]
    assert all(item.config.host == "0.0.0.0" and item.config.proxy_headers is False
               and item.config.access_log is False for item in servers)


def test_listener_bind_exit_is_supervisable():
    from llm_gateway.infrastructure.gateway import _serve_listener
    class FailedServer:
        async def serve(self):
            raise SystemExit(1)
    with pytest.raises(RuntimeError, match="could not start"):
        run(_serve_listener(FailedServer()))


def test_schema_failure_closes_created_dependencies_without_starting_model(tmp_path, monkeypatch):
    from llm_gateway.infrastructure import gateway as module
    config = load_bootstrap(write(tmp_path, bootstrap(tmp_path)))
    instance = module.Gateway(config, "must-not-connect")
    events = []
    async def fail(*args, **kwargs):
        raise RuntimeError("incompatible schema")
    async def closed():
        events.append("transports")
    monkeypatch.setattr(module, "verify_schema", fail)
    monkeypatch.setattr(instance.transports, "aclose", closed)
    for attribute in ("credentials", "fingerprints", "resolver"):
        original = getattr(instance, attribute).close
        def close(name=attribute, callback=original):
            events.append(name)
            callback()
        monkeypatch.setattr(getattr(instance, attribute), "close", close)
    async def scenario():
        async with instance.hold():
            pytest.fail("Invalid schema must not reach listener startup")
    with pytest.raises(RuntimeError, match="incompatible schema"):
        run(scenario())
    assert instance.process is None and not instance.ready
    assert events == ["transports", "credentials", "fingerprints", "resolver"]


@pytest.mark.parametrize("endpoint", ["http://127.0.0.1:4318", "http://[::1]:14318"])
def test_explicit_local_telemetry_config(tmp_path, endpoint):
    value = bootstrap(tmp_path)
    value["telemetry"] = {"endpoint": endpoint}
    assert load_bootstrap(write(tmp_path, value)).telemetry.endpoint == endpoint


def test_shipped_dev_config_explicitly_enables_local_collector():
    value = json.loads(Path("deploy/bootstrap.dev.json").read_text())
    assert value["telemetry"] == {"endpoint": "http://127.0.0.1:4318"}


@pytest.mark.parametrize("telemetry", [{}, {"endpoint": "http://localhost:4318"},
    {"endpoint": "https://example.com:4318"}, {"endpoint": "http://10.0.0.1:4318"},
    {"endpoint": "http://127.0.0.1:4318/v1/traces"}, {"endpoint": "http://user@127.0.0.1:4318"},
    {"endpoint": "http://127.0.0.1:4318?token=private"}, {"endpoint": "http://127.0.0.1:4318#fragment"},
    {"endpoint": "http://127.0.0.1:0"}, {"endpoint": "http://127.0.0.1:65536"},
    {"endpoint": "http://127.0.0.1:bad"}, {"endpoint": "http://127.0.0.1"},
    {"endpoint": "http://127.0.0.1:4318", "headers": {}},
    {"endpoint": " http://127.0.0.1:4318"}])
def test_telemetry_config_rejects_implicit_or_external_destinations(tmp_path, telemetry):
    value = bootstrap(tmp_path)
    value["telemetry"] = telemetry
    with pytest.raises(ValueError):
        load_bootstrap(write(tmp_path, value))


@pytest.mark.parametrize("enabled,failure", [(False, None), (True, None), (True, "construct"),
    (True, "startup"), (True, "body"), (True, "cancel")])
def test_configured_gateway_owns_output_until_gateway_drain(tmp_path, monkeypatch, enabled, failure):
    import asyncio
    from contextlib import asynccontextmanager
    from llm_gateway.infrastructure import gateway as module
    value = bootstrap(tmp_path)
    if enabled:
        value["telemetry"] = {"endpoint": "http://127.0.0.1:4318"}
    config = load_bootstrap(write(tmp_path, value))
    events = []
    monkeypatch.setattr(module, "read_database_url", lambda config: "synthetic-dsn")

    class Output:
        def __init__(self, endpoint, *, allow_plaintext):
            assert endpoint == "http://127.0.0.1:4318" and allow_plaintext

        @asynccontextmanager
        async def hold(self):
            events.append("output-open")
            try:
                yield self
            finally:
                events.append("output-close")

    class Gateway:
        def __init__(self, config, dsn, *, enable_streaming, trace_output):
            assert dsn == "synthetic-dsn" and enable_streaming
            assert isinstance(trace_output, Output) if enabled else trace_output is None
            if failure == "construct":
                raise RuntimeError("construct")

        @asynccontextmanager
        async def hold(self):
            events.append("gateway-open")
            try:
                if failure == "startup":
                    raise RuntimeError("startup")
                yield self
            finally:
                events.append("gateway-drained")

    monkeypatch.setattr(module, "OtlpHttpOutput", Output)
    monkeypatch.setattr(module, "Gateway", Gateway)

    async def scenario():
        async with module.configured_gateway(config, enable_streaming=True):
            if failure == "body":
                raise RuntimeError("body")
            if failure == "cancel":
                raise asyncio.CancelledError()

    if failure:
        with pytest.raises(asyncio.CancelledError if failure == "cancel" else RuntimeError):
            run(scenario())
    else:
        run(scenario())
    expected = [] if failure == "construct" else ["gateway-open", "gateway-drained"]
    assert events == (["output-open", *expected, "output-close"] if enabled else expected)
