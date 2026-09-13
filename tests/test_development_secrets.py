import asyncio
import json
import pickle
from datetime import datetime, timezone
from uuid import uuid4

import httpx
import pytest

from llm_gateway.adapters.openai_compatible import OpenAICompatibleCompletion
from llm_gateway.adapters.provider_credentials import CredentialUnavailable
from llm_gateway.infrastructure.development_secrets import DevelopmentSecretSource
from tests.test_completion import REQUEST, envelope


NOW = datetime(2026, 9, 11, tzinfo=timezone.utc)
VERSION = "b8288e71-5d3e-4075-9fd0-6eeb129e1d1e"


def record(**changes):
    return json.dumps(dict(secret_version=VERSION, value="fake-test-token",
                           valid_until="2026-09-12T00:00:00+00:00", revoked=False) | changes)


def environment_source(environment):
    return DevelopmentSecretSource(startup_profile="dev", files={},
        environment_names={"provider-key": "EXPLICIT_RECORD"}, enable_environment=True,
        environ=environment, now=lambda: NOW)


def test_environment_requires_explicit_dev_opt_in():
    for profile, enabled in (("prod", True), ("production", True), ("dev", False)):
        with pytest.raises(ValueError):
            DevelopmentSecretSource(startup_profile=profile, files={},
                environment_names={"key": "TOKEN"}, enable_environment=enabled)


def test_no_implicit_environment_or_dotenv_lookup(monkeypatch):
    monkeypatch.setenv("provider-key", record())
    source = DevelopmentSecretSource(startup_profile="dev", files={}, now=lambda: NOW)
    with pytest.raises(CredentialUnavailable):
        source.resolve("provider-key")


def test_each_resolution_reads_fresh_material_and_existing_lease_is_immutable():
    env = {"EXPLICIT_RECORD": record()}
    source = environment_source(env)
    with source.resolve("provider-key") as first:
        replacement = str(uuid4())
        env["EXPLICIT_RECORD"] = record(secret_version=replacement, value="rotated-test-token")
        with source.resolve("provider-key") as second:
            assert second.metadata.secret_version == replacement
            assert second.bearer_value() == "rotated-test-token"
            assert first.metadata.secret_version == VERSION
            assert first.bearer_value() == "fake-test-token"
        del env["EXPLICIT_RECORD"]
        with pytest.raises(CredentialUnavailable):
            source.resolve("provider-key")
    with pytest.raises(CredentialUnavailable):
        first.bearer_value()


def test_mounted_record_is_bounded_and_missing_file_does_not_fall_back(tmp_path):
    path = tmp_path / "credential.json"
    path.write_text(record(), encoding="utf-8")
    source = DevelopmentSecretSource(startup_profile="dev", files={"provider-key": path}, now=lambda: NOW)
    with source.resolve("provider-key") as lease:
        assert lease.metadata.source == "mounted_file"
        assert lease.bearer_value() == "fake-test-token"
    path.write_bytes(b"x" * 16385)
    with pytest.raises(CredentialUnavailable):
        source.resolve("provider-key")
    path.unlink()
    with pytest.raises(CredentialUnavailable):
        source.resolve("provider-key")


@pytest.mark.parametrize("changes", [
    {"revoked": True}, {"revoked": 0}, {"valid_until": "2026-09-11T00:00:00Z"},
    {"valid_until": "2026-09-12T00:00:00"}, {"valid_until": "2026-09-12T00:00:00+01:00"},
    {"value": ""}, {"value": "token\r\ninjection"}, {"value": " secret "},
    {"value": "密钥"}, {"value": "x" * 8193}, {"value": 123},
    {"secret_version": "secret-derived-hash"}, {"secret_version": VERSION.upper()},
    {"extra": "forbidden"},
])
def test_invalid_or_unusable_records_are_safe_failures(changes):
    source = environment_source({"EXPLICIT_RECORD": record(**changes)})
    with pytest.raises(CredentialUnavailable) as error:
        source.resolve("provider-key")
    assert str(error.value) == "Provider credentials unavailable"
    assert error.value.__context__ is None
    assert error.value.__cause__ is None


@pytest.mark.parametrize("raw", ["{private", "[]", "\ufeff{}", '{"value":1,"value":2}', "x" * 16385])
def test_invalid_encoding_structure_and_bounds(raw):
    with pytest.raises(CredentialUnavailable):
        environment_source({"EXPLICIT_RECORD": raw}).resolve("provider-key")


def test_lease_is_redacted_non_serializable_and_closed_on_error():
    lease = environment_source({"EXPLICIT_RECORD": record()}).resolve("provider-key")
    assert "fake-test-token" not in repr(lease)
    assert "fake-test-token" not in repr(lease.metadata)
    with pytest.raises(TypeError):
        pickle.dumps(lease)
    with pytest.raises(RuntimeError):
        with lease:
            raise RuntimeError("test")
    assert not lease._material
    lease.close()
    with pytest.raises(CredentialUnavailable):
        with lease:
            pass


def test_cancellation_closes_lease():
    lease = environment_source({"EXPLICIT_RECORD": record()}).resolve("provider-key")
    async def run():
        with lease:
            raise asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(run())
    with pytest.raises(CredentialUnavailable):
        lease.bearer_value()


def test_resolved_credential_stays_at_adapter_boundary():
    async def run():
        def handler(request):
            assert request.headers["authorization"] == "Bearer fake-test-token"
            assert b"fake-test-token" not in request.content
            return httpx.Response(200, json=envelope())
        source = environment_source({"EXPLICIT_RECORD": record()})
        with source.resolve("provider-key") as lease:
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False) as client:
                result = await OpenAICompatibleCompletion(client, base_url="https://provider.invalid/v1",
                    credential=lease.bearer_value()).complete(REQUEST)
        assert "fake-test-token" not in repr(result)
    asyncio.run(run())
