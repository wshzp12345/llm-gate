from concurrent.futures import ThreadPoolExecutor
import json

import pytest

from llm_gateway.application.model_rate import ModelRate
from llm_gateway.adapters.active_configuration import CanonicalSnapshotDecoder
from llm_gateway.adapters.configuration_dto import ModelAlias
from llm_gateway.domain.configuration import RevisionExport
from tests.config_fixtures import bundle_submission
from tests.test_configuration_dto import parse, LIMITS
from tests.test_configuration_preparation import draft
from llm_gateway.adapters.configuration_json import ConfigurationStructureError


def test_default_five_rolling_minute_and_rejections_do_not_extend_window():
    now = [0]
    rate = ModelRate(clock=lambda: now[0])
    assert all(rate.try_consume("general") for _ in range(5))
    now[0] = 59_999_999_999
    assert not rate.try_consume("general")
    assert rate.try_consume("other")
    now[0] = 60_000_000_000
    assert rate.try_consume("general")
    assert len(rate._requests["general"]) == 1


def test_staggered_expiry_and_config_changes_preserve_recent_count():
    now = [0]
    rate = ModelRate(clock=lambda: now[0])
    assert rate.try_consume("general", 2)
    now[0] = 30_000_000_000
    assert rate.try_consume("general", 2)
    assert not rate.try_consume("general", 1)
    assert rate.try_consume("general", 3)
    now[0] = 60_000_000_000
    assert not rate.try_consume("general", 2)
    now[0] = 90_000_000_000
    assert rate.try_consume("general", 1)
    assert not rate.try_consume("general", 1)


def test_concurrent_consumption_is_atomic():
    rate = ModelRate(clock=lambda: 0)
    with ThreadPoolExecutor(max_workers=16) as pool:
        assert sum(pool.map(lambda _: rate.try_consume("general"), range(100))) == 5


def test_idle_models_expire_and_rejected_requests_do_not_accumulate():
    now = [0]
    rate = ModelRate(clock=lambda: now[0])
    for index in range(100):
        assert rate.try_consume(str(index), 1)
        assert not rate.try_consume(str(index), 1)
    now[0] = 60_000_000_000
    assert rate.try_consume("new")
    assert set(rate._requests) == {"new"}


@pytest.mark.parametrize("value", [0, -1, True, 1.5, "5", None])
def test_rpm_requires_positive_integer_in_model_definition(value):
    body = bundle_submission()
    body["bundle"]["model_aliases"]["general"]["requests_per_minute"] = value
    with pytest.raises(ConfigurationStructureError):
        parse(body)
    with pytest.raises(ValueError):
        ModelRate().try_consume("general", value)


@pytest.mark.parametrize("section,key", [("providers", "provider-a"),
    ("provider_model_bindings", "binding-a"), ("routing_policies", "route-a")])
def test_alias_rpm_cannot_be_configured_on_other_resources(section, key):
    body = bundle_submission()
    body["bundle"][section][key]["requests_per_minute"] = 5
    with pytest.raises(ConfigurationStructureError):
        parse(body)


def test_default_preserves_legacy_snapshot_digest_and_nondefault_roundtrips():
    body = bundle_submission()
    original = draft(body)
    alias = body["bundle"]["model_aliases"]["general"]
    assert ModelAlias.model_validate(alias).requests_per_minute == 5
    assert "requests_per_minute" not in json.loads(original.snapshot_json)["model_aliases"]["general"]
    alias["requests_per_minute"] = 5
    assert draft(body).snapshot_digest == original.snapshot_digest
    for rpm in (5, 9):
        alias["requests_per_minute"] = rpm
        prepared = draft(body)
        stored = RevisionExport("1", prepared.snapshot_digest, prepared.snapshot_json, "test")
        decoded = CanonicalSnapshotDecoder(LIMITS).decode(stored)
        assert decoded.snapshot_digest == prepared.snapshot_digest
        assert json.loads(decoded.snapshot_json)["model_aliases"]["general"].get("requests_per_minute", 5) == rpm
    assert draft(body).snapshot_digest != original.snapshot_digest


def test_invalid_clock_rejected_without_consumption():
    now = [1]
    rate = ModelRate(clock=lambda: now[0])
    assert rate.try_consume("general", 1)
    now[0] = 0
    with pytest.raises(ValueError):
        rate.try_consume("other")
    assert "other" not in rate._requests


def test_workflow_uses_changed_model_definition_without_resetting_window():
    import asyncio
    from datetime import datetime, timezone
    from types import SimpleNamespace
    from tests.test_fingerprinted_text_invocation import backend, QUERY, RESULT
    from tests.test_fingerprint_leases import setup
    from tests.test_text_fingerprints import snapshot, AUTH
    from llm_gateway.application.model_api import ModelInvocationRejected

    async def scenario():
        keys, _, _ = setup()
        assert await keys.validate_active()
        configuration = SimpleNamespace(current=snapshot())
        admitted = []

        class Admission:
            async def admit(self, record, *args, **kwargs):
                admitted.append(record)
                return datetime.now(timezone.utc)

        class Execution:
            async def execute(self, *args, **kwargs):
                return RESULT

        service = backend(keys, configuration, Admission(), Execution(), model_rate=ModelRate(clock=lambda: 0))
        for _ in range(5):
            await service.invoke_authorized(QUERY, AUTH)
        with pytest.raises(ModelInvocationRejected, match="rate_limited"):
            await service.invoke_authorized(QUERY, AUTH)
        content = json.loads(configuration.current.snapshot_json)
        content["model_aliases"]["general"]["requests_per_minute"] = 6
        configuration.current = snapshot(content, revision="2")
        await service.invoke_authorized(QUERY, AUTH)
        with pytest.raises(ModelInvocationRejected, match="rate_limited"):
            await service.invoke_authorized(QUERY, AUTH)
        content["model_aliases"]["general"]["requests_per_minute"] = 2
        configuration.current = snapshot(content, revision="3")
        with pytest.raises(ModelInvocationRejected, match="rate_limited"):
            await service.invoke_authorized(QUERY, AUTH)
        assert len(admitted) == 6
        assert admitted[-1].configuration_revision == "2"
        assert keys.in_use == 0
    asyncio.run(scenario())
