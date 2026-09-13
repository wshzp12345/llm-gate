import asyncio
import json
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import timedelta

import pytest

from llm_gateway.adapters.active_configuration import CanonicalSnapshotDecoder
from llm_gateway.adapters.canonical_json import canonical_bytes, canonical_digest
from llm_gateway.application.active_configuration import ActiveConfigurationLoader
from llm_gateway.domain.configuration import (
    ConfigurationDiagnostic, ConfigurationPersistenceUnavailable,
    ConfigurationValidationFailed, RevisionExport,
)
from tests.test_configuration_dto import LIMITS
from tests.test_configuration_preparation import draft
from tests.test_configuration_http import local_validator


def stored():
    prepared = draft()
    return RevisionExport("1", prepared.snapshot_digest, prepared.snapshot_json, "creation")


class UnitOfWork:
    def __init__(self, selected):
        self.selected, self.reads = selected, 0

    @asynccontextmanager
    async def transaction(self, timeout_seconds):
        assert timeout_seconds == 3
        yield self

    async def get_active_export(self):
        self.reads += 1
        return self.selected


def loader(uow, validator=None):
    return ActiveConfigurationLoader(uow, CanonicalSnapshotDecoder(LIMITS), validator or local_validator())


def test_load_is_fresh_and_returns_immutable_domain_content():
    async def scenario():
        uow = UnitOfWork(stored())
        service = loader(uow)
        selected = await service.load(timeout_seconds=3)
        assert selected.revision == "1"
        assert selected.snapshot_json == stored().snapshot_json
        assert selected.snapshot_digest == stored().snapshot_digest
        uow.selected = None
        assert await service.load(timeout_seconds=3) is None
        assert uow.reads == 2
    asyncio.run(scenario())


@pytest.mark.parametrize("content", [b"[]", b"{", b'{"x":1,"x":2}', b'{"x":NaN}'])
def test_malformed_stored_content_is_a_safe_persistence_failure(content):
    with pytest.raises(ConfigurationPersistenceUnavailable):
        CanonicalSnapshotDecoder(LIMITS).decode(replace(stored(), snapshot_json=content))


@pytest.mark.parametrize("mutation", ["digest", "envelope", "defaults", "schema"])
def test_corrupt_or_non_materialized_snapshot_is_not_silently_accepted(mutation):
    original = stored()
    content = json.loads(original.snapshot_json)
    if mutation == "envelope":
        content["metadata"] = {"description": "must not be here"}
    elif mutation == "defaults":
        del content["replay_policies"]
    elif mutation == "schema":
        content["schema_version"] = "unsupported"
    digest = "sha256:" + "0" * 64 if mutation == "digest" else canonical_digest(content)
    invalid = replace(original, snapshot_json=canonical_bytes(content), snapshot_digest=digest)
    with pytest.raises(ConfigurationPersistenceUnavailable):
        CanonicalSnapshotDecoder(LIMITS).decode(invalid)


def test_database_json_whitespace_does_not_change_content_identity():
    original = stored()
    reordered = json.dumps(json.loads(original.snapshot_json), indent=2).encode()
    assert CanonicalSnapshotDecoder(LIMITS).decode(replace(original, snapshot_json=reordered)).snapshot_digest == original.snapshot_digest


def test_current_deployment_validation_is_required_and_diagnostics_bounded():
    class Invalid:
        async def validate(self, candidate):
            return (ConfigurationDiagnostic("reference_not_found", None),) * 101

    with pytest.raises(ConfigurationValidationFailed) as failure:
        asyncio.run(loader(UnitOfWork(stored()), Invalid()).load(timeout_seconds=3))
    assert len(failure.value.diagnostics) == 100 and failure.value.truncated


def test_cancellation_is_not_reinterpreted_as_empty_configuration():
    class Cancelled:
        async def validate(self, candidate):
            raise asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(loader(UnitOfWork(stored()), Cancelled()).load(timeout_seconds=3))


@pytest.mark.parametrize("invalid", [None, "identity", "leaf", "limits"])
def test_published_ca_expiry_is_runtime_eligibility_not_snapshot_integrity_failure(invalid):
    from llm_gateway.adapters.trust_bundle import LocalTrustBundleValidator, TrustBundleIntegrityValidator, pem_identity
    from llm_gateway.application.configuration_validation import LocalConfigurationValidator
    from tests.test_configuration_validation import POLICY
    from tests.config_fixtures import bundle_submission
    from tests.test_trust_bundle import NOW, certificate
    async def run():
        body = bundle_submission()
        pem = certificate(start=NOW - timedelta(days=3), end=NOW - timedelta(days=1), ca=invalid != "leaf")
        body["bundle"]["providers"]["provider-a"]["transport"]["tls"] = {"trust": "bundle", "trust_bundle": {
            "identity": "sha256:" + "0" * 64 if invalid == "identity" else pem_identity(pem), "pem": pem}}
        prepared = draft(body)
        candidate_validator = LocalConfigurationValidator(POLICY, LocalTrustBundleValidator(clock=lambda: NOW))
        assert any(item.reason == "trust_bundle_invalid" for item in await candidate_validator.validate(prepared))
        active_validator = LocalConfigurationValidator(POLICY,
            TrustBundleIntegrityValidator(max_bytes=1 if invalid == "limits" else 1048576))
        service = loader(UnitOfWork(RevisionExport("1", prepared.snapshot_digest, prepared.snapshot_json, None)), active_validator)
        if invalid is None:
            assert (await service.load(timeout_seconds=3)).snapshot_digest == prepared.snapshot_digest
        else:
            with pytest.raises(ConfigurationValidationFailed):
                await service.load(timeout_seconds=3)
    asyncio.run(run())
