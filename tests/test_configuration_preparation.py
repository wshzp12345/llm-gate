import asyncio
import json

import pytest

from llm_gateway.adapters.configuration_dto import BundleSubmission
from llm_gateway.adapters.configuration_mapping import CanonicalConfigurationCodec, bundle_draft
from llm_gateway.application.configuration_preparation import BundlePreparer
from llm_gateway.domain.configuration import ConfigurationDiagnostic, ConfigurationValidationFailed
from llm_gateway.domain.configuration_validation import AdapterCapabilities, validate_relationships
from tests.config_fixtures import bundle_submission


CONTRACTS = {("compatible", "v1"): AdapterCapabilities(False, False, "none")}


def draft(body=None):
    return bundle_draft(BundleSubmission.model_validate(body or bundle_submission()).bundle)


def diagnostics(body):
    return validate_relationships(draft(body).validation_resources, CONTRACTS)


def test_reference_fixture_passes_relationship_checks_only():
    assert diagnostics(bundle_submission()) == ()


@pytest.mark.parametrize("section,resource,field", [
    ("provider_model_bindings", "binding-a", "provider"),
    ("provider_model_bindings", "binding-a", "pricing_table"),
    ("model_aliases", "general", "routing_policy"),
    ("model_aliases", "general", "safety_policy"),
])
def test_missing_references_have_safe_paths(section, resource, field):
    body = bundle_submission()
    body["bundle"][section][resource][field] = "missing-reference"
    errors = diagnostics(body)
    assert errors == (ConfigurationDiagnostic("reference_not_found", f"/bundle/{section}/{resource}/{field}"),)
    assert "missing-reference" not in repr(errors)


def test_adapter_contract_cannot_be_expanded():
    body = bundle_submission()
    body["bundle"]["provider_model_bindings"]["binding-a"]["capabilities"]["streaming"] = True
    assert diagnostics(body)[0].reason == "capability_invalid"


def test_unknown_adapter_is_not_inferred():
    body = bundle_submission()
    body["bundle"]["providers"]["provider-a"]["adapter"]["version"] = "v2"
    assert diagnostics(body)[0].reason == "adapter_incompatible"


def test_alias_output_ceiling_must_fit_full_candidate():
    body = bundle_submission()
    body["bundle"]["model_aliases"]["general"]["generation_defaults"]["max_output_tokens"] = 5000
    assert diagnostics(body)[0].reason == "capability_invalid"


def test_retry_cross_field_rules():
    body = bundle_submission()
    body["bundle"]["routing_policies"]["route-a"]["retry"]["max_attempts"] = 1
    assert diagnostics(body)[0].reason == "routing_invalid"


def test_reduced_enabled_requires_reduced_candidate():
    body = bundle_submission()
    body["bundle"]["routing_policies"]["route-a"]["degradation"]["reduced_service"]["enabled"] = True
    assert diagnostics(body)[0].reason == "routing_invalid"


def test_digest_excludes_base_and_metadata_and_materializes_defaults():
    original = draft()
    body = bundle_submission()
    body["bundle"]["base_revision"] = "9007199254740993"
    body["bundle"]["metadata"] = {"description": "a new description"}
    body["bundle"]["replay_policies"] = {"mode": "execution_dedup_only"}
    body["bundle"]["providers"]["provider-a"]["transport"]["connect_timeout_seconds"] = 5
    changed = draft(body)
    assert changed.snapshot_digest == original.snapshot_digest
    assert changed.base_revision == "9007199254740993"
    assert changed.description == "a new description"
    assert "metadata" not in json.loads(changed.snapshot_json)


def test_preparation_uses_required_validator_and_produces_initial_change_set():
    class Validator:
        async def validate(self, candidate):
            return validate_relationships(candidate.validation_resources, CONTRACTS)
    class Transaction:
        async def get_snapshot(self, revision):
            raise AssertionError("Initial base must not perform lookup")
    prepared = asyncio.run(BundlePreparer(draft(), Validator(), CanonicalConfigurationCodec()).prepare(Transaction()))
    changes = json.loads(prepared.change_set_json)
    assert changes["base_revision"] is None
    assert len(changes["operations"]) == 8
    assert [op["op"] for op in changes["operations"]] == ["add"] * 6 + ["replace"] * 2


def test_identical_bundle_derives_empty_changeset():
    original = draft()
    body = bundle_submission()
    body["bundle"]["base_revision"] = "1"
    class Validator:
        async def validate(self, candidate):
            return ()  # Test fixture only; production must supply full validation.
    class Transaction:
        async def get_snapshot(self, revision):
            assert revision == "1"
            return original.snapshot_json
    prepared = asyncio.run(BundlePreparer(draft(body), Validator(), CanonicalConfigurationCodec()).prepare(Transaction()))
    assert json.loads(prepared.change_set_json)["operations"] == []


def test_semantic_failure_prevents_base_read_and_preparation():
    class Validator:
        async def validate(self, candidate):
            return (ConfigurationDiagnostic("secret_reference_invalid", "/bundle/providers/provider-a/credential/secret_ref"),)
    with pytest.raises(ConfigurationValidationFailed):
        asyncio.run(BundlePreparer(draft(), Validator(), CanonicalConfigurationCodec()).prepare(object()))


def test_canonical_order_does_not_change_diagnostic_indices():
    body = bundle_submission()
    body["bundle"]["model_aliases"]["general"]["candidates"].insert(0, {"binding": "z-missing", "service_level": "reduced", "priority": 0, "weight": 1})
    candidate = draft(body)
    errors = validate_relationships(candidate.validation_resources, CONTRACTS)
    assert errors[0].path.endswith("candidates/0/binding")
    assert json.loads(candidate.snapshot_json)["model_aliases"]["general"]["candidates"][0]["binding"] == "binding-a"
