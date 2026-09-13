import json
from dataclasses import replace

import pytest

from llm_gateway.adapters.configuration_json import ConfigurationStructureError, ParseLimits, parse_bundle_json
from tests.config_fixtures import bundle_submission
from llm_gateway.adapters.configuration_json import parse_configuration_json
from llm_gateway.adapters.configuration_dto import PublishRequest, RebaseOrRollbackRequest
from pydantic import ValidationError


LIMITS = ParseLimits(100000, 32, 10000, 1000, 10000)


def parse(value):
    return parse_bundle_json(json.dumps(value).encode(), LIMITS)


def test_bundle_materializes_only_documented_defaults():
    result = parse(bundle_submission()).bundle
    assert result.provider_model_bindings["binding-a"].tokenizer is None
    assert result.replay_policies.mode == "execution_dedup_only"
    assert result.providers["provider-a"].transport.connect_timeout_seconds == 5
    assert result.resource_policies.routing_evidence_ttl_seconds == 2592000
    assert result.model_aliases["general"].generation_defaults.max_output_tokens == 4096


@pytest.mark.parametrize("section", ["providers", "provider_model_bindings", "model_aliases", "routing_policies", "pricing_tables", "safety_policies", "resource_policies"])
def test_required_sections_never_inferred(section):
    value = bundle_submission()
    del value["bundle"][section]
    with pytest.raises(ConfigurationStructureError):
        parse(value)


@pytest.mark.parametrize("base", [0, None, "01", "-1", "9223372036854775808"])
def test_invalid_base_is_structural(base):
    value = bundle_submission()
    value["bundle"]["base_revision"] = base
    with pytest.raises(ConfigurationStructureError):
        parse(value)


@pytest.mark.parametrize("field,value", [("max_concurrency", True), ("max_concurrency", "2"), ("qps", 0), ("burst", 1.5)])
def test_no_integer_coercion(field, value):
    body = bundle_submission()
    body["bundle"]["providers"]["provider-a"]["rate_limit"][field] = value
    with pytest.raises(ConfigurationStructureError):
        parse(body)


@pytest.mark.parametrize("enabled", [0, 1, "false"])
def test_literal_cache_boolean_rejects_integer_lookalikes(enabled):
    body = bundle_submission()
    body["bundle"]["routing_policies"]["route-a"]["degradation"]["cache"]["enabled"] = enabled
    with pytest.raises(ConfigurationStructureError):
        parse(body)


@pytest.mark.parametrize("description", ["", " leading", "trailing ", "line\nline", "\u0000", "中" * 342])
def test_description_is_bounded_single_line_utf8(description):
    body = bundle_submission()
    body["bundle"]["metadata"] = {"description": description}
    with pytest.raises(ConfigurationStructureError):
        parse(body)


def test_unknown_fields_and_plaintext_credentials_rejected_without_disclosure():
    body = bundle_submission()
    body["bundle"]["providers"]["provider-a"]["credential"]["api_key"] = "sensitive-fixture"
    with pytest.raises(ConfigurationStructureError) as failure:
        parse(body)
    assert "sensitive-fixture" not in str(failure.value)
    assert "api_key" not in repr(failure.value.diagnostics)


@pytest.mark.parametrize("body", [b'{"a":1,"a":2}', b'{"a":NaN}', b'\xff', b'{"a":"\\ud800"}', b'\xef\xbb\xbf{}'])
def test_strict_json(body):
    with pytest.raises(ConfigurationStructureError):
        parse_bundle_json(body, LIMITS)


@pytest.mark.parametrize("field,value", [("max_bytes", 10), ("max_depth", 2), ("max_nodes", 10), ("max_collection_items", 2), ("max_string_bytes", 2)])
def test_parser_ceilings(field, value):
    with pytest.raises(ConfigurationStructureError):
        parse_bundle_json(json.dumps(bundle_submission()).encode(), replace(LIMITS, **{field: value}))


def test_nesting_preflight_ignores_braces_inside_strings():
    body = bundle_submission()
    body["bundle"]["metadata"] = {"description": "[" * 100}
    assert parse(body).bundle.metadata.description == "[" * 100


def test_forbidden_outer_branch_mixing():
    body = bundle_submission()
    body["change_set"] = {}
    with pytest.raises(ConfigurationStructureError):
        parse(body)


def changeset(operations):
    return json.dumps({"kind": "change_set", "change_set": {
        "schema_version": "gateway.config-change-set/v1", "base_revision": "1", "operations": operations,
    }}).encode()


def test_empty_changeset_is_allowed():
    assert parse_configuration_json(changeset([]), LIMITS).change_set.operations == []


def test_changeset_values_use_same_typed_resource_schema():
    value = bundle_submission()["bundle"]["safety_policies"]["safety-a"]
    operation = {"op": "replace", "section": "safety_policies", "resource_id": "safety-a", "value": value}
    parsed = parse_configuration_json(changeset([operation]), LIMITS)
    assert parsed.change_set.operations[0].value == value
    operation["value"]["extra"] = True
    with pytest.raises(ConfigurationStructureError):
        parse_configuration_json(changeset([operation]), LIMITS)


@pytest.mark.parametrize("operation", [
    {"op": "remove", "section": "providers", "resource_id": "a", "value": None},
    {"op": "replace", "section": "providers", "resource_id": "a"},
    {"op": "remove", "section": "resource_policies"},
    {"op": "replace", "section": "replay_policies", "resource_id": None, "value": {"mode": "execution_dedup_only"}},
    {"op": "remove", "section": "providers", "resource_id": None},
])
def test_changeset_closed_variants(operation):
    with pytest.raises(ConfigurationStructureError):
        parse_configuration_json(changeset([operation]), LIMITS)


def test_changeset_duplicate_targets_are_structural():
    operation = {"op": "remove", "section": "providers", "resource_id": "a"}
    with pytest.raises(ConfigurationStructureError):
        parse_configuration_json(changeset([operation, operation]), LIMITS)


def test_lifecycle_dtos_keep_preconditions_in_body_and_command_id_outside():
    data = {"expected_active_revision": "0", "candidate_snapshot_digest": "sha256:" + "0" * 64, "description": "publish"}
    assert PublishRequest.model_validate(data).expected_active_revision == "0"
    with pytest.raises(ValidationError):
        PublishRequest.model_validate(data | {"command_id": "not-allowed"})
    with pytest.raises(ValidationError):
        RebaseOrRollbackRequest.model_validate({"expected_active_revision": "0", "description": "rebase"})
