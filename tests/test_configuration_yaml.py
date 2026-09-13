import json
from dataclasses import replace

import pytest

from llm_gateway.adapters.configuration_json import ConfigurationStructureError
from llm_gateway.adapters.configuration_mapping import bundle_draft
from llm_gateway.adapters.configuration_yaml import export_bundle_yaml, parse_bundle_yaml
from tests.config_fixtures import bundle_submission
from tests.test_configuration_dto import LIMITS
from tests.test_configuration_preparation import draft


def yaml_fixture():
    return export_bundle_yaml(draft().snapshot_json, "1")


def test_yaml_and_json_have_the_same_domain_digest():
    body = json.dumps(bundle_submission()["bundle"]).encode()
    parsed = parse_bundle_yaml(body, LIMITS)
    assert bundle_draft(parsed).snapshot_digest == draft().snapshot_digest
    assert parsed.base_revision == "0"


def test_export_is_deterministic_and_resubmittable():
    original = draft()
    exported = export_bundle_yaml(original.snapshot_json, "9007199254740993", "export test")
    assert exported == export_bundle_yaml(original.snapshot_json, "9007199254740993", "export test")
    assert b"\r" not in exported and not exported.startswith(b"\xef\xbb\xbf")
    assert exported.endswith(b"\n") and not exported.endswith(b"\n\n")
    parsed = parse_bundle_yaml(exported, LIMITS)
    assert parsed.base_revision == "9007199254740993"
    assert parsed.metadata.description == "export test"
    assert bundle_draft(parsed).snapshot_digest == original.snapshot_digest


@pytest.mark.parametrize("body", [
    b"a: &anchor hello\nb: *anchor\n", b"a: !!str hello\n", b"a: !custom hello\n",
    b"a: 1\na: 2\n", b"a: 1\n'a': 2\n", b"<<: {}\n", b"---\na: 1\n---\na: 2\n",
    b"%YAML 1.1\n---\na: 1\n", b"a: ${ENV_VAR}\n", b"1: value\n",
    b"a: .nan\n", b"a: 1e9999\n", b"a: !!python/object:object {}\n",
])
def test_forbidden_yaml_constructs(body):
    with pytest.raises(ConfigurationStructureError):
        parse_bundle_yaml(body, LIMITS)


def test_numeric_revision_is_not_coerced_to_string():
    exported = yaml_fixture()
    assert b"base_revision: '1'" in exported
    with pytest.raises(ConfigurationStructureError):
        parse_bundle_yaml(exported.replace(b"base_revision: '1'", b"base_revision: 1"), LIMITS)


def test_yaml_does_not_accept_json_submission_wrapper():
    with pytest.raises(ConfigurationStructureError):
        parse_bundle_yaml(json.dumps(bundle_submission()).encode(), LIMITS)


@pytest.mark.parametrize("field,value", [("max_bytes", 10), ("max_nodes", 10), ("max_depth", 2), ("max_collection_items", 2), ("max_string_bytes", 2)])
def test_yaml_resource_ceilings(field, value):
    with pytest.raises(ConfigurationStructureError):
        parse_bundle_yaml(yaml_fixture(), replace(LIMITS, **{field: value}))


def test_explicit_yaml12_and_unquoted_timestamp_remain_strings():
    exported = yaml_fixture()
    parsed = parse_bundle_yaml(b"%YAML 1.2\n---\n" + exported, LIMITS)
    assert parsed.pricing_tables["price-a"].effective_from == "2026-09-07T00:00:00Z"


def test_yaml_error_does_not_disclose_parser_text():
    with pytest.raises(ConfigurationStructureError) as failure:
        parse_bundle_yaml(b"sensitive-fixture: [", LIMITS)
    assert "sensitive-fixture" not in str(failure.value)


@pytest.mark.parametrize("old,new", [
    (b"base_revision: '1'", b"base_revision: !!str 1"),
    (b"status: enabled", b"status: &state enabled"),
    (b"streaming: false", b"streaming: no"),
])
def test_forbidden_features_inside_otherwise_valid_bundle(old, new):
    exported = yaml_fixture()
    assert old in exported
    with pytest.raises(ConfigurationStructureError):
        parse_bundle_yaml(exported.replace(old, new, 1), LIMITS)


def test_yaml_core_hex_integer_matches_json_integer():
    exported = yaml_fixture()
    assert b"qps: 2" in exported
    parsed = parse_bundle_yaml(exported.replace(b"qps: 2", b"qps: 0x2"), LIMITS)
    assert parsed.providers["provider-a"].rate_limit.qps == 2
    assert bundle_draft(parsed).snapshot_digest == draft().snapshot_digest
