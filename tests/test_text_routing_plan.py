import json
from dataclasses import replace

import pytest

from llm_gateway.adapters.text_routing_plan import prepare_text_routing, text_routing_requirements
from llm_gateway.adapters.structured_output import parse_output_format
from llm_gateway.domain.routing_eligibility import StaticRoutingReason
from llm_gateway.domain.model import OutputFormat
from llm_gateway.domain.routing_failure import classify_unattempted_rejections
from tests.test_text_fingerprints import QUERY, snapshot


def test_explicit_initial_checks_precede_order_and_effective_requests_are_locked():
    selected = snapshot()
    requirement, assessed = text_routing_requirements(selected, QUERY)
    assert requirement.max_output_tokens == 4096 and len(assessed) == 1
    with pytest.raises(ValueError, match="Complete initial"):
        prepare_text_routing(selected, QUERY, seed_hex="a"*64, initial_rejections={})
    plan = prepare_text_routing(selected, QUERY, seed_hex="a"*64, initial_rejections={"binding-a": ()})
    assert plan.full[0].binding_id == "binding-a" and plan.reduced == ()
    assert plan.full[0].request.max_output_tokens == 4096
    assert plan.full[0].request.messages == QUERY.messages
    assert plan.preselection.candidates[0].initial_order == 0
    assert plan.retry.max_attempts == 3 and plan.retry.base_delay_ms == 100


def test_filtered_candidates_never_receive_an_initial_position_or_request():
    selected = snapshot()
    plan = prepare_text_routing(selected, QUERY, seed_hex="a"*64,
        initial_rejections={"binding-a": (StaticRoutingReason("trust_bundle_expired"),)})
    assert plan.full == plan.reduced == ()
    assert plan.preselection.candidates[0].initial_order is None
    content = json.loads(selected.snapshot_json)
    content["providers"]["provider-a"]["status"] = "disabled"
    plan = prepare_text_routing(snapshot(content), QUERY, seed_hex="a"*64, initial_rejections={})
    assert plan.full == ()
    assert plan.preselection.candidates[0].reasons == (StaticRoutingReason("provider_disabled"),)


def test_structured_format_requires_matching_binding_capability_and_reaches_adapter_request():
    selected = snapshot()
    query = replace(QUERY, output_format=OutputFormat("json_object"))
    requirement, assessed = text_routing_requirements(selected, query)
    assert requirement.structured_output == "json_object" and not assessed[0].statically_eligible
    content = json.loads(selected.snapshot_json)
    content["provider_model_bindings"]["binding-a"]["capabilities"]["structured_output"] = "json_object"
    selected = snapshot(content)
    requirement, assessed = text_routing_requirements(selected, query)
    assert assessed[0].statically_eligible
    plan = prepare_text_routing(selected, query, seed_hex="a" * 64, initial_rejections={"binding-a": ()})
    assert plan.full[0].request.output_format.type == query.output_format.type
    assert plan.full[0].request.output_format.local_extraction is True


def test_schema_features_reject_open_object_before_anthropic_attempt_with_evidence():
    content = json.loads(snapshot().snapshot_json)
    content["providers"]["provider-a"]["adapter"] = {"type": "anthropic_messages", "version": "v1"}
    content["provider_model_bindings"]["binding-a"]["capabilities"]["structured_output"] = "json_schema"
    selected = snapshot(content)

    def query_for(schema):
        return replace(QUERY, output_format=parse_output_format({"type": "json_schema", "json_schema": {
            "name": "answer", "schema": schema}}))

    open_query = query_for({"type": "object", "properties": {"answer": {"type": "string"}}})
    requirement, assessed = text_routing_requirements(selected, open_query)
    assert requirement.schema_features == ("object-open",)
    assert not assessed[0].statically_eligible
    reason = StaticRoutingReason("capability_mismatch", "structured_output.feature.object-open")
    assert reason in assessed[0].reasons
    rejected = prepare_text_routing(selected, open_query, seed_hex="a" * 64, initial_rejections={})
    assert rejected.full == () and rejected.preselection.feature_ids == {"object-open"}
    assert reason in rejected.preselection.candidates[0].reasons
    assert classify_unattempted_rejections(static=(frozenset({"capability_mismatch"}),), runtime=()).code == "unsupported_capability"

    closed_query = query_for({"type": "object", "properties": {"answer": {"type": "string"}},
                              "required": ["answer"], "additionalProperties": False})
    requirement, assessed = text_routing_requirements(selected, closed_query)
    assert requirement.schema_features == ("object-closed", "required")
    assert assessed[0].statically_eligible
    accepted = prepare_text_routing(selected, closed_query, seed_hex="a" * 64,
                                    initial_rejections={"binding-a": ()})
    assert accepted.full[0].request.output_format.schema_json == closed_query.output_format.schema_json
    assert accepted.full[0].request.output_format.local_extraction is True

    nested_open = query_for({"type": "object", "additionalProperties": False,
                             "properties": {"nested": {"type": "object"}}})
    _, nested_assessed = text_routing_requirements(selected, nested_open)
    assert reason in nested_assessed[0].reasons

    unbounded_array = query_for({"type": "array"})
    _, array_assessed = text_routing_requirements(selected, unbounded_array)
    assert StaticRoutingReason("capability_mismatch", "structured_output.feature.array-unconstrained") in array_assessed[0].reasons

    unconstrained_child = query_for({"type": "object", "additionalProperties": False,
                                     "properties": {"value": {}}})
    _, child_assessed = text_routing_requirements(selected, unconstrained_child)
    assert StaticRoutingReason("capability_mismatch", "structured_output.feature.unconstrained") in child_assessed[0].reasons


def test_reduced_order_is_separate_not_permission_to_enter_degradation():
    content = json.loads(snapshot().snapshot_json)
    content["provider_model_bindings"]["binding-b"] = dict(content["provider_model_bindings"]["binding-a"])
    content["model_aliases"]["general"]["candidates"].append(
        {"binding": "binding-b", "service_level": "reduced", "priority": 0, "weight": 1})
    content["routing_policies"]["route-a"]["degradation"]["reduced_service"]["enabled"] = True
    plan = prepare_text_routing(snapshot(content), replace(QUERY, max_output_tokens=32), seed_hex="a"*64,
                               initial_rejections={"binding-a": (), "binding-b": ()})
    assert tuple(value.binding_id for value in plan.full) == ("binding-a",)
    assert tuple(value.binding_id for value in plan.reduced) == ("binding-b",)
    assert all(value.request.max_output_tokens == 32 for value in plan.full+plan.reduced)


@pytest.mark.parametrize("code,expected", [("trust_bundle_expired", "provider_unavailable"),
    ("egress_policy_violation", "provider_unavailable"), ("security_invalidated", "provider_unavailable"),
    ("health_unavailable", "provider_unavailable"), ("circuit_open", "provider_unavailable"),
    ("concurrency_exhausted", "rate_limited"), ("qps_exhausted", "rate_limited"),
    ("provider_credentials_unavailable", "provider_credentials_unavailable")])
def test_gate_failure_has_same_classification_before_order_or_before_attempt(code, expected):
    group = (frozenset({code}),)
    assert classify_unattempted_rejections(static=group, runtime=()).code == expected
    assert classify_unattempted_rejections(static=(), runtime=group).code == expected
