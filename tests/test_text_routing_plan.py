import json
from dataclasses import replace

import pytest

from llm_gateway.adapters.text_routing_plan import prepare_text_routing, text_routing_requirements
from llm_gateway.domain.routing_eligibility import StaticRoutingReason
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
