from dataclasses import replace

import pytest

from llm_gateway.adapters.routing_projection import project_alias, UnknownLogicalModel
from llm_gateway.application.active_configuration import LoadedConfiguration
from llm_gateway.domain.configuration_validation import AdapterCapabilities
from llm_gateway.domain.routing_eligibility import StaticCandidate, StaticServiceRequirement, assess_static_candidates
from llm_gateway.domain.routing_order import WeightedCandidate
from tests.test_configuration_preparation import draft


REQUEST = StaticServiceRequirement(False, False, "none", 2048)
CANDIDATE = StaticCandidate(WeightedCandidate("binding-a", "full", 0, 1), "provider-a", "model-name",
                            True, True, AdapterCapabilities(False, False, "none"), 4096)


def assess(candidate=CANDIDATE, request=REQUEST, reduced_enabled=False):
    return assess_static_candidates(request, (candidate,), reduced_enabled=reduced_enabled)[0]


def test_static_success_is_not_a_runtime_permission():
    result = assess()
    assert result.statically_eligible and result.reasons == ()
    assert result.candidate.resolved_model == "model-name"


@pytest.mark.parametrize("changes,code,subject", [
    ({"streaming": True}, "capability_mismatch", "streaming"),
    ({"tool_calling": True}, "capability_mismatch", "tool_calling"),
    ({"structured_output": "json_object"}, "capability_mismatch", "structured_output.json_object"),
    ({"structured_output": "json_schema"}, "capability_mismatch", "structured_output.json_schema"),
    ({"max_output_tokens": 4097}, "limit_exceeded", "max_output_tokens"),
    ({"provider_override": "provider-b"}, "provider_override_mismatch", "provider-b"),
])
def test_specific_static_rejection_reasons(changes, code, subject):
    result = assess(request=replace(REQUEST, **changes))
    assert not result.statically_eligible
    assert [(item.code, item.subject) for item in result.reasons] == [(code, subject)]


def test_provider_override_matches_provider_not_upstream_model_name():
    candidate = replace(CANDIDATE, resolved_model="provider-b")
    assert assess(candidate, replace(REQUEST, provider_override="provider-a")).statically_eligible
    assert not assess(candidate, replace(REQUEST, provider_override="provider-b")).statically_eligible


def test_json_schema_capability_satisfies_json_object_requirement():
    candidate = replace(CANDIDATE, capabilities=AdapterCapabilities(True, True, "json_schema"))
    assert assess(candidate, StaticServiceRequirement(True, True, "json_object", 4096)).statically_eligible


def test_all_rejections_are_retained_and_stably_ordered():
    candidate = replace(CANDIDATE, provider_enabled=False, binding_enabled=False,
                        routing=WeightedCandidate("binding-a", "reduced", 0, 0))
    result = assess(candidate, StaticServiceRequirement(True, True, "json_schema", 5000, "provider-b"))
    assert [item.code for item in result.reasons] == [
        "binding_disabled", "provider_disabled", "service_level_disabled", "weight_zero",
        "capability_mismatch", "capability_mismatch", "capability_mismatch", "limit_exceeded",
        "provider_override_mismatch",
    ]
    assert [item.subject for item in result.reasons[4:7]] == ["streaming", "structured_output.json_schema", "tool_calling"]
    assert all(item.subject is None for item in result.reasons[:4])


def test_enabling_reduced_service_never_relaxes_capabilities():
    candidate = replace(CANDIDATE, routing=WeightedCandidate("binding-a", "reduced", 0, 1))
    assert assess(candidate, reduced_enabled=True).statically_eligible
    assert not assess(candidate, replace(REQUEST, streaming=True), reduced_enabled=True).statically_eligible


def test_assessment_order_does_not_depend_on_input_order():
    other = replace(CANDIDATE, routing=WeightedCandidate("binding-b", "full", 1, 2))
    assert assess_static_candidates(REQUEST, (CANDIDATE, other), reduced_enabled=False) == assess_static_candidates(REQUEST, (other, CANDIDATE), reduced_enabled=False)
    with pytest.raises(ValueError):
        assess_static_candidates(REQUEST, (CANDIDATE, CANDIDATE), reduced_enabled=False)


@pytest.mark.parametrize("changes", [{"streaming": 1}, {"max_output_tokens": True}, {"max_output_tokens": 0},
                                     {"provider_override": "../provider"}, {"structured_output": "other"}])
def test_invalid_requirements_are_rejected(changes):
    with pytest.raises(ValueError):
        replace(REQUEST, **changes)


def test_projection_preserves_logical_and_resolved_identity_without_secret_reference():
    prepared = draft()
    snapshot = LoadedConfiguration("7", prepared.snapshot_digest, prepared.snapshot_json, prepared.resources)
    projection = project_alias(snapshot, "general")
    assert projection.requested_model == "general" and projection.configuration_revision == "7"
    assert projection.routing_policy == "route-a" and projection.reduced_enabled is False
    assert projection.candidates[0].resolved_model == "Upstream-Model"
    assert projection.candidates[0].provider == "provider-a"
    assert "test-provider-reference" not in repr(projection)
    assert "provider.invalid" not in repr(projection)
    assert assess(projection.candidates[0]).statically_eligible
    with pytest.raises(UnknownLogicalModel):
        project_alias(snapshot, "Upstream-Model")
