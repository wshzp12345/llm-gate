"""Lock text Candidate order from static assessment and actual initial gates.

The caller must supply a completed initial-gate result for every statically
eligible Binding. An empty tuple explicitly means checked/eligible, not a
default gate. This does not perform Provider I/O or authorize later Attempts.
"""

import json
from dataclasses import asdict, dataclass

from llm_gateway.adapters.canonical_json import canonical_digest
from llm_gateway.adapters.routing_projection import project_alias
from llm_gateway.adapters.text_completion_projection import prepare_text_completion
from llm_gateway.application.attempt_execution import ExecutionCandidate
from llm_gateway.application.model_api import ModelInvocationRejected
from llm_gateway.domain.recovery import RetryPolicy
from llm_gateway.domain.routing_eligibility import StaticServiceRequirement, assess_static_candidates
from llm_gateway.domain.routing_evidence import EvidenceCandidate, RoutingPreselection, ordered_reasons
from llm_gateway.domain.routing_order import weighted_candidate_order


@dataclass(frozen=True)
class TextRoutingPlan:
    preselection: RoutingPreselection
    full: tuple[ExecutionCandidate, ...]
    reduced: tuple[ExecutionCandidate, ...]
    retry: RetryPolicy


def text_routing_requirements(snapshot, query):
    content = json.loads(snapshot.snapshot_json)
    alias = content["model_aliases"][query.requested_model]
    ceiling = alias["generation_defaults"]["max_output_tokens"]
    maximum = ceiling if query.max_output_tokens is None else query.max_output_tokens
    if maximum > ceiling:
        raise ModelInvocationRejected("invalid_request")
    requirement = StaticServiceRequirement(query.stream, False, "none", maximum)
    projected = project_alias(snapshot, query.requested_model)
    assessed = assess_static_candidates(requirement, projected.candidates, reduced_enabled=projected.reduced_enabled)
    return requirement, assessed


def prepare_text_routing(snapshot, query, *, seed_hex: str, initial_rejections) -> TextRoutingPlan:
    requirement, assessed = text_routing_requirements(snapshot, query)
    required = {item.candidate.routing.binding for item in assessed if item.statically_eligible}
    if set(initial_rejections) != required:
        raise ValueError("Complete initial eligibility checks required")
    reasons = {}
    for item in assessed:
        binding = item.candidate.routing.binding
        reasons[binding] = ordered_reasons(item.reasons if item.reasons else initial_rejections[binding], requirement=requirement)
    order = weighted_candidate_order(tuple(item.candidate.routing for item in assessed
                                          if not reasons[item.candidate.routing.binding]), seed_hex)
    positions = {binding: index for index, binding in enumerate(order.full + order.reduced)}
    content = json.loads(snapshot.snapshot_json)
    alias = content["model_aliases"][query.requested_model]
    policy = content["routing_policies"][alias["routing_policy"]]
    preselection = RoutingPreselection(canonical_digest(alias), alias["routing_policy"], canonical_digest(policy),
        canonical_digest(asdict(requirement)), seed_hex, requirement,
        tuple(EvidenceCandidate(item.candidate.routing, positions.get(item.candidate.routing.binding),
                                reasons[item.candidate.routing.binding]) for item in assessed))
    def candidates(names):
        return tuple(ExecutionCandidate(binding, prepare_text_completion(snapshot, query, binding).request) for binding in names)
    return TextRoutingPlan(preselection, candidates(order.full), candidates(order.reduced),
                           RetryPolicy(**{key: value for key, value in policy["retry"].items() if key != "jitter"}))
