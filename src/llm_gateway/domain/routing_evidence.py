"""Closed, content-free preselection facts for durable routing explanation."""

import re
from dataclasses import dataclass

from llm_gateway.domain.routing_eligibility import StaticRoutingReason, StaticServiceRequirement
from llm_gateway.domain.routing_order import WeightedCandidate, weighted_candidate_order


_REASON_STAGES = (
    ("weight_zero", "service_level_disabled", "provider_disabled", "binding_disabled"),
    ("capability_mismatch", "limit_exceeded", "parameter_unsupported"),
    ("provider_override_mismatch",), ("safety_ineligible",),
    ("egress_policy_violation", "trust_bundle_expired", "security_invalidated"),
    ("provider_credentials_unavailable",), ("health_unavailable", "circuit_open", "circuit_half_open_busy"),
    ("concurrency_exhausted", "qps_exhausted", "deadline_insufficient", "attempt_budget_exhausted", "commitment_reached"),
    ("degradation_not_enabled", "cache_ineligible", "cache_backend_unavailable", "cache_miss"),
)
_STAGE = {code: stage for stage, codes in enumerate(_REASON_STAGES) for code in codes}
_PARAMETERS = {"temperature", "top_p", "max_output_tokens", "stop", "presence_penalty",
               "frequency_penalty", "reasoning_effort", "verbosity"}


def _resource(value):
    return isinstance(value, str) and len(value) <= 128 and re.fullmatch(r"[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*", value)


def ordered_reasons(reasons: tuple[StaticRoutingReason, ...], *, requirement: StaticServiceRequirement,
                    feature_ids: frozenset[str] = frozenset()) -> tuple[StaticRoutingReason, ...]:
    if not isinstance(reasons, tuple) or not isinstance(feature_ids, frozenset) or any(not _resource(v) for v in feature_ids):
        raise ValueError("Immutable reasons and feature registry required")
    for reason in reasons:
        if not isinstance(reason, StaticRoutingReason) or reason.code not in _STAGE:
            raise ValueError("Unregistered routing reason")
        allowed = {
            "capability_mismatch": {"streaming", "tool_calling", "structured_output.json_object", "structured_output.json_schema"}
                | {"structured_output.feature." + feature for feature in feature_ids},
            "limit_exceeded": {"context_tokens", "max_output_tokens"},
            "parameter_unsupported": _PARAMETERS,
            "provider_override_mismatch": {requirement.provider_override} if requirement.provider_override else set(),
        }.get(reason.code, {None})
        if reason.subject not in allowed:
            raise ValueError("Unregistered routing reason subject")
    if len(set(reasons)) != len(reasons):
        raise ValueError("Duplicate routing reasons")
    return tuple(sorted(reasons, key=lambda item: (_STAGE[item.code], item.code, item.subject or "")))


@dataclass(frozen=True)
class EvidenceCandidate:
    candidate: WeightedCandidate
    initial_order: int | None
    reasons: tuple[StaticRoutingReason, ...] = ()

    def __post_init__(self):
        if not isinstance(self.candidate, WeightedCandidate) or not isinstance(self.reasons, tuple):
            raise ValueError("Immutable Candidate evidence required")
        if self.initial_order is not None and (type(self.initial_order) is not int or self.initial_order < 0):
            raise ValueError("Invalid initial order")
        if bool(self.reasons) != (self.initial_order is None):
            raise ValueError("Rejected Candidates have no order; eligible Candidates require order")
        if self.candidate.weight == 0 and StaticRoutingReason("weight_zero") not in self.reasons:
            raise ValueError("Zero weight rejection must be recorded")


@dataclass(frozen=True)
class RoutingPreselection:
    alias_digest: str
    routing_policy: str
    routing_policy_digest: str
    requirement_digest: str
    seed_hex: str
    requirement: StaticServiceRequirement
    candidates: tuple[EvidenceCandidate, ...]
    feature_ids: frozenset[str] = frozenset()

    def __post_init__(self):
        if any(not isinstance(value, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", value)
               for value in (self.alias_digest, self.routing_policy_digest, self.requirement_digest)):
            raise ValueError("Canonical evidence digests required")
        if not _resource(self.routing_policy) or not isinstance(self.requirement, StaticServiceRequirement):
            raise ValueError("Invalid routing identity or requirement")
        if not isinstance(self.candidates, tuple) or any(not isinstance(item, EvidenceCandidate) for item in self.candidates):
            raise ValueError("Immutable Candidate snapshot required")
        if len({item.candidate.binding for item in self.candidates}) != len(self.candidates):
            raise ValueError("Duplicate Candidate")
        ordered_reasons((), requirement=self.requirement, feature_ids=self.feature_ids)
        for item in self.candidates:
            if ordered_reasons(item.reasons, requirement=self.requirement, feature_ids=self.feature_ids) != item.reasons:
                raise ValueError("Reasons must be in canonical evaluation order")
        order = weighted_candidate_order(tuple(item.candidate for item in self.candidates if not item.reasons), self.seed_hex)
        expected = {binding: index for index, binding in enumerate(order.full + order.reduced)}
        if any(item.initial_order != expected.get(item.candidate.binding) for item in self.candidates):
            raise ValueError("Candidate order does not match the locked seed and policy")
