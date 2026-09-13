"""Static routing checks only; success is not authorization to start an Attempt."""

import re
from dataclasses import dataclass

from llm_gateway.domain.configuration_validation import AdapterCapabilities
from llm_gateway.domain.routing_order import WeightedCandidate


_STRUCTURED = {"none": 0, "json_object": 1, "json_schema": 2}


def _resource_id(value):
    return isinstance(value, str) and len(value) <= 128 and re.fullmatch(r"[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*", value)


@dataclass(frozen=True)
class StaticServiceRequirement:
    streaming: bool
    tool_calling: bool
    structured_output: str
    max_output_tokens: int
    provider_override: str | None = None

    def __post_init__(self):
        if type(self.streaming) is not bool or type(self.tool_calling) is not bool or self.structured_output not in _STRUCTURED:
            raise ValueError("Invalid capability requirement")
        if type(self.max_output_tokens) is not int or self.max_output_tokens < 1:
            raise ValueError("Invalid output token limit")
        if self.provider_override is not None and not _resource_id(self.provider_override):
            raise ValueError("Invalid Provider override")


@dataclass(frozen=True)
class StaticCandidate:
    routing: WeightedCandidate
    provider: str
    resolved_model: str
    provider_enabled: bool
    binding_enabled: bool
    capabilities: AdapterCapabilities
    max_output_tokens: int

    def __post_init__(self):
        if not _resource_id(self.provider) or not isinstance(self.resolved_model, str) or not self.resolved_model:
            raise ValueError("Invalid Provider or model identity")
        if any(type(value) is not bool for value in (self.provider_enabled, self.binding_enabled,
                                                   self.capabilities.streaming, self.capabilities.tool_calling)):
            raise ValueError("Invalid capability or status flag")
        if self.capabilities.structured_output not in _STRUCTURED:
            raise ValueError("Invalid structured capability")
        if type(self.max_output_tokens) is not int or self.max_output_tokens < 1:
            raise ValueError("Invalid Binding limit")


@dataclass(frozen=True)
class StaticRoutingReason:
    code: str
    subject: str | None = None


@dataclass(frozen=True)
class StaticCandidateAssessment:
    candidate: StaticCandidate
    reasons: tuple[StaticRoutingReason, ...]

    @property
    def statically_eligible(self) -> bool:
        return not self.reasons


def assess_static_candidates(requirement: StaticServiceRequirement, candidates: tuple[StaticCandidate, ...],
                             *, reduced_enabled: bool) -> tuple[StaticCandidateAssessment, ...]:
    if type(reduced_enabled) is not bool:
        raise ValueError("Explicit reduced-service policy required")
    if len({item.routing.binding for item in candidates}) != len(candidates):
        raise ValueError("Duplicate Candidate Binding")
    result = []
    for candidate in sorted(candidates, key=lambda item: item.routing.binding.encode("utf-8")):
        stages = []

        def reject(stage, code, subject=None):
            stages.append((stage, StaticRoutingReason(code, subject)))

        # Stage order is explicit, independent of caller/configuration order.
        if candidate.routing.weight == 0:
            reject(0, "weight_zero")
        if candidate.routing.service_level == "reduced" and not reduced_enabled:
            reject(0, "service_level_disabled")
        if not candidate.provider_enabled:
            reject(0, "provider_disabled")
        if not candidate.binding_enabled:
            reject(0, "binding_disabled")
        if requirement.streaming and not candidate.capabilities.streaming:
            reject(1, "capability_mismatch", "streaming")
        if requirement.tool_calling and not candidate.capabilities.tool_calling:
            reject(1, "capability_mismatch", "tool_calling")
        if _STRUCTURED[requirement.structured_output] > _STRUCTURED[candidate.capabilities.structured_output]:
            reject(1, "capability_mismatch", "structured_output." + requirement.structured_output)
        if requirement.max_output_tokens > candidate.max_output_tokens:
            reject(1, "limit_exceeded", "max_output_tokens")
        if requirement.provider_override is not None and requirement.provider_override != candidate.provider:
            reject(2, "provider_override_mismatch", requirement.provider_override)
        stages.sort(key=lambda item: (item[0], item[1].code.encode("utf-8"), (item[1].subject or "").encode("utf-8")))
        result.append(StaticCandidateAssessment(candidate, tuple(reason for _, reason in stages)))
    return tuple(result)
