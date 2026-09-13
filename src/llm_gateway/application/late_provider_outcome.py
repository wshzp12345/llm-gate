"""Content-free observations after cancellation wins, not billing adjustments."""

from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

from llm_gateway.domain.model import FailureCode, ProviderFailure, ProviderResult, Usage


@dataclass(frozen=True)
class LateTextOutcome:
    event_id: UUID
    attempt_number: int
    outcome: str
    error_code: FailureCode | None
    requested_model: str | None
    resolved_model: str | None
    finish_reason: str | None
    usage: Usage
    safety_refused: bool

    def __post_init__(self):
        if (not isinstance(self.event_id, UUID) or self.event_id.version != 4
                or type(self.attempt_number) is not int or not 1 <= self.attempt_number <= 3
                or not isinstance(self.usage, Usage) or type(self.safety_refused) is not bool):
            raise ValueError("Typed late Provider observation required")
        if self.outcome == "succeeded":
            if (self.error_code is not None
                    or any(not isinstance(value, str) or not value for value in (self.requested_model, self.resolved_model))
                    or self.finish_reason not in ({"stop", "length", "content_filter"} if self.safety_refused else {"stop", "length"})):
                raise ValueError("Invalid late text completion metadata")
        elif (self.outcome not in {"failed", "uncertain"} or not isinstance(self.error_code, FailureCode)
                or (self.outcome == "uncertain") != (self.error_code == FailureCode.UNCERTAIN)
                or any(value is not None for value in (self.requested_model, self.finish_reason))
                or self.resolved_model is not None and (type(self.resolved_model) is not str or not 1 <= len(self.resolved_model) <= 256)
                or self.safety_refused):
            raise ValueError("Invalid late Provider failure metadata")

    @classmethod
    def from_result(cls, event_id, attempt_number, result):
        # No raw model content/refusal text is retained in this value.
        if isinstance(result, ProviderResult):
            return cls(event_id, attempt_number, "succeeded", None, result.requested_model,
                       result.resolved_model, result.finish_reason, result.usage,
                       result.disposition == "safety_refused")
        if isinstance(result, ProviderFailure):
            return cls(event_id, attempt_number, "uncertain" if result.code == FailureCode.UNCERTAIN else "failed",
                       result.code, None, result.observed_model, None, result.observed_usage or Usage(), False)
        raise ValueError("Typed Provider result required")


class LateProviderOutcomePort(Protocol):
    async def record(self, outcome: LateTextOutcome) -> None:
        """Commit an idempotent observation bound to one admitted Invocation.

        No terminal rewrite, automatic reconciliation, replay or regeneration.
        """
        ...
