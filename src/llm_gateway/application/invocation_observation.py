"""Content-free durable evidence for restricted telemetry, not public responses."""

from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

from llm_gateway.domain.model import Usage
from llm_gateway.domain.correlation import BusinessCorrelation


@dataclass(frozen=True)
class ObservedAttempt:
    number: int
    binding_id: str
    candidate_attempt: int
    outcome: str | None
    recovery_action: str | None
    actual_model: str | None
    model_conflict: bool
    usage: Usage
    late_usage: Usage | None
    pricing_revision: int | None
    pricing_resource_id: str | None


@dataclass(frozen=True)
class ObservedCost:
    currency: str
    total_cost: str | None
    completeness: str
    certainty: str


@dataclass(frozen=True)
class InvocationObservation:
    call_id: UUID
    trace_id: str
    state: str
    terminal_outcome: str | None
    error_code: str | None
    actual_model: str | None
    model_conflict: bool
    attempts: tuple[ObservedAttempt, ...]
    settled_usage: Usage
    costs: tuple[ObservedCost, ...]
    correlation: BusinessCorrelation


class InvocationObservationReader(Protocol):
    async def read(self, call_id: UUID) -> InvocationObservation | None:
        """Read one consistent snapshot outside request execution resources."""
        ...
