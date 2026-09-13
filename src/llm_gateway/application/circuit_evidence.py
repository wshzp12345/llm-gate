"""Content-free transition evidence, not restorable Circuit state."""

import math
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol
from uuid import UUID

from llm_gateway.domain.configuration import revision_number
from llm_gateway.domain.provider_circuit import CircuitTransition


@dataclass(frozen=True)
class CircuitTransitionEvidence:
    event_id: UUID
    binding_id: str
    configuration_revision: str
    occurred_at: datetime
    transition: CircuitTransition
    call_id: UUID | None = None
    attempt_number: int | None = None

    def __post_init__(self):
        if not isinstance(self.event_id, UUID) or self.event_id.version != 4:
            raise ValueError("Circuit event UUIDv4 required")
        if not isinstance(self.binding_id, str) or len(self.binding_id) > 128 or not re.fullmatch(r"[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*", self.binding_id):
            raise ValueError("Binding resource identity required")
        revision_number(self.configuration_revision)
        if not isinstance(self.occurred_at, datetime) or self.occurred_at.tzinfo is None or self.occurred_at.utcoffset() != timedelta(0):
            raise ValueError("UTC Circuit event time required")
        if self.call_id is not None and (not isinstance(self.call_id, UUID) or self.call_id.version != 4):
            raise ValueError("Invocation UUIDv4 required")
        if self.attempt_number is not None and (self.call_id is None or type(self.attempt_number) is not int or not 1 <= self.attempt_number <= 3):
            raise ValueError("Valid Attempt correlation required")
        t = self.transition
        if not isinstance(t, CircuitTransition) or (t.old_state, t.new_state, t.cause) not in {
            ("closed", "open", "consecutive_failures"), ("closed", "open", "failure_rate"),
            ("open", "half_open", "cooldown_elapsed"), ("half_open", "open", "trial_failure"),
            ("half_open", "closed", "trial_successes"),
        }:
            raise ValueError("Registered Circuit transition required")
        if (type(t.observed_at) not in (int, float) or not math.isfinite(t.observed_at) or t.observed_at < 0
                or any(type(n) is not int for n in (t.eligible_samples, t.failed_samples, t.consecutive_failures, t.open_seconds))
                or not 0 <= t.failed_samples <= t.eligible_samples <= 20 or t.consecutive_failures < 0
                or t.open_seconds not in {30, 60, 120, 240, 300}):
            raise ValueError("Invalid Circuit measurements")
        thresholds = (t.sample_window_seconds, t.sample_limit, t.minimum_samples, t.failure_percent, t.consecutive_failure_threshold)
        if any(type(n) is not int for n in thresholds) or thresholds != (60, 20, 10, 50, 5):
            raise ValueError("Fixed Circuit thresholds required")
        if t.cause == "consecutive_failures" and t.consecutive_failures < 5:
            raise ValueError("Consecutive-failure threshold was not reached")
        if t.cause == "failure_rate" and (t.eligible_samples < 10 or t.failed_samples * 2 < t.eligible_samples):
            raise ValueError("Failure-rate threshold was not reached")


class CircuitEvidencePort(Protocol):
    async def append(self, evidence: CircuitTransitionEvidence) -> None:
        """Commit immutable event; the same ID with different facts must fail."""
        ...
