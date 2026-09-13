from dataclasses import replace
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from llm_gateway.application.circuit_evidence import CircuitTransitionEvidence
from llm_gateway.domain.provider_circuit import CircuitTransition


def event(revision="1", **changes):
    return CircuitTransitionEvidence(uuid4(), "a", revision, datetime(2026, 9, 11, tzinfo=timezone.utc),
        CircuitTransition("closed", "open", "consecutive_failures", 42.0, 5, 5, 5, 30), **changes)


@pytest.mark.parametrize("changes", [
    {"binding_id": "raw endpoint https://private"}, {"configuration_revision": "01"},
    {"occurred_at": datetime(2026, 9, 11)}, {"attempt_number": 1}, {"call_id": "not-uuid"},
])
def test_invalid_circuit_identity_or_correlation_is_rejected(changes):
    with pytest.raises(ValueError):
        replace(event(), **changes)


@pytest.mark.parametrize("changes", [
    {"cause": "raw provider error"}, {"new_state": "closed"}, {"eligible_samples": 21},
    {"failed_samples": 6}, {"open_seconds": 31}, {"minimum_samples": 1},
    {"observed_at": float("nan")}, {"consecutive_failures": 4},
    {"cause": "failure_rate", "eligible_samples": 9},
])
def test_invalid_transition_or_fixed_threshold_is_rejected(changes):
    value = event()
    with pytest.raises(ValueError):
        replace(value, transition=replace(value.transition, **changes))
