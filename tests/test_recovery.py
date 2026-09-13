from dataclasses import replace

import pytest

from llm_gateway.domain.recovery import RetryPolicy, RecoveryPlan, plan_recovery


def plan(policy=RetryPolicy(), **changes):
    state = dict(attempts_started=1, candidate_attempts=1, remaining_ms=10000, retryable=True,
                 failover_eligible=True, committed=False, retry_after_ms=None, draw_jitter=lambda maximum: maximum)
    state.update(changes)
    return plan_recovery(policy, **state)


def test_same_candidate_retry_precedes_fallback():
    assert plan() == RecoveryPlan("retry", 200)
    assert plan(attempts_started=2, candidate_attempts=2) == RecoveryPlan("advance")
    assert plan(attempts_started=3, candidate_attempts=1) == RecoveryPlan("stop")


def test_structured_retry_cannot_enable_fallback():
    assert plan(failover_eligible=False) == RecoveryPlan("retry", 200)
    assert plan(attempts_started=2, candidate_attempts=2, failover_eligible=False) == RecoveryPlan("stop")
    assert plan(retryable=False, failover_eligible=False) == RecoveryPlan("stop")


@pytest.mark.parametrize("changes", [{"committed": True}, {"remaining_ms": 0}, {"remaining_ms": -1},
                                     {"attempts_started": 3}])
def test_terminal_gate_prevents_jitter_and_recovery(changes):
    def forbidden(_):
        pytest.fail("Terminal gate must not sample jitter")
    assert plan(draw_jitter=forbidden, **changes).action == "stop"


@pytest.mark.parametrize("value,expected", [(0, 200), (100, 200), (3000, 3000), (5000, 5000)])
def test_trusted_retry_after_is_a_minimum_delay(value, expected):
    assert plan(retry_after_ms=value).delay_ms == expected


def test_retry_after_beyond_ceiling_or_deadline_skips_candidate_not_early_retry():
    assert plan(retry_after_ms=5001).action == "advance"
    assert plan(retry_after_ms=5000, remaining_ms=5000).action == "advance"
    assert plan(retry_after_ms=5001, failover_eligible=False).action == "stop"
    assert plan(replace(RetryPolicy(), max_retry_after_seconds=0), retry_after_ms=999999).delay_ms == 200


def test_jitter_and_deadline_are_bounded():
    assert plan(draw_jitter=lambda _: 0).delay_ms == 0
    assert plan(remaining_ms=200).action == "advance"
    assert plan(remaining_ms=201).action == "retry"
    assert plan(replace(RetryPolicy(), base_delay_ms=2000), remaining_ms=2001).delay_ms == 2000


@pytest.mark.parametrize("value", [-1, 201, True, 0.5])
def test_invalid_jitter_never_starts_a_retry(value):
    with pytest.raises(ValueError):
        plan(draw_jitter=lambda _: value)


@pytest.mark.parametrize("changes", [{"max_attempts": 4}, {"max_attempts_per_candidate": 3},
    {"max_attempts": 1}, {"base_delay_ms": 0}, {"multiplier": 5}, {"max_delay_ms": 2001},
    {"max_retry_after_seconds": 6}, {"max_attempts": True}])
def test_policy_limits_cannot_expand_protocol(changes):
    with pytest.raises(ValueError):
        replace(RetryPolicy(), **changes)


@pytest.mark.parametrize("changes", [
    {"attempts_started": 0}, {"attempts_started": 4}, {"attempts_started": True},
    {"candidate_attempts": 0}, {"candidate_attempts": 2},
    {"attempts_started": 3, "candidate_attempts": 3},
    {"remaining_ms": True}, {"retryable": 1}, {"failover_eligible": 0},
    {"committed": None}, {"retry_after_ms": -1}, {"retry_after_ms": True},
])
def test_invalid_recovery_state_is_rejected_before_sampling(changes):
    def forbidden(_):
        pytest.fail("Invalid state must not sample jitter")
    with pytest.raises(ValueError):
        plan(draw_jitter=forbidden, **changes)


def test_tightened_policy_and_non_retryable_failover_do_not_sleep():
    def forbidden(_):
        pytest.fail("No same-candidate retry must mean no backoff sampling")
    assert plan(replace(RetryPolicy(), max_attempts_per_candidate=1), draw_jitter=forbidden).action == "advance"
    assert plan(retryable=False, draw_jitter=forbidden).action == "advance"
    assert plan(replace(RetryPolicy(), max_attempts=1, max_attempts_per_candidate=1),
                draw_jitter=forbidden).action == "stop"
