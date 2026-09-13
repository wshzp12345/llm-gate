"""Bounded recovery planning from an explicit classified retry/fallback decision.

No request rewriting, sleeping, hidden repair or Provider I/O. Callers retain
the same invocation deadline and must recheck gates before starting an Attempt.
"""

from collections.abc import Callable
from dataclasses import dataclass


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 3
    max_attempts_per_candidate: int = 2
    base_delay_ms: int = 200
    multiplier: int = 2
    max_delay_ms: int = 2000
    max_retry_after_seconds: int = 5

    def __post_init__(self):
        if any(type(value) is not int for value in self.__dict__.values()):
            raise ValueError("Integer retry limits required")
        if not (1 <= self.max_attempts <= 3 and 1 <= self.max_attempts_per_candidate <= min(2, self.max_attempts)
                and 1 <= self.base_delay_ms <= self.max_delay_ms <= 2000 and 1 <= self.multiplier <= 4
                and 0 <= self.max_retry_after_seconds <= 5):
            raise ValueError("Retry policy exceeds protocol bounds")


@dataclass(frozen=True)
class RecoveryPlan:
    action: str
    delay_ms: int = 0


def plan_recovery(policy: RetryPolicy, *, attempts_started: int, candidate_attempts: int,
                  remaining_ms: int, retryable: bool, failover_eligible: bool, committed: bool,
                  retry_after_ms: int | None, draw_jitter: Callable[[int], int]) -> RecoveryPlan:
    if (type(attempts_started) is not int or type(candidate_attempts) is not int
            or not 1 <= candidate_attempts <= attempts_started <= policy.max_attempts
            or candidate_attempts > policy.max_attempts_per_candidate):
        raise ValueError("Invalid consumed Attempt counts")
    if type(remaining_ms) is not int or any(type(value) is not bool for value in (retryable, failover_eligible, committed)):
        raise ValueError("Invalid recovery state")
    if retry_after_ms is not None and (type(retry_after_ms) is not int or retry_after_ms < 0):
        raise ValueError("Retry-After must be a validated nonnegative duration")
    if committed or remaining_ms <= 0 or attempts_started >= policy.max_attempts:
        return RecoveryPlan("stop")
    advance = RecoveryPlan("advance" if failover_eligible else "stop")
    if not retryable or candidate_attempts >= policy.max_attempts_per_candidate:
        return advance
    honored_after = 0
    if policy.max_retry_after_seconds and retry_after_ms is not None:
        if retry_after_ms > policy.max_retry_after_seconds * 1000 or retry_after_ms >= remaining_ms:
            return advance
        honored_after = retry_after_ms
    ceiling = min(policy.max_delay_ms, policy.base_delay_ms * policy.multiplier ** (candidate_attempts - 1))
    jitter = draw_jitter(ceiling)
    if type(jitter) is not int or not 0 <= jitter <= ceiling:
        raise ValueError("Full-jitter sample outside requested range")
    delay = max(jitter, honored_after)
    if delay >= remaining_ms:
        return advance
    return RecoveryPlan("retry", delay)
