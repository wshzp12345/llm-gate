"""Fixed single-instance Circuit policy (FR-360 through FR-366).

Callers serialize access, supply monotonic time, persist returned transitions,
and release every permit. No wall clock, framework, storage or sleeping here.
"""

import math
from collections import deque
from dataclasses import dataclass


@dataclass(frozen=True)
class CircuitTransition:
    old_state: str
    new_state: str
    cause: str
    observed_at: float
    eligible_samples: int
    failed_samples: int
    consecutive_failures: int
    open_seconds: int
    sample_window_seconds: int = 60
    sample_limit: int = 20
    minimum_samples: int = 10
    failure_percent: int = 50
    consecutive_failure_threshold: int = 5


@dataclass(frozen=True, eq=False)
class CircuitPermit:
    generation: int


@dataclass(frozen=True)
class CircuitAdmission:
    permit: CircuitPermit | None
    rejection: str | None = None
    transition: CircuitTransition | None = None


class ProviderCircuit:
    def __init__(self):
        self._state = "closed"
        self._samples = deque(maxlen=20)
        self._consecutive_failures = 0
        self._trial_successes = 0
        self._open_seconds = 30
        self._open_until = None
        self._trial_busy = False
        self._generation = 0
        self._permits = set()
        self._last_time = None

    @property
    def state(self):
        return self._state

    def _time(self, now):
        if type(now) not in (int, float) or not math.isfinite(now) or now < 0 or (self._last_time is not None and now < self._last_time):
            raise ValueError("Nondecreasing finite monotonic time required")
        self._last_time = now
        while self._samples and self._samples[0][0] <= now - 60:
            self._samples.popleft()

    def rejection(self, now: float) -> str | None:
        """Inspect eligibility without transitions, sample eviction or a permit."""
        if type(now) not in (int, float) or not math.isfinite(now) or now < 0 or (self._last_time is not None and now < self._last_time):
            raise ValueError("Nondecreasing finite monotonic time required")
        if self.state == "open" and now < self._open_until:
            return "circuit_open"
        if self.state == "half_open" and self._trial_busy:
            return "circuit_half_open_busy"
        return None

    def _transition(self, state, cause, now):
        transition = CircuitTransition(self.state, state, cause, now, len(self._samples),
            sum(not success for _, success in self._samples), self._consecutive_failures, self._open_seconds)
        self._state = state
        self._generation += 1
        return transition

    def acquire(self, now: float) -> CircuitAdmission:
        self._time(now)
        transition = None
        if self.state == "open":
            if now < self._open_until:
                return CircuitAdmission(None, "circuit_open")
            transition = self._transition("half_open", "cooldown_elapsed", now)
            self._trial_successes = 0
        if self.state == "half_open" and self._trial_busy:
            return CircuitAdmission(None, "circuit_half_open_busy", transition)
        permit = CircuitPermit(self._generation)
        self._permits.add(permit)
        if self.state == "half_open":
            self._trial_busy = True
        return CircuitAdmission(permit, transition=transition)

    def finish(self, permit: CircuitPermit, success: bool | None, now: float) -> CircuitTransition | None:
        """None excludes cancellation, unstarted gates, credentials and validation.

        A permit is consumed once. Late results from an older Circuit generation
        cannot undo opening or contaminate a recovered generation's samples.
        """
        if success is not None and type(success) is not bool:
            raise ValueError("Explicit eligible success/failure or exclusion required")
        if permit not in self._permits:
            raise ValueError("Unknown or already completed Circuit permit")
        self._time(now)
        self._permits.remove(permit)
        if permit.generation != self._generation:
            return None
        if success is not None:
            self._samples.append((now, success))
            self._consecutive_failures = 0 if success else self._consecutive_failures + 1
        if self.state == "half_open":
            self._trial_busy = False
            if success is None:
                return None
            if success:
                self._trial_successes += 1
                if self._trial_successes < 2:
                    return None
                transition = self._transition("closed", "trial_successes", now)
                self._samples.clear()
                self._consecutive_failures = 0
                self._open_seconds = 30
                self._open_until = None
                return transition
            self._open_seconds = min(300, self._open_seconds * 2)
            self._open_until = now + self._open_seconds
            return self._transition("open", "trial_failure", now)
        if success is None:
            return None
        failures = sum(not value for _, value in self._samples)
        cause = None
        if self._consecutive_failures >= 5:
            cause = "consecutive_failures"
        elif len(self._samples) >= 10 and failures * 2 >= len(self._samples):
            cause = "failure_rate"
        if cause is not None:
            self._open_until = now + self._open_seconds
            return self._transition("open", cause, now)
        return None
