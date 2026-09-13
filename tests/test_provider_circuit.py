import copy
import asyncio
from contextlib import asynccontextmanager
from concurrent.futures import ThreadPoolExecutor

import pytest

from llm_gateway.application.provider_circuits import ProviderCircuits, circuit_observation
from llm_gateway.domain.model import FailureCode, ProviderFailure
from llm_gateway.domain.provider_circuit import ProviderCircuit
from tests.test_attempt_execution import SUCCESS, FAILURE, CANDIDATES, Harness
from llm_gateway.application.attempt_execution import NoEligibleCandidate
from llm_gateway.domain.recovery import RetryPolicy


def observe(circuit, success, now=0):
    admission = circuit.acquire(now)
    assert admission.permit is not None
    return circuit.finish(admission.permit, success, now)


def opened():
    circuit = ProviderCircuit()
    for _ in range(5):
        transition = observe(circuit, False)
    assert transition.cause == "consecutive_failures"
    return circuit


def test_five_failures_open_without_ten_samples_and_reject_until_boundary():
    circuit = opened()
    assert circuit.acquire(29.999).rejection == "circuit_open"
    trial = circuit.acquire(30)
    assert trial.transition.old_state == "open" and trial.transition.new_state == "half_open"
    assert circuit.acquire(30).rejection == "circuit_half_open_busy"
    circuit.finish(trial.permit, None, 30)
    assert circuit.acquire(30).permit is not None


def test_rate_threshold_requires_ten_samples_and_uses_exact_half():
    circuit = ProviderCircuit()
    for success in [True, False] * 4 + [True]:
        assert observe(circuit, success) is None
    transition = observe(circuit, False)
    assert (transition.cause, transition.eligible_samples, transition.failed_samples) == ("failure_rate", 10, 5)
    assert transition.open_seconds == 30


def test_window_is_sixty_seconds_and_capped_at_twenty():
    circuit = ProviderCircuit()
    for _ in range(25):
        observe(circuit, True, 0)
    assert len(circuit._samples) == 20
    for success in [True, False] * 4:
        observe(circuit, success, 60)
    assert len(circuit._samples) == 8 and circuit.state == "closed"
    observe(circuit, True, 60)
    assert observe(circuit, False, 60).cause == "failure_rate"


def test_two_successful_trials_close_and_reset_next_duration():
    circuit = opened()
    assert observe(circuit, False, 30).open_seconds == 60
    assert observe(circuit, True, 90) is None
    # An excluded input/credential outcome must not reset the eligible streak.
    assert observe(circuit, None, 90) is None
    transition = observe(circuit, True, 90)
    assert transition.new_state == "closed" and transition.cause == "trial_successes"
    assert not circuit._samples
    for _ in range(5):
        transition = observe(circuit, False, 90)
    assert transition.open_seconds == 30


def test_repeated_half_open_failures_double_to_five_minute_cap():
    circuit = opened()
    now = 30
    for duration in (60, 120, 240, 300, 300):
        transition = observe(circuit, False, now)
        assert transition.open_seconds == duration
        now += duration


def test_excluded_outcomes_never_enter_samples_or_reset_failure_streak():
    circuit = ProviderCircuit()
    for _ in range(4):
        observe(circuit, False)
        observe(circuit, None)
    assert len(circuit._samples) == 4 and circuit.state == "closed"
    assert observe(circuit, False).cause == "consecutive_failures"


def test_stale_inflight_success_cannot_close_or_contaminate_recovered_generation():
    circuit = ProviderCircuit()
    old = circuit.acquire(0).permit
    for _ in range(5):
        observe(circuit, False)
    observe(circuit, True, 30)
    observe(circuit, True, 30)
    assert circuit.state == "closed" and not circuit._samples
    assert circuit.finish(old, False, 30) is None
    assert circuit.state == "closed" and not circuit._samples


def test_permits_are_identity_checked_and_consumed_once():
    circuit = ProviderCircuit()
    permit = circuit.acquire(0).permit
    with pytest.raises(ValueError):
        circuit.finish(copy.copy(permit), False, 0)
    circuit.finish(permit, None, 0)
    with pytest.raises(ValueError):
        circuit.finish(permit, False, 0)


@pytest.mark.parametrize("now", [-1, float("nan"), float("inf"), True])
def test_invalid_time_is_not_accepted(now):
    with pytest.raises(ValueError):
        ProviderCircuit().acquire(now)


def test_clock_regression_fails_without_consuming_permit():
    circuit = ProviderCircuit()
    permit = circuit.acquire(10).permit
    with pytest.raises(ValueError):
        circuit.finish(permit, False, 9)
    circuit.finish(permit, True, 10)


@pytest.mark.parametrize("code,retryable,expected", [
    (FailureCode.RATE_LIMITED, True, False), (FailureCode.UPSTREAM_TIMEOUT, True, False),
    (FailureCode.PROVIDER_UNAVAILABLE, True, False), (FailureCode.PROVIDER_UNAVAILABLE, False, None),
    (FailureCode.PROVIDER_CREDENTIALS_UNAVAILABLE, True, None), (FailureCode.INVALID_REQUEST, True, None),
    (FailureCode.PROVIDER_PROTOCOL_ERROR, False, None),
])
def test_only_explicit_availability_failures_count(code, retryable, expected):
    assert circuit_observation(ProviderFailure(code, retryable)) is expected
    assert circuit_observation(SUCCESS) is True


def test_registry_is_binding_scoped_and_restart_starts_closed():
    registry = ProviderCircuits()
    for _ in range(5):
        admission = registry.acquire("a", 0)
        registry.finish("a", admission.permit, False, 0)
    assert registry.acquire("a", 0).rejection == "circuit_open"
    assert registry.acquire("b", 0).permit is not None
    assert ProviderCircuits().acquire("a", 0).permit is not None


def test_concurrent_half_open_admission_has_exactly_one_trial():
    registry = ProviderCircuits()
    for _ in range(5):
        registry.finish("a", registry.acquire("a", 0).permit, False, 0)
    with ThreadPoolExecutor(max_workers=8) as workers:
        results = list(workers.map(lambda _: registry.acquire("a", 30), range(20)))
    assert sum(result.permit is not None for result in results) == 1
    assert sum(result.rejection == "circuit_half_open_busy" for result in results) == 19
    trial = next(result.permit for result in results if result.permit is not None)
    registry.finish("a", trial, None, 30)
    assert registry.acquire("a", 30).permit is not None


def circuit_harness(registry, result):
    """Synthetic remaining-gate composition, not the production runtime."""
    harness = Harness([result])
    original = harness.acquire
    @asynccontextmanager
    async def acquire(binding):
        admission = registry.acquire(binding, 0)
        if admission.permit is None:
            harness.events.append(("circuit_rejected", binding, admission.rejection))
            yield None
            return
        observation = None
        class Observed:
            async def complete(self, request):
                nonlocal observation
                result = await harness.complete(request)
                observation = circuit_observation(result)
                return result
        try:
            async with original(binding):
                yield Observed()
        except BaseException:
            registry.finish(binding, admission.permit, None, 0)
            raise
        else:
            transition = registry.finish(binding, admission.permit, observation, 0)
            if transition:
                harness.events.append(("transition", binding, transition.new_state))
    harness.acquire = acquire
    return harness


def test_executor_skips_open_candidate_without_attempt_and_uses_next_candidate():
    registry = ProviderCircuits()
    for _ in range(5):
        harness = circuit_harness(registry, FAILURE)
        asyncio.run(harness.run(CANDIDATES[:1], policy=RetryPolicy(max_attempts=1, max_attempts_per_candidate=1)))
    assert ("transition", "a", "open") in harness.events
    following = circuit_harness(registry, SUCCESS)
    assert asyncio.run(following.run()) == SUCCESS
    assert ("circuit_rejected", "a", "circuit_open") in following.events
    assert ("start", 1, "b", 1) in following.events
    assert following.events.count(("provider",)) == 1


def test_executor_circuit_exhaustion_does_not_fabricate_provider_work():
    registry = ProviderCircuits()
    for _ in range(5):
        registry.finish("a", registry.acquire("a", 0).permit, False, 0)
    harness = circuit_harness(registry, SUCCESS)
    with pytest.raises(NoEligibleCandidate):
        asyncio.run(harness.run(CANDIDATES[:1]))
    assert not any(event[0] in {"provider", "start", "sleep"} for event in harness.events)
