import asyncio
from copy import deepcopy
from contextlib import asynccontextmanager
from dataclasses import replace

import pytest

from llm_gateway.adapters.text_routing_plan import text_routing_requirements
from llm_gateway.domain.provider_circuit import ProviderCircuit
from llm_gateway.infrastructure.text_initial_eligibility import TextInitialEligibility
from tests.test_text_attempt_runtime import Harness
from tests.test_text_fingerprints import QUERY


def test_repeated_initial_checks_leave_all_attempt_budgets_untouched():
    harness = Harness()

    async def run():
        initial = TextInitialEligibility(credential_source=harness, security_gates=harness.security,
            circuits=harness.coordinator, capacity=harness.capacity, rate=harness.rate)
        requirement, assessed = text_routing_requirements(harness.snapshot, QUERY)
        for _ in range(3):
            assert await initial(harness.admission, harness.snapshot, requirement, assessed,
                deadline=asyncio.get_running_loop().time() + 5) == {"binding-a": ()}
            harness.assert_released()
            assert harness.rate._buckets == {} and harness.circuits._circuits == {}
        assert "transport" not in harness.events
        await harness.run()

    asyncio.run(run())
    assert harness.events.count("resolve") == 4 and harness.events.count("io") == 1


@pytest.mark.parametrize("mode,code", [("credential", "provider_credentials_unavailable"),
    ("security", "trust_bundle_expired"), ("circuit", "circuit_open"),
    ("capacity", "concurrency_exhausted"), ("qps", "qps_exhausted")])
def test_initial_rejections_do_not_spend_or_allocate(mode, code):
    harness = Harness(mode=mode)
    buckets = deepcopy(harness.rate._buckets)
    leases = set(harness.capacity._leases)

    async def run():
        initial = TextInitialEligibility(credential_source=harness, security_gates=harness.security,
            circuits=harness.coordinator, capacity=harness.capacity, rate=harness.rate)
        requirement, assessed = text_routing_requirements(harness.snapshot, QUERY)
        result = await initial(harness.admission, harness.snapshot, requirement, assessed,
            deadline=asyncio.get_running_loop().time() + 5)
        assert tuple(reason.code for reason in result["binding-a"]) == (code,)

    asyncio.run(run())
    assert harness.rate._buckets == buckets and harness.capacity._leases == leases
    assert not harness.coordinator._leases and all(not lease._material for lease in harness.leases)
    assert "transport" not in harness.events
    for lease in harness.held:
        lease.release()


def test_circuit_initial_check_does_not_transition_or_reserve_half_open_trial():
    circuit = ProviderCircuit()
    for _ in range(5):
        circuit.finish(circuit.acquire(0).permit, False, 0)
    for _ in range(3):
        assert circuit.rejection(29) == "circuit_open"
        assert circuit.rejection(30) is None
        assert circuit.state == "open" and not circuit._permits and circuit._last_time == 0
    admitted = circuit.acquire(30)
    assert admitted.transition.new_state == "half_open"
    assert circuit.rejection(30) == "circuit_half_open_busy"
    circuit.finish(admitted.permit, None, 30)
    assert circuit.rejection(30) is None


def test_rate_preview_matches_consumption_without_refill_or_policy_mutation():
    from llm_gateway.application.provider_rate import ProviderRate
    harness = Harness()
    now = [0]
    rate = ProviderRate(clock=lambda: now[0])
    assert rate.try_consume(harness.limit) and rate.try_consume(harness.limit)
    before = deepcopy(rate._buckets)
    for at, expected in [(499999999, False), (500000000, True)]:
        now[0] = at
        assert rate.available(harness.limit) is expected
        assert rate._buckets == before
    faster = replace(harness.limit, qps=100, burst=100)
    assert rate.available(faster) and rate._buckets == before
    assert rate.try_consume(faster)
    assert not rate.available(harness.limit)


def test_excluded_static_candidates_never_resolve_credentials():
    harness = Harness()

    async def run():
        initial = TextInitialEligibility(credential_source=harness, security_gates=harness.security,
            circuits=harness.coordinator, capacity=harness.capacity, rate=harness.rate)
        requirement, _ = text_routing_requirements(harness.snapshot, QUERY)
        assert await initial(harness.admission, harness.snapshot, requirement, (),
            deadline=asyncio.get_running_loop().time() + 5) == {}

    asyncio.run(run())
    assert harness.events == []


def test_pending_circuit_evidence_is_not_bypassed_by_initial_check():
    from llm_gateway.domain.invocation import InvocationPersistenceUnavailable
    harness = Harness()
    harness.coordinator._pending["binding-a"] = object()
    with pytest.raises(InvocationPersistenceUnavailable):
        harness.coordinator.rejection("binding-a")
    assert harness.circuits._circuits == {}


@pytest.mark.parametrize("termination", ["cancel", "deadline"])
def test_initial_security_wait_releases_credentials_on_termination(termination):
    async def run():
        harness = Harness()
        entered = asyncio.Event()
        released = []

        @asynccontextmanager
        async def security(*args):
            try:
                entered.set()
                await asyncio.Future()
                yield ()
            finally:
                released.append(True)

        initial = TextInitialEligibility(credential_source=harness, security_gates=security,
            circuits=harness.coordinator, capacity=harness.capacity, rate=harness.rate)
        requirement, assessed = text_routing_requirements(harness.snapshot, QUERY)
        task = asyncio.create_task(initial(harness.admission, harness.snapshot, requirement, assessed,
            deadline=asyncio.get_running_loop().time() + (0.05 if termination == "deadline" else 5)))
        await asyncio.wait_for(entered.wait(), 2)
        if termination == "cancel":
            task.cancel()
        with pytest.raises(asyncio.CancelledError if termination == "cancel" else TimeoutError):
            await task
        assert released == [True]
        harness.assert_released()
        assert not harness.rate._buckets and not harness.circuits._circuits

    asyncio.run(run())


@pytest.mark.parametrize("now", [-1, True, float("nan"), float("inf")])
def test_initial_circuit_clock_validation_is_non_mutating(now):
    circuit = ProviderCircuit()
    with pytest.raises(ValueError):
        circuit.rejection(now)
    assert circuit._last_time is None and not circuit._permits
