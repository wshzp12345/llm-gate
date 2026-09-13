import asyncio
import json
from copy import deepcopy
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from uuid import uuid4

import httpx
import pytest

from llm_gateway.adapters.egress_pool import ProviderTransportRetired
from llm_gateway.adapters.provider_credentials import CredentialMetadata, CredentialUnavailable, ProviderCredentialLease
from llm_gateway.adapters.provider_rate_projection import project_provider_rate
from llm_gateway.adapters.text_routing_plan import prepare_text_routing
from llm_gateway.application.attempt_capacity import AttemptCapacity
from llm_gateway.application.attempt_execution import FailureRecovery, NoEligibleCandidate, SynchronousAttemptExecutor
from llm_gateway.application.circuit_runtime import CircuitCoordinator
from llm_gateway.application.provider_circuits import ProviderCircuits
from llm_gateway.application.provider_rate import ProviderRate
from llm_gateway.domain.invocation import InvocationAdmission, InvocationPersistenceUnavailable
from llm_gateway.domain.routing_eligibility import StaticRoutingReason
from llm_gateway.infrastructure.text_attempt_runtime import TextAttemptRuntimeFactory
from tests.test_completion import envelope
from tests.test_text_fingerprints import AUTH, QUERY, snapshot


class Harness:
    def __init__(self, *, mode=None, statuses=(200,)):
        self.events, self.leases, self.transitions = [], [], []
        self.mode, self.statuses = mode, iter(statuses)
        self.snapshot = snapshot()
        self.admission = InvocationAdmission(uuid4(), "a" * 32, "1", "general", AUTH)
        self.capacity, self.rate = AttemptCapacity(), ProviderRate(clock=lambda: 0)
        self.circuits = ProviderCircuits()
        self.coordinator = CircuitCoordinator(circuits=self.circuits, evidence=self, clock=lambda: 0)
        self.limit = project_provider_rate(self.snapshot, ("binding-a",))["binding-a"]
        self.held = []
        if mode == "capacity":
            self.held = [self.capacity.try_acquire(self.limit.capacity) for _ in range(2)]
        if mode == "qps":
            assert self.rate.try_consume(self.limit) and self.rate.try_consume(self.limit)
        if mode == "circuit":
            for _ in range(5):
                self.circuits.finish("binding-a", self.circuits.acquire("binding-a", 0).permit, False, 0)

    async def append(self, event):
        self.transitions.append(event)

    async def resolve(self, reference):
        assert reference == "test-provider-reference"
        self.events.append("resolve")
        if self.mode == "credential":
            raise CredentialUnavailable()
        lease = ProviderCredentialLease(CredentialMetadata(reference, "v1", "test",
            datetime(2030, 1, 1, tzinfo=timezone.utc)), b"synthetic-test-token")
        self.leases.append(lease)
        return lease

    @asynccontextmanager
    async def security(self, admission, snapshot, binding, metadata):
        assert admission is self.admission and snapshot is self.snapshot and binding == "binding-a"
        assert metadata == self.leases[-1].metadata
        self.events.append("security")
        try:
            if self.mode == "security":
                yield (StaticRoutingReason("trust_bundle_expired"),)
            elif self.mode == "invalid_security":
                yield True
            else:
                yield ()
        finally:
            self.events.append("security_release")

    def acquire(self, plan):
        self.events.append("transport")
        if self.mode == "retired":
            raise ProviderTransportRetired()
        return self.client

    async def reject(self, binding, reasons):
        assert binding == "binding-a"
        self.events.append(reasons[0].code)

    async def started(self, number, binding, candidate_attempt):
        assert self.leases[-1].bearer_value()
        assert self.capacity._providers == {"provider-a": 1}
        assert len(self.coordinator._leases) == 1
        self.events.append("start")
        if self.mode == "start":
            raise InvocationPersistenceUnavailable()

    async def finished(self, number, result, recovery):
        self.events.append("finish")
        if self.mode == "finish":
            raise InvocationPersistenceUnavailable()

    async def handler(self, request):
        self.events.append("io")
        assert request.headers["authorization"] == "Bearer synthetic-test-token"
        if self.mode == "cancel":
            raise asyncio.CancelledError()
        return httpx.Response(next(self.statuses), json=envelope())

    async def sleep(self, seconds):
        assert not self.capacity._leases and not self.coordinator._leases
        assert all(not lease._material for lease in self.leases)
        self.events.append("sleep")

    async def run(self):
        async with httpx.AsyncClient(transport=httpx.MockTransport(self.handler), trust_env=False) as client:
            self.client = client
            factory = TextAttemptRuntimeFactory(credential_source=self, transports=self, circuits=self.coordinator,
                capacity=self.capacity, rate=self.rate, security_gates=self.security)
            resources = factory(self.admission, self.snapshot, journal=self, reject=self.reject)
            plan = prepare_text_routing(self.snapshot, QUERY, seed_hex="a" * 64, initial_rejections={"binding-a": ()})
            executor = SynchronousAttemptExecutor(runtime=resources.runtime, journal=resources.journal,
                classify=lambda result: FailureRecovery(result.retryable, True), draw_jitter=lambda maximum: 0, sleep=self.sleep)
            try:
                return await executor.execute(plan.full, policy=plan.retry, deadline=asyncio.get_running_loop().time() + 5)
            finally:
                for lease in self.held:
                    lease.release()

    def assert_released(self):
        assert not self.capacity._leases and not self.coordinator._leases
        assert all(not lease._material for lease in self.leases)


def test_composed_retry_reacquires_all_gates_and_releases_before_backoff():
    harness = Harness(statuses=(503, 200))
    assert asyncio.run(harness.run()).output.text == "你好"
    cycle = ["resolve", "security", "transport", "start", "io", "finish", "security_release"]
    assert harness.events == cycle + ["sleep"] + cycle
    harness.assert_released()
    assert harness.rate._buckets["provider-a"].credit == 0


@pytest.mark.parametrize("mode,reason", [("credential", "provider_credentials_unavailable"),
    ("security", "trust_bundle_expired"), ("circuit", "circuit_open"),
    ("capacity", "concurrency_exhausted"), ("qps", "qps_exhausted"), ("retired", "security_invalidated")])
def test_rejection_stops_without_attempt_or_io(mode, reason):
    harness = Harness(mode=mode)
    with pytest.raises(NoEligibleCandidate):
        asyncio.run(harness.run())
    assert reason in harness.events
    assert "start" not in harness.events and "io" not in harness.events
    if mode != "retired":
        assert "transport" not in harness.events
    if mode in {"credential", "security", "circuit", "capacity"}:
        assert not harness.rate._buckets
    harness.assert_released()


@pytest.mark.parametrize("mode,error", [("start", InvocationPersistenceUnavailable),
    ("finish", InvocationPersistenceUnavailable), ("cancel", asyncio.CancelledError), ("invalid_security", TypeError)])
def test_failure_cleans_up_without_replay_or_false_circuit_observation(mode, error):
    harness = Harness(mode=mode, statuses=(503,))
    with pytest.raises(error):
        asyncio.run(harness.run())
    assert harness.events.count("io") <= 1 and "sleep" not in harness.events
    assert harness.transitions == []
    harness.assert_released()


def test_new_invocation_runtime_does_not_reset_process_rate_budget():
    harness = Harness(statuses=(200, 200))

    async def run():
        for _ in range(2):
            harness.admission = InvocationAdmission(uuid4(), "a" * 32, "1", "general", AUTH)
            await harness.run()
        harness.admission = InvocationAdmission(uuid4(), "a" * 32, "1", "general", AUTH)
        with pytest.raises(NoEligibleCandidate):
            await harness.run()

    asyncio.run(run())
    assert harness.events.count("io") == 2
    assert harness.events.count("qps_exhausted") == 1
    harness.assert_released()


@pytest.mark.parametrize("outcome_commits", [False, True])
def test_only_committed_fifth_failure_opens_shared_circuit(outcome_commits):
    harness = Harness(mode=None if outcome_commits else "finish", statuses=(503,))
    for _ in range(4):
        harness.circuits.finish("binding-a", harness.circuits.acquire("binding-a", 0).permit, False, 0)
    if outcome_commits:
        result = asyncio.run(harness.run())
        assert result.retryable
        assert "circuit_open" in harness.events
        assert len(harness.transitions) == 1
        assert harness.transitions[0].transition.new_state == "open"
        assert harness.transitions[0].attempt_number == 1
    else:
        with pytest.raises(InvocationPersistenceUnavailable):
            asyncio.run(harness.run())
        check = harness.circuits.acquire("binding-a", 0)
        assert check.permit is not None
        harness.circuits.finish("binding-a", check.permit, None, 0)
        assert harness.transitions == []
    assert harness.events.count("io") == 1
    harness.assert_released()


def test_runtime_factory_plugs_into_settled_text_execution(monkeypatch):
    from tests.test_full_service_text_execution import setup, invoke
    from llm_gateway.domain.model import Usage

    async def run():
        backend, admission, selected, checkpoints = setup(monkeypatch)
        harness = Harness()
        harness.admission, harness.snapshot = admission, selected
        async with httpx.AsyncClient(transport=httpx.MockTransport(harness.handler), trust_env=False) as client:
            harness.client = client
            backend._runtime_factory = TextAttemptRuntimeFactory(credential_source=harness, transports=harness,
                circuits=harness.coordinator, capacity=harness.capacity, rate=harness.rate, security_gates=harness.security)
            result = await invoke(backend, admission, selected)
        assert result.output.text == "你好" and result.usage == Usage(7, 9)
        assert checkpoints == ["initial", "preselect", "allowed", "started", "finished", "settle"]
        assert harness.events == ["resolve", "security", "transport", "io", "security_release"]
        harness.assert_released()

    asyncio.run(run())


def test_disabled_provider_candidate_does_not_break_runtime_for_eligible_candidate():
    harness = Harness()
    content = json.loads(harness.snapshot.snapshot_json)
    content["providers"]["provider-disabled"] = deepcopy(content["providers"]["provider-a"])
    content["providers"]["provider-disabled"]["status"] = "disabled"
    content["provider_model_bindings"]["binding-disabled"] = dict(
        content["provider_model_bindings"]["binding-a"], provider="provider-disabled")
    content["model_aliases"]["general"]["candidates"].append(
        {"binding": "binding-disabled", "service_level": "full", "priority": 0, "weight": 1})
    harness.snapshot = snapshot(content)
    assert asyncio.run(harness.run()).output.text == "你好"
    assert harness.events.count("io") == 1
    harness.assert_released()
