import asyncio
import json
from contextlib import asynccontextmanager
from dataclasses import replace
from uuid import uuid4

import pytest

from llm_gateway.application.attempt_execution import FailureRecovery
from llm_gateway.application.cancellation import CancellationReservation, ProviderCancelResult
from llm_gateway.domain.invocation import InvocationAdmission, InvocationPersistenceUnavailable
from llm_gateway.domain.model import FailureCode, ProviderFailure, ProviderResult, TextOutput, Usage
from llm_gateway.domain.routing_eligibility import StaticRoutingReason
from llm_gateway.domain.routing_failure import classify_unattempted_rejections
from llm_gateway.infrastructure import full_service_text_execution as module
from tests.test_text_fingerprints import AUTH, QUERY, snapshot


def setup(monkeypatch, *, failure=None, rejection=False, initial=None, pause=None, provider_failures=0):
    events = []
    selected = snapshot()
    record = InvocationAdmission(uuid4(), "a" * 32, selected.revision, QUERY.requested_model, AUTH)
    success = ProviderResult("general", "upstream", TextOutput("answer"), Usage(2, 3), "stop")

    def checkpoint(name):
        events.append(name)
        if name == failure:
            raise InvocationPersistenceUnavailable()

    async def wait_at(name):
        if pause is not None and pause[0] == name:
            pause[1].set()
            await asyncio.Future()

    class Evidence:
        def __init__(self, store):
            pass

        async def preselect(self, call_id, plan):
            assert call_id == record.call_id
            checkpoint("preselect")
            await wait_at("preselect")

        async def gate(self, call_id, binding, reasons):
            assert call_id == record.call_id and binding == "binding-a"
            checkpoint("rejected" if reasons else "allowed")
            await wait_at("allowed")

    class Settlement:
        def __init__(self, store, call_id):
            assert call_id == record.call_id

        async def settle(self, result):
            checkpoint("settle")
            await wait_at("settle")
            return Usage(7, 9)

        async def settle_exhausted(self):
            checkpoint("exhausted")
            return classify_unattempted_rejections(static=(), runtime=(frozenset({"qps_exhausted"}),))

    class Store:
        def journal(self, call_id):
            assert call_id == record.call_id
            return self

        async def started(self, number, binding, candidate_attempt):
            assert number == candidate_attempt and binding == "binding-a"
            checkpoint("started")

        async def finished(self, number, result, recovery):
            assert number == events.count("started")
            checkpoint("finished")

    class Provider:
        supports_remote_cancellation = False

        async def complete(self, request, *, context=None):
            assert context is not None and context.handle.call_id == record.call_id
            assert request.messages == QUERY.messages
            checkpoint("provider")
            await wait_at("provider")
            if events.count("provider") <= provider_failures:
                return ProviderFailure(FailureCode.PROVIDER_UNAVAILABLE, True)
            return success

        async def cancel(self, handle, reason, deadline):
            return ProviderCancelResult.NOT_SUPPORTED

    class Cancellation:
        def __init__(self, store):
            pass

        async def reserve(self, admission, reason):
            checkpoint("cancel_reserved")
            return CancellationReservation.RESERVED

        async def finalize(self, admission, cleanup):
            checkpoint("cancelled")

    async def deadline_settlement(store, admission, *, deadline, observation=None):
        assert observation is None or not hasattr(observation, "output")
        checkpoint("deadline_settled")

    async def initial_gates(admission, config, requirement, assessed, *, deadline):
        assert admission is record and config is selected
        checkpoint("initial")
        await wait_at("initial")
        if initial is not None:
            return initial
        return {"binding-a": ()}

    def runtime_factory(admission, config, *, journal, reject):
        assert admission is record and config is selected
        checkpoint("runtime")

        class Runtime:
            @asynccontextmanager
            async def acquire(self, binding):
                checkpoint("acquire")
                try:
                    if rejection:
                        await reject(binding, (StaticRoutingReason("qps_exhausted"),))
                        yield None
                    else:
                        yield Provider()
                finally:
                    events.append("release")

        return module.TextAttemptResources(Runtime(), journal)

    monkeypatch.setattr(module, "PostgresRoutingEvidence", Evidence)
    monkeypatch.setattr(module, "PostgresInvocationSettlement", Settlement)
    monkeypatch.setattr(module, "PostgresLocalCancellationStore", Cancellation)
    monkeypatch.setattr(module, "settle_deadline_exceeded", deadline_settlement)
    backend = module.PostgresFullServiceTextExecution(store=Store(), initial_gates=initial_gates,
        runtime_factory=runtime_factory, classify=lambda result: FailureRecovery(result.retryable, False), draw_jitter=lambda maximum: 0)
    return backend, record, selected, events


def invoke(backend, record, selected):
    return backend.execute(record, QUERY, selected, routing_seed="a" * 64,
                           deadline=asyncio.get_running_loop().time() + 10)


def test_durable_order_leases_and_aggregate_result(monkeypatch):
    backend, record, selected, events = setup(monkeypatch)

    async def run():
        result = await invoke(backend, record, selected)
        assert result.usage == Usage(7, 9)
        assert result.output.text == "answer"

    asyncio.run(run())
    assert events == ["initial", "preselect", "runtime", "acquire", "allowed", "started", "provider",
                      "finished", "release", "settle"]


@pytest.mark.parametrize("failure", ["initial", "preselect", "allowed", "started", "finished", "settle"])
def test_checkpoint_failure_never_replays_or_returns_success(monkeypatch, failure):
    backend, record, selected, events = setup(monkeypatch, failure=failure)

    async def run():
        with pytest.raises(InvocationPersistenceUnavailable):
            await invoke(backend, record, selected)

    asyncio.run(run())
    assert events.count("provider") <= 1
    if failure in {"initial", "preselect", "allowed", "started"}:
        assert "provider" not in events
    if "acquire" in events:
        assert "release" in events
    if failure != "settle":
        assert "settle" not in events


def test_dynamic_rejection_has_no_allowed_checkpoint_or_attempt(monkeypatch):
    backend, record, selected, events = setup(monkeypatch, rejection=True)

    async def run():
        result = await invoke(backend, record, selected)
        assert result.code == "rate_limited"

    asyncio.run(run())
    assert events == ["initial", "preselect", "runtime", "acquire", "rejected", "release", "exhausted"]


def test_missing_initial_check_stops_before_preselection(monkeypatch):
    backend, record, selected, events = setup(monkeypatch, initial={})

    async def run():
        with pytest.raises(ValueError, match="Complete initial"):
            await invoke(backend, record, selected)

    asyncio.run(run())
    assert events == ["initial"]


@pytest.mark.parametrize("feature", ["cache", "reduced_service"])
def test_unsupported_policy_rejected_before_any_side_effect(monkeypatch, feature):
    backend, record, selected, events = setup(monkeypatch)
    content = json.loads(selected.snapshot_json)
    content["routing_policies"]["route-a"]["degradation"][feature]["enabled"] = True
    selected = snapshot(content)

    async def run():
        with pytest.raises(ValueError, match="does not support"):
            await invoke(backend, record, selected)

    asyncio.run(run())
    assert events == []


def test_admission_identity_mismatch_stops_before_gates(monkeypatch):
    backend, record, selected, events = setup(monkeypatch)

    async def run():
        for changed in (replace(record, configuration_revision="2"), replace(record, requested_model="other")):
            with pytest.raises(ValueError, match="admitted Snapshot"):
                await invoke(backend, changed, selected)

    asyncio.run(run())
    assert events == []


def test_retry_rechecks_gates_and_settles_once(monkeypatch):
    backend, record, selected, events = setup(monkeypatch, provider_failures=1)

    async def run():
        result = await invoke(backend, record, selected)
        assert isinstance(result, ProviderResult)

    asyncio.run(run())
    assert events == ["initial", "preselect", "runtime"] + [
        "acquire", "allowed", "started", "provider", "finished", "release"] * 2 + ["settle"]


@pytest.mark.parametrize("stage", ["initial", "preselect", "allowed", "provider", "settle"])
@pytest.mark.parametrize("termination", ["cancel", "deadline"])
def test_cancellation_and_absolute_deadline_span_all_phases(monkeypatch, stage, termination):
    async def run():
        entered = asyncio.Event()
        backend, record, selected, events = setup(monkeypatch, pause=(stage, entered))
        deadline = asyncio.get_running_loop().time() + (0.05 if termination == "deadline" else 10)
        task = asyncio.create_task(backend.execute(record, QUERY, selected, routing_seed="a" * 64, deadline=deadline))
        await asyncio.wait_for(entered.wait(), 2)
        if termination == "cancel":
            task.cancel()
        with pytest.raises(asyncio.CancelledError if termination == "cancel" else TimeoutError):
            await task
        if "acquire" in events:
            assert "release" in events
        assert events.count("provider") <= 1
        assert events.count("preselect") <= 1
        assert "exhausted" not in events
        if stage != "settle":
            assert "settle" not in events
        assert events.count("cancelled" if termination == "cancel" else "deadline_settled") == 1
        assert ("cancel_reserved" in events) == (termination == "cancel")

    asyncio.run(run())


@pytest.mark.parametrize("deadline", [0, float("nan"), float("inf"), True])
def test_bad_deadline_has_no_side_effect(monkeypatch, deadline):
    backend, record, selected, events = setup(monkeypatch)

    async def run():
        with pytest.raises((ValueError, TimeoutError)):
            await backend.execute(record, QUERY, selected, routing_seed="a" * 64, deadline=deadline)

    asyncio.run(run())
    assert events == (["deadline_settled"] if deadline == 0 and type(deadline) is int else [])
