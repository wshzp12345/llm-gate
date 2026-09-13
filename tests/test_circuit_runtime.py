import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from llm_gateway.application.circuit_runtime import CircuitCoordinator, CircuitInvocationStage
from llm_gateway.application.provider_circuits import ProviderCircuits
from llm_gateway.domain.invocation import InvocationPersistenceUnavailable
from llm_gateway.domain.recovery import RecoveryPlan, RetryPolicy
from tests.test_attempt_execution import Harness, CANDIDATES, FAILURE, SUCCESS


class Sink:
    def __init__(self):
        self.events = []
        self.fail = False
        self.entered = asyncio.Event()
        self.hold = None

    async def append(self, event):
        self.events.append(event)
        self.entered.set()
        if self.hold is not None:
            await self.hold.wait()
        if self.fail:
            raise InvocationPersistenceUnavailable()


def controller(sink, circuits=None, clock=lambda: 0):
    return CircuitCoordinator(circuits=circuits or ProviderCircuits(), evidence=sink, clock=clock,
        utcnow=lambda: datetime(2026, 9, 11, tzinfo=timezone.utc))


def seeded():
    circuits = ProviderCircuits()
    for _ in range(5):
        circuits.finish("a", circuits.acquire("a", 0).permit, False, 0)
    return circuits


def test_failed_transition_ack_blocks_work_and_later_delivery_reuses_event_id():
    async def run():
        sink = Sink()
        coordinator = controller(sink)
        call = uuid4()
        for _ in range(4):
            lease, _ = await coordinator.acquire("a", "1", call)
            await coordinator.finish(lease, False, 1)
        sink.fail = True
        lease, _ = await coordinator.acquire("a", "1", call)
        with pytest.raises(InvocationPersistenceUnavailable):
            await coordinator.finish(lease, False, 1)
        first = sink.events[-1]
        with pytest.raises(InvocationPersistenceUnavailable):
            await coordinator.acquire("a", "1", call)
        assert sink.events[-1] is first
        sink.fail = False
        lease, rejection = await coordinator.acquire("a", "1", call)
        assert lease is None and rejection == "circuit_open"
        assert sink.events[-1] is first and not coordinator._pending
        assert not coordinator._leases
    asyncio.run(run())


def test_half_open_transition_commits_before_trial_is_returned():
    async def run():
        sink = Sink()
        sink.hold = asyncio.Event()
        coordinator = controller(sink, seeded(), lambda: 30)
        task = asyncio.create_task(coordinator.acquire("a", "1", uuid4()))
        await sink.entered.wait()
        assert not task.done()
        sink.hold.set()
        lease, rejection = await task
        assert rejection is None and sink.events[0].transition.new_state == "half_open"
        await coordinator.finish(lease, None)
    asyncio.run(run())


@pytest.mark.parametrize("cancel", [False, True])
def test_failed_or_cancelled_gate_ack_releases_trial_and_preserves_pending_event(cancel):
    async def run():
        sink = Sink()
        coordinator = controller(sink, seeded(), lambda: 30)
        if cancel:
            sink.hold = asyncio.Event()
            task = asyncio.create_task(coordinator.acquire("a", "1", uuid4()))
            await sink.entered.wait()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            sink.hold = None
        else:
            sink.fail = True
            with pytest.raises(InvocationPersistenceUnavailable):
                await coordinator.acquire("a", "1", uuid4())
            sink.fail = False
        assert not coordinator._leases and coordinator._pending
        lease, reason = await coordinator.acquire("a", "1", uuid4())
        assert lease is not None and reason is None
        await coordinator.finish(lease, None)
    asyncio.run(run())


@pytest.mark.parametrize("fail_at", [None, "start", "finish"])
def test_stage_observation_is_after_durable_outcome_and_checkpoint_failure_is_excluded(fail_at):
    async def run():
        sink = Sink()
        coordinator = controller(sink)
        # Four historical eligible failures; the next committed one opens.
        for _ in range(4):
            lease, _ = await coordinator.acquire("a", "1", uuid4())
            await coordinator.finish(lease, False, 1)
        rejected = []
        async def reject(binding, reasons):
            rejected.append(reasons)
        stage = CircuitInvocationStage(coordinator=coordinator, revision="1", call_id=uuid4(), reject=reject)
        harness = Harness([FAILURE], fail_at=fail_at)
        original = harness.acquire
        @asynccontextmanager
        async def acquire(binding):
            async with stage.acquire(binding) as allowed:
                if not allowed:
                    yield None
                    return
                async with original(binding) as provider:
                    yield provider
        harness.acquire = acquire
        executor = harness.executor()
        executor._journal = stage.journal(harness)
        async def execute():
            return await executor.execute(CANDIDATES[:1], policy=RetryPolicy(max_attempts=1, max_attempts_per_candidate=1),
                deadline=asyncio.get_running_loop().time() + 5)
        if fail_at:
            with pytest.raises(RuntimeError):
                await execute()
            assert not sink.events
        else:
            assert await execute() == FAILURE
            assert sink.events[0].attempt_number == 1
            assert sink.events[0].transition.new_state == "open"
        assert not coordinator._leases and not stage._active and not harness.leased
    asyncio.run(run())


def test_stage_rejects_open_candidate_without_start_and_can_close_after_two_trials():
    async def run():
        sink = Sink()
        now = [0]
        coordinator = controller(sink, seeded(), lambda: now[0])
        rejections = []
        async def reject(binding, reasons):
            rejections.extend(reasons)
        stage = CircuitInvocationStage(coordinator=coordinator, revision="1", call_id=uuid4(), reject=reject)
        async with stage.acquire("a") as allowed:
            assert not allowed
        assert rejections[0].code == "circuit_open"
        class Journal:
            async def started(self, *args): pass
            async def finished(self, *args): pass
        now[0] = 30
        journal = stage.journal(Journal())
        for number in (1, 2):
            async with stage.acquire("a") as allowed:
                assert allowed
                await journal.started(number, "a", number)
                await journal.finished(number, SUCCESS, RecoveryPlan("stop"))
        assert [event.transition.new_state for event in sink.events] == ["half_open", "closed"]
        assert not coordinator._leases
    asyncio.run(run())
