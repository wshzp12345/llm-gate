import asyncio
from contextlib import asynccontextmanager
from uuid import uuid4

import pytest

from llm_gateway.application.attempt_control import AttemptExecutionControl
from llm_gateway.application.attempt_execution import FailureRecovery, SynchronousAttemptExecutor
from llm_gateway.application.cancellation import LocalCancellationReason as Reason, ProviderCancelResult
from llm_gateway.domain.recovery import RetryPolicy
from tests.test_attempt_execution import Harness, CANDIDATES, FAILURE, SUCCESS


class ControlledHarness(Harness):
    supports_remote_cancellation = False

    def __init__(self, results):
        super().__init__(results)
        self.control = AttemptExecutionControl(uuid4())
        self.contexts = []

    async def complete(self, request, *, context):
        assert self.events[-1][0] == "start"
        assert context.cancellation is self.control.token
        assert context.handle.number == len(self.contexts) + 1
        self.contexts.append(context)
        # Match the Adapter's nested subscription around the Provider call.
        with context.cancellation.abort_on_cancel(asyncio.current_task().cancel):
            return await super().complete(request)

    async def run(self):
        self.deadline = asyncio.get_running_loop().time() + 5
        return await SynchronousAttemptExecutor(runtime=self, journal=self, control=self.control,
            classify=lambda failure: FailureRecovery(True, True), draw_jitter=lambda n: 0,
            sleep=self.sleep).execute(CANDIDATES, policy=RetryPolicy(), deadline=self.deadline)


def test_context_is_created_only_after_committed_start_and_keeps_shared_deadline():
    async def scenario():
        harness = ControlledHarness([FAILURE, SUCCESS])
        assert await harness.run() == SUCCESS
        assert len(harness.contexts) == 2
        assert all(ctx.deadline == harness.deadline for ctx in harness.contexts)
        assert len({ctx.handle.reference for ctx in harness.contexts}) == 2
        assert harness.control.stopped and not harness.leased
        assert harness.control.stop(Reason.CONTEXT_CANCELLED) is None
        await asyncio.sleep(0)  # Completion removed all task-abort subscriptions.
        with pytest.raises(RuntimeError):
            await harness.run()
    asyncio.run(scenario())


@pytest.mark.parametrize("checkpointed", [False, True])
def test_stopped_snapshot_requires_local_exit_and_never_retains_output(checkpointed):
    async def scenario():
        control = AttemptExecutionControl(uuid4())
        with control.scope():
            with control.attempt(1, object(), asyncio.get_running_loop().time() + 1) as context:
                control.observed(context, SUCCESS)
                with pytest.raises(RuntimeError, match="stopped execution"):
                    _ = control.stopped_observation
                if checkpointed:
                    control.checkpointed(1)
        observation = control.stopped_observation
        if checkpointed:
            assert observation is None
        else:
            assert observation.resolved_model == SUCCESS.resolved_model
            assert observation.usage == SUCCESS.usage and not hasattr(observation, "output")
    asyncio.run(scenario())


def test_failed_start_never_registers_a_provider_context():
    async def scenario():
        harness = ControlledHarness([SUCCESS])
        harness.fail_at = "start"
        with pytest.raises(RuntimeError):
            await harness.run()
        assert not harness.contexts and harness.control.stopped and not harness.leased
    asyncio.run(scenario())


def test_cancellation_before_start_prevents_gate_or_attempt():
    async def scenario():
        harness = ControlledHarness([SUCCESS])
        harness.control.stop(Reason.CONTEXT_CANCELLED)
        task = asyncio.create_task(harness.run())
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not harness.events and harness.control.stopped
    asyncio.run(scenario())


def test_cancel_in_backoff_survives_nested_provider_subscription_exit():
    async def scenario():
        harness, entered = ControlledHarness([FAILURE, SUCCESS]), asyncio.Event()
        async def backoff(seconds):
            entered.set()
            await asyncio.Event().wait()
        harness.sleep = backoff
        task = asyncio.create_task(harness.run())
        try:
            await asyncio.wait_for(entered.wait(), 1)
            assert harness.control.stop(Reason.CONTEXT_CANCELLED) is None
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, 1)
            assert len(harness.contexts) == 1 and harness.control.stopped
            assert not harness.leased
        finally:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    asyncio.run(scenario())


def test_control_is_not_stopped_until_runtime_lease_exit_finishes():
    async def scenario():
        harness = ControlledHarness([SUCCESS])
        in_provider, in_exit, allow_exit = asyncio.Event(), asyncio.Event(), asyncio.Event()
        @asynccontextmanager
        async def acquire(binding):
            harness.leased = True
            try:
                yield harness
            finally:
                in_exit.set()
                await allow_exit.wait()
                harness.leased = False
        async def complete(request, *, context):
            in_provider.set()
            await asyncio.Event().wait()
        harness.acquire, harness.complete = acquire, complete
        task = asyncio.create_task(harness.run())
        try:
            await asyncio.wait_for(in_provider.wait(), 1)
            handle = harness.control.stop(Reason.CLIENT_DISCONNECTED)
            assert handle.number == 1
            await asyncio.wait_for(in_exit.wait(), 1)
            assert not harness.control.stopped and harness.leased
            # A repeated stop does not interrupt the first lease exit.
            assert harness.control.stop(Reason.CONTEXT_CANCELLED) == handle
            allow_exit.set()
            with pytest.raises(asyncio.CancelledError):
                await task
            await harness.control.wait_stopped()
            assert harness.control.stopped and not harness.leased
        finally:
            allow_exit.set()
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    asyncio.run(scenario())


def test_late_provider_success_cannot_escape_a_cancelled_execution():
    async def scenario():
        harness, entered = ControlledHarness([SUCCESS]), asyncio.Event()
        async def complete(request, *, context):
            entered.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                return SUCCESS
        harness.complete = complete
        task = asyncio.create_task(harness.run())
        try:
            await asyncio.wait_for(entered.wait(), 1)
            harness.control.stop(Reason.CONTEXT_CANCELLED)
            with pytest.raises(asyncio.CancelledError):
                await task
            assert not any(event[0] == "finish" for event in harness.events)
            assert harness.control.stopped and not harness.leased
        finally:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    asyncio.run(scenario())


def test_remote_cancel_capability_survives_active_provider_reference_release():
    async def scenario():
        control, entered = AttemptExecutionControl(uuid4()), asyncio.Event()
        class Provider:
            supports_remote_cancellation = True
            async def cancel(self, handle, reason, deadline):
                return ProviderCancelResult.ACKNOWLEDGED
        async def execute():
            with control.scope(), control.attempt(1, Provider(), asyncio.get_running_loop().time() + 5):
                entered.set()
                await asyncio.Event().wait()
        task = asyncio.create_task(execute())
        try:
            await asyncio.wait_for(entered.wait(), 1)
            handle = control.stop(Reason.CONTEXT_CANCELLED)
            with pytest.raises(asyncio.CancelledError):
                await task
            assert await control.cancel(handle, Reason.CONTEXT_CANCELLED,
                asyncio.get_running_loop().time() + 1) == ProviderCancelResult.ACKNOWLEDGED
            assert control.supports_remote_cancellation is True
            assert control._cancel_target is None
        finally:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    asyncio.run(scenario())


@pytest.mark.parametrize("where", ["provider", "checkpoint", "backoff"])
def test_only_uncheckpointed_observations_enter_late_sink(where):
    async def scenario():
        harness, entered, observations = ControlledHarness([FAILURE if where == "backoff" else SUCCESS]), asyncio.Event(), []
        class Sink:
            async def record(self, observation):
                assert not harness.leased
                observations.append(observation)
        harness.control = AttemptExecutionControl(uuid4(), late_outcomes=Sink())
        if where == "provider":
            async def complete(request, *, context):
                entered.set()
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    return SUCCESS
            harness.complete = complete
        elif where == "checkpoint":
            async def finished(*args):
                entered.set()
                await asyncio.Event().wait()
            harness.finished = finished
        else:
            async def sleep(seconds):
                entered.set()
                await asyncio.Event().wait()
            harness.sleep = sleep
        task = asyncio.create_task(harness.run())
        try:
            await asyncio.wait_for(entered.wait(), 1)
            harness.control.stop(Reason.CONTEXT_CANCELLED)
            with pytest.raises(asyncio.CancelledError):
                await task
            assert len(observations) == (0 if where == "backoff" else 1)
            if observations:
                assert observations[0].attempt_number == 1
                assert observations[0].outcome == "succeeded"
                assert not hasattr(observations[0], "output")
            assert harness.control.stopped
        finally:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    asyncio.run(scenario())
