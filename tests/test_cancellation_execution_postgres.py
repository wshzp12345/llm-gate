import asyncio
import json

import httpx
import pytest

from llm_gateway.application.attempt_control import AttemptExecutionControl
from llm_gateway.application.attempt_execution import ExecutionCandidate, SynchronousAttemptExecutor
from llm_gateway.application.cancellation import CancellationReservation, LocalCancellationReason
from llm_gateway.application.cancellation_execution import LocalCancellationCoordinator
from llm_gateway.application.text_failure_recovery import classify_text_failure
from llm_gateway.domain.recovery import RetryPolicy
from llm_gateway.infrastructure.full_service_text_execution import _CheckpointedRuntime
from llm_gateway.infrastructure.local_cancellation import PostgresLocalCancellationStore
from llm_gateway.infrastructure.late_provider_outcome import PostgresLateProviderOutcome
from llm_gateway.infrastructure.routing_evidence import PostgresRoutingEvidence
from tests.test_completion import REQUEST, envelope
from tests.test_configuration import database, run, scalar
from tests.test_invocation_postgres import setup
from tests.test_credentialed_runtime import Harness


@pytest.mark.postgres
@pytest.mark.parametrize("late", [False, True])
def test_coordinator_closes_real_adapter_stream_then_commits_cancelled(database, late):
    async def scenario():
        store, record = await setup(database)
        entered, closed = asyncio.Event(), asyncio.Event()

        class Body(httpx.AsyncByteStream):
            async def __aiter__(self):
                entered.set()
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    if not late:
                        raise
                    yield json.dumps(envelope(usage={"prompt_tokens": 10, "completion_tokens": 6})).encode()

            async def aclose(self):
                closed.set()

        calls = []
        def handler(request):
            calls.append(request)
            return httpx.Response(200, headers={"Content-Type": "application/json"}, stream=Body())

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False) as client:
            harness = Harness()
            control = AttemptExecutionControl(record.call_id, late_outcomes=PostgresLateProviderOutcome(store, record.call_id))
            executor = SynchronousAttemptExecutor(
                runtime=_CheckpointedRuntime(harness.runtime(client), PostgresRoutingEvidence(store), record.call_id),
                journal=store.journal(record.call_id), classify=classify_text_failure,
                draw_jitter=lambda n: 0, control=control)
            task = asyncio.create_task(executor.execute((ExecutionCandidate("a", REQUEST),),
                policy=RetryPolicy(), deadline=asyncio.get_running_loop().time() + 3))
            dispatches = []
            original_cancel = control.cancel
            async def cancel(*args):
                dispatches.append(args[0])
                return await original_cancel(*args)
            control.cancel = cancel
            try:
                await asyncio.wait_for(entered.wait(), timeout=1)
                coordinator = LocalCancellationCoordinator(PostgresLocalCancellationStore(store))
                args = dict(execution=control, provider=control,
                            deadline=asyncio.get_running_loop().time() + 1, downstream_writable=False)
                def observed_abort():
                    # Independent connection proves reservation committed
                    # before the Token's local abort callbacks are dispatched.
                    assert scalar(database, "SELECT state FROM model_invocation") == "cancelling"
                with control.token.abort_on_cancel(observed_abort):
                    assert await coordinator.cancel(record, LocalCancellationReason.CLIENT_DISCONNECTED, **args) == CancellationReservation.CANCELLED
                assert control.stopped and task.cancelled() and closed.is_set()
                assert all(not lease._material for lease in harness.leases)
                assert await coordinator.cancel(record, LocalCancellationReason.CLIENT_DISCONNECTED, **args) == CancellationReservation.CANCELLED
                assert len(dispatches) == 1 and dispatches[0].call_id == record.call_id
                assert len(calls) == 1
            finally:
                if not task.done():
                    task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        assert scalar(database, "SELECT state FROM model_invocation") == "cancelled"
        assert scalar(database, "SELECT outcome FROM provider_attempt_outcome") == "uncertain"
        assert scalar(database, "SELECT billing_outcome FROM attempt_cancellation_billing") == "uncertain"
        assert scalar(database, "SELECT provider_result FROM invocation_cancellation_cleanup") == "not_supported"
        assert scalar(database, "SELECT count(*) FROM late_provider_outcome") == int(late)
        if late:
            assert scalar(database, "SELECT input_tokens FROM late_provider_outcome") == 10
            assert scalar(database, "SELECT outcome FROM late_provider_outcome") == "succeeded"
            assert scalar(database, "SELECT total_cost FROM cost_accrual") is None
    run(scenario())
