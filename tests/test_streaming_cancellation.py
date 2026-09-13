import asyncio
from contextlib import asynccontextmanager

import httpx
import psycopg
import pytest

from llm_gateway.adapters.openai_compatible import OpenAICompatibleCompletion
from llm_gateway.application.admitted_execution import AdmittedExecutionLifecycle
from llm_gateway.application.attempt_control import AttemptExecutionControl
from llm_gateway.application.streaming_attempt_execution import SharedStreamAttemptJournal, StreamingAttemptExecutor
from llm_gateway.application.text_failure_recovery import classify_text_failure
from llm_gateway.domain.recovery import RetryPolicy
from llm_gateway.infrastructure.late_provider_outcome import PostgresLateProviderOutcome
from llm_gateway.infrastructure.local_cancellation import PostgresLocalCancellationStore
from llm_gateway.infrastructure.routing_evidence import PostgresRoutingEvidence
from llm_gateway.infrastructure.stream_commit import PostgresStreamCommit
from llm_gateway.infrastructure.cancellation_settlement import settle_deadline_exceeded
from tests.test_attempt_execution import CANDIDATES
from tests.test_configuration import database, run
from tests.test_invocation_postgres import setup
from tests.test_provider_streaming import chunk, wire
from tests.test_streaming_attempt_execution import Harness


@pytest.mark.postgres
@pytest.mark.parametrize("stage", ["delta", "usage", "checkpoint"])
@pytest.mark.parametrize("exit_kind", ["cancel", "deadline"])
def test_interruption_owns_stream_exit_and_preserves_observed_metadata(database, stage, exit_kind):
    async def scenario():
        store, record = await setup(database)
        reached, exited = asyncio.Event(), asyncio.Event()
        calls = []
        usage = {"prompt_tokens": 10, "completion_tokens": 6, "total_tokens": 16}

        class Body(httpx.AsyncByteStream):
            async def __aiter__(self):
                yield wire(chunk({"content": "private-content"}))
                if stage != "delta":
                    yield wire(chunk(finish="stop", usage=usage))
                if stage == "checkpoint":
                    yield wire("[DONE]")
                else:
                    reached.set()
                    await asyncio.Event().wait()

            async def aclose(self):
                exited.set()

        async def handler(request):
            calls.append(request)
            return httpx.Response(200, stream=Body(), headers={"content-type": "text/event-stream"})

        control = AttemptExecutionControl(record.call_id,
            late_outcomes=PostgresLateProviderOutcome(store, record.call_id))
        output = Harness([])
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False) as client:
            adapter = OpenAICompatibleCompletion(client, base_url="https://provider.invalid/v1", credential="synthetic")

            class Runtime:
                @asynccontextmanager
                async def acquire(self, binding):
                    await PostgresRoutingEvidence(store).gate(record.call_id, binding)
                    yield adapter

            class Journal(SharedStreamAttemptJournal):
                async def finished(self, number, terminal, recovery):
                    if stage == "checkpoint":
                        reached.set()
                        await asyncio.Event().wait()
                    await super().finished(number, terminal, recovery)

            executor = StreamingAttemptExecutor(runtime=Runtime(), journal=Journal(store.journal(record.call_id)),
                output=output, commit=PostgresStreamCommit(store, record.call_id), control=control,
                classify=classify_text_failure, draw_jitter=lambda _: 0)
            async def deadline_settlement(*args, **kwargs):
                assert exit_kind == "deadline"
                await settle_deadline_exceeded(store, *args, **kwargs, observation=control.stopped_observation)
            lifecycle = AdmittedExecutionLifecycle(cancellation_store=PostgresLocalCancellationStore(store),
                settle_deadline=deadline_settlement)
            deadline = asyncio.get_running_loop().time() + (1.5 if exit_kind == "deadline" else 10)
            task = asyncio.create_task(lifecycle.run(record, control=control,
                work=lambda: executor.execute(CANDIDATES, policy=RetryPolicy(), deadline=deadline), deadline=deadline))
            await asyncio.wait_for(reached.wait(), 5)
            if exit_kind == "cancel":
                task.cancel()
            with pytest.raises(asyncio.CancelledError if exit_kind == "cancel" else TimeoutError):
                await task
            assert exited.is_set() and control.stopped and not adapter._active
            assert len(calls) == 1 and len(output.sent) == 1 and not client.is_closed
        from llm_gateway.infrastructure.invocation_observation import PostgresInvocationObservation
        observation = await PostgresInvocationObservation(database).read(record.call_id)
        assert observation.actual_model == "actual-model" and not observation.model_conflict
        attempt, = observation.attempts
        if exit_kind == "cancel":
            assert attempt.late_usage is not None
            assert attempt.late_usage.input_tokens == (None if stage == "delta" else 10)
            assert attempt.late_usage.output_tokens == (None if stage == "delta" else 6)
        else:
            assert attempt.late_usage is None
        assert observation.terminal_outcome != "completed"
        assert "private-content" not in repr(observation)
        with psycopg.connect(database) as connection:
            assert connection.execute("SELECT state FROM model_invocation").fetchone()[0] in ({"cancelled", "uncertain"} if exit_kind == "cancel" else {"failed"})
            rows = connection.execute("SELECT outcome,resolved_model,input_tokens,output_tokens FROM late_provider_outcome").fetchall()
            if exit_kind == "cancel":
                assert rows == [("succeeded" if stage == "checkpoint" else "uncertain", "actual-model",
                                 None if stage == "delta" else 10, None if stage == "delta" else 6)]
            else:
                assert rows == []
                assert connection.execute("SELECT outcome,resolved_model,input_tokens,output_tokens FROM provider_attempt_outcome").fetchone() == (
                    "uncertain", "actual-model", None if stage == "delta" else 10, None if stage == "delta" else 6)
                assert connection.execute("SELECT error_code,resolved_model,input_tokens,output_tokens FROM invocation_settlement").fetchone() == (
                    "deadline_exceeded", "actual-model", None if stage == "delta" else 10, None if stage == "delta" else 6)
                assert connection.execute("SELECT count(*) FROM invocation_local_cancellation").fetchone() == (0,)
                assert connection.execute("SELECT count(*) FROM cost_accrual").fetchone() == (1,)
            assert connection.execute("SELECT count(*) FROM invocation_stream_commit").fetchone() == (1,)
            assert connection.execute("SELECT count(*) FROM provider_attempt").fetchone() == (1,)
            assert connection.execute("SELECT count(*) FROM invocation_settlement WHERE outcome='completed'").fetchone() == (0,)
            assert "private-content" not in str(connection.execute("SELECT to_jsonb(o) FROM late_provider_outcome o").fetchall())
    run(scenario())
