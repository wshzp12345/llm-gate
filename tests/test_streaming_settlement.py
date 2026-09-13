"""Streaming executor -> shared durable journal -> failed terminal settlement."""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
from decimal import Decimal

import psycopg
import pytest

from llm_gateway.application.streaming_attempt_execution import SharedStreamAttemptJournal, StreamingAttemptExecutor
from llm_gateway.application.text_failure_recovery import classify_text_failure
from llm_gateway.domain.invocation import InvocationPersistenceUnavailable
from llm_gateway.domain.model import FailureCode, ProviderFailure, Usage
from llm_gateway.domain.recovery import RetryPolicy
from llm_gateway.domain.streaming import StreamFailed
from llm_gateway.infrastructure.routing_evidence import PostgresRoutingEvidence
from llm_gateway.infrastructure.settlement import PostgresInvocationSettlement
from llm_gateway.infrastructure.stream_commit import PostgresStreamCommit
from tests.test_attempt_execution import CANDIDATES
from tests.test_configuration import database, run
from tests.test_invocation_postgres import setup
from tests.test_streaming_attempt_execution import Harness, delta


def test_stream_failure_normalization_preserves_only_safe_observations():
    usage = Usage(100, 50, 0, 0)
    event = StreamFailed(2, ProviderFailure(FailureCode.UNCERTAIN, False), usage, "actual")
    result = event.as_result()
    assert result.code == FailureCode.UNCERTAIN and not result.retryable
    assert result.observed_usage == usage and result.observed_model == "actual"
    assert not hasattr(result, "text")


@pytest.mark.parametrize("changes", [dict(observed_usage={}), dict(observed_model=""),
    dict(observed_model=1), dict(observed_model="m" * 257)])
def test_failure_observations_require_typed_bounded_metadata(changes):
    with pytest.raises(ValueError):
        ProviderFailure(FailureCode.PROVIDER_PROTOCOL_ERROR, False, **changes)


@pytest.mark.postgres
@pytest.mark.parametrize("code", [FailureCode.PROVIDER_PROTOCOL_ERROR, FailureCode.UNCERTAIN])
@pytest.mark.parametrize("usage,completeness,cost", [
    (Usage(100, 50, 0, 0), "complete", Decimal("0.000225")),
    (Usage(100, cached_tokens=0), "partial", None),
    (Usage(100), "unavailable", None), (Usage(), "unavailable", None),
])
def test_failed_stream_keeps_usage_and_actual_model_without_success(database, code, usage, completeness, cost):
    async def scenario():
        store, record = await setup(database)
        harness = Harness([[delta(), StreamFailed(2, ProviderFailure(code, False), usage, "actual")]])

        class Runtime:
            @asynccontextmanager
            async def acquire(self, binding):
                await PostgresRoutingEvidence(store).gate(record.call_id, binding)
                async with harness.acquire(binding) as provider:
                    yield provider

        executor = StreamingAttemptExecutor(runtime=Runtime(),
            journal=SharedStreamAttemptJournal(store.journal(record.call_id)), output=harness,
            commit=PostgresStreamCommit(store, record.call_id),
            classify=classify_text_failure, draw_jitter=lambda _: 0)
        terminal = await executor.execute(CANDIDATES, policy=RetryPolicy(),
            deadline=asyncio.get_running_loop().time() + 10)
        assert isinstance(terminal, StreamFailed) and len(harness.calls) == 1
        result = terminal.as_result()
        settlement = PostgresInvocationSettlement(store, record.call_id)
        for changed in (replace(result, observed_model="invented"),
                        replace(result, observed_usage=Usage(999))):
            with pytest.raises(InvocationPersistenceUnavailable):
                await settlement.settle(changed)
        assert await settlement.settle(result) == usage
        with psycopg.connect(database) as connection:
            state = "uncertain" if code == FailureCode.UNCERTAIN else "failed"
            assert connection.execute("SELECT state FROM model_invocation").fetchone() == (state,)
            assert connection.execute("SELECT outcome,resolved_model,input_tokens,output_tokens,recovery_action FROM provider_attempt_outcome").fetchone() == (
                state, "actual", usage.input_tokens, usage.output_tokens, "stop")
            assert connection.execute("SELECT outcome,resolved_model,finish_reason,service_level,input_tokens,output_tokens FROM invocation_settlement").fetchone() == (
                state, "actual", None, None, usage.input_tokens, usage.output_tokens)
            assert connection.execute("SELECT completeness,total_cost FROM invocation_cost_summary").fetchone() == (completeness, cost)
    run(scenario())
