import asyncio
from dataclasses import asdict, replace
from uuid import uuid4

import psycopg
import pytest

from llm_gateway.application.cancellation import CancellationCleanup, LocalCancellationReason
from llm_gateway.application.late_provider_outcome import LateTextOutcome
from llm_gateway.domain.invocation import InvocationPersistenceUnavailable
from llm_gateway.domain.model import FailureCode, ProviderFailure, RefusalOutput, TextOutput, Usage
from llm_gateway.infrastructure.late_provider_outcome import PostgresLateProviderOutcome
from llm_gateway.infrastructure.local_cancellation import PostgresLocalCancellationStore
from llm_gateway.infrastructure.routing_evidence import PostgresRoutingEvidence
from tests.test_attempt_execution import SUCCESS
from tests.test_configuration import database, run, scalar
from tests.test_invocation_postgres import setup


RESULT = replace(SUCCESS, output=TextOutput("private-late-output"), usage=Usage(10, 6, 0, 0, 16))


@pytest.mark.parametrize("result", [RESULT, replace(RESULT, output=RefusalOutput("private-late-output", "private-refusal")),
    ProviderFailure(FailureCode.PROVIDER_UNAVAILABLE, True), ProviderFailure(FailureCode.UNCERTAIN, False),
    ProviderFailure(FailureCode.UNCERTAIN, False, observed_usage=Usage(10, 6), observed_model="actual")])
def test_late_metadata_projection_keeps_no_model_content(result):
    value = LateTextOutcome.from_result(uuid4(), 1, result)
    assert "private-" not in repr(asdict(value))
    assert value.usage == (result.usage if hasattr(result, "usage") else result.observed_usage or Usage())


@pytest.mark.parametrize("changes", [{"event_id": "raw"}, {"attempt_number": True}, {"safety_refused": 1},
    {"error_code": FailureCode.UNCERTAIN}, {"usage": {}}, {"finish_reason": "content_filter"}])
def test_invalid_late_metadata_is_rejected(changes):
    with pytest.raises(ValueError):
        replace(LateTextOutcome.from_result(uuid4(), 1, RESULT), **changes)


async def pending(database):
    store, record = await setup(database)
    await PostgresRoutingEvidence(store).gate(record.call_id, "a")
    await store.journal(record.call_id).started(1, "a", 1)
    cancellation = PostgresLocalCancellationStore(store)
    await cancellation.reserve(record, LocalCancellationReason.CONTEXT_CANCELLED)
    return store, record, cancellation, PostgresLateProviderOutcome(store, record.call_id)


@pytest.mark.postgres
def test_before_and_after_cancelled_observations_never_rewrite_terminal_or_cost(database):
    async def scenario():
        store, record, cancellation, port = await pending(database)
        first = LateTextOutcome.from_result(uuid4(), 1, RESULT)
        await port.record(first)
        assert scalar(database, "SELECT count(*) FROM cost_accrual") == 0
        await cancellation.finalize(record, CancellationCleanup(False, None, False))
        tables = ("model_invocation", "provider_attempt_outcome", "invocation_settlement", "cost_accrual", "invocation_cost_summary", "routing_terminal")
        with psycopg.connect(database) as connection:
            before = {table: connection.execute(f"SELECT * FROM {table}").fetchall() for table in tables}
        await port.record(first)  # Same event remains idempotent across state change.
        await port.record(LateTextOutcome.from_result(uuid4(), 1, replace(RESULT, usage=Usage(12, 7, 0, 0, 19))))
        with psycopg.connect(database) as connection:
            assert {table: connection.execute(f"SELECT * FROM {table}").fetchall() for table in tables} == before
            rows = connection.execute("SELECT sequence,observed_state,input_tokens FROM late_provider_outcome ORDER BY sequence").fetchall()
            assert rows == [(1, "cancelling", 10), (2, "cancelled", 12)]
            assert "private-late-output" not in str(connection.execute("SELECT to_jsonb(o) FROM late_provider_outcome o").fetchall())
            assert connection.execute("""SELECT count(*) FROM late_provider_outcome l JOIN attempt_pricing p USING(call_id,number)
                JOIN cost_accrual c USING(call_id,number)""").fetchone()[0] == 2
    run(scenario())


@pytest.mark.postgres
def test_duplicate_concurrency_is_idempotent_and_conflicts_do_not_append(database):
    async def scenario():
        _, _, _, port = await pending(database)
        value = LateTextOutcome.from_result(uuid4(), 1, RESULT)
        await asyncio.gather(port.record(value), port.record(value))
        for conflicting in (replace(value, attempt_number=2), replace(value, usage=Usage(20, 1, 0, 0))):
            with pytest.raises(InvocationPersistenceUnavailable):
                await port.record(conflicting)
    run(scenario())
    assert scalar(database, "SELECT count(*) FROM late_provider_outcome") == 1


@pytest.mark.postgres
@pytest.mark.parametrize("case", ["not_cancelled", "no_attempt", "wrong_alias"])
def test_late_write_requires_cancelled_attempt_identity(database, case):
    async def scenario():
        store, record = await setup(database)
        if case != "no_attempt":
            await PostgresRoutingEvidence(store).gate(record.call_id, "a")
            await store.journal(record.call_id).started(1, "a", 1)
        if case != "not_cancelled":
            await PostgresLocalCancellationStore(store).reserve(record, LocalCancellationReason.CONTEXT_CANCELLED)
        value = LateTextOutcome.from_result(uuid4(), 1, RESULT)
        if case == "wrong_alias":
            value = replace(value, requested_model="other")
        with pytest.raises(InvocationPersistenceUnavailable):
            await PostgresLateProviderOutcome(store, record.call_id).record(value)
    run(scenario())
    assert scalar(database, "SELECT count(*) FROM late_provider_outcome") == 0


@pytest.mark.postgres
def test_late_evidence_is_immutable_and_survives_routing_deletion(database):
    async def scenario():
        _, record, cancellation, port = await pending(database)
        await cancellation.finalize(record, CancellationCleanup(False, None, False))
        await port.record(LateTextOutcome.from_result(uuid4(), 1, RESULT))
        with psycopg.connect(database) as connection:
            connection.execute("DELETE FROM routing_decision WHERE call_id=%s", (record.call_id,))
        await port.record(LateTextOutcome.from_result(uuid4(), 1, ProviderFailure(FailureCode.UNCERTAIN, False)))
    run(scenario())
    assert scalar(database, "SELECT count(*) FROM late_provider_outcome") == 2
    for statement in ("DELETE FROM late_provider_outcome", "UPDATE late_provider_outcome SET input_tokens=0"):
        with psycopg.connect(database) as connection:
            with pytest.raises(psycopg.Error):
                connection.execute(statement)
