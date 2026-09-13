import asyncio
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal

import psycopg
import pytest

from llm_gateway.application.synchronous_invocation import SynchronousInvocation
from llm_gateway.domain.invocation import InvocationPersistenceUnavailable, InvocationTerminalConflict
from llm_gateway.domain.recovery import RecoveryPlan, RetryPolicy
from llm_gateway.domain.model import Usage
from llm_gateway.infrastructure.routing_evidence import PostgresRoutingEvidence
from llm_gateway.infrastructure.settlement import PostgresInvocationSettlement
from tests.test_attempt_execution import CANDIDATES, FAILURE, Harness
from tests.test_configuration import database, run, scalar
from tests.test_cost_postgres import KNOWN_SUCCESS
from tests.test_invocation_postgres import setup, install_runtime_gate


async def executed(database, results=(KNOWN_SUCCESS,)):
    store, record = await setup(database)
    harness = Harness(results)
    install_runtime_gate(harness, store, record)
    executor = harness.executor()
    executor._journal = store.journal(record.call_id)
    result = await executor.execute(CANDIDATES, policy=RetryPolicy(), deadline=asyncio.get_running_loop().time() + 10)
    return store, record, result


@pytest.mark.postgres
def test_success_returns_after_atomic_usage_cost_and_routing_settlement(database):
    async def scenario():
        store, record = await setup(database)
        harness = Harness([KNOWN_SUCCESS])
        install_runtime_gate(harness, store, record)
        executor = harness.executor()
        executor._journal = store.journal(record.call_id)
        result = await SynchronousInvocation(executor, PostgresInvocationSettlement(store, record.call_id)).execute(
            CANDIDATES, policy=RetryPolicy(), deadline=asyncio.get_running_loop().time() + 10)
        assert result is KNOWN_SUCCESS
        with psycopg.connect(database) as connection:
            assert connection.execute("SELECT state FROM model_invocation").fetchone()[0] == "completed"
            row = connection.execute("SELECT input_tokens,output_tokens,service_level,validation_status,terminal_at FROM invocation_settlement").fetchone()
            assert row[:4] == (100, 50, "full", "not_requested")
            terminal = connection.execute("SELECT terminal_at,expires_at FROM routing_terminal").fetchone()
            assert terminal[0] == row[4] and terminal[1] - terminal[0] == timedelta(days=30)
            assert connection.execute("SELECT total_cost,completeness FROM invocation_cost_summary").fetchone() == (Decimal("0.000225"), "complete")
            assert connection.execute("SELECT kind FROM routing_event ORDER BY sequence DESC LIMIT 1").fetchone()[0] == "routing_terminal"
    run(scenario())


@pytest.mark.postgres
def test_unknown_retry_usage_does_not_become_complete_invocation_total(database):
    async def scenario():
        store, record, result = await executed(database, (FAILURE, KNOWN_SUCCESS))
        await PostgresInvocationSettlement(store, record.call_id).settle(result)
    run(scenario())
    with psycopg.connect(database) as connection:
        assert connection.execute("SELECT attempt_count,input_tokens,output_tokens FROM invocation_settlement").fetchone() == (2, None, None)
        assert connection.execute("SELECT total_cost,completeness FROM invocation_cost_summary").fetchone() == (None, "partial")


@pytest.mark.postgres
@pytest.mark.parametrize("table", ["invocation_cost_summary", "routing_terminal"])
def test_terminal_fault_rolls_back_all_settlement_rows(database, table):
    async def scenario():
        store, record, result = await executed(database)
        with psycopg.connect(database) as connection:
            connection.execute(psycopg.sql.SQL("ALTER TABLE {} ADD CONSTRAINT reject_test_terminal CHECK(false)")
                               .format(psycopg.sql.Identifier(table)))
        with pytest.raises(InvocationPersistenceUnavailable):
            await PostgresInvocationSettlement(store, record.call_id).settle(result)
    run(scenario())
    assert scalar(database, "SELECT state FROM model_invocation") == "running"
    for table in ("invocation_settlement", "invocation_cost_summary", "routing_terminal"):
        assert scalar(database, f"SELECT count(*) FROM {table}") == 0
    assert scalar(database, "SELECT count(*) FROM routing_event WHERE kind='routing_terminal'") == 0
    assert scalar(database, "SELECT count(*) FROM cost_accrual") == 1


@pytest.mark.postgres
def test_cancellation_reservation_and_duplicate_settlement_cannot_be_overwritten(database):
    async def scenario():
        store, record, result = await executed(database)
        settlement = PostgresInvocationSettlement(store, record.call_id)
        with psycopg.connect(database) as connection:
            connection.execute("UPDATE model_invocation SET state='cancelling' WHERE call_id=%s", (record.call_id,))
        with pytest.raises(InvocationTerminalConflict):
            await settlement.settle(result)
        assert scalar(database, "SELECT count(*) FROM invocation_settlement") == 0
        store2, record2, result2 = await executed(database)
        settlement2 = PostgresInvocationSettlement(store2, record2.call_id)
        outcomes = await asyncio.gather(settlement2.settle(result2), settlement2.settle(result2), return_exceptions=True)
        assert sum(isinstance(value, Usage) for value in outcomes) == 1
        assert sum(isinstance(value, InvocationTerminalConflict) for value in outcomes) == 1
    run(scenario())
    assert scalar(database, "SELECT count(*) FROM invocation_settlement") == 1


@pytest.mark.postgres
def test_mismatched_result_and_unfinished_attempt_never_settle_success(database):
    async def scenario():
        store, record = await setup(database)
        await PostgresRoutingEvidence(store).gate(record.call_id, "a")
        await store.journal(record.call_id).started(1, "a", 1)
        settlement = PostgresInvocationSettlement(store, record.call_id)
        with pytest.raises(InvocationPersistenceUnavailable):
            await settlement.settle(KNOWN_SUCCESS)
        await store.journal(record.call_id).finished(1, KNOWN_SUCCESS, RecoveryPlan("stop"))
        with pytest.raises(InvocationPersistenceUnavailable):
            await settlement.settle(replace(KNOWN_SUCCESS, resolved_model="different"))
    run(scenario())
    assert scalar(database, "SELECT count(*) FROM invocation_settlement") == 0


@pytest.mark.postgres
def test_exhausted_failure_keeps_original_error_and_no_service_level(database):
    async def scenario():
        store, record, result = await executed(database, (FAILURE, FAILURE, FAILURE))
        await PostgresInvocationSettlement(store, record.call_id).settle(result)
    run(scenario())
    with psycopg.connect(database) as connection:
        assert connection.execute("SELECT outcome,error_code,service_level FROM invocation_settlement").fetchone() == ("failed", FAILURE.code, None)
        assert connection.execute("SELECT total_cost,completeness FROM invocation_cost_summary").fetchone() == (None, "unavailable")


@pytest.mark.postgres
def test_currency_summaries_are_not_added_or_converted_across_fallback(database):
    async def scenario():
        store, record = await setup(database, secondary_currency="EUR")
        harness = Harness([FAILURE, FAILURE, KNOWN_SUCCESS])
        install_runtime_gate(harness, store, record)
        executor = harness.executor()
        executor._journal = store.journal(record.call_id)
        await SynchronousInvocation(executor, PostgresInvocationSettlement(store, record.call_id)).execute(
            CANDIDATES, policy=RetryPolicy(), deadline=asyncio.get_running_loop().time() + 10)
    run(scenario())
    with psycopg.connect(database) as connection:
        assert connection.execute("SELECT currency,attempt_count,total_cost FROM invocation_cost_summary ORDER BY currency").fetchall() == [
            ("EUR", 1, Decimal("0.000225")), ("USD", 2, None)]


@pytest.mark.postgres
def test_settlement_is_immutable_and_survives_routing_evidence_removal(database):
    async def scenario():
        store, record, result = await executed(database)
        await PostgresInvocationSettlement(store, record.call_id).settle(result)
    run(scenario())
    with psycopg.connect(database) as connection:
        for query in ("DELETE FROM invocation_settlement", "UPDATE invocation_settlement SET input_tokens=0",
                      "DELETE FROM invocation_cost_summary", "UPDATE invocation_cost_summary SET total_cost=0"):
            with pytest.raises(psycopg.Error):
                with connection.transaction():
                    connection.execute(query)
        connection.execute("DELETE FROM routing_decision")
    assert scalar(database, "SELECT count(*) FROM invocation_settlement") == 1
    assert scalar(database, "SELECT count(*) FROM invocation_cost_summary") == 1
    assert scalar(database, "SELECT count(*) FROM routing_terminal") == 0
    assert scalar(database, "SELECT state FROM model_invocation") == "completed"
