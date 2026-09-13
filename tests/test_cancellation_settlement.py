import asyncio

import psycopg
import pytest

from llm_gateway.application.cancellation import CancellationCleanup as Cleanup, ProviderCancelResult as Result, LocalCancellationReason as Reason
from llm_gateway.domain.invocation import InvocationPersistenceUnavailable, InvocationTerminalConflict
from llm_gateway.domain.model import FailureCode, ProviderFailure
from llm_gateway.domain.recovery import RecoveryPlan
from llm_gateway.infrastructure.local_cancellation import PostgresLocalCancellationStore
from llm_gateway.infrastructure.routing_evidence import PostgresRoutingEvidence
from tests.test_attempt_execution import SUCCESS
from tests.test_configuration import database, run, scalar
from tests.test_invocation_postgres import setup


@pytest.mark.parametrize("args", [(True, None, False), (False, Result.UNKNOWN, False), (True, "unknown", False), (1, None, False), (False, None, 0)])
def test_cleanup_requires_typed_observed_facts(args):
    with pytest.raises(ValueError):
        Cleanup(*args)


async def started(store, record):
    await PostgresRoutingEvidence(store).gate(record.call_id, "a")
    await store.journal(record.call_id).started(1, "a", 1)


@pytest.mark.postgres
@pytest.mark.parametrize("routed", [True, False])
def test_pre_attempt_cancellation_has_no_provider_or_cost_rows(database, routed):
    async def scenario():
        store, record = await setup(database, admit=routed)
        if not routed:
            await store.admit_unkeyed_shell(record)
        cancellation = PostgresLocalCancellationStore(store)
        await cancellation.reserve(record, Reason.CONTEXT_CANCELLED)
        await cancellation.finalize(record, Cleanup(False, None, False))
    run(scenario())
    assert scalar(database, "SELECT state FROM model_invocation") == "cancelled"
    assert scalar(database, "SELECT attempt_count FROM invocation_settlement") == 0
    assert scalar(database, "SELECT input_tokens FROM invocation_settlement") is None
    for table in ("provider_attempt", "provider_attempt_outcome", "cost_accrual", "invocation_cost_summary"):
        assert scalar(database, f"SELECT count(*) FROM {table}") == 0
    assert scalar(database, "SELECT count(*) FROM routing_terminal") == int(routed)


@pytest.mark.postgres
@pytest.mark.parametrize("result", [None, *Result])
def test_incomplete_attempt_keeps_billing_uncertain_for_every_cancel_response(database, result):
    async def scenario():
        store, record = await setup(database)
        await started(store, record)
        cancellation = PostgresLocalCancellationStore(store)
        await cancellation.reserve(record, Reason.CLIENT_DISCONNECTED)
        await cancellation.finalize(record, Cleanup(result is not None, result, False))
        with pytest.raises(InvocationTerminalConflict):
            await cancellation.finalize(record, Cleanup(result is not None, result, False))
    run(scenario())
    assert scalar(database, "SELECT outcome FROM invocation_settlement") == "cancelled"
    assert scalar(database, "SELECT error_code FROM invocation_settlement") == "cancelled"
    assert scalar(database, "SELECT outcome FROM provider_attempt_outcome") == ("cancelled" if result == Result.ACKNOWLEDGED else "uncertain")
    assert scalar(database, "SELECT billing_outcome FROM attempt_cancellation_billing") == "uncertain"
    assert scalar(database, "SELECT total_cost FROM invocation_cost_summary") is None
    assert scalar(database, "SELECT recovery_action FROM provider_attempt_outcome") == "stop"
    assert scalar(database, "SELECT extract(epoch FROM expires_at-terminal_at) FROM routing_terminal") == 2592000


@pytest.mark.postgres
@pytest.mark.parametrize("uncertain", [False, True])
def test_known_provider_facts_survive_cancellation(database, uncertain):
    async def scenario():
        store, record = await setup(database)
        await started(store, record)
        result = ProviderFailure(FailureCode.UNCERTAIN, False) if uncertain else SUCCESS
        await store.journal(record.call_id).finished(1, result, RecoveryPlan("stop"))
        with psycopg.connect(database) as connection:
            before = connection.execute("SELECT * FROM provider_attempt_outcome").fetchone()
            cost = connection.execute("SELECT * FROM cost_accrual").fetchone()
        cancellation = PostgresLocalCancellationStore(store)
        await cancellation.reserve(record, Reason.SHUTDOWN_DRAIN_EXPIRED)
        await cancellation.finalize(record, Cleanup(False, None, False))
        with psycopg.connect(database) as connection:
            assert connection.execute("SELECT * FROM provider_attempt_outcome").fetchone() == before
            assert connection.execute("SELECT * FROM cost_accrual").fetchone() == cost
        assert scalar(database, "SELECT input_tokens FROM invocation_settlement") == (None if uncertain else SUCCESS.usage.input_tokens)
    run(scenario())
    assert scalar(database, "SELECT count(*) FROM attempt_cancellation_billing") == int(uncertain)
    assert scalar(database, "SELECT state FROM model_invocation") == "cancelled"


@pytest.mark.postgres
def test_terminal_failure_rolls_back_all_cleanup_and_missing_attempt_evidence(database):
    async def scenario():
        store, record = await setup(database)
        await started(store, record)
        cancellation = PostgresLocalCancellationStore(store)
        await cancellation.reserve(record, Reason.CONTEXT_CANCELLED)
        with psycopg.connect(database) as connection:
            connection.execute("""CREATE FUNCTION reject_cancel_terminal() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN RAISE EXCEPTION 'test terminal failure'; END; $$""")
            connection.execute("""CREATE TRIGGER reject_cancel_terminal BEFORE INSERT ON invocation_settlement
                FOR EACH ROW EXECUTE FUNCTION reject_cancel_terminal()""")
        with pytest.raises(InvocationPersistenceUnavailable):
            await cancellation.finalize(record, Cleanup(True, Result.UNKNOWN, False))
    run(scenario())
    assert scalar(database, "SELECT state FROM model_invocation") == "cancelling"
    for table in ("invocation_cancellation_cleanup", "attempt_cancellation_billing", "provider_attempt_outcome", "cost_accrual", "invocation_settlement", "routing_terminal"):
        assert scalar(database, f"SELECT count(*) FROM {table}") == 0


@pytest.mark.postgres
def test_concurrent_finalization_has_exactly_one_terminal(database):
    async def scenario():
        store, record = await setup(database)
        cancellation = PostgresLocalCancellationStore(store)
        with pytest.raises(InvocationTerminalConflict):
            await cancellation.finalize(record, Cleanup(False, None, True))
        await cancellation.reserve(record, Reason.CONTEXT_CANCELLED)
        results = await asyncio.gather(*(cancellation.finalize(record, Cleanup(False, None, True)) for _ in range(2)), return_exceptions=True)
        assert results.count(None) == 1
        assert sum(isinstance(result, InvocationTerminalConflict) for result in results) == 1
    run(scenario())
    assert scalar(database, "SELECT count(*) FROM invocation_settlement") == 1
    assert scalar(database, "SELECT count(*) FROM invocation_cancellation_cleanup") == 1
