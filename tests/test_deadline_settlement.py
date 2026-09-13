import asyncio
from dataclasses import replace
from uuid import uuid4

import psycopg
import pytest

from llm_gateway.application.cancellation import LocalCancellationReason
from llm_gateway.domain.invocation import InvocationPersistenceUnavailable, InvocationTerminalConflict
from llm_gateway.application.late_provider_outcome import LateTextOutcome
from llm_gateway.domain.model import Usage
from llm_gateway.domain.recovery import RecoveryPlan
from llm_gateway.infrastructure.cancellation_settlement import settle_deadline_exceeded
from llm_gateway.infrastructure.local_cancellation import PostgresLocalCancellationStore
from llm_gateway.infrastructure.routing_evidence import PostgresRoutingEvidence
from tests.test_attempt_execution import SUCCESS
from tests.test_configuration import database, run, scalar
from tests.test_invocation import admission
from tests.test_invocation_postgres import setup


@pytest.mark.parametrize("deadline", [True, float("nan"), float("inf")])
def test_invalid_deadline_prevents_persistence(deadline):
    with pytest.raises(ValueError):
        run(settle_deadline_exceeded(None, admission(), deadline=deadline))


def test_unexpired_deadline_prevents_persistence():
    async def scenario():
        with pytest.raises(ValueError):
            await settle_deadline_exceeded(None, admission(), deadline=asyncio.get_running_loop().time() + 10)
    run(scenario())


@pytest.mark.postgres
@pytest.mark.parametrize("stage", ["before_routing", "before_attempt", "inflight", "known"])
def test_deadline_settlement_preserves_known_facts_and_never_fabricates_cancel(database, stage):
    async def scenario():
        store, record = await setup(database, admit=stage != "before_routing")
        if stage == "before_routing":
            await store.admit_unkeyed_shell(record)
        if stage in {"inflight", "known"}:
            await PostgresRoutingEvidence(store).gate(record.call_id, "a")
            await store.journal(record.call_id).started(1, "a", 1)
            if stage == "known":
                await store.journal(record.call_id).finished(1, SUCCESS, RecoveryPlan("stop"))
        await settle_deadline_exceeded(store, record, deadline=asyncio.get_running_loop().time() - 1)
        with pytest.raises(InvocationTerminalConflict):
            await settle_deadline_exceeded(store, record, deadline=0)
    run(scenario())
    assert scalar(database, "SELECT state FROM model_invocation") == "failed"
    assert scalar(database, "SELECT error_code FROM invocation_settlement") == "deadline_exceeded"
    assert scalar(database, "SELECT count(*) FROM invocation_local_cancellation") == 0
    assert scalar(database, "SELECT count(*) FROM invocation_cancellation_cleanup") == 0
    count = int(stage in {"inflight", "known"})
    assert scalar(database, "SELECT count(*) FROM provider_attempt") == count
    assert scalar(database, "SELECT count(*) FROM cost_accrual") == count
    if count:
        assert scalar(database, "SELECT outcome FROM provider_attempt_outcome") == ("succeeded" if stage == "known" else "uncertain")


@pytest.mark.postgres
def test_cancel_reservation_wins_over_later_deadline_settlement(database):
    async def scenario():
        store, record = await setup(database)
        await PostgresLocalCancellationStore(store).reserve(record, LocalCancellationReason.CONTEXT_CANCELLED)
        with pytest.raises(InvocationTerminalConflict):
            await settle_deadline_exceeded(store, record, deadline=0)
    run(scenario())
    assert scalar(database, "SELECT state FROM model_invocation") == "cancelling"
    assert scalar(database, "SELECT count(*) FROM invocation_settlement") == 0


@pytest.mark.postgres
@pytest.mark.parametrize("case", ["rollback", "wrong_attempt", "checkpointed"])
def test_stopped_observation_is_atomic_and_does_not_rewrite_checkpoint(database, case):
    async def scenario():
        store, record = await setup(database)
        await PostgresRoutingEvidence(store).gate(record.call_id, "a")
        await store.journal(record.call_id).started(1, "a", 1)
        observation = LateTextOutcome.from_result(uuid4(), 2 if case == "wrong_attempt" else 1,
            replace(SUCCESS, usage=Usage(10, 6, 0, 0)))
        if case == "checkpointed":
            await store.journal(record.call_id).finished(1, SUCCESS, RecoveryPlan("stop"))
        if case == "rollback":
            with psycopg.connect(database) as connection:
                connection.execute("ALTER TABLE invocation_cost_summary ADD CONSTRAINT test_reject CHECK(false)")
        if case in {"rollback", "wrong_attempt"}:
            with pytest.raises(InvocationPersistenceUnavailable):
                await settle_deadline_exceeded(store, record, deadline=0, observation=observation)
            assert scalar(database, "SELECT state FROM model_invocation") == "running"
            for table in ("provider_attempt_outcome", "cost_accrual", "invocation_settlement"):
                assert scalar(database, f"SELECT count(*) FROM {table}") == 0
        else:
            await settle_deadline_exceeded(store, record, deadline=0, observation=observation)
            assert scalar(database, "SELECT outcome FROM provider_attempt_outcome") == "succeeded"
            assert scalar(database, "SELECT input_tokens FROM provider_attempt_outcome") is None
            assert scalar(database, "SELECT count(*) FROM cost_accrual") == 1
            assert scalar(database, "SELECT error_code FROM invocation_settlement") == "deadline_exceeded"
    run(scenario())
