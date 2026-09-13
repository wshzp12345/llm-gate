import asyncio
from dataclasses import replace

import psycopg
import pytest

from llm_gateway.application.cancellation import CancellationReservation as Reservation, LocalCancellationReason as Reason
from llm_gateway.domain.invocation import InvocationPersistenceUnavailable, InvocationTerminalConflict
from llm_gateway.domain.recovery import RecoveryPlan
from llm_gateway.infrastructure.local_cancellation import PostgresLocalCancellationStore
from llm_gateway.infrastructure.routing_evidence import PostgresRoutingEvidence
from llm_gateway.infrastructure.settlement import PostgresInvocationSettlement
from tests.test_attempt_execution import SUCCESS
from tests.test_configuration import database, run, scalar
from tests.test_invocation import admission
from tests.test_invocation_postgres import setup


def test_untyped_reason_fails_before_transaction():
    with pytest.raises(ValueError):
        run(PostgresLocalCancellationStore(None).reserve(admission(), "client_disconnected"))


@pytest.mark.postgres
@pytest.mark.parametrize("reason", list(Reason))
def test_reservation_is_committed_and_repeated_cancellation_keeps_first_initiator(database, reason):
    async def scenario():
        store, record = await setup(database)
        cancellation = PostgresLocalCancellationStore(store)
        assert await cancellation.reserve(record, reason) == Reservation.RESERVED
        assert scalar(database, "SELECT state FROM model_invocation") == "cancelling"
        assert await cancellation.reserve(record, Reason.CONTEXT_CANCELLED) == Reservation.CANCELLING
    run(scenario())
    assert scalar(database, "SELECT reason FROM invocation_local_cancellation") == reason
    assert scalar(database, "SELECT count(*) FROM invocation_local_cancellation") == 1
    assert scalar(database, "SELECT count(*) FROM invocation_settlement") == 0
    with psycopg.connect(database) as connection:
        with pytest.raises(psycopg.Error):
            connection.execute("DELETE FROM invocation_local_cancellation")


@pytest.mark.postgres
def test_reservation_rolls_back_state_when_evidence_write_fails(database):
    async def scenario():
        store, record = await setup(database)
        # A fixture-only database trigger simulates failure at the audit write.
        with psycopg.connect(database) as connection:
            connection.execute("""CREATE FUNCTION reject_cancellation() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN RAISE EXCEPTION 'test write failure'; END; $$""")
            connection.execute("""CREATE TRIGGER reject_cancellation BEFORE INSERT ON invocation_local_cancellation
                FOR EACH ROW EXECUTE FUNCTION reject_cancellation()""")
        with pytest.raises(InvocationPersistenceUnavailable):
            await PostgresLocalCancellationStore(store).reserve(record, Reason.CONTEXT_CANCELLED)
    run(scenario())
    assert scalar(database, "SELECT state FROM model_invocation") == "accepted"
    assert scalar(database, "SELECT count(*) FROM invocation_local_cancellation") == 0


@pytest.mark.postgres
def test_other_admission_identity_cannot_reserve(database):
    async def scenario():
        store, record = await setup(database)
        other = replace(record, authorization=replace(record.authorization, subject="other"))
        with pytest.raises(InvocationPersistenceUnavailable):
            await PostgresLocalCancellationStore(store).reserve(other, Reason.CONTEXT_CANCELLED)
    run(scenario())
    assert scalar(database, "SELECT state FROM model_invocation") == "accepted"


@pytest.mark.postgres
def test_simultaneous_cancellation_has_one_cleanup_owner_and_cancelled_is_idempotent(database):
    async def scenario():
        store, record = await setup(database)
        cancellation = PostgresLocalCancellationStore(store)
        results = await asyncio.gather(*(cancellation.reserve(record, reason) for reason in Reason))
        assert results.count(Reservation.RESERVED) == 1
        assert results.count(Reservation.CANCELLING) == 2
        # Fixture simulates a later finalizer; this reservation port does not
        # claim to provide complete cancelled settlement or Provider cleanup.
        async with store.transaction() as connection:
            await connection.execute("UPDATE model_invocation SET state='cancelled' WHERE call_id=%s", (record.call_id,))
        assert await cancellation.reserve(record, Reason.CONTEXT_CANCELLED) == Reservation.CANCELLED
    run(scenario())
    assert scalar(database, "SELECT count(*) FROM invocation_local_cancellation") == 1


@pytest.mark.postgres
@pytest.mark.parametrize("order", ["cancel_first", "complete_first", "concurrent"])
def test_completion_and_cancel_share_one_terminal_owner(database, order):
    async def scenario():
        store, record = await setup(database)
        await PostgresRoutingEvidence(store).gate(record.call_id, "a")
        journal = store.journal(record.call_id)
        await journal.started(1, "a", 1)
        await journal.finished(1, SUCCESS, RecoveryPlan("stop"))
        cancellation = PostgresLocalCancellationStore(store)
        settlement = PostgresInvocationSettlement(store, record.call_id)
        if order == "cancel_first":
            assert await cancellation.reserve(record, Reason.CLIENT_DISCONNECTED) == Reservation.RESERVED
            with pytest.raises(InvocationTerminalConflict):
                await settlement.settle(SUCCESS)
        elif order == "complete_first":
            await settlement.settle(SUCCESS)
            assert await cancellation.reserve(record, Reason.CLIENT_DISCONNECTED) == Reservation.ALREADY_TERMINAL
        else:
            cancelled, completed = await asyncio.gather(
                cancellation.reserve(record, Reason.CLIENT_DISCONNECTED), settlement.settle(SUCCESS),
                return_exceptions=True)
            if cancelled == Reservation.RESERVED:
                assert isinstance(completed, InvocationTerminalConflict)
            else:
                assert cancelled == Reservation.ALREADY_TERMINAL
                assert not isinstance(completed, BaseException)
        state = scalar(database, "SELECT state FROM model_invocation")
        assert state in {"cancelling", "completed"}
        assert scalar(database, "SELECT count(*) FROM invocation_local_cancellation") == (state == "cancelling")
        assert scalar(database, "SELECT count(*) FROM invocation_settlement") == (state == "completed")
        # Cancellation does not erase known Provider outcome/Usage/cost.
        assert scalar(database, "SELECT count(*) FROM provider_attempt_outcome") == 1
        assert scalar(database, "SELECT count(*) FROM cost_accrual") == 1
    run(scenario())
