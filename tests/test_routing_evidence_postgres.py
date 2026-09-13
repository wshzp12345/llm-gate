from dataclasses import replace
from uuid import uuid4

import psycopg
import pytest

from llm_gateway.domain.invocation import InvocationPersistenceUnavailable
from llm_gateway.domain.routing_evidence import EvidenceCandidate
from llm_gateway.domain.routing_eligibility import StaticRoutingReason
from llm_gateway.domain.routing_order import WeightedCandidate
from llm_gateway.domain.recovery import RecoveryPlan
from llm_gateway.infrastructure.routing_evidence import PostgresRoutingEvidence
from tests.test_attempt_execution import FAILURE, SUCCESS
from tests.test_configuration import database, run, scalar
from tests.test_invocation_postgres import setup
from tests.test_routing_evidence import preselection


@pytest.mark.postgres
def test_attempt_requires_matching_fresh_allowed_gate(database):
    async def scenario():
        store, record = await setup(database)
        journal = store.journal(record.call_id)
        evidence = PostgresRoutingEvidence(store)
        with pytest.raises(InvocationPersistenceUnavailable):
            await journal.started(1, "a", 1)
        await evidence.gate(record.call_id, "b")
        with pytest.raises(InvocationPersistenceUnavailable):
            await journal.started(1, "a", 1)
        await evidence.gate(record.call_id, "a", (StaticRoutingReason("concurrency_exhausted"),))
        with pytest.raises(InvocationPersistenceUnavailable):
            await journal.started(1, "a", 1)
        await evidence.gate(record.call_id, "a")
        await journal.started(1, "a", 1)
        await journal.finished(1, FAILURE, RecoveryPlan("retry", 200))
        with pytest.raises(InvocationPersistenceUnavailable):
            await journal.started(2, "a", 2)
    run(scenario())
    with psycopg.connect(database) as connection:
        rows = connection.execute("SELECT sequence,kind,detail FROM routing_event ORDER BY sequence").fetchall()
        assert [row[0] for row in rows] == list(range(1, len(rows) + 1))
        assert rows[5][1] == "candidate_skipped"
        assert rows[5][2] == {"decision": "skipped", "reasons": [{"code": "concurrency_exhausted", "subject": None}]}


@pytest.mark.postgres
@pytest.mark.parametrize("kind", ["attempt_started", "attempt_finished"])
def test_event_failure_rolls_back_corresponding_attempt_checkpoint(database, kind):
    async def scenario():
        store, record = await setup(database)
        journal = store.journal(record.call_id)
        await PostgresRoutingEvidence(store).gate(record.call_id, "a")
        if kind == "attempt_finished":
            await journal.started(1, "a", 1)
        with psycopg.connect(database) as connection:
            connection.execute(psycopg.sql.SQL("ALTER TABLE routing_event ADD CONSTRAINT reject_test_event CHECK (kind <> {})")
                               .format(psycopg.sql.Literal(kind)))
        with pytest.raises(InvocationPersistenceUnavailable):
            if kind == "attempt_started":
                await journal.started(1, "a", 1)
            else:
                await journal.finished(1, SUCCESS, RecoveryPlan("stop"))
    run(scenario())
    assert scalar(database, "SELECT count(*) FROM provider_attempt") == (0 if kind == "attempt_started" else 1)
    assert scalar(database, "SELECT count(*) FROM provider_attempt_outcome") == 0
    assert scalar(database, "SELECT state FROM model_invocation") == ("accepted" if kind == "attempt_started" else "running")


@pytest.mark.postgres
def test_preselection_is_atomic_immutable_and_rejects_unregistered_reasons(database):
    async def scenario():
        store, record = await setup(database)
        evidence = PostgresRoutingEvidence(store)
        with pytest.raises(InvocationPersistenceUnavailable):
            await evidence.preselect(record.call_id, preselection())
        with pytest.raises(ValueError):
            await evidence.gate(record.call_id, "a", (StaticRoutingReason("health_unavailable", "raw endpoint"),))
    run(scenario())
    assert scalar(database, "SELECT count(*) FROM routing_decision") == 1
    assert scalar(database, "SELECT count(*) FROM routing_candidate") == 3
    assert scalar(database, "SELECT count(*) FROM routing_event") == 3
    with psycopg.connect(database) as connection:
        with pytest.raises(psycopg.Error):
            with connection.transaction():
                connection.execute("UPDATE routing_candidate SET weight=99")
        with pytest.raises(psycopg.Error):
            with connection.transaction():
                connection.execute("DELETE FROM routing_event")
        # Whole-aggregate removal is the retention primitive; Invocation survives.
        connection.execute("DELETE FROM routing_decision")
    assert scalar(database, "SELECT count(*) FROM model_invocation") == 1
    assert scalar(database, "SELECT count(*) FROM routing_candidate") == 0
    assert scalar(database, "SELECT count(*) FROM routing_event") == 0


@pytest.mark.postgres
def test_rejected_candidate_reason_is_durable_and_cannot_pass_gate(database):
    async def scenario():
        store, record = await setup(database)
        second = replace(record, call_id=uuid4())
        await store.admit_unkeyed_shell(second)
        snapshot = preselection()
        rejected = EvidenceCandidate(WeightedCandidate("d", "full", 0, 0), None,
                                     (StaticRoutingReason("weight_zero"),))
        await PostgresRoutingEvidence(store).preselect(second.call_id,
            replace(snapshot, candidates=snapshot.candidates + (rejected,)))
        with pytest.raises(InvocationPersistenceUnavailable):
            await PostgresRoutingEvidence(store).gate(second.call_id, "d")
    run(scenario())
    with psycopg.connect(database) as connection:
        row = connection.execute("""SELECT c.initial_order,e.detail FROM routing_candidate c
            JOIN routing_event e USING(call_id,binding_id) WHERE c.binding_id='d'""").fetchone()
        assert row == (None, {"decision": "rejected", "reasons": [{"code": "weight_zero", "subject": None}]})


@pytest.mark.postgres
def test_preselection_failure_leaves_no_partial_header_or_candidates(database):
    async def scenario():
        store, record = await setup(database)
        second = replace(record, call_id=uuid4())
        await store.admit_unkeyed_shell(second)
        with psycopg.connect(database) as connection:
            connection.execute("ALTER TABLE routing_event ADD CONSTRAINT reject_new_call CHECK (kind <> 'preselection') NOT VALID")
        with pytest.raises(InvocationPersistenceUnavailable):
            await PostgresRoutingEvidence(store).preselect(second.call_id, preselection())
    run(scenario())
    assert scalar(database, "SELECT count(*) FROM model_invocation") == 2
    assert scalar(database, "SELECT count(*) FROM routing_decision") == 1
    assert scalar(database, "SELECT count(*) FROM routing_candidate") == 3


@pytest.mark.postgres
def test_event_sequence_and_cross_call_attempt_identity_are_enforced(database):
    async def scenario():
        store, first = await setup(database)
        await PostgresRoutingEvidence(store).gate(first.call_id, "a")
        await store.journal(first.call_id).started(1, "a", 1)
        second = replace(first, call_id=uuid4())
        await store.admit_unkeyed_shell(second)
        await PostgresRoutingEvidence(store).preselect(second.call_id, preselection())
        return first.call_id, second.call_id
    first, second = run(scenario())
    with psycopg.connect(database) as connection:
        with pytest.raises(psycopg.Error):
            with connection.transaction():
                connection.execute("""INSERT INTO routing_event(call_id,sequence,kind,binding_id,detail)
                    VALUES (%s,99,'attempt_gate','a','{}')""", (first,))
        with pytest.raises(psycopg.Error):
            with connection.transaction():
                connection.execute("""INSERT INTO routing_event(call_id,sequence,kind,binding_id,attempt_number,detail)
                    VALUES (%s,4,'attempt_started','a',1,'{}')""", (second,))


@pytest.mark.postgres
def test_terminal_foreign_key_finality_and_whole_aggregate_delete(database):
    async def scenario():
        store, record = await setup(database)
        await PostgresRoutingEvidence(store).gate(record.call_id, "a")
        await store.journal(record.call_id).started(1, "a", 1)
        return store, record
    store, record = run(scenario())
    # Schema-level terminal fixture, not a substitute for final Usage/Cost settlement.
    with psycopg.connect(database) as connection:
        with pytest.raises(psycopg.Error):
            with connection.transaction():
                connection.execute("""INSERT INTO routing_terminal(call_id,sequence,terminal_at,expires_at)
                    VALUES (%s,5,statement_timestamp(),statement_timestamp()+interval '1 day')""", (record.call_id,))
        connection.execute("""INSERT INTO routing_event(call_id,sequence,kind,detail)
            VALUES (%s,6,'routing_terminal','{"outcome":"uncertain","error_code":"persistence_unavailable"}')""", (record.call_id,))
        connection.execute("""INSERT INTO routing_terminal(call_id,sequence,terminal_at,expires_at)
            VALUES (%s,6,statement_timestamp(),statement_timestamp()+interval '1 day')""", (record.call_id,))
    with pytest.raises(InvocationPersistenceUnavailable):
        run(PostgresRoutingEvidence(store).gate(record.call_id, "a"))
    with psycopg.connect(database) as connection:
        with pytest.raises(psycopg.Error):
            with connection.transaction():
                connection.execute("UPDATE routing_terminal SET expires_at=expires_at+interval '1 day'")
        connection.execute("DELETE FROM routing_decision WHERE call_id=%s", (record.call_id,))
    assert scalar(database, "SELECT count(*) FROM routing_terminal") == 0
    assert scalar(database, "SELECT count(*) FROM routing_event") == 0
    assert scalar(database, "SELECT count(*) FROM provider_attempt") == 1
    assert scalar(database, "SELECT count(*) FROM model_invocation") == 1
