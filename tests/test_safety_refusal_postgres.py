from dataclasses import replace

import psycopg
import pytest

from llm_gateway.domain.invocation import InvocationPersistenceUnavailable
from llm_gateway.domain.model import RefusalOutput, TextOutput
from llm_gateway.domain.recovery import RecoveryPlan
from llm_gateway.infrastructure.routing_evidence import PostgresRoutingEvidence
from llm_gateway.infrastructure.settlement import PostgresInvocationSettlement
from tests.test_configuration import database, run, scalar
from tests.test_invocation_postgres import setup
from tests.test_cost_postgres import KNOWN_SUCCESS


REFUSAL = replace(KNOWN_SUCCESS, output=RefusalOutput(None, "private refusal text"), finish_reason="content_filter")


async def started(database):
    store, record = await setup(database)
    await PostgresRoutingEvidence(store).gate(record.call_id, "a")
    await store.journal(record.call_id).started(1, "a", 1)
    return store, record


@pytest.mark.postgres
def test_valid_refusal_commits_policy_evidence_usage_cost_and_terminal(database):
    async def scenario():
        store, record = await started(database)
        await store.journal(record.call_id).finished(1, REFUSAL, RecoveryPlan("stop"))
        usage = await PostgresInvocationSettlement(store, record.call_id).settle(REFUSAL)
        assert usage == REFUSAL.usage
    run(scenario())
    with psycopg.connect(database) as connection:
        assert connection.execute("SELECT outcome,safety_refused,recovery_action FROM provider_attempt_outcome").fetchone() == ("succeeded", True, "stop")
        assert connection.execute("SELECT outcome,safety_refused,finish_reason FROM invocation_settlement").fetchone() == ("completed", True, "content_filter")
        assert connection.execute("SELECT resource_id,enforcement_identity FROM invocation_safety_policy").fetchone() == ("safety-a", "provider_refusal_terminal")
        row = connection.execute("SELECT to_jsonb(r) FROM safety_refusal r").fetchone()[0]
        assert row["signal"] == "both" and row["provider_policy_identity_state"] == "unavailable"
        assert "private refusal text" not in str(row)
        assert connection.execute("SELECT detail->>'disposition' FROM routing_event WHERE kind='routing_terminal'").fetchone()[0] == "safety_refused"
        assert connection.execute("SELECT count(*) FROM invocation_cost_summary").fetchone()[0] == 1


@pytest.mark.postgres
def test_refusal_blocks_raw_attempt_and_degradation_inserts(database):
    async def scenario():
        store, record = await started(database)
        await store.journal(record.call_id).finished(1, REFUSAL, RecoveryPlan("stop"))
        return record
    record = run(scenario())
    with psycopg.connect(database, autocommit=True) as connection:
        with pytest.raises(psycopg.Error, match="No Attempt after refusal"):
            connection.execute("INSERT INTO provider_attempt(call_id,number,binding_id,candidate_attempt) VALUES (%s,2,'b',1)", (record.call_id,))
        with pytest.raises(psycopg.Error, match="No routing recovery"):
            connection.execute("""INSERT INTO routing_event(call_id,sequence,kind,detail)
                SELECT %s,max(sequence)+1,'degradation_entered','{}'::jsonb FROM routing_event WHERE call_id=%s""", (record.call_id, record.call_id))
    assert scalar(database, "SELECT count(*) FROM provider_attempt") == 1


@pytest.mark.postgres
def test_outcome_failure_rolls_back_refusal_and_cost_evidence(database):
    async def scenario():
        store, record = await started(database)
        with psycopg.connect(database) as connection:
            connection.execute("ALTER TABLE provider_attempt_outcome ADD CONSTRAINT fail_test CHECK(false)")
        with pytest.raises(InvocationPersistenceUnavailable):
            await store.journal(record.call_id).finished(1, REFUSAL, RecoveryPlan("stop"))
    run(scenario())
    assert scalar(database, "SELECT count(*) FROM safety_refusal") == 0
    assert scalar(database, "SELECT count(*) FROM provider_attempt_outcome") == 0
    assert scalar(database, "SELECT count(*) FROM cost_accrual") == 0


@pytest.mark.postgres
def test_refusal_cannot_be_settled_as_ordinary_success(database):
    async def scenario():
        store, record = await started(database)
        await store.journal(record.call_id).finished(1, REFUSAL, RecoveryPlan("stop"))
        with pytest.raises(InvocationPersistenceUnavailable):
            await PostgresInvocationSettlement(store, record.call_id).settle(replace(REFUSAL, output=TextOutput("fake success")))
    run(scenario())
    assert scalar(database, "SELECT count(*) FROM invocation_settlement") == 0


@pytest.mark.postgres
@pytest.mark.parametrize("table", ["safety_refusal", "invocation_safety_policy"])
def test_refusal_and_policy_facts_are_append_only(database, table):
    async def scenario():
        store, record = await started(database)
        await store.journal(record.call_id).finished(1, REFUSAL, RecoveryPlan("stop"))
    run(scenario())
    with psycopg.connect(database, autocommit=True) as connection:
        with pytest.raises(psycopg.Error, match="append-only"):
            connection.execute(psycopg.sql.SQL("DELETE FROM {}").format(psycopg.sql.Identifier(table)))
