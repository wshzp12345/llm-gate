import asyncio
from dataclasses import replace

import psycopg
import pytest

from llm_gateway.domain.invocation import InvocationPersistenceUnavailable
from llm_gateway.domain.recovery import RecoveryPlan
from llm_gateway.infrastructure.routing_evidence import PostgresRoutingEvidence
from llm_gateway.infrastructure.stream_commit import PostgresStreamCommit
from llm_gateway.infrastructure.migrate import migrate
from tests.test_attempt_execution import FAILURE
from tests.test_configuration import database, run, scalar
from tests.test_invocation_postgres import setup
from tests.test_streaming_attempt_execution import Harness, delta, success, execute


def test_commit_precedes_output_and_only_occurs_once():
    harness = Harness([[delta(), delta("second", 2), success(3, "visiblesecond")]])
    calls = []

    class Commit:
        async def first_delta(self, number, event):
            assert harness.sent == []
            calls.append((number, event.sequence))

    execute(harness, commit=Commit())
    assert calls == [(1, 1)] and len(harness.sent) == 2


def test_commit_failure_sends_no_business_data_and_never_retries():
    harness = Harness([[delta(), success()]])

    class Commit:
        async def first_delta(self, number, event):
            raise InvocationPersistenceUnavailable()

    with pytest.raises(InvocationPersistenceUnavailable):
        execute(harness, commit=Commit())
    assert harness.sent == [] and len(harness.calls) == 1 and harness.closed == 1


async def active(database):
    store, record = await setup(database)
    await PostgresRoutingEvidence(store).gate(record.call_id, "a")
    await store.journal(record.call_id).started(1, "a", 1)
    return store, record, PostgresStreamCommit(store, record.call_id)


@pytest.mark.postgres
def test_commit_is_content_free_immutable_and_forbids_direct_sql_recovery(database):
    async def scenario():
        store, record, commit = await active(database)
        await commit.first_delta(1, delta("private-visible-text"))
        with psycopg.connect(database) as connection:
            row = connection.execute("SELECT number,resolved_model,delta_kind FROM invocation_stream_commit").fetchone()
            assert row == (1, "actual", "text")
            assert "private-visible-text" not in str(connection.execute("SELECT * FROM invocation_stream_commit").fetchall())
            for query in ("UPDATE invocation_stream_commit SET resolved_model='other'",
                          "DELETE FROM invocation_stream_commit",
                          "INSERT INTO provider_attempt(call_id,number,binding_id,candidate_attempt) SELECT call_id,2,'a',2 FROM invocation_stream_commit",
                          "INSERT INTO provider_attempt(call_id,number,binding_id,candidate_attempt) SELECT call_id,2,'b',1 FROM invocation_stream_commit",
                          "INSERT INTO provider_attempt_outcome(call_id,number,outcome,error_code,recovery_action,recovery_delay_ms) SELECT call_id,1,'failed','provider_unavailable','retry',0 FROM invocation_stream_commit"):
                with pytest.raises(psycopg.Error):
                    with connection.transaction():
                        connection.execute(query)
        for action in ("retry", "advance"):
            with pytest.raises(InvocationPersistenceUnavailable):
                await store.journal(record.call_id).finished(1, FAILURE, RecoveryPlan(action))
        with pytest.raises(InvocationPersistenceUnavailable):
            await store.journal(record.call_id).finished(1, replace(FAILURE, observed_model="other"), RecoveryPlan("stop"))
        await store.journal(record.call_id).finished(1, replace(FAILURE, observed_model="actual"), RecoveryPlan("stop"))
    run(scenario())
    assert scalar(database, "SELECT count(*) FROM provider_attempt_outcome") == 1


@pytest.mark.postgres
@pytest.mark.parametrize("state", ["finished", "cancelling", "no_attempt"])
def test_commit_requires_live_unfinished_attempt(database, state):
    async def scenario():
        if state == "no_attempt":
            store, record = await setup(database)
            commit = PostgresStreamCommit(store, record.call_id)
        else:
            store, record, commit = await active(database)
            if state == "finished":
                await store.journal(record.call_id).finished(1, FAILURE, RecoveryPlan("retry"))
            else:
                with psycopg.connect(database) as connection:
                    connection.execute("UPDATE model_invocation SET state='cancelling'")
        with pytest.raises(InvocationPersistenceUnavailable):
            await commit.first_delta(1, delta())
    run(scenario())
    assert scalar(database, "SELECT count(*) FROM invocation_stream_commit") == 0


@pytest.mark.postgres
def test_concurrent_commit_has_exactly_one_winner(database):
    async def scenario():
        store, record, commit = await active(database)
        outcomes = await asyncio.gather(commit.first_delta(1, delta()), commit.first_delta(1, delta()), return_exceptions=True)
        assert outcomes.count(None) == 1
        assert sum(isinstance(value, InvocationPersistenceUnavailable) for value in outcomes) == 1
    run(scenario())
    assert scalar(database, "SELECT count(*) FROM invocation_stream_commit") == 1


@pytest.mark.postgres
def test_commit_racing_next_attempt_cannot_both_win(database):
    async def scenario():
        store, record, commit = await active(database)

        async def next_attempt():
            async with store.transaction() as connection:
                await connection.execute("INSERT INTO provider_attempt(call_id,number,binding_id,candidate_attempt) VALUES (%s,2,'b',1)", (record.call_id,))

        outcomes = await asyncio.gather(commit.first_delta(1, delta()), next_attempt(), return_exceptions=True)
        assert outcomes.count(None) == 1
        assert sum(isinstance(value, InvocationPersistenceUnavailable) for value in outcomes) == 1
        with psycopg.connect(database) as connection:
            committed = connection.execute("SELECT count(*) FROM invocation_stream_commit").fetchone()[0]
            attempts = connection.execute("SELECT count(*) FROM provider_attempt").fetchone()[0]
            assert (committed, attempts) in {(1, 1), (0, 2)}
    run(scenario())


@pytest.mark.postgres
@pytest.mark.parametrize("database", ["0015_prompt_assets.sql"], indirect=True)
def test_upgrade_preserves_existing_invocations_and_adds_no_fake_commit(database):
    async def scenario():
        store, record, commit = await active(database)
        with psycopg.connect(database) as connection:
            before = dict(connection.execute("SELECT version,checksum FROM gateway_schema_migration"))
        await migrate(database)
        assert scalar(database, "SELECT count(*) FROM invocation_stream_commit") == 0
        assert scalar(database, "SELECT count(*) FROM provider_attempt") == 1
        with psycopg.connect(database) as connection:
            after = dict(connection.execute("SELECT version,checksum FROM gateway_schema_migration"))
        assert len(after) == 18 and all(after[key] == value for key, value in before.items())
        await commit.first_delta(1, delta())
    run(scenario())
