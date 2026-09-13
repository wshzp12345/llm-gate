from contextlib import asynccontextmanager
from dataclasses import replace
from uuid import uuid4

import psycopg
import pytest

from llm_gateway.domain.correlation import BusinessCorrelation
from llm_gateway.infrastructure.invocation_observation import PostgresInvocationObservation
from llm_gateway.infrastructure.migrate import migrate
from tests.test_configuration import database, run, scalar
from tests.test_fingerprint_admission_postgres import prepare, admit


@pytest.mark.parametrize("value", ["", "a b", "a\n", "中文", "a" * 129, 1, True])
def test_domain_rejects_invalid_correlation_without_http(value):
    with pytest.raises(ValueError):
        BusinessCorrelation(task_id=value)


@pytest.mark.postgres
def test_correlation_is_atomic_immutable_and_returned_by_observation(database):
    async def scenario():
        store, record = await prepare(database)
        correlation = BusinessCorrelation("task.1", "turn:2", "step_3")
        record = replace(record, correlation=correlation)
        @asynccontextmanager
        async def reject_at_commit(fence):
            calls = 0
            def check(actual):
                nonlocal calls
                calls += 1
                if calls >= 2:
                    raise RuntimeError("revoked")
            yield check
        with pytest.raises(RuntimeError):
            await admit(store, record, security_scope=reject_at_commit)
        assert scalar(database, "SELECT count(*) FROM model_invocation") == 0
        await admit(store, record)
        result = await PostgresInvocationObservation(database).read(record.call_id)
        assert result.correlation == correlation
        with psycopg.connect(database, autocommit=True) as connection:
            for column in ("task_id", "turn_id", "step_id"):
                with pytest.raises(psycopg.Error):
                    connection.execute(f"UPDATE model_invocation SET {column}='other',state='running' WHERE call_id=%s", (record.call_id,))
            for value in ("", "a b", "a\n", "中文", "x" * 129):
                with pytest.raises(psycopg.Error):
                    connection.execute("""INSERT INTO model_invocation
                        SELECT (jsonb_populate_record(NULL::model_invocation,
                            to_jsonb(i) || jsonb_build_object('call_id',%s::text,'task_id',%s::text))).*
                        FROM model_invocation i WHERE call_id=%s""", (str(uuid4()), value, record.call_id))
        assert (await PostgresInvocationObservation(database).read(record.call_id)).correlation == correlation
    run(scenario())


@pytest.mark.postgres
@pytest.mark.parametrize("database", ["0017_late_stream_observation.sql"], indirect=True)
def test_upgrade_retains_every_old_field_and_does_not_invent_business_ids(database):
    async def scenario():
        store, record = await prepare(database)
        await admit(store, record)
        with psycopg.connect(database) as connection:
            before = connection.execute("SELECT * FROM model_invocation").fetchone()
        await migrate(database)
        with psycopg.connect(database) as connection:
            after = connection.execute("SELECT * FROM model_invocation").fetchone()
        assert after[:len(before)] == before and after[len(before):] == (None, None, None)
        assert (await PostgresInvocationObservation(database).read(record.call_id)).correlation == BusinessCorrelation()
    run(scenario())
