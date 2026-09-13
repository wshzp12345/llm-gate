import asyncio
from dataclasses import replace

import psycopg
import pytest

from llm_gateway.infrastructure.invocation import PostgresInvocationStore
from llm_gateway.infrastructure.model_execution_owner import ModelOwnershipBusy, ModelOwnershipUnavailable, PostgresModelExecutionOwner
from tests.test_configuration import database, run, scalar
from tests.test_invocation import admission
from tests.test_invocation_postgres import setup


@pytest.mark.parametrize("timeout", [0, -1, True, float("nan"), float("inf")])
def test_owner_rejects_invalid_deadline_before_connection(timeout):
    with pytest.raises(ValueError):
        PostgresModelExecutionOwner("must not connect", on_loss=lambda: None, timeout_seconds=timeout)


def test_owner_requires_loss_handler_and_store_rejects_untyped_owner():
    with pytest.raises(ValueError):
        PostgresModelExecutionOwner("must not connect", on_loss=None)
    with pytest.raises(ValueError):
        PostgresInvocationStore("must not connect", ownership=True)


@pytest.mark.postgres
def test_owner_is_exclusive_and_successive_claims_have_immutable_history(database):
    async def scenario():
        losses = []
        first = PostgresModelExecutionOwner(database, on_loss=lambda: losses.append("first"))
        second = PostgresModelExecutionOwner(database, on_loss=lambda: losses.append("second"))
        async with first.hold():
            await first.verify()
            with pytest.raises(ModelOwnershipBusy):
                async with second.hold():
                    raise AssertionError("busy owner entered")
            assert losses == []
            async with PostgresInvocationStore(database, ownership=first).transaction() as connection:
                assert (await (await connection.execute("SELECT 1")).fetchone())[0] == 1
        assert losses == ["first"]
        with pytest.raises(ModelOwnershipUnavailable):
            await first.verify()
        with pytest.raises(RuntimeError):
            async with first.hold():
                pass
        third = PostgresModelExecutionOwner(database, on_loss=lambda: losses.append("third"))
        async with third.hold():
            assert scalar(database, "SELECT previous_owner_id FROM model_execution_ownership_event ORDER BY acquired_at DESC LIMIT 1") == first.owner_id
            assert scalar(database, "SELECT owner_id FROM model_execution_owner") == third.owner_id
        assert losses == ["first", "third"]
    run(scenario())
    assert scalar(database, "SELECT count(*) FROM model_execution_ownership_event") == 2
    with psycopg.connect(database) as connection:
        with pytest.raises(psycopg.Error):
            connection.execute("DELETE FROM model_execution_ownership_event")


@pytest.mark.postgres
def test_connection_loss_stops_admission_once_and_fences_stale_invocation_store(database):
    async def scenario():
        _, record = await setup(database, admit=False)
        losses = []
        old = PostgresModelExecutionOwner(database, on_loss=lambda: losses.append("stop"))
        async with old.hold():
            old_store = PostgresInvocationStore(database, ownership=old)
            await old_store.admit_unkeyed_shell(record)
            # Exact backend PID belongs to this fixture's own lease connection
            # inside the disposable test database, never a user process.
            with psycopg.connect(database) as connection:
                assert connection.execute("SELECT pg_terminate_backend(%s,1000)", (old._connection.info.backend_pid,)).fetchone()[0]
            for _ in range(2):
                with pytest.raises(ModelOwnershipUnavailable):
                    await old.verify()
            assert losses == ["stop"]
            new = PostgresModelExecutionOwner(database, on_loss=lambda: None)
            async with new.hold():
                with pytest.raises(ModelOwnershipUnavailable):
                    await old_store.admit_unkeyed_shell(replace(record, call_id=admission().call_id))
                await PostgresInvocationStore(database, ownership=new).admit_unkeyed_shell(
                    replace(record, call_id=admission().call_id))
            assert losses == ["stop"]
    run(scenario())
    assert scalar(database, "SELECT count(*) FROM model_invocation") == 2


@pytest.mark.postgres
def test_takeover_cannot_cross_an_already_authorized_invocation_transaction(database):
    async def scenario():
        _, record = await setup(database, admit=False)
        old = PostgresModelExecutionOwner(database, on_loss=lambda: None)
        async with old.hold():
            store = PostgresInvocationStore(database, ownership=old)
            async with store.transaction() as writing:
                await store.insert_shell(writing, record)
                with psycopg.connect(database) as connection:
                    assert connection.execute("SELECT pg_terminate_backend(%s,1000)", (old._connection.info.backend_pid,)).fetchone()[0]
                contender = PostgresModelExecutionOwner(database, on_loss=lambda: None, timeout_seconds=.1)
                with pytest.raises(ModelOwnershipUnavailable) as failure:
                    async with contender.hold():
                        raise AssertionError("takeover crossed active epoch guard")
                assert not isinstance(failure.value, ModelOwnershipBusy)
                assert scalar(database, "SELECT owner_id FROM model_execution_owner") == old.owner_id
            assert scalar(database, "SELECT count(*) FROM model_invocation") == 1
        new = PostgresModelExecutionOwner(database, on_loss=lambda: None)
        async with new.hold():
            await new.verify()
            assert scalar(database, "SELECT owner_id FROM model_execution_owner") == new.owner_id
    run(scenario())
    assert scalar(database, "SELECT count(*) FROM model_execution_ownership_event") == 2


@pytest.mark.postgres
@pytest.mark.parametrize("database", ["0012_late_provider_outcome.sql"], indirect=True)
def test_ownership_never_migrates_old_schema_on_start(database):
    async def scenario():
        owner = PostgresModelExecutionOwner(database, on_loss=lambda: None)
        with pytest.raises(ModelOwnershipUnavailable):
            async with owner.hold():
                pass
    run(scenario())
    assert scalar(database, "SELECT to_regclass('model_execution_owner')") is None
    assert scalar(database, "SELECT count(*) FROM gateway_schema_migration") == 12
