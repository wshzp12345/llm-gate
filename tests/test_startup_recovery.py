from dataclasses import replace

import psycopg
import pytest

from llm_gateway.application.cancellation import CancellationCleanup, LocalCancellationReason
from llm_gateway.domain.invocation import InvocationPersistenceUnavailable
from llm_gateway.domain.recovery import RecoveryPlan
from llm_gateway.infrastructure.invocation import PostgresInvocationStore
from llm_gateway.infrastructure.local_cancellation import PostgresLocalCancellationStore
from llm_gateway.infrastructure.model_execution_owner import ModelOwnershipUnavailable, PostgresModelExecutionOwner
from llm_gateway.infrastructure.startup_recovery import PostgresStartupRecovery, SCHEMA_REVISION
from tests.test_cost_postgres import KNOWN_SUCCESS as SUCCESS
from tests.test_cancellation_settlement import started
from tests.test_configuration import database, run, scalar
from tests.test_invocation import admission
from tests.test_invocation_postgres import setup


def test_recovery_requires_owned_store_before_any_io():
    with pytest.raises(ValueError):
        PostgresStartupRecovery(PostgresInvocationStore("must not connect"), configuration_revision=1)
    owner = PostgresModelExecutionOwner("must not connect", on_loss=lambda: None)
    store = PostgresInvocationStore("must not connect", ownership=owner)
    for size in (0, 1001, True, 1.5):
        with pytest.raises(ValueError):
            PostgresStartupRecovery(store, configuration_revision=1, batch_size=size)
    for revision in (0, -1, True, "1"):
        with pytest.raises(ValueError):
            PostgresStartupRecovery(store, configuration_revision=revision)
    with pytest.raises(ModelOwnershipUnavailable):
        run(PostgresStartupRecovery(store, configuration_revision=1).run())


@pytest.mark.postgres
@pytest.mark.parametrize("prior", ["accepted", "running", "cancelling"])
def test_restart_reconciles_without_dispatch_and_repeat_start_preserves_terminal(database, prior):
    async def scenario():
        store, record = await setup(database)
        if prior != "accepted":
            await started(store, record)
        if prior == "cancelling":
            await PostgresLocalCancellationStore(store).reserve(record, LocalCancellationReason.CLIENT_DISCONNECTED)
        owner = PostgresModelExecutionOwner(database, on_loss=lambda: None)
        async with owner.hold():
            recovery = PostgresStartupRecovery(PostgresInvocationStore(database, ownership=owner),
                                              configuration_revision=int(record.configuration_revision))
            assert await recovery.run() == 1
            with pytest.raises(RuntimeError):
                await recovery.run()
            with psycopg.connect(database) as connection:
                audit = connection.execute("SELECT prior_state,owner_id,schema_revision,configuration_revision FROM invocation_restart_recovery").fetchone()
                assert audit == (prior, owner.owner_id, SCHEMA_REVISION, int(record.configuration_revision))
        next_owner = PostgresModelExecutionOwner(database, on_loss=lambda: None)
        async with next_owner.hold():
            assert await PostgresStartupRecovery(PostgresInvocationStore(database, ownership=next_owner),
                configuration_revision=int(record.configuration_revision)).run() == 0
    run(scenario())
    assert scalar(database, "SELECT state FROM model_invocation") == "uncertain"
    assert scalar(database, "SELECT validation_status FROM invocation_settlement") == "unavailable"
    assert scalar(database, "SELECT count(*) FROM invocation_restart_recovery") == 1
    assert scalar(database, "SELECT count(*) FROM invocation_cancellation_cleanup") == 0
    assert scalar(database, "SELECT extract(epoch FROM expires_at-terminal_at) FROM routing_terminal") == 2592000
    assert scalar(database, "SELECT input_tokens FROM invocation_settlement") is None
    assert scalar(database, "SELECT count(*) FROM provider_attempt") == int(prior != "accepted")
    if prior != "accepted":
        assert scalar(database, "SELECT outcome FROM provider_attempt_outcome") == "uncertain"
        assert scalar(database, "SELECT total_cost FROM cost_accrual") is None
    with psycopg.connect(database) as connection:
        with pytest.raises(psycopg.Error):
            connection.execute("DELETE FROM invocation_restart_recovery")


@pytest.mark.postgres
def test_known_provider_success_and_cost_are_not_rewritten_or_gateway_completion_invented(database):
    async def scenario():
        store, record = await setup(database)
        await started(store, record)
        await store.journal(record.call_id).finished(1, SUCCESS, RecoveryPlan("stop"))
        with psycopg.connect(database) as connection:
            facts = connection.execute("SELECT * FROM provider_attempt_outcome").fetchall()
            costs = connection.execute("SELECT * FROM cost_accrual").fetchall()
        owner = PostgresModelExecutionOwner(database, on_loss=lambda: None)
        async with owner.hold():
            assert await PostgresStartupRecovery(PostgresInvocationStore(database, ownership=owner),
                configuration_revision=int(record.configuration_revision)).run() == 1
        with psycopg.connect(database) as connection:
            assert connection.execute("SELECT * FROM provider_attempt_outcome").fetchall() == facts
            assert connection.execute("SELECT * FROM cost_accrual").fetchall() == costs
        assert scalar(database, "SELECT input_tokens FROM invocation_settlement") == SUCCESS.usage.input_tokens
    run(scenario())
    assert scalar(database, "SELECT state FROM model_invocation") == "uncertain"


@pytest.mark.postgres
def test_batched_recovery_preserves_terminal_and_excludes_current_owner_admissions(database):
    async def scenario():
        store, record = await setup(database)
        cancellation = PostgresLocalCancellationStore(store)
        await cancellation.reserve(record, LocalCancellationReason.CONTEXT_CANCELLED)
        await cancellation.finalize(record, CancellationCleanup(False, None, False))
        for _ in range(3):
            await store.admit_unkeyed_shell(replace(record, call_id=admission().call_id))
        owner = PostgresModelExecutionOwner(database, on_loss=lambda: None)
        async with owner.hold():
            owned = PostgresInvocationStore(database, ownership=owner)
            current = replace(record, call_id=admission().call_id)
            await owned.admit_unkeyed_shell(current)
            assert await PostgresStartupRecovery(owned, configuration_revision=int(record.configuration_revision),
                                                batch_size=1).run() == 3
        assert scalar(database, "SELECT count(*) FROM model_invocation WHERE state='cancelled'") == 1
        assert scalar(database, "SELECT count(*) FROM model_invocation WHERE state='accepted'") == 1
        assert scalar(database, "SELECT count(*) FROM model_invocation WHERE state='uncertain'") == 3
        assert scalar(database, "SELECT count(*) FROM provider_attempt") == 0
    run(scenario())


@pytest.mark.postgres
def test_recovery_terminal_and_audit_rollback_together(database, monkeypatch):
    from llm_gateway.infrastructure import startup_recovery
    async def scenario():
        store, record = await setup(database)
        await started(store, record)
        original = startup_recovery.finalize_interrupted
        async def fail_after_terminal(*args, **kwargs):
            await original(*args, **kwargs)
            raise InvocationPersistenceUnavailable()
        owner = PostgresModelExecutionOwner(database, on_loss=lambda: None)
        async with owner.hold():
            with monkeypatch.context() as patch:
                patch.setattr(startup_recovery, "finalize_interrupted", fail_after_terminal)
                with pytest.raises(InvocationPersistenceUnavailable):
                    await PostgresStartupRecovery(PostgresInvocationStore(database, ownership=owner),
                        configuration_revision=int(record.configuration_revision)).run()
        assert scalar(database, "SELECT state FROM model_invocation") == "running"
        for table in ("invocation_settlement", "invocation_restart_recovery", "provider_attempt_outcome", "cost_accrual", "routing_terminal"):
            assert scalar(database, f"SELECT count(*) FROM {table}") == 0
        next_owner = PostgresModelExecutionOwner(database, on_loss=lambda: None)
        async with next_owner.hold():
            assert await PostgresStartupRecovery(PostgresInvocationStore(database, ownership=next_owner),
                configuration_revision=int(record.configuration_revision)).run() == 1
    run(scenario())


@pytest.mark.postgres
@pytest.mark.parametrize("succeeded", [True, False])
def test_committed_completion_or_failure_is_preserved_byte_for_byte(database, succeeded):
    from llm_gateway.domain.model import FailureCode, ProviderFailure
    from llm_gateway.infrastructure.settlement import PostgresInvocationSettlement
    async def scenario():
        store, record = await setup(database)
        await started(store, record)
        result = SUCCESS if succeeded else ProviderFailure(FailureCode.PROVIDER_UNAVAILABLE, False)
        await store.journal(record.call_id).finished(1, result, RecoveryPlan("stop"))
        await PostgresInvocationSettlement(store, record.call_id).settle(result)
        tables = ("model_invocation", "invocation_settlement", "provider_attempt_outcome", "cost_accrual",
                  "invocation_cost_summary", "routing_event", "routing_terminal")
        with psycopg.connect(database) as connection:
            before = {table: connection.execute(f"SELECT * FROM {table}").fetchall() for table in tables}
        owner = PostgresModelExecutionOwner(database, on_loss=lambda: None)
        async with owner.hold():
            assert await PostgresStartupRecovery(PostgresInvocationStore(database, ownership=owner),
                configuration_revision=int(record.configuration_revision)).run() == 0
        with psycopg.connect(database) as connection:
            for table in tables:
                assert connection.execute(f"SELECT * FROM {table}").fetchall() == before[table]
        assert scalar(database, "SELECT count(*) FROM invocation_restart_recovery") == 0
    run(scenario())


@pytest.mark.postgres
@pytest.mark.parametrize("database", ["0013_model_execution_owner.sql"], indirect=True)
def test_old_schema_is_rejected_without_implicit_migration(database):
    from llm_gateway.domain.configuration import ConfigurationPersistenceUnavailable
    async def scenario():
        owner = PostgresModelExecutionOwner(database, on_loss=lambda: None)
        async with owner.hold():
            with pytest.raises(ConfigurationPersistenceUnavailable):
                await PostgresStartupRecovery(PostgresInvocationStore(database, ownership=owner),
                                             configuration_revision=1).run()
        assert scalar(database, "SELECT count(*) FROM gateway_schema_migration") == 13
        assert scalar(database, "SELECT to_regclass('invocation_restart_recovery')") is None
    run(scenario())


@pytest.mark.postgres
def test_absent_recovery_configuration_prevents_changes(database):
    async def scenario():
        _, record = await setup(database)
        owner = PostgresModelExecutionOwner(database, on_loss=lambda: None)
        async with owner.hold():
            with pytest.raises(InvocationPersistenceUnavailable):
                await PostgresStartupRecovery(PostgresInvocationStore(database, ownership=owner),
                    configuration_revision=int(record.configuration_revision) + 100).run()
        assert scalar(database, "SELECT state FROM model_invocation") == "accepted"
        assert scalar(database, "SELECT count(*) FROM invocation_restart_recovery") == 0
    run(scenario())


@pytest.mark.postgres
def test_recovery_uses_owner_epoch_even_when_old_admission_time_is_in_the_future(database):
    async def scenario():
        _, record = await setup(database, admit=False)
        old = PostgresModelExecutionOwner(database, on_loss=lambda: None)
        async with old.hold():
            await PostgresInvocationStore(database, ownership=old).admit_unkeyed_shell(record)
            # Simulate a restored record from a clock ahead of this process.
            # Insert a new fixture row; never bypass the immutable-row guard.
            with psycopg.connect(database) as connection:
                connection.execute("""
                    INSERT INTO model_invocation
                    SELECT * FROM jsonb_populate_record(NULL::model_invocation,
                        (SELECT to_jsonb(i) FROM model_invocation i WHERE call_id=%s)
                        || jsonb_build_object('call_id',%s::text,
                            'accepted_at',clock_timestamp()+interval '1 day'))
                    """, (record.call_id, admission().call_id))
        owner = PostgresModelExecutionOwner(database, on_loss=lambda: None)
        async with owner.hold():
            assert await PostgresStartupRecovery(PostgresInvocationStore(database, ownership=owner),
                configuration_revision=int(record.configuration_revision)).run() == 2
        assert scalar(database, "SELECT count(*) FROM model_invocation WHERE state='uncertain'") == 2
        with psycopg.connect(database) as connection:
            assert connection.execute("SELECT DISTINCT execution_owner_id FROM model_invocation").fetchall() == [(old.owner_id,)]
    run(scenario())
