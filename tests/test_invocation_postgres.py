import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import psycopg
import pytest

from llm_gateway.application.configuration import ConfigurationCommands
from llm_gateway.domain.invocation import InvocationAuthorizationExpired, InvocationPersistenceUnavailable
from llm_gateway.domain.recovery import RecoveryPlan
from llm_gateway.infrastructure.configuration import PostgresConfigurationUnitOfWork
from llm_gateway.infrastructure.invocation import PostgresInvocationStore
from llm_gateway.infrastructure.migrate import migrate, verify_schema
from llm_gateway.infrastructure.routing_evidence import PostgresRoutingEvidence
from llm_gateway.infrastructure.safety_refusal import lock_safety_policy
from tests.test_attempt_execution import CANDIDATES, FAILURE, SUCCESS, Harness
from tests.test_configuration import database, run, scalar, identity, PreparedFixture
from tests.test_invocation import admission
from tests.test_routing_evidence import preselection
from llm_gateway.adapters.canonical_json import canonical_bytes, canonical_digest
from tests.config_fixtures import bundle_submission


async def setup(database, *, price_changes=None, secondary_currency=None, admit=True):
    commands = ConfigurationCommands(PostgresConfigurationUnitOfWork(database))
    prepared = PreparedFixture()
    # Deliberately scoped storage fixture, not a complete production Bundle.
    content = {"provider_model_bindings": {name: {"pricing_table": "price-a", "upstream_model": "model",
                                                "provider": "provider-b" if name == "b" else "provider-a",
                                                "limits": {"max_output_tokens": 4096}} for name in ("a", "b", "c")},
               "providers": {name: {"rate_limit": {"max_concurrency": 1, "qps": 2, "burst": 2}} for name in ("provider-a", "provider-b")},
               "pricing_tables": bundle_submission()["bundle"]["pricing_tables"],
               "resource_policies": {"routing_evidence_ttl_seconds": 2592000},
               "safety_policies": {"safety-a": {"mode": "provider_refusal_terminal"}},
               "model_aliases": {"general": {"safety_policy": "safety-a", "candidates": [{"binding": name} for name in ("a", "b", "c")],
                   "generation_defaults": {"max_output_tokens": 4096, "temperature": 1.0, "top_p": 1.0}}}}
    content["pricing_tables"]["price-a"]["effective_from"] = "2000-01-01T00:00:00Z"
    if price_changes:
        content["pricing_tables"]["price-a"].update(price_changes)
    if secondary_currency:
        content["pricing_tables"]["price-b"] = {**content["pricing_tables"]["price-a"], "currency": secondary_currency}
        content["provider_model_bindings"]["b"]["pricing_table"] = "price-b"
    prepared.candidate = replace(prepared.candidate, snapshot_json=canonical_bytes(content), snapshot_digest=canonical_digest(content))
    outcome = await commands.create(identity(), None, prepared, timeout_seconds=5)
    record = admission(outcome.revision.revision)
    store = PostgresInvocationStore(database)
    if admit:
        await store.admit_unkeyed_shell(record)
        async with store.transaction() as connection:
            await lock_safety_policy(connection, record.call_id)
        await PostgresRoutingEvidence(store).preselect(record.call_id, preselection())
    return store, record


def install_runtime_gate(harness, store, record):
    original = harness.acquire
    @asynccontextmanager
    async def acquire(binding_id):
        async with original(binding_id) as provider:
            await PostgresRoutingEvidence(store).gate(record.call_id, binding_id)
            yield provider
    harness.acquire = acquire


@pytest.mark.postgres
def test_execution_uses_committed_checkpoints_without_raw_content(database):
    async def scenario():
        store, record = await setup(database)
        journal = store.journal(record.call_id)
        harness = Harness([FAILURE, FAILURE, SUCCESS])
        install_runtime_gate(harness, store, record)
        executor = harness.executor()
        executor._journal = journal
        original = harness.complete
        async def complete(request):
            # Independent connection proves the start transaction already committed.
            with psycopg.connect(database) as connection:
                count = connection.execute("SELECT count(*) FROM provider_attempt").fetchone()[0]
                outcomes = connection.execute("SELECT count(*) FROM provider_attempt_outcome").fetchone()[0]
                assert count == outcomes + 1
            return await original(request)
        harness.complete = complete
        from llm_gateway.domain.recovery import RetryPolicy
        result = await executor.execute(CANDIDATES, policy=RetryPolicy(), deadline=asyncio.get_running_loop().time() + 10)
        assert result == SUCCESS
    run(scenario())
    assert scalar(database, "SELECT count(*) FROM provider_attempt") == 3
    assert scalar(database, "SELECT count(*) FROM provider_attempt_outcome") == 3
    assert scalar(database, "SELECT state FROM model_invocation") == "running"  # Not terminal settlement.
    with psycopg.connect(database) as connection:
        outcome = connection.execute("SELECT to_jsonb(o) FROM provider_attempt_outcome o WHERE number=3").fetchone()[0]
        assert outcome["input_tokens"] is None and outcome["output_tokens"] is None
        assert "hello" not in str(outcome)


@pytest.mark.postgres
def test_expired_authorization_creates_no_shell(database):
    async def scenario():
        store, record = await setup(database)
        expired = replace(record, call_id=admission().call_id,
                          authorization=replace(record.authorization, expires_at=datetime.now(timezone.utc) - timedelta(seconds=1)))
        with pytest.raises(InvocationAuthorizationExpired):
            await store.admit_unkeyed_shell(expired)
    run(scenario())
    assert scalar(database, "SELECT count(*) FROM model_invocation") == 1
    assert scalar(database, "SELECT count(*) FROM provider_attempt") == 0


@pytest.mark.postgres
def test_missing_outcome_and_duplicate_start_cannot_authorize_more_io(database):
    async def scenario():
        store, record = await setup(database)
        journal = store.journal(record.call_id)
        await PostgresRoutingEvidence(store).gate(record.call_id, "a")
        results = await asyncio.gather(journal.started(1, "a", 1), journal.started(1, "a", 1), return_exceptions=True)
        assert sum(result is None for result in results) == 1
        assert sum(isinstance(result, InvocationPersistenceUnavailable) for result in results) == 1
        with pytest.raises(InvocationPersistenceUnavailable):
            await journal.started(2, "a", 2)
        with pytest.raises(InvocationPersistenceUnavailable):
            await journal.started(2, "b", 1)
    run(scenario())
    assert scalar(database, "SELECT count(*) FROM provider_attempt") == 1


@pytest.mark.postgres
def test_stop_outcome_cannot_be_rewritten_or_followed(database):
    async def scenario():
        store, record = await setup(database)
        journal = store.journal(record.call_id)
        await PostgresRoutingEvidence(store).gate(record.call_id, "a")
        await journal.started(1, "a", 1)
        await journal.finished(1, SUCCESS, RecoveryPlan("stop"))
        with pytest.raises(InvocationPersistenceUnavailable):
            await journal.finished(1, FAILURE, RecoveryPlan("retry", 200))
        with pytest.raises(InvocationPersistenceUnavailable):
            await journal.started(2, "b", 1)
    run(scenario())
    with psycopg.connect(database) as connection:
        for table in ("provider_attempt", "provider_attempt_outcome"):
            with pytest.raises(psycopg.Error):
                with connection.transaction():
                    connection.execute(psycopg.sql.SQL("DELETE FROM {}").format(psycopg.sql.Identifier(table)))
    assert scalar(database, "SELECT count(*) FROM provider_attempt_outcome") == 1


@pytest.mark.postgres
def test_checkpoint_failure_stops_executor_after_possible_provider_work(database):
    async def scenario():
        store, record = await setup(database)
        harness = Harness([FAILURE, SUCCESS])
        install_runtime_gate(harness, store, record)
        executor = harness.executor()
        executor._journal = store.journal(record.call_id)
        original = harness.complete
        async def complete(request):
            result = await original(request)
            # Fault only in the disposable fixture schema; the failed insert rolls back.
            with psycopg.connect(database) as connection:
                connection.execute("ALTER TABLE provider_attempt_outcome ADD CONSTRAINT reject_test_write CHECK (false)")
            return result
        harness.complete = complete
        from llm_gateway.domain.recovery import RetryPolicy
        with pytest.raises(InvocationPersistenceUnavailable):
            await executor.execute(CANDIDATES, policy=RetryPolicy(), deadline=asyncio.get_running_loop().time() + 10)
        assert harness.events.count(("provider",)) == 1
        assert not harness.leased
    run(scenario())
    assert scalar(database, "SELECT count(*) FROM provider_attempt") == 1
    assert scalar(database, "SELECT count(*) FROM provider_attempt_outcome") == 0


@pytest.mark.postgres
@pytest.mark.parametrize("database", ["0001_configuration.sql"], indirect=True)
def test_upgrade_preserves_existing_configuration_and_original_checksum(database):
    async def scenario():
        commands = ConfigurationCommands(PostgresConfigurationUnitOfWork(database))
        await commands.create(identity(), None, PreparedFixture(), timeout_seconds=5)
    run(scenario())
    with psycopg.connect(database) as connection:
        before = connection.execute("SELECT to_jsonb(r) FROM config_revision r").fetchall()
        original = connection.execute("SELECT checksum FROM gateway_schema_migration").fetchone()[0]
    run(migrate(database))
    run(migrate(database))
    run(verify_schema(database, expected_version="0018_invocation_correlation.sql", timeout_seconds=3))
    with psycopg.connect(database) as connection:
        assert connection.execute("SELECT to_jsonb(r) FROM config_revision r").fetchall() == before
        assert connection.execute("SELECT checksum FROM gateway_schema_migration WHERE version='0001_configuration.sql'").fetchone()[0] == original
    assert scalar(database, "SELECT count(*) FROM gateway_schema_migration") == 18
    assert scalar(database, "SELECT count(*) FROM model_invocation") == 0


@pytest.mark.postgres
def test_checkpoint_lock_timeout_leaves_no_attempt(database):
    async def scenario():
        store, record = await setup(database)
        await PostgresRoutingEvidence(store).gate(record.call_id, "a")
        bounded_store = PostgresInvocationStore(database, checkpoint_timeout_seconds=0.05)
        async with await psycopg.AsyncConnection.connect(database) as blocker:
            await blocker.execute("SELECT call_id FROM model_invocation WHERE call_id=%s FOR UPDATE", (record.call_id,))
            with pytest.raises(InvocationPersistenceUnavailable):
                await bounded_store.journal(record.call_id).started(1, "a", 1)
        await store.journal(record.call_id).started(1, "a", 1)
    run(scenario())
    assert scalar(database, "SELECT count(*) FROM provider_attempt") == 1
