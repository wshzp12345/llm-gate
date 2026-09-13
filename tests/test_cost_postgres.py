import asyncio
from dataclasses import replace
from decimal import Decimal

import psycopg
import pytest

from llm_gateway.application.configuration import ConfigurationCommands
from llm_gateway.domain.invocation import InvocationPersistenceUnavailable
from llm_gateway.domain.model import Usage
from llm_gateway.domain.recovery import RecoveryPlan, RetryPolicy
from llm_gateway.infrastructure.configuration import PostgresConfigurationUnitOfWork
from llm_gateway.infrastructure.routing_evidence import PostgresRoutingEvidence
from llm_gateway.adapters.canonical_json import canonical_bytes, canonical_digest
from tests.test_attempt_execution import CANDIDATES, FAILURE, SUCCESS, Harness
from tests.test_configuration import database, run, scalar, identity, PreparedFixture
from tests.test_invocation_postgres import setup, install_runtime_gate


KNOWN_SUCCESS = replace(SUCCESS, usage=Usage(100, 50, 0, 0))


@pytest.mark.postgres
def test_retry_attempts_have_separate_pricing_and_accrual_without_invented_usage(database):
    async def scenario():
        store, record = await setup(database)
        harness = Harness([FAILURE, FAILURE, KNOWN_SUCCESS])
        install_runtime_gate(harness, store, record)
        original = harness.complete
        async def complete(request):
            assert scalar(database, "SELECT count(*) FROM attempt_pricing") == scalar(database, "SELECT count(*) FROM provider_attempt")
            assert scalar(database, "SELECT count(*) FROM cost_accrual") == scalar(database, "SELECT count(*) FROM provider_attempt_outcome")
            return await original(request)
        harness.complete = complete
        executor = harness.executor()
        executor._journal = store.journal(record.call_id)
        assert await executor.execute(CANDIDATES, policy=RetryPolicy(), deadline=asyncio.get_running_loop().time() + 10) == KNOWN_SUCCESS
    run(scenario())
    with psycopg.connect(database) as connection:
        assert connection.execute("SELECT number,total_cost,completeness FROM cost_accrual ORDER BY number").fetchall() == [
            (1, None, "unavailable"), (2, None, "unavailable"), (3, Decimal("0.000225"), "complete")]
        assert connection.execute("SELECT count(*) FROM attempt_pricing").fetchone()[0] == 3
        assert connection.execute("SELECT state FROM model_invocation").fetchone()[0] == "running"


@pytest.mark.postgres
def test_accrual_fault_rolls_back_outcome_and_routing_event_and_stops_recovery(database):
    async def scenario():
        store, record = await setup(database)
        harness = Harness([FAILURE, KNOWN_SUCCESS])
        install_runtime_gate(harness, store, record)
        with psycopg.connect(database) as connection:
            connection.execute("ALTER TABLE cost_accrual ADD CONSTRAINT reject_test_accrual CHECK(false)")
        executor = harness.executor()
        executor._journal = store.journal(record.call_id)
        with pytest.raises(InvocationPersistenceUnavailable):
            await executor.execute(CANDIDATES, policy=RetryPolicy(), deadline=asyncio.get_running_loop().time() + 10)
        assert harness.events.count(("provider",)) == 1 and not harness.leased
    run(scenario())
    assert scalar(database, "SELECT count(*) FROM attempt_pricing") == 1
    assert scalar(database, "SELECT count(*) FROM provider_attempt_outcome") == 0
    assert scalar(database, "SELECT count(*) FROM routing_event WHERE kind='attempt_finished'") == 0
    assert scalar(database, "SELECT count(*) FROM cost_accrual") == 0


@pytest.mark.postgres
@pytest.mark.parametrize("price_changes", [{"effective_from": "2100-01-01T00:00:00Z"}, {"rates": {}}])
def test_inapplicable_pricing_fails_before_committed_attempt(database, price_changes):
    async def scenario():
        store, record = await setup(database, price_changes=price_changes)
        await PostgresRoutingEvidence(store).gate(record.call_id, "a")
        with pytest.raises(InvocationPersistenceUnavailable):
            await store.journal(record.call_id).started(1, "a", 1)
    run(scenario())
    assert scalar(database, "SELECT count(*) FROM provider_attempt") == 0
    assert scalar(database, "SELECT count(*) FROM attempt_pricing") == 0
    assert scalar(database, "SELECT state FROM model_invocation") == "accepted"


@pytest.mark.postgres
def test_new_publication_cannot_change_started_attempt_price(database):
    async def scenario():
        store, record = await setup(database)
        with psycopg.connect(database) as connection:
            snapshot = connection.execute("SELECT snapshot FROM config_revision WHERE revision=%s", (int(record.configuration_revision),)).fetchone()[0]
        commands = ConfigurationCommands(PostgresConfigurationUnitOfWork(database))
        first = await commands.publish(identity("publish"), record.configuration_revision, None,
                                       canonical_digest(snapshot), "publish", timeout_seconds=5)
        assert first.error is None
        await PostgresRoutingEvidence(store).gate(record.call_id, "a")
        journal = store.journal(record.call_id)
        await journal.started(1, "a", 1)
        snapshot["pricing_tables"]["price-a"]["rates"]["input"] = "100"
        prepared = PreparedFixture(base=record.configuration_revision)
        prepared.candidate = replace(prepared.candidate, snapshot_json=canonical_bytes(snapshot), snapshot_digest=canonical_digest(snapshot))
        new = await commands.create(identity(), record.configuration_revision, prepared, timeout_seconds=5)
        published = await commands.publish(identity("publish"), new.revision.revision, record.configuration_revision,
                                           new.revision.snapshot_digest, "publish new", timeout_seconds=5)
        assert published.error is None
        await journal.finished(1, KNOWN_SUCCESS, RecoveryPlan("stop"))
        with psycopg.connect(database) as connection:
            assert connection.execute("SELECT configuration_revision,input_rate FROM attempt_pricing").fetchone() == (int(record.configuration_revision), "1.25")
            assert connection.execute("SELECT total_cost FROM cost_accrual").fetchone()[0] == Decimal("0.000225")
    run(scenario())


@pytest.mark.postgres
def test_accrual_and_pricing_are_immutable_and_survive_evidence_retention(database):
    async def scenario():
        store, record = await setup(database)
        await PostgresRoutingEvidence(store).gate(record.call_id, "a")
        await store.journal(record.call_id).started(1, "a", 1)
        await store.journal(record.call_id).finished(1, KNOWN_SUCCESS, RecoveryPlan("stop"))
    run(scenario())
    with psycopg.connect(database) as connection:
        for query in ("UPDATE cost_accrual SET total_cost=0", "DELETE FROM cost_accrual",
                      "UPDATE attempt_pricing SET input_rate='0'", "DELETE FROM attempt_pricing"):
            with pytest.raises(psycopg.Error):
                with connection.transaction():
                    connection.execute(query)
        connection.execute("DELETE FROM routing_decision")
    assert scalar(database, "SELECT count(*) FROM cost_accrual") == 1
    assert scalar(database, "SELECT count(*) FROM attempt_pricing") == 1
