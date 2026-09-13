import asyncio
from dataclasses import replace

import psycopg
import pytest

from llm_gateway.application.provider_circuits import ProviderCircuits
from llm_gateway.domain.invocation import InvocationPersistenceUnavailable
from llm_gateway.infrastructure.circuit_evidence import PostgresCircuitEvidence
from llm_gateway.infrastructure.migrate import migrate, verify_schema
from llm_gateway.infrastructure.routing_evidence import PostgresRoutingEvidence
from tests.test_circuit_evidence import event
from tests.test_configuration import database, run, scalar
from tests.test_invocation_postgres import setup


@pytest.mark.postgres
def test_transition_is_immutable_idempotent_and_not_restored_on_restart(database):
    async def scenario():
        store, invocation = await setup(database)
        evidence = event(invocation.configuration_revision, call_id=invocation.call_id)
        sink = PostgresCircuitEvidence(store)
        await asyncio.gather(sink.append(evidence), sink.append(evidence))
        assert scalar(database, "SELECT count(*) FROM circuit_transition_evidence") == 1
        with pytest.raises(InvocationPersistenceUnavailable):
            await sink.append(replace(evidence, transition=replace(evidence.transition, observed_at=43)))
        assert scalar(database, "SELECT transition->>'observed_at' FROM circuit_transition_evidence") == "42.0"
        assert ProviderCircuits().acquire("a", 0).permit is not None
    run(scenario())
    for statement in ("UPDATE circuit_transition_evidence SET binding_id='b'", "DELETE FROM circuit_transition_evidence"):
        with pytest.raises(psycopg.Error), psycopg.connect(database) as connection:
            connection.execute(statement)


@pytest.mark.postgres
@pytest.mark.parametrize("fault", ["binding", "attempt_binding", "write"])
def test_invalid_reference_or_write_failure_persists_no_event(database, fault):
    async def scenario():
        store, invocation = await setup(database)
        value = event(invocation.configuration_revision, call_id=invocation.call_id)
        if fault == "binding":
            value = replace(value, binding_id="missing")
        elif fault == "attempt_binding":
            await PostgresRoutingEvidence(store).gate(invocation.call_id, "b")
            await store.journal(invocation.call_id).started(1, "b", 1)
            value = replace(value, attempt_number=1)
        else:
            with psycopg.connect(database) as connection:
                connection.execute("ALTER TABLE circuit_transition_evidence ADD CONSTRAINT reject_test_event CHECK(false)")
        with pytest.raises(InvocationPersistenceUnavailable):
            await PostgresCircuitEvidence(store).append(value)
    run(scenario())
    assert scalar(database, "SELECT count(*) FROM circuit_transition_evidence") == 0


@pytest.mark.postgres
def test_attempt_correlation_and_configuration_identity_are_preserved(database):
    async def scenario():
        store, invocation = await setup(database)
        await PostgresRoutingEvidence(store).gate(invocation.call_id, "a")
        await store.journal(invocation.call_id).started(1, "a", 1)
        value = event(invocation.configuration_revision, call_id=invocation.call_id, attempt_number=1)
        await PostgresCircuitEvidence(store).append(value)
        with psycopg.connect(database) as connection:
            row = connection.execute("SELECT call_id,attempt_number,configuration_revision FROM circuit_transition_evidence").fetchone()
        assert row == (invocation.call_id, 1, int(invocation.configuration_revision))
    run(scenario())


@pytest.mark.postgres
@pytest.mark.parametrize("database", ["0005_invocation_settlement.sql"], indirect=True)
def test_upgrade_preserves_prior_checksums(database):
    with psycopg.connect(database) as connection:
        before = dict(connection.execute("SELECT version,checksum FROM gateway_schema_migration").fetchall())
    run(migrate(database))
    run(verify_schema(database, expected_version="0018_invocation_correlation.sql", timeout_seconds=3))
    with psycopg.connect(database) as connection:
        after = dict(connection.execute("SELECT version,checksum FROM gateway_schema_migration").fetchall())
    assert len(after) == 18 and all(after[version] == checksum for version, checksum in before.items())
    assert scalar(database, "SELECT count(*) FROM circuit_transition_evidence") == 0
