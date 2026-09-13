"""Append-only Circuit transition facts, never a runtime recovery source."""

from dataclasses import asdict

from psycopg.types.json import Jsonb

from llm_gateway.application.circuit_evidence import CircuitTransitionEvidence
from llm_gateway.domain.invocation import InvocationPersistenceUnavailable


class PostgresCircuitEvidence:
    def __init__(self, store):
        self._store = store

    async def append(self, evidence: CircuitTransitionEvidence) -> None:
        if not isinstance(evidence, CircuitTransitionEvidence):
            raise ValueError("Typed Circuit evidence required")
        expected = (evidence.binding_id, int(evidence.configuration_revision), evidence.occurred_at,
                    evidence.call_id, evidence.attempt_number, asdict(evidence.transition))
        async with self._store.transaction() as connection:
            cursor = await connection.execute("""SELECT snapshot->'provider_model_bindings' ? %s
                FROM config_revision WHERE revision=%s""", (evidence.binding_id, int(evidence.configuration_revision)))
            row = await cursor.fetchone()
            if row is None or row[0] is not True:
                raise InvocationPersistenceUnavailable()
            if evidence.call_id is not None:
                cursor = await connection.execute("SELECT configuration_revision FROM model_invocation WHERE call_id=%s", (evidence.call_id,))
                row = await cursor.fetchone()
                if row is None or row[0] != int(evidence.configuration_revision):
                    raise InvocationPersistenceUnavailable()
            if evidence.attempt_number is not None:
                cursor = await connection.execute("SELECT binding_id FROM provider_attempt WHERE call_id=%s AND number=%s",
                    (evidence.call_id, evidence.attempt_number))
                row = await cursor.fetchone()
                if row is None or row[0] != evidence.binding_id:
                    raise InvocationPersistenceUnavailable()
            await connection.execute("""
                INSERT INTO circuit_transition_evidence(event_id,binding_id,configuration_revision,occurred_at,call_id,attempt_number,transition)
                VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (event_id) DO NOTHING
                """, (evidence.event_id, *expected[:-1], Jsonb(expected[-1])))
            cursor = await connection.execute("""SELECT binding_id,configuration_revision,occurred_at,call_id,attempt_number,transition
                FROM circuit_transition_evidence WHERE event_id=%s""", (evidence.event_id,))
            if await cursor.fetchone() != expected:
                raise InvocationPersistenceUnavailable()
