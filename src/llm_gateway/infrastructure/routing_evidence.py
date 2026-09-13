"""PostgreSQL preselection and gate checkpoints; no raw content persistence."""

from dataclasses import asdict

from psycopg.types.json import Jsonb

from llm_gateway.domain.invocation import InvocationPersistenceUnavailable
from llm_gateway.domain.routing_evidence import RoutingPreselection, ordered_reasons
from llm_gateway.domain.routing_eligibility import StaticRoutingReason, StaticServiceRequirement


async def _append_event(connection, call_id, kind, detail, *, binding_id=None, attempt_number=None):
    # Every caller holds the Invocation lock. The database additionally guards
    # sequence and terminal finality, including writes outside this repository.
    await connection.execute("""
        INSERT INTO routing_event(call_id,sequence,kind,binding_id,attempt_number,detail)
        SELECT %s,COALESCE(max(sequence),0)+1,%s,%s,%s,%s FROM routing_event WHERE call_id=%s
        """, (call_id, kind, binding_id, attempt_number, Jsonb(detail), call_id))


async def require_attempt_gate(connection, call_id, binding_id):
    cursor = await connection.execute("""
        SELECT e.kind,e.binding_id,e.detail,c.initial_order
        FROM routing_event e JOIN routing_candidate c ON c.call_id=e.call_id AND c.binding_id=%s
        WHERE e.call_id=%s ORDER BY e.sequence DESC LIMIT 1
        """, (binding_id, call_id))
    row = await cursor.fetchone()
    if row is None or row[:3] != ("attempt_gate", binding_id, {"decision": "allowed", "reasons": []}) or row[3] is None:
        raise InvocationPersistenceUnavailable()


class PostgresRoutingEvidence:
    def __init__(self, store):
        self._store = store

    async def _lock(self, connection, call_id):
        cursor = await connection.execute(
            "SELECT state FROM model_invocation WHERE call_id=%s FOR UPDATE", (call_id,))
        row = await cursor.fetchone()
        if row is None or row[0] not in {"accepted", "running"}:
            raise InvocationPersistenceUnavailable()
        return row[0]

    async def preselect(self, call_id, snapshot: RoutingPreselection):
        if not isinstance(snapshot, RoutingPreselection):
            raise ValueError("Validated routing preselection required")
        async with self._store.transaction() as connection:
            if await self._lock(connection, call_id) != "accepted":
                raise InvocationPersistenceUnavailable()
            await connection.execute("""
                INSERT INTO routing_decision(call_id,alias_digest,routing_policy,routing_policy_digest,
                    requirement_digest,seed_hex,requirement,feature_ids)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """, (call_id, snapshot.alias_digest, snapshot.routing_policy, snapshot.routing_policy_digest,
                      snapshot.requirement_digest, snapshot.seed_hex, Jsonb(asdict(snapshot.requirement)), sorted(snapshot.feature_ids)))
            for item in sorted(snapshot.candidates, key=lambda item: item.candidate.binding):
                candidate = item.candidate
                await connection.execute("""
                    INSERT INTO routing_candidate(call_id,binding_id,service_level,priority,weight,initial_order)
                    VALUES (%s,%s,%s,%s,%s,%s)
                    """, (call_id, candidate.binding, candidate.service_level, candidate.priority, candidate.weight, item.initial_order))
                await _append_event(connection, call_id, "preselection", {
                    "decision": "rejected" if item.reasons else "eligible",
                    "reasons": [asdict(reason) for reason in item.reasons],
                }, binding_id=candidate.binding)

    async def gate(self, call_id, binding_id: str, reasons: tuple[StaticRoutingReason, ...] = ()):
        """Persist the caller's actual gate result, not a substitute for checking it."""
        async with self._store.transaction() as connection:
            await self._lock(connection, call_id)
            cursor = await connection.execute("""
                SELECT d.requirement,d.feature_ids,c.initial_order FROM routing_decision d
                JOIN routing_candidate c USING (call_id) WHERE d.call_id=%s AND c.binding_id=%s
                """, (call_id, binding_id))
            row = await cursor.fetchone()
            if row is None or row[2] is None:
                raise InvocationPersistenceUnavailable()
            ordered = ordered_reasons(reasons, requirement=StaticServiceRequirement(**row[0]), feature_ids=frozenset(row[1]))
            await _append_event(connection, call_id, "attempt_gate", {
                "decision": "rejected" if ordered else "allowed", "reasons": [asdict(reason) for reason in ordered],
            }, binding_id=binding_id)
            if ordered:
                await _append_event(connection, call_id, "candidate_skipped", {
                    "decision": "skipped", "reasons": [asdict(reason) for reason in ordered],
                }, binding_id=binding_id)
