"""Commit a supported exhausted route from complete durable rejection facts."""

from llm_gateway.domain.invocation import InvocationPersistenceUnavailable, InvocationTerminalConflict
from llm_gateway.domain.routing_evidence import ordered_reasons
from llm_gateway.domain.routing_eligibility import StaticRoutingReason, StaticServiceRequirement
from llm_gateway.domain.routing_failure import classify_unattempted_rejections
from llm_gateway.infrastructure.routing_evidence import _append_event


async def settle_unattempted_route(store, call_id):
    async with store.transaction() as connection:
        cursor = await connection.execute("""SELECT i.state,i.routing_evidence_ttl_seconds,
                c.snapshot->'model_aliases'->i.requested_model->'candidates'
            FROM model_invocation i JOIN config_revision c ON c.revision=i.configuration_revision
            WHERE i.call_id=%s FOR UPDATE OF i""", (call_id,))
        invocation = await cursor.fetchone()
        if invocation is None:
            raise InvocationPersistenceUnavailable()
        state, ttl, configured_candidates = invocation
        if state not in {"accepted", "running"}:
            raise InvocationTerminalConflict()
        cursor = await connection.execute("SELECT count(*) FROM provider_attempt WHERE call_id=%s", (call_id,))
        if state != "accepted" or ttl is None or (await cursor.fetchone())[0] != 0:
            raise InvocationPersistenceUnavailable()
        cursor = await connection.execute("SELECT requirement,feature_ids FROM routing_decision WHERE call_id=%s", (call_id,))
        header = await cursor.fetchone()
        if header is None:
            raise InvocationPersistenceUnavailable()
        cursor = await connection.execute("""
            SELECT c.binding_id,c.initial_order,e.kind,e.detail FROM routing_candidate c
            LEFT JOIN LATERAL (SELECT kind,detail FROM routing_event
                WHERE call_id=c.call_id AND binding_id=c.binding_id ORDER BY sequence DESC LIMIT 1) e ON true
            WHERE c.call_id=%s ORDER BY c.binding_id
            """, (call_id,))
        rows = await cursor.fetchall()
        static, runtime = [], []
        try:
            if not isinstance(configured_candidates, list) or sorted(item["binding"] for item in configured_candidates) != sorted(row[0] for row in rows):
                raise ValueError()
            requirement = StaticServiceRequirement(**header[0])
            for binding_id, initial_order, kind, detail in rows:
                expected = ("preselection", "rejected") if initial_order is None else ("candidate_skipped", "skipped")
                if kind != expected[0] or detail["decision"] != expected[1] or not detail["reasons"]:
                    raise ValueError()
                reasons = tuple(StaticRoutingReason(**reason) for reason in detail["reasons"])
                if ordered_reasons(reasons, requirement=requirement, feature_ids=frozenset(header[1])) != reasons:
                    raise ValueError()
                (static if initial_order is None else runtime).append(frozenset(reason.code for reason in reasons))
            result = classify_unattempted_rejections(static=tuple(static), runtime=tuple(runtime))
        except (KeyError, TypeError, ValueError):
            raise InvocationPersistenceUnavailable() from None
        cursor = await connection.execute("""
            INSERT INTO invocation_settlement(call_id,outcome,error_code,attempt_count,validation_status)
            VALUES (%s,'failed',%s,0,'not_requested') RETURNING terminal_at
            """, (call_id, result.code))
        terminal_at = (await cursor.fetchone())[0]
        # No Attempt means no Provider Usage measurement, price lock, accrual or
        # invented currency/zero-cost row. attempt_count=0 is the execution fact.
        await _append_event(connection, call_id, "routing_terminal", {
            "outcome": "failed", "error_code": result.code, "service_level": None, "degradation_stage": "terminal",
        })
        await connection.execute("""
            INSERT INTO routing_terminal(call_id,sequence,terminal_at,expires_at)
            SELECT call_id,sequence,%s,%s::timestamptz + %s * interval '1 second'
            FROM routing_event WHERE call_id=%s AND kind='routing_terminal'
            """, (terminal_at, terminal_at, ttl, call_id))
        await connection.execute("UPDATE model_invocation SET state='failed' WHERE call_id=%s", (call_id,))
    return result
