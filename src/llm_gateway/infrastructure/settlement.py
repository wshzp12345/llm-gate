"""Atomic synchronous text-result settlement from durable Attempt evidence.

No content persistence or HTTP response production. Cancelled/uncertain and
pre-Attempt outcomes require their own lifecycle paths, not fabricated success.
"""

from psycopg.rows import dict_row

from llm_gateway.domain.invocation import InvocationPersistenceUnavailable, InvocationTerminalConflict
from llm_gateway.domain.model import FailureCode, ProviderFailure, ProviderResult, Usage
from llm_gateway.infrastructure.routing_evidence import _append_event
from llm_gateway.infrastructure.cost import summarize_invocation_cost
from llm_gateway.infrastructure.unattempted_settlement import settle_unattempted_route


class PostgresInvocationSettlement:
    def __init__(self, store, call_id):
        self._store, self._call_id = store, call_id

    async def settle_exhausted(self):
        return await settle_unattempted_route(self._store, self._call_id)

    async def settle(self, result: ProviderResult | ProviderFailure) -> Usage:
        if not isinstance(result, (ProviderResult, ProviderFailure)):
            raise ValueError("Typed terminal result required")
        success = isinstance(result, ProviderResult)
        refused = success and result.disposition == "safety_refused"
        if not success and not isinstance(result.code, FailureCode):
            raise ValueError("Stable Gateway failure required")
        async with self._store.transaction() as connection:
            async with connection.cursor(row_factory=dict_row) as cursor:
                await cursor.execute("""SELECT state,requested_model,routing_evidence_ttl_seconds
                    FROM model_invocation WHERE call_id=%s FOR UPDATE""", (self._call_id,))
                invocation = await cursor.fetchone()
                if invocation is None:
                    raise InvocationPersistenceUnavailable()
                if invocation["state"] not in {"accepted", "running"}:
                    raise InvocationTerminalConflict()
                ttl = invocation["routing_evidence_ttl_seconds"]
                if ttl is None:
                    raise InvocationPersistenceUnavailable()
                await cursor.execute("""
                    SELECT a.number,o.outcome,o.error_code,o.resolved_model,o.finish_reason,o.input_tokens,
                        o.output_tokens,o.cached_tokens,o.reasoning_tokens,o.provider_reported_total,
                        c.number AS cost_number,r.service_level,o.safety_refused
                    FROM provider_attempt a
                    LEFT JOIN provider_attempt_outcome o USING(call_id,number)
                    LEFT JOIN cost_accrual c USING(call_id,number)
                    LEFT JOIN routing_candidate r ON r.call_id=a.call_id AND r.binding_id=a.binding_id
                    WHERE a.call_id=%s ORDER BY a.number
                    """, (self._call_id,))
                attempts = await cursor.fetchall()
            if (not attempts or len(attempts) > 3 or any(row["number"] != index for index, row in enumerate(attempts, 1))
                    or any(row["outcome"] not in {"succeeded", "failed", "uncertain"} or row["cost_number"] is None
                           or row["service_level"] is None for row in attempts)):
                raise InvocationPersistenceUnavailable()
            last = attempts[-1]
            if any(row["outcome"] == "uncertain" for row in attempts[:-1]):
                raise InvocationPersistenceUnavailable()
            if success:
                usage = Usage(*(last[key] for key in ("input_tokens", "output_tokens", "cached_tokens", "reasoning_tokens", "provider_reported_total")))
                matches = (last["outcome"] == "succeeded" and result.requested_model == invocation["requested_model"]
                           and result.resolved_model == last["resolved_model"] and result.finish_reason == last["finish_reason"]
                           and result.usage == usage)
                matches = matches and last["safety_refused"] == refused
            else:
                matches = last["outcome"] == ("uncertain" if result.code == FailureCode.UNCERTAIN else "failed") and last["error_code"] == result.code
                observed = Usage(*(last[key] for key in (
                    "input_tokens", "output_tokens", "cached_tokens", "reasoning_tokens", "provider_reported_total")))
                matches = (matches and last["resolved_model"] == result.observed_model
                           and observed == (result.observed_usage or Usage()))
            if not matches:
                raise InvocationPersistenceUnavailable()
            outcome, error = ("completed", None) if success else ("uncertain" if result.code == FailureCode.UNCERTAIN else "failed", result.code)
            service_level = last["service_level"] if success else None
            # Unknown measurements poison the aggregate instead of turning the
            # sum of known measurements into an allegedly complete total.
            def total(field):
                values = [row[field] for row in attempts]
                return sum(values) if all(value is not None for value in values) else None
            cursor = await connection.execute("""
                INSERT INTO invocation_settlement(call_id,outcome,error_code,resolved_model,finish_reason,service_level,
                    attempt_count,input_tokens,output_tokens,cached_tokens,reasoning_tokens,validation_status,safety_refused)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'not_requested',%s) RETURNING terminal_at
                """, (self._call_id, outcome, error, last["resolved_model"],
                      last["finish_reason"] if success else None, service_level, len(attempts),
                      *(total(field) for field in ("input_tokens", "output_tokens", "cached_tokens", "reasoning_tokens")), refused))
            terminal_at = (await cursor.fetchone())[0]
            await summarize_invocation_cost(connection, self._call_id)
            await _append_event(connection, self._call_id, "routing_terminal", {
                "outcome": outcome, "error_code": error, "service_level": service_level,
                "degradation_stage": "terminal" if not success else "reduced-service" if service_level == "reduced" else "none",
                **({"disposition": "safety_refused"} if refused else {}),
            })
            await connection.execute("""
                INSERT INTO routing_terminal(call_id,sequence,terminal_at,expires_at)
                SELECT call_id,sequence,%s,%s::timestamptz + %s * interval '1 second'
                FROM routing_event WHERE call_id=%s AND kind='routing_terminal'
                """, (terminal_at, terminal_at, ttl, self._call_id))
            await connection.execute("UPDATE model_invocation SET state=%s WHERE call_id=%s", (outcome, self._call_id))
            aggregate_usage = Usage(*(total(field) for field in (
                "input_tokens", "output_tokens", "cached_tokens", "reasoning_tokens", "provider_reported_total")))
        # Returning outside the transaction context is essential: commit may fail.
        return aggregate_usage
