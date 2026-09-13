"""Append-only late completion metadata; no terminal or Cost Ledger mutation."""

from uuid import UUID

from llm_gateway.application.late_provider_outcome import LateTextOutcome
from llm_gateway.domain.invocation import InvocationPersistenceUnavailable


class PostgresLateProviderOutcome:
    def __init__(self, store, call_id):
        if not isinstance(call_id, UUID) or call_id.version != 4:
            raise ValueError("Invocation UUID required")
        self._store, self._call_id = store, call_id

    async def record(self, outcome):
        if not isinstance(outcome, LateTextOutcome):
            raise ValueError("Content-free late observation required")
        values = (outcome.attempt_number, outcome.outcome, outcome.error_code, outcome.requested_model,
                  outcome.resolved_model, outcome.finish_reason, outcome.usage.input_tokens,
                  outcome.usage.output_tokens, outcome.usage.cached_tokens, outcome.usage.reasoning_tokens,
                  outcome.usage.provider_reported_total, outcome.safety_refused)
        async with self._store.transaction() as connection:
            cursor = await connection.execute("""
                SELECT state,requested_model FROM model_invocation WHERE call_id=%s FOR UPDATE
                """, (self._call_id,))
            invocation = await cursor.fetchone()
            if (invocation is None or invocation[0] not in {"cancelling", "cancelled", "uncertain"}
                    or outcome.requested_model is not None and outcome.requested_model != invocation[1]):
                raise InvocationPersistenceUnavailable()
            cursor = await connection.execute("""
                SELECT number,outcome,error_code,requested_model,resolved_model,finish_reason,input_tokens,
                    output_tokens,cached_tokens,reasoning_tokens,provider_reported_total,safety_refused
                FROM late_provider_outcome WHERE call_id=%s AND event_id=%s
                """, (self._call_id, outcome.event_id))
            existing = await cursor.fetchone()
            if existing is not None:
                if existing != values:
                    raise InvocationPersistenceUnavailable()
            else:
                await connection.execute("""
                    INSERT INTO late_provider_outcome(call_id,event_id,observed_state,number,outcome,error_code,
                        requested_model,resolved_model,finish_reason,input_tokens,output_tokens,cached_tokens,
                        reasoning_tokens,provider_reported_total,safety_refused,sequence)
                    SELECT %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,COALESCE(max(sequence),0)+1
                    FROM late_provider_outcome WHERE call_id=%s AND number=%s
                    """, (self._call_id, outcome.event_id, invocation[0], *values, self._call_id, outcome.attempt_number))
