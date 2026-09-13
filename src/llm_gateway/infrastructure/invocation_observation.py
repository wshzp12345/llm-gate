"""Read-only, bounded projection; never changes settlement or reprices late facts."""

from uuid import UUID
from psycopg.rows import dict_row

from llm_gateway.application.invocation_observation import InvocationObservation, ObservedAttempt, ObservedCost
from llm_gateway.domain.model import Usage
from llm_gateway.domain.correlation import BusinessCorrelation
from llm_gateway.infrastructure.invocation import PostgresInvocationStore


def _usage(row, prefix=""):
    return Usage(*(None if row[prefix + field] is None else int(row[prefix + field])
                   for field in ("input_tokens", "output_tokens", "cached_tokens", "reasoning_tokens")),
                 provider_reported_total=(int(row[prefix + "provider_reported_total"])
                     if row.get(prefix + "provider_reported_total") is not None else None))


def _model(*values):
    known = {value for value in values if value is not None}
    return (next(iter(known)) if len(known) == 1 else None, len(known) > 1)


class PostgresInvocationObservation:
    def __init__(self, dsn, *, timeout_seconds=5):
        # Separate read resources: no model owner guard, Attempt lease or
        # invocation deadline is inherited by telemetry enrichment.
        self._store = PostgresInvocationStore(dsn, checkpoint_timeout_seconds=timeout_seconds)

    async def read(self, call_id):
        if not isinstance(call_id, UUID):
            raise ValueError("Invocation UUID required")
        async with self._store.transaction() as connection:
            await connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            async with connection.cursor(row_factory=dict_row) as cursor:
                await cursor.execute("""
                    SELECT i.trace_id,i.state,i.task_id,i.turn_id,i.step_id,s.outcome,s.error_code,s.resolved_model,
                           s.input_tokens,s.output_tokens,s.cached_tokens,s.reasoning_tokens
                    FROM model_invocation i LEFT JOIN invocation_settlement s USING(call_id)
                    WHERE i.call_id=%s
                    """, (call_id,))
                invocation = await cursor.fetchone()
                if invocation is None:
                    return None
                await cursor.execute("""
                    SELECT a.number,a.binding_id,a.candidate_attempt,o.outcome,o.recovery_action,
                        o.resolved_model,o.input_tokens,o.output_tokens,o.cached_tokens,o.reasoning_tokens,o.provider_reported_total,
                        c.resolved_model AS committed_model,l.resolved_model AS late_model,
                        l.sequence AS late_sequence,l.input_tokens AS late_input_tokens,
                        l.output_tokens AS late_output_tokens,l.cached_tokens AS late_cached_tokens,
                        l.reasoning_tokens AS late_reasoning_tokens,l.provider_reported_total AS late_provider_reported_total,
                        p.configuration_revision,p.pricing_resource_id
                    FROM provider_attempt a
                    LEFT JOIN provider_attempt_outcome o USING(call_id,number)
                    LEFT JOIN invocation_stream_commit c USING(call_id,number)
                    LEFT JOIN attempt_pricing p USING(call_id,number)
                    LEFT JOIN LATERAL (
                        SELECT sequence,resolved_model,input_tokens,output_tokens,cached_tokens,reasoning_tokens,provider_reported_total
                        FROM late_provider_outcome WHERE call_id=a.call_id AND number=a.number
                        ORDER BY sequence DESC LIMIT 1
                    ) l ON true
                    WHERE a.call_id=%s ORDER BY a.number LIMIT 3
                    """, (call_id,))
                attempts = []
                for row in await cursor.fetchall():
                    actual, conflict = _model(row["resolved_model"], row["committed_model"], row["late_model"])
                    attempts.append(ObservedAttempt(row["number"], row["binding_id"], row["candidate_attempt"],
                        row["outcome"], row["recovery_action"], actual, conflict, _usage(row),
                        _usage(row, "late_") if row["late_sequence"] is not None else None,
                        row["configuration_revision"], row["pricing_resource_id"]))
                await cursor.execute("""
                    SELECT currency,total_cost,completeness,certainty FROM invocation_cost_summary
                    WHERE call_id=%s ORDER BY currency LIMIT 3
                    """, (call_id,))
                costs = tuple(ObservedCost(row["currency"],
                    str(row["total_cost"]) if row["total_cost"] is not None else None,
                    row["completeness"], row["certainty"]) for row in await cursor.fetchall())
        # Only the last Attempt can supply a missing final actual_model; an
        # earlier failed candidate must not masquerade as the final model.
        last = attempts[-1] if attempts else None
        actual, conflict = _model(invocation["resolved_model"], last.actual_model if last else None)
        conflict = conflict or bool(last and last.model_conflict)
        return InvocationObservation(call_id, invocation["trace_id"], invocation["state"],
            invocation["outcome"], invocation["error_code"], None if conflict else actual, conflict,
            tuple(attempts), _usage(invocation), costs,
            BusinessCorrelation(invocation["task_id"], invocation["turn_id"], invocation["step_id"]))
