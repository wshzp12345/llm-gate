"""Cancellation/deadline settlement and shared interrupted-work accounting.

The lifecycle owner must have stopped local execution before calling. Logical
cancellation does not assert remote completion, billability or zero token use.
Startup recovery uses only the locked-transaction accounting primitive below.
"""

import asyncio
import math

from llm_gateway.application.cancellation import CancellationCleanup, ProviderCancelResult
from llm_gateway.application.late_provider_outcome import LateTextOutcome
from llm_gateway.domain.invocation import InvocationAdmission, InvocationPersistenceUnavailable, InvocationTerminalConflict
from llm_gateway.domain.model import Usage
from llm_gateway.infrastructure.cost import accrue_attempt_cost, summarize_invocation_cost
from llm_gateway.infrastructure.routing_evidence import _append_event


async def settle_local_cancellation(store, admission, cleanup):
    if not isinstance(admission, InvocationAdmission) or not isinstance(cleanup, CancellationCleanup):
        raise ValueError("Admitted local execution and typed cleanup facts required")
    await _settle_stopped(store, admission, cleanup, "cancelled", "cancelled")


async def settle_deadline_exceeded(store, admission, *, deadline, observation=None):
    """Caller must have stopped local work; no fresh Provider cleanup budget."""
    if (not isinstance(admission, InvocationAdmission) or type(deadline) not in (int, float)
            or not math.isfinite(deadline) or asyncio.get_running_loop().time() < deadline):
        raise ValueError("Admitted Invocation and exhausted monotonic deadline required")
    if observation is not None and (not isinstance(observation, LateTextOutcome)
            or observation.requested_model is not None and observation.requested_model != admission.requested_model):
        raise ValueError("Matching content-free stopped observation required")
    await _settle_stopped(store, admission, None, "failed", "deadline_exceeded", observation=observation)


async def _settle_stopped(store, admission, cleanup, terminal, terminal_error, *, observation=None):
    call_id = admission.call_id
    async with store.transaction() as connection:
        cursor = await connection.execute("""
            SELECT state,tenant_id,subject,trace_id,configuration_revision,requested_model,
                routing_evidence_ttl_seconds FROM model_invocation WHERE call_id=%s FOR UPDATE
            """, (call_id,))
        row = await cursor.fetchone()
        expected = (admission.authorization.tenant_id, admission.authorization.subject,
                    admission.trace_id, int(admission.configuration_revision), admission.requested_model)
        if row is None or tuple(row[1:6]) != expected or row[6] is None:
            raise InvocationPersistenceUnavailable()
        if row[0] not in ({"cancelling"} if cleanup is not None else {"accepted", "running"}):
            raise InvocationTerminalConflict()
        ttl = row[6]
        if cleanup is not None:
            cursor = await connection.execute("SELECT 1 FROM invocation_local_cancellation WHERE call_id=%s", (call_id,))
            if await cursor.fetchone() is None:
                raise InvocationPersistenceUnavailable()
        await finalize_interrupted(connection, call_id, ttl, terminal, terminal_error, cleanup=cleanup, observation=observation)


async def finalize_interrupted(connection, call_id, ttl, terminal, terminal_error, *, cleanup=None,
                               validation_status="not_requested", observation=None):
    """Caller holds the Invocation row lock and validates its transition.

    Append missing outcomes and terminal accounting in that same transaction.
    Existing Provider facts are immutable; unknown measurements stay unknown.
    """
    cursor = await connection.execute("SELECT 1 FROM routing_decision WHERE call_id=%s", (call_id,))
    routed = await cursor.fetchone() is not None
    cursor = await connection.execute("""
        SELECT a.number,a.binding_id,o.outcome,c.number FROM provider_attempt a
        LEFT JOIN provider_attempt_outcome o USING(call_id,number)
        LEFT JOIN cost_accrual c USING(call_id,number)
        WHERE a.call_id=%s ORDER BY a.number
        """, (call_id,))
    attempts = await cursor.fetchall()
    if (len(attempts) > 3 or any(a[0] != n for n, a in enumerate(attempts, 1))
            or any(a[2] is None for a in attempts[:-1])
            or any((a[2] is None) != (a[3] is None) for a in attempts)
            or attempts and not routed):
        raise InvocationPersistenceUnavailable()
    if observation is not None and (not isinstance(observation, LateTextOutcome) or cleanup is not None
            or not attempts or observation.attempt_number != attempts[-1][0]):
        raise InvocationPersistenceUnavailable()
    if cleanup is not None:
        if not attempts and cleanup.dispatched:
            raise ValueError("Cannot dispatch Provider cancellation without an Attempt")
        await connection.execute("""
            INSERT INTO invocation_cancellation_cleanup(call_id,dispatched,provider_result,downstream_writable)
            VALUES (%s,%s,%s,%s)
            """, (call_id, cleanup.dispatched, cleanup.result, cleanup.downstream_writable))
    for number, binding, outcome, _ in attempts:
        if outcome is not None:
            if outcome == "uncertain" and cleanup is not None:
                await connection.execute("""
                    INSERT INTO attempt_cancellation_billing(call_id,number,billing_outcome) VALUES (%s,%s,'uncertain')
                    """, (call_id, number))
            continue
        acknowledged = cleanup is not None and cleanup.result == ProviderCancelResult.ACKNOWLEDGED
        outcome, error = ("cancelled", "cancelled") if acknowledged else ("uncertain", "uncertain")
        usage = observation.usage if observation is not None else Usage()
        model = observation.resolved_model if observation is not None else None
        await connection.execute("""
            INSERT INTO provider_attempt_outcome(call_id,number,outcome,error_code,recovery_action,recovery_delay_ms,
                resolved_model,input_tokens,output_tokens,cached_tokens,reasoning_tokens,provider_reported_total)
            VALUES (%s,%s,%s,%s,'stop',0,%s,%s,%s,%s,%s,%s)
            """, (call_id, number, outcome, error, model, usage.input_tokens, usage.output_tokens,
                  usage.cached_tokens, usage.reasoning_tokens, usage.provider_reported_total))
        await accrue_attempt_cost(connection, call_id, number, usage)
        if cleanup is not None:
            await connection.execute("""
                INSERT INTO attempt_cancellation_billing(call_id,number,billing_outcome) VALUES (%s,%s,'uncertain')
                """, (call_id, number))
        await _append_event(connection, call_id, "attempt_finished", {
            "outcome": outcome, "error_code": error, "recovery": {"action": "stop", "delay_ms": 0},
        }, binding_id=binding, attempt_number=number)
    # Aggregate only complete known measurements. With no Attempt, all
    # measurement fields remain NULL and there is no invented currency.
    cursor = await connection.execute("""
        INSERT INTO invocation_settlement(call_id,outcome,error_code,attempt_count,validation_status,
            input_tokens,output_tokens,cached_tokens,reasoning_tokens,resolved_model)
        SELECT %s,%s,%s,count(*),%s,
            CASE WHEN count(input_tokens)=count(*) THEN sum(input_tokens) END,
            CASE WHEN count(output_tokens)=count(*) THEN sum(output_tokens) END,
            CASE WHEN count(cached_tokens)=count(*) THEN sum(cached_tokens) END,
            CASE WHEN count(reasoning_tokens)=count(*) THEN sum(reasoning_tokens) END,
            (SELECT resolved_model FROM provider_attempt_outcome WHERE call_id=%s ORDER BY number DESC LIMIT 1)
        FROM provider_attempt_outcome WHERE call_id=%s RETURNING terminal_at
        """, (call_id, terminal, terminal_error, validation_status, call_id, call_id))
    terminal_at = (await cursor.fetchone())[0]
    await summarize_invocation_cost(connection, call_id)
    if routed:
        await _append_event(connection, call_id, "routing_terminal", {
            "outcome": terminal, "error_code": terminal_error, "service_level": None,
            "degradation_stage": "terminal",
        })
        await connection.execute("""
            INSERT INTO routing_terminal(call_id,sequence,terminal_at,expires_at)
            SELECT call_id,sequence,%s,%s::timestamptz + %s * interval '1 second'
            FROM routing_event WHERE call_id=%s AND kind='routing_terminal'
            """, (terminal_at, terminal_at, ttl, call_id))
    await connection.execute("UPDATE model_invocation SET state=%s WHERE call_id=%s", (terminal, call_id))
