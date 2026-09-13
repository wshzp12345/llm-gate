"""Initial PostgreSQL unkeyed shell and implementation of AttemptJournal.

No raw request/output storage, implicit migration, retry, terminal settlement or
claim of complete admission. Routing preselection and all outer admission gates
must still be durably established by the composition before invoking execution.
"""

import asyncio
import math
import re
from contextlib import asynccontextmanager
from uuid import UUID

import psycopg

from llm_gateway.domain.invocation import (
    InvocationAdmission, InvocationAuthorizationExpired, InvocationPersistenceUnavailable,
)
from llm_gateway.domain.model import FailureCode, ProviderFailure, ProviderResult, Usage
from llm_gateway.domain.recovery import RecoveryPlan
from llm_gateway.infrastructure.routing_evidence import _append_event, require_attempt_gate
from llm_gateway.infrastructure.cost import lock_attempt_pricing, accrue_attempt_cost
from llm_gateway.infrastructure.safety_refusal import record_refusal
from llm_gateway.infrastructure.model_execution_owner import PostgresModelExecutionOwner


class PostgresInvocationStore:
    def __init__(self, dsn: str, *, checkpoint_timeout_seconds: float = 5, ownership=None, lifetime_deadline=None):
        if (type(checkpoint_timeout_seconds) not in (int, float)
                or not math.isfinite(checkpoint_timeout_seconds) or checkpoint_timeout_seconds <= 0):
            raise ValueError("Positive checkpoint deadline required")
        if ownership is not None and not isinstance(ownership, PostgresModelExecutionOwner):
            raise ValueError("Typed Model execution ownership required")
        if lifetime_deadline is not None and not callable(lifetime_deadline):
            raise ValueError("Optional process deadline reader must be callable")
        self._dsn = dsn
        self._timeout = checkpoint_timeout_seconds
        self._ownership = ownership
        self._lifetime_deadline = lifetime_deadline

    @asynccontextmanager
    async def transaction(self):
        timeout = self._timeout
        if self._lifetime_deadline is not None:
            deadline = self._lifetime_deadline()
            if deadline is not None:
                if type(deadline) not in (int, float) or not math.isfinite(deadline):
                    raise ValueError("Finite monotonic process deadline required")
                timeout = min(timeout, deadline - asyncio.get_running_loop().time())
                if timeout <= 0:
                    raise InvocationPersistenceUnavailable()
        try:
            async with asyncio.timeout(timeout):
                async with await psycopg.AsyncConnection.connect(
                    self._dsn, connect_timeout=max(1, math.ceil(self._timeout)),
                ) as connection:
                    if self._ownership is not None:
                        await self._ownership.guard(connection)
                    yield connection
        except (psycopg.Error, TimeoutError):
            raise InvocationPersistenceUnavailable() from None

    async def admit_unkeyed_shell(self, admission: InvocationAdmission) -> None:
        async with self.transaction() as connection:
            await self.insert_shell(connection, admission)

    @staticmethod
    async def insert_shell(connection, admission: InvocationAdmission):
        """Caller-owned transaction; return PostgreSQL admission time and TTL."""
        auth = admission.authorization
        cursor = await connection.execute("""
                SELECT snapshot->'resource_policies'->'routing_evidence_ttl_seconds'
                FROM config_revision WHERE revision=%s
                """, (int(admission.configuration_revision),))
        policy = await cursor.fetchone()
        if policy is None or type(policy[0]) is not int or not 86400 <= policy[0] <= 31536000:
            raise InvocationPersistenceUnavailable()
        correlation = admission.correlation
        values = (correlation.task_id, correlation.turn_id, correlation.step_id)
        # Unspecified optional identity columns retain their NULL defaults.
        has_correlation = any(value is not None for value in values)
        columns = ",task_id,turn_id,step_id" if has_correlation else ""
        placeholders = ",%s,%s,%s" if has_correlation else ""
        cursor = await connection.execute(f"""
                INSERT INTO model_invocation
                    (call_id,trace_id,configuration_revision,requested_model,tenant_id,subject,scopes,
                     issuer,audience,authorization_expires_at,authentication_method,routing_evidence_ttl_seconds,
                     execution_owner_id{columns})
                SELECT %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    (SELECT owner_id FROM model_execution_owner WHERE singleton){placeholders}
                WHERE %s::timestamptz > statement_timestamp()
                RETURNING accepted_at
                """, (admission.call_id, admission.trace_id, int(admission.configuration_revision),
                      admission.requested_model, auth.tenant_id, auth.subject, sorted(auth.scopes),
                      auth.issuer, auth.audience, auth.expires_at, auth.authentication_method, policy[0],
                      *(values if has_correlation else ()), auth.expires_at))
        row = await cursor.fetchone()
        if row is None:
            raise InvocationAuthorizationExpired()
        if admission.prompt_reference is not None:
            await connection.execute(
                "INSERT INTO invocation_prompt (call_id,tenant_id,asset_id,version_id) VALUES (%s,%s,%s,%s)",
                (admission.call_id, auth.tenant_id, admission.prompt_reference.asset_id, admission.prompt_reference.version_id))
        return row[0], policy[0]

    def journal(self, call_id: UUID):
        if not isinstance(call_id, UUID) or call_id.version != 4:
            raise ValueError("Invocation requires UUIDv4")
        return PostgresAttemptJournal(self, call_id)


class PostgresAttemptJournal:
    def __init__(self, store: PostgresInvocationStore, call_id: UUID):
        self._store = store
        self._call_id = call_id

    async def _lock(self, connection):
        cursor = await connection.execute(
            "SELECT state FROM model_invocation WHERE call_id=%s FOR UPDATE", (self._call_id,))
        row = await cursor.fetchone()
        if row is None or row[0] not in {"accepted", "running"}:
            raise InvocationPersistenceUnavailable()
        return row[0]

    async def started(self, number: int, binding_id: str, candidate_attempt: int) -> None:
        if (type(number) is not int or not 1 <= number <= 3
                or type(candidate_attempt) is not int or not 1 <= candidate_attempt <= 2
                or not isinstance(binding_id, str) or len(binding_id) > 128
                or not re.fullmatch(r"[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*", binding_id)):
            raise ValueError("Invalid Attempt identity")
        async with self._store.transaction() as connection:
            state = await self._lock(connection)
            await require_attempt_gate(connection, self._call_id, binding_id)
            cursor = await connection.execute("""
                SELECT a.number,a.binding_id,a.candidate_attempt,o.recovery_action
                FROM provider_attempt a LEFT JOIN provider_attempt_outcome o USING (call_id,number)
                WHERE a.call_id=%s ORDER BY a.number
                """, (self._call_id,))
            previous = await cursor.fetchall()
            valid = number == len(previous) + 1
            valid &= candidate_attempt == 1 + sum(row[1] == binding_id for row in previous)
            if previous:
                last = previous[-1]
                # A locked retry can encounter a newly rejected dynamic gate.
                # Its durable gate/skip evidence belongs to CandidateRuntime.
                valid &= (last[3] == "retry" and last[1] == binding_id
                          or last[3] in {"retry", "advance"} and last[1] != binding_id and candidate_attempt == 1)
            if not valid:
                raise InvocationPersistenceUnavailable()
            await connection.execute("""
                INSERT INTO provider_attempt(call_id,number,binding_id,candidate_attempt) VALUES (%s,%s,%s,%s)
                """, (self._call_id, number, binding_id, candidate_attempt))
            await lock_attempt_pricing(connection, self._call_id, number, binding_id)
            if state == "accepted":
                await connection.execute("UPDATE model_invocation SET state='running' WHERE call_id=%s", (self._call_id,))
            await _append_event(connection, self._call_id, "attempt_started", {
                "decision": "started", "candidate_attempt": candidate_attempt,
            }, binding_id=binding_id, attempt_number=number)

    async def finished(self, number: int, result: ProviderResult | ProviderFailure, recovery: RecoveryPlan) -> None:
        if (type(number) is not int or not 1 <= number <= 3
                or not isinstance(result, (ProviderResult, ProviderFailure))
                or recovery.action not in {"retry", "advance", "stop"}
                or type(recovery.delay_ms) is not int or not 0 <= recovery.delay_ms <= 5000
                or recovery.action != "retry" and recovery.delay_ms != 0):
            raise ValueError("Invalid Attempt outcome")
        success = isinstance(result, ProviderResult)
        refused = success and result.disposition == "safety_refused"
        if not success and not isinstance(result.code, FailureCode):
            raise ValueError("Only stable Gateway errors may be persisted")
        if success and result.finish_reason not in ({"stop", "length", "content_filter"} if refused else {"stop", "length"}):
            raise ValueError("Unsupported text completion finish reason")
        if success and recovery.action != "stop":
            raise ValueError("Successful Attempt cannot recover")
        uncertain = not success and result.code == FailureCode.UNCERTAIN
        if uncertain and recovery.action != "stop":
            raise ValueError("Uncertain Attempt cannot recover")
        outcome = "succeeded" if success else "uncertain" if uncertain else "failed"
        usage = result.usage if success else result.observed_usage
        async with self._store.transaction() as connection:
            await self._lock(connection)
            cursor = await connection.execute(
                "SELECT candidate_attempt,binding_id FROM provider_attempt WHERE call_id=%s AND number=%s", (self._call_id, number))
            attempt = await cursor.fetchone()
            if (attempt is None or number == 3 and recovery.action != "stop"
                    or attempt[0] == 2 and recovery.action == "retry"):
                raise InvocationPersistenceUnavailable()
            if refused:
                await record_refusal(connection, self._call_id, number, result)
            await connection.execute("""
                INSERT INTO provider_attempt_outcome
                    (call_id,number,outcome,error_code,resolved_model,finish_reason,input_tokens,output_tokens,
                     cached_tokens,reasoning_tokens,provider_reported_total,recovery_action,recovery_delay_ms,safety_refused)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (self._call_id, number, outcome, None if success else result.code,
                      result.resolved_model if success else result.observed_model, result.finish_reason if success else None,
                      usage.input_tokens if usage else None, usage.output_tokens if usage else None,
                      usage.cached_tokens if usage else None, usage.reasoning_tokens if usage else None,
                      usage.provider_reported_total if usage else None, recovery.action, recovery.delay_ms, refused))
            await accrue_attempt_cost(connection, self._call_id, number, usage if usage is not None else Usage())
            await _append_event(connection, self._call_id, "attempt_finished", {
                "outcome": outcome, "error_code": None if success else result.code,
                "recovery": {"action": recovery.action, "delay_ms": recovery.delay_ms},
                **({"disposition": "safety_refused"} if refused else {}),
            }, binding_id=attempt[1], attempt_number=number)
