"""Row-locked cancellation reservation, sharing settlement's terminal race."""

from llm_gateway.application.cancellation import CancellationReservation, LocalCancellationReason
from llm_gateway.domain.invocation import InvocationAdmission, InvocationPersistenceUnavailable
from llm_gateway.infrastructure.cancellation_settlement import settle_local_cancellation


class PostgresLocalCancellationStore:
    def __init__(self, store):
        self._store = store

    async def finalize(self, admission, cleanup):
        await settle_local_cancellation(self._store, admission, cleanup)

    async def reserve(self, admission, reason):
        if not isinstance(admission, InvocationAdmission) or not isinstance(reason, LocalCancellationReason):
            raise ValueError("Admitted local execution and typed cancellation reason required")
        async with self._store.transaction() as connection:
            cursor = await connection.execute("""
                SELECT state,tenant_id,subject,trace_id,configuration_revision,requested_model
                FROM model_invocation WHERE call_id=%s FOR UPDATE
                """, (admission.call_id,))
            row = await cursor.fetchone()
            expected = (admission.authorization.tenant_id, admission.authorization.subject,
                        admission.trace_id, int(admission.configuration_revision), admission.requested_model)
            if row is None or tuple(row[1:]) != expected:
                raise InvocationPersistenceUnavailable()
            state = row[0]
            if state in {"accepted", "running"}:
                await connection.execute("UPDATE model_invocation SET state='cancelling' WHERE call_id=%s",
                                         (admission.call_id,))
                await connection.execute("""
                    INSERT INTO invocation_local_cancellation(call_id,reason) VALUES (%s,%s)
                    """, (admission.call_id, reason.value))
                result = CancellationReservation.RESERVED
            elif state == "cancelling":
                result = CancellationReservation.CANCELLING
            elif state == "cancelled":
                result = CancellationReservation.CANCELLED
            else:
                result = CancellationReservation.ALREADY_TERMINAL
        return result
