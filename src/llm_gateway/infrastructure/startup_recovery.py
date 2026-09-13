"""Restart reconciliation under the process lease, before opening admission.

Never dispatches or queries a Provider. Durable Attempt success alone cannot
prove the Gateway validation/commitment/terminal checkpoint completed. Keep
those known Provider facts while recording the unresolved logical terminal.
The launcher must exclude legacy unowned writers and hold ownership throughout
its admitted-work lifetime; this component does not launch the Model service.
"""

from llm_gateway.domain.invocation import InvocationPersistenceUnavailable
from llm_gateway.infrastructure.cancellation_settlement import finalize_interrupted
from llm_gateway.infrastructure.invocation import PostgresInvocationStore
from llm_gateway.infrastructure.migrate import verify_schema
from llm_gateway.infrastructure.model_execution_owner import PostgresModelExecutionOwner


SCHEMA_REVISION = "0018_invocation_correlation.sql"


class PostgresStartupRecovery:
    def __init__(self, store, *, configuration_revision, batch_size=100):
        if (not isinstance(store, PostgresInvocationStore)
                or not isinstance(store._ownership, PostgresModelExecutionOwner)
                or type(configuration_revision) is not int or configuration_revision <= 0
                or type(batch_size) is not int or not 1 <= batch_size <= 1000):
            raise ValueError("Owned store, configuration revision and bounded batch required")
        self._store = store
        self._revision = configuration_revision
        self._batch_size = batch_size
        self._started = False

    async def run(self):
        """Return committed recovery count; any failure must keep admission shut.

        Each call is atomic. A later process resumes remaining rows after a
        crash; it does not re-settle previously committed terminals. Selection
        excludes calls admitted under this owner's persisted epoch, without
        relying on wall-clock ordering (which can change on restart/restore).
        """
        if self._started:
            raise RuntimeError("Startup recovery is one-shot")
        self._started = True
        await self._store._ownership.verify()
        await verify_schema(self._store._dsn, expected_version=SCHEMA_REVISION,
                            timeout_seconds=self._store._timeout)
        count = 0
        while True:
            async with self._store.transaction() as connection:
                cursor = await connection.execute("SELECT 1 FROM config_revision WHERE revision=%s", (self._revision,))
                if await cursor.fetchone() is None:
                    raise InvocationPersistenceUnavailable()
                cursor = await connection.execute("""
                    SELECT i.call_id FROM model_invocation i
                    WHERE i.state IN ('accepted','running','cancelling')
                        AND i.execution_owner_id IS DISTINCT FROM %s
                    ORDER BY i.call_id LIMIT %s
                    """, (self._store._ownership.owner_id, self._batch_size))
                calls = await cursor.fetchall()
            if not calls:
                return count
            for (call_id,) in calls:
                async with self._store.transaction() as connection:
                    cursor = await connection.execute("""
                        SELECT state,routing_evidence_ttl_seconds FROM model_invocation
                        WHERE call_id=%s FOR UPDATE
                        """, (call_id,))
                    row = await cursor.fetchone()
                    if row is None:
                        raise InvocationPersistenceUnavailable()
                    if row[0] not in {"accepted", "running", "cancelling"}:
                        continue
                    # Legacy records lacking the locked TTL cannot be repaired
                    # by substituting today's configuration or inventing expiry.
                    if row[1] is None:
                        raise InvocationPersistenceUnavailable()
                    await finalize_interrupted(connection, call_id, row[1], "uncertain", "uncertain",
                                               validation_status="unavailable")
                    await connection.execute("""
                        INSERT INTO invocation_restart_recovery(call_id,owner_id,prior_state,
                            uncertainty_reason,configuration_revision,schema_revision)
                        VALUES (%s,%s,%s,'gateway_terminal_not_committed',%s,%s)
                        """, (call_id, self._store._ownership.owner_id, row[0], self._revision, SCHEMA_REVISION))
                count += 1
