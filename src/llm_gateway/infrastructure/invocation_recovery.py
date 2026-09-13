"""Read-only response projection from committed Attempt facts, never guesses."""

from llm_gateway.application.model_api import RecoverySummary
from llm_gateway.domain.invocation import InvocationPersistenceUnavailable


class PostgresInvocationRecovery:
    def __init__(self, store):
        self._store = store

    async def __call__(self, call_id):
        async with self._store.transaction() as connection:
            cursor = await connection.execute(
                "SELECT i.state,a.number,a.binding_id,a.candidate_attempt,o.outcome "
                "FROM model_invocation i LEFT JOIN provider_attempt a USING(call_id) "
                "LEFT JOIN provider_attempt_outcome o USING(call_id,number) "
                "WHERE i.call_id=%s ORDER BY a.number", (call_id,))
            rows = await cursor.fetchall()
        if not rows or rows[0][0] not in {"completed", "failed", "uncertain"}:
            raise InvocationPersistenceUnavailable()
        if len(rows) == 1 and rows[0][1] is None:
            return RecoverySummary(0, 0, False)
        if len(rows) > 3 or any(row[1] != index or row[4] is None for index, row in enumerate(rows, 1)):
            raise InvocationPersistenceUnavailable()
        return RecoverySummary(len(rows), sum(row[3] > 1 for row in rows), len({row[2] for row in rows}) > 1)
