"""PostgreSQL version protection. No key bytes or invalidation commands.

Guard/extend use the caller's transaction so dependency creation and its horizon
commit together. The caller must also recheck the local fence at its final
decision/commit boundary; these helpers do not own admission or retention.
"""

from llm_gateway.application.fingerprint_keys import FingerprintFence, FingerprintVersionInvalidated
from llm_gateway.domain.fingerprint_keys import FingerprintKeyMember, FingerprintKeyRing
from llm_gateway.domain.invocation import InvocationPersistenceUnavailable
from llm_gateway.infrastructure.invocation import PostgresInvocationStore


class PostgresFingerprintProtection:
    def __init__(self, store: PostgresInvocationStore):
        self._store = store

    async def check_ring(self, ring: FingerprintKeyRing) -> None:
        async with self._store.transaction() as connection:
            await connection.execute("SET TRANSACTION READ ONLY")
            cursor = await connection.execute("""
                SELECT key_id,key_version,invalidated_at FROM fingerprint_key_protection
                WHERE protected_until > statement_timestamp() OR invalidated_at IS NOT NULL
                """)
            identities = {member.identity for member in ring.members}
            for key_id, key_version, invalidated_at in await cursor.fetchall():
                present = (key_id, key_version) in identities
                if invalidated_at is not None and present:
                    raise FingerprintVersionInvalidated()
                if invalidated_at is None and not present:
                    raise InvocationPersistenceUnavailable()

    async def generation(self, member: FingerprintKeyMember) -> int:
        async with self._store.transaction() as connection:
            await connection.execute("SET TRANSACTION READ ONLY")
            cursor = await connection.execute("""
                SELECT invalidation_generation,invalidated_at FROM fingerprint_key_protection
                WHERE key_id=%s AND key_version=%s
                """, member.identity)
            row = await cursor.fetchone()
            if row is None:
                return 0
            if row[1] is not None:
                raise FingerprintVersionInvalidated()
            return row[0]

    @staticmethod
    async def guard(connection, fence: FingerprintFence, *, check_observed) -> None:
        """Lock existing dependency's version; absence is not proof of safety."""
        check_observed(fence)
        cursor = await connection.execute("""
            SELECT invalidation_generation,invalidated_at FROM fingerprint_key_protection
            WHERE key_id=%s AND key_version=%s FOR UPDATE
            """, (fence.key_id, fence.key_version))
        row = await cursor.fetchone()
        if row is None or row[1] is not None or row[0] != fence.invalidation_generation:
            raise InvocationPersistenceUnavailable()
        check_observed(fence)

    @staticmethod
    async def extend(connection, fence: FingerprintFence, *, ttl_seconds: int, check_observed) -> None:
        """Extend from database time in the SAME transaction as its dependency."""
        if type(ttl_seconds) is not int or ttl_seconds <= 0:
            raise ValueError("Positive dependency retention required")
        check_observed(fence)
        await connection.execute("""
            INSERT INTO fingerprint_key_protection (key_id,key_version,protected_until)
            VALUES (%s,%s,statement_timestamp()) ON CONFLICT (key_id,key_version) DO NOTHING
            """, (fence.key_id, fence.key_version))
        await PostgresFingerprintProtection.guard(connection, fence, check_observed=check_observed)
        await connection.execute("""
            UPDATE fingerprint_key_protection
            SET protected_until=GREATEST(protected_until,statement_timestamp()+%s*interval '1 second')
            WHERE key_id=%s AND key_version=%s
            """, (ttl_seconds, fence.key_id, fence.key_version))
        check_observed(fence)
