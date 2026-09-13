"""Single-Model-process lease and transaction fencing for startup recovery.

The launcher must hold this lease for its full admitted-work lifetime and bind
its Invocation store to it. Unowned legacy stores are not magically fenced.
No reconnect, takeover loop, listener, recovery scan or background task here.
"""

import asyncio
import math
from contextlib import asynccontextmanager
from uuid import uuid4

import psycopg

from llm_gateway.domain.invocation import InvocationPersistenceUnavailable


_LOCK_KEY = 7181040002  # Distinct from migration serialization.


class ModelOwnershipUnavailable(InvocationPersistenceUnavailable):
    pass


class ModelOwnershipBusy(ModelOwnershipUnavailable):
    """Another Model process still owns this database; startup cannot proceed."""


class PostgresModelExecutionOwner:
    def __init__(self, dsn, *, on_loss, timeout_seconds=5):
        if (type(timeout_seconds) not in (int, float) or not math.isfinite(timeout_seconds)
                or timeout_seconds <= 0 or not callable(on_loss)):
            raise ValueError("Finite positive owner deadline and loss callback required")
        self._dsn, self._on_loss, self._timeout = dsn, on_loss, timeout_seconds
        self.owner_id = uuid4()
        self._connection = None
        self._entered = self._owned = self._lost = False
        self._operation = asyncio.Lock()

    def _invalidate(self):
        if not self._lost:
            self._lost = True
            if self._owned:
                self._on_loss()  # Synchronous: stop admission before returning.

    @asynccontextmanager
    async def hold(self):
        if self._entered or self._lost:
            raise RuntimeError("Model ownership object cannot be reused")
        self._entered = True
        try:
            try:
                async with asyncio.timeout(self._timeout):
                    self._connection = await psycopg.AsyncConnection.connect(self._dsn,
                        autocommit=True, connect_timeout=max(1, math.ceil(self._timeout)))
                    cursor = await self._connection.execute("SELECT pg_try_advisory_lock(%s)", (_LOCK_KEY,))
                    if not (await cursor.fetchone())[0]:
                        raise ModelOwnershipBusy()
                    async with self._connection.transaction():
                        cursor = await self._connection.execute("SELECT owner_id FROM model_execution_owner WHERE singleton FOR UPDATE")
                        previous = await cursor.fetchone()
                        cursor = await self._connection.execute("""
                            INSERT INTO model_execution_owner(singleton,owner_id) VALUES (true,%s)
                            ON CONFLICT (singleton) DO UPDATE SET owner_id=EXCLUDED.owner_id,acquired_at=clock_timestamp()
                            RETURNING acquired_at
                            """, (self.owner_id,))
                        acquired_at = (await cursor.fetchone())[0]
                        await self._connection.execute("""
                            INSERT INTO model_execution_ownership_event(owner_id,previous_owner_id,acquired_at)
                            VALUES (%s,%s,%s)
                            """, (self.owner_id, previous[0] if previous else None, acquired_at))
                    self._owned = True
            except (psycopg.Error, TimeoutError):
                raise ModelOwnershipUnavailable() from None
            yield self
        finally:
            try:
                self._invalidate()
            finally:
                if self._connection is not None:
                    await self._connection.close()

    async def verify(self):
        """Observe lease loss; once lost, this owner never becomes valid again."""
        try:
            async with asyncio.timeout(self._timeout):
                async with self._operation:
                    if not self._owned or self._lost or self._connection is None or self._connection.closed:
                        raise ModelOwnershipUnavailable()
                    cursor = await self._connection.execute("""
                        SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype='advisory'
                            AND pid=pg_backend_pid() AND classid=%s::oid AND objid=%s::oid AND objsubid=1
                            AND mode='ExclusiveLock' AND granted)
                        AND EXISTS (SELECT 1 FROM model_execution_owner WHERE singleton AND owner_id=%s)
                        """, (_LOCK_KEY >> 32, _LOCK_KEY & 0xffffffff, self.owner_id))
                    if not (await cursor.fetchone())[0]:
                        raise ModelOwnershipUnavailable()
        except (psycopg.Error, TimeoutError, ModelOwnershipUnavailable):
            self._invalidate()
            raise ModelOwnershipUnavailable() from None

    async def guard(self, connection):
        """Fence an Invocation transaction until commit, not just a precheck.

        New ownership takes an exclusive row lock to change the epoch. This
        shared lock orders takeover after any already-authorized transaction;
        a stale owner cannot authorize a subsequent one under the new epoch.
        """
        await self.verify()
        cursor = await connection.execute("SELECT owner_id FROM model_execution_owner WHERE singleton FOR SHARE")
        row = await cursor.fetchone()
        if row is None or row[0] != self.owner_id or self._lost:
            self._invalidate()
            raise ModelOwnershipUnavailable()
