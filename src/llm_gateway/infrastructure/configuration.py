import asyncio
import hashlib
import json
import math
from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import datetime

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from llm_gateway.domain.configuration import (
    CommandIdentity, CommandOutcome, ConfigurationDeadlineExceeded,
    ConfigurationPersistenceUnavailable, IndexedOutcome, PreparedCandidate,
    Revision, RevisionState, revision_number, ConfigurationDiagnostic, RevisionExport,
)


def _revision(row: dict) -> Revision:
    return Revision(
        str(row["revision"]), RevisionState(row["state"]),
        str(row["base_revision"]) if row["base_revision"] is not None else None,
        row["snapshot_digest"], row["created_at"],
        str(row["rollback_of"]) if row["rollback_of"] is not None else None,
    )


def _stored_outcome(outcome: CommandOutcome) -> dict:
    result = asdict(outcome)
    if outcome.revision is not None:
        result["revision"]["created_at"] = outcome.revision.created_at.isoformat()
    return result


def _outcome(data: dict) -> CommandOutcome:
    row = data["revision"]
    if row is not None:
        row = dict(row, created_at=datetime.fromisoformat(row["created_at"]))
    return CommandOutcome(_revision(row) if row else None, data["error"],
                          tuple(ConfigurationDiagnostic(**item) for item in data["diagnostics"]),
                          data["diagnostics_truncated"])


class PostgresConfigurationTransaction:
    def __init__(self, connection: psycopg.AsyncConnection) -> None:
        self._connection = connection

    async def indexed_outcome(self, identity: CommandIdentity) -> IndexedOutcome | None:
        key = (identity.tenant_id, identity.subject, identity.operation, identity.command_id)
        # Collisions only serialize unrelated commands; the full tuple remains the authority.
        lock = int.from_bytes(hashlib.sha256(json.dumps(key).encode()).digest()[:8], signed=True)
        await self._connection.execute("SELECT pg_advisory_xact_lock(%s)", (lock,))
        cursor = await self._connection.execute(
            "SELECT command_digest, outcome FROM config_command_index "
            "WHERE tenant_id=%s AND subject=%s AND operation=%s AND command_id=%s", key,
        )
        row = await cursor.fetchone()
        return IndexedOutcome(row["command_digest"], _outcome(row["outcome"])) if row else None

    async def lock_active(self) -> str | None:
        cursor = await self._connection.execute(
            "SELECT revision FROM config_active WHERE singleton FOR UPDATE",
        )
        row = await cursor.fetchone()
        if row is None:
            raise ConfigurationPersistenceUnavailable()
        return str(row["revision"]) if row["revision"] is not None else None

    async def get_revision(self, revision: str) -> Revision | None:
        cursor = await self._connection.execute(
            "SELECT * FROM config_revision WHERE revision=%s", (revision_number(revision),),
        )
        row = await cursor.fetchone()
        return _revision(row) if row else None

    async def insert_candidate(self, candidate: PreparedCandidate) -> Revision:
        cursor = await self._connection.execute(
            "INSERT INTO config_revision (state, base_revision, snapshot_digest, snapshot, change_set, creation_description, rollback_of) "
            "VALUES ('candidate', %s, %s, %s, %s, %s, %s) RETURNING *",
            (revision_number(candidate.base_revision) if candidate.base_revision is not None else None,
             candidate.snapshot_digest, Jsonb(json.loads(candidate.snapshot_json)),
             Jsonb(json.loads(candidate.change_set_json)), candidate.description,
             revision_number(candidate.rollback_of) if candidate.rollback_of is not None else None),
        )
        return _revision(await cursor.fetchone())

    async def get_snapshot(self, revision: str) -> bytes:
        cursor = await self._connection.execute(
            "SELECT snapshot FROM config_revision WHERE revision=%s", (revision_number(revision),),
        )
        row = await cursor.fetchone()
        if row is None:
            raise ConfigurationPersistenceUnavailable()
        return json.dumps(row["snapshot"], ensure_ascii=False, allow_nan=False).encode("utf-8")

    async def activate(self, revision: str) -> Revision:
        number = revision_number(revision)
        await self._connection.execute("UPDATE config_revision SET state='superseded' WHERE state='active'")
        cursor = await self._connection.execute(
            "UPDATE config_revision SET state='active' WHERE revision=%s RETURNING *", (number,),
        )
        row = await cursor.fetchone()
        await self._connection.execute("UPDATE config_revision SET state='stale' WHERE state='candidate'")
        await self._connection.execute("UPDATE config_active SET revision=%s WHERE singleton", (number,))
        return _revision(row)

    async def get_export(self, revision: str) -> RevisionExport | None:
        cursor = await self._connection.execute(
            "SELECT revision, snapshot_digest, snapshot, creation_description FROM config_revision WHERE revision=%s",
            (revision_number(revision),),
        )
        row = await cursor.fetchone()
        if row is None:
            return None
        return RevisionExport(str(row["revision"]), row["snapshot_digest"],
                              json.dumps(row["snapshot"], ensure_ascii=False, allow_nan=False).encode("utf-8"),
                              row["creation_description"])

    async def get_change_set(self, revision: str) -> bytes:
        cursor = await self._connection.execute(
            "SELECT change_set FROM config_revision WHERE revision=%s", (revision_number(revision),),
        )
        row = await cursor.fetchone()
        if row is None:
            raise ConfigurationPersistenceUnavailable()
        return json.dumps(row["change_set"], ensure_ascii=False, allow_nan=False).encode("utf-8")

    async def get_active_export(self) -> RevisionExport | None:
        cursor = await self._connection.execute(
            "SELECT a.revision AS active_revision, r.revision, r.state, "
            "r.snapshot_digest, r.snapshot, r.creation_description "
            "FROM config_active a LEFT JOIN config_revision r ON r.revision=a.revision "
            "WHERE a.singleton=true"
        )
        row = await cursor.fetchone()
        if row is None:
            raise ConfigurationPersistenceUnavailable()
        if row["active_revision"] is None:
            return None
        if row["revision"] is None or row["state"] != "active":
            raise ConfigurationPersistenceUnavailable()
        return RevisionExport(str(row["revision"]), row["snapshot_digest"],
                              json.dumps(row["snapshot"], ensure_ascii=False, allow_nan=False).encode("utf-8"),
                              row["creation_description"])

    async def record_outcome(
        self, identity: CommandIdentity, outcome: CommandOutcome, description: str | None,
    ) -> None:
        key = (identity.tenant_id, identity.subject, identity.operation, identity.command_id)
        await self._connection.execute(
            "INSERT INTO config_command_index "
            "(tenant_id,subject,operation,command_id,command_digest,outcome) VALUES (%s,%s,%s,%s,%s,%s)",
            (*key, identity.digest, Jsonb(_stored_outcome(outcome))),
        )
        await self._connection.execute(
            "INSERT INTO config_command_audit "
            "(tenant_id,subject,operation,command_id,revision,error_code,description) VALUES (%s,%s,%s,%s,%s,%s,%s)",
            (*key, revision_number(outcome.revision.revision) if outcome.revision else None, outcome.error, description),
        )


class PostgresConfigurationUnitOfWork:
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    @asynccontextmanager
    async def transaction(self, timeout_seconds: float):
        if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
            raise ConfigurationDeadlineExceeded()
        try:
            async with asyncio.timeout(timeout_seconds):
                async with await psycopg.AsyncConnection.connect(
                    self._dsn, row_factory=dict_row, connect_timeout=max(1, math.ceil(timeout_seconds)),
                ) as connection:
                    yield PostgresConfigurationTransaction(connection)
        except TimeoutError:
            raise ConfigurationDeadlineExceeded() from None
        except psycopg.Error:
            raise ConfigurationPersistenceUnavailable() from None
