"""Versioned, checksummed, transactionally serialized PostgreSQL migration runner."""

import asyncio
import hashlib
import math
import os
from importlib.resources import files

import psycopg

from llm_gateway.domain.configuration import ConfigurationDeadlineExceeded, ConfigurationPersistenceUnavailable


def _manifest():
    resources = files("llm_gateway.infrastructure").joinpath("migrations")
    result = []
    for resource in sorted(resources.iterdir(), key=lambda item: item.name):
        if resource.name.endswith(".sql"):
            body = resource.read_bytes()
            result.append((resource.name, body, hashlib.sha256(body).hexdigest()))
    return tuple(result)


async def verify_schema(dsn: str, *, expected_version: str, timeout_seconds: float) -> None:
    """Startup check only: exact packaged history, no DDL, repair or migration.

    expected_version is the final packaged SQL filename, not a domain Snapshot
    schema version. A newer database is incompatible with this binary too.
    """
    if isinstance(timeout_seconds, bool) or not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
        raise ConfigurationDeadlineExceeded()
    manifest = _manifest()
    if not manifest or expected_version != manifest[-1][0]:
        raise ConfigurationPersistenceUnavailable()
    expected = {name: checksum for name, _, checksum in manifest}
    try:
        async with asyncio.timeout(timeout_seconds):
            async with await psycopg.AsyncConnection.connect(
                dsn, connect_timeout=max(1, math.ceil(timeout_seconds)),
            ) as connection:
                await connection.execute("SET TRANSACTION READ ONLY")
                cursor = await connection.execute("SELECT version, checksum FROM gateway_schema_migration")
                rows = await cursor.fetchall()
                if len(rows) != len(expected) or dict(rows) != expected:
                    raise ConfigurationPersistenceUnavailable()
    except TimeoutError:
        raise ConfigurationDeadlineExceeded() from None
    except psycopg.Error:
        raise ConfigurationPersistenceUnavailable() from None


async def migrate(dsn: str) -> None:
    async with await psycopg.AsyncConnection.connect(dsn, connect_timeout=5) as connection:
        await connection.execute("SELECT pg_advisory_xact_lock(7181040001)")
        await connection.execute("""CREATE TABLE IF NOT EXISTS gateway_schema_migration (
            version text PRIMARY KEY, checksum text NOT NULL,
            applied_at timestamptz NOT NULL DEFAULT clock_timestamp())""")
        for name, body, checksum in _manifest():
            cursor = await connection.execute(
                "SELECT checksum FROM gateway_schema_migration WHERE version = %s", (name,),
            )
            existing = await cursor.fetchone()
            if existing is not None:
                if existing[0] != checksum:
                    raise RuntimeError("Applied migration checksum mismatch")
                continue
            await connection.execute(body.decode("utf-8"))
            await connection.execute(
                "INSERT INTO gateway_schema_migration(version, checksum) VALUES (%s, %s)",
                (name, checksum),
            )


def main() -> None:
    dsn = os.environ.get("GATEWAY_DATABASE_URL")
    if not dsn:
        raise SystemExit("GATEWAY_DATABASE_URL is required")
    try:
        asyncio.run(migrate(dsn), loop_factory=asyncio.SelectorEventLoop)
    except (psycopg.Error, RuntimeError):
        raise SystemExit("Database migration failed; check database access and migration history") from None


if __name__ == "__main__":
    main()
