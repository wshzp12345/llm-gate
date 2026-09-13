import asyncio
from types import SimpleNamespace

import pytest

from llm_gateway.infrastructure import deployment_migrate as job


def test_job_orders_connection_schema_migration_and_verification(monkeypatch):
    events = []
    class Connection:
        async def __aenter__(self):
            return self
        async def __aexit__(self, *args):
            pass
        async def execute(self, statement):
            events.append(statement)
    async def connect(*args, **kwargs):
        return Connection()
    async def migrate(dsn):
        events.append("migrate")
    async def verify(dsn, **kwargs):
        events.append("verify")
    monkeypatch.setattr(job, "read_database_url", lambda config: "fixture")
    monkeypatch.setattr(job.psycopg.AsyncConnection, "connect", connect)
    monkeypatch.setattr(job, "migrate", migrate)
    monkeypatch.setattr(job, "verify_schema", verify)
    asyncio.run(job.run(SimpleNamespace(expected_schema_version="fixture")))
    assert events == ["SELECT 1", "CREATE SCHEMA IF NOT EXISTS llm_gateway", "migrate", "verify"]


def test_migration_failure_is_not_retried_or_reported_verified(monkeypatch):
    calls = []
    class Connection:
        async def __aenter__(self):
            return self
        async def __aexit__(self, *args):
            pass
        async def execute(self, statement):
            pass
    async def connect(*args, **kwargs):
        return Connection()
    async def migrate(dsn):
        calls.append("migrate")
        raise RuntimeError("fixture migration failure")
    async def verify(*args, **kwargs):
        pytest.fail("Failed migration must not proceed")
    monkeypatch.setattr(job, "read_database_url", lambda config: "fixture")
    monkeypatch.setattr(job.psycopg.AsyncConnection, "connect", connect)
    monkeypatch.setattr(job, "migrate", migrate)
    monkeypatch.setattr(job, "verify_schema", verify)
    with pytest.raises(RuntimeError):
        asyncio.run(job.run(SimpleNamespace(expected_schema_version="fixture")))
    assert calls == ["migrate"]
