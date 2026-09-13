"""PostgreSQL transactions for protected Prompt assets and immutable versions."""

import asyncio
from contextlib import asynccontextmanager

import psycopg
from psycopg.types.json import Jsonb

from llm_gateway.application.prompts import PromptAsset, PromptPersistenceUnavailable
from llm_gateway.domain.model import Message
from llm_gateway.domain.prompts import PromptVersion


class PostgresPromptTransaction:
    def __init__(self, connection):
        self._connection = connection

    async def create_asset(self, tenant_id, asset_id):
        await self._connection.execute("INSERT INTO prompt_asset (tenant_id, asset_id) VALUES (%s, %s)", (tenant_id, asset_id))

    async def asset(self, tenant_id, asset_id, *, lock=False):
        cursor = await self._connection.execute(
            "SELECT tenant_id, asset_id, generation, published_version FROM prompt_asset "
            "WHERE tenant_id=%s AND asset_id=%s" + (" FOR UPDATE" if lock else ""), (tenant_id, asset_id))
        row = await cursor.fetchone()
        return PromptAsset(*row) if row else None

    async def insert_version(self, version):
        await self._connection.execute(
            "INSERT INTO prompt_version (tenant_id, asset_id, version_id, messages) VALUES (%s, %s, %s, %s)",
            (version.tenant_id, version.asset_id, version.version_id,
             Jsonb([{"role": item.role, "text": item.text} for item in version.messages])))

    async def version(self, tenant_id, asset_id, version_id, *, published_only=False):
        cursor = await self._connection.execute(
            "SELECT v.messages FROM prompt_version v WHERE v.tenant_id=%s AND v.asset_id=%s AND v.version_id=%s"
            + (" AND EXISTS (SELECT 1 FROM prompt_publication p WHERE p.tenant_id=v.tenant_id "
               "AND p.asset_id=v.asset_id AND p.version_id=v.version_id)" if published_only else ""),
            (tenant_id, asset_id, version_id))
        row = await cursor.fetchone()
        return PromptVersion(tenant_id, asset_id, version_id,
                             tuple(Message(item["role"], item["text"]) for item in row[0])) if row else None

    async def publish(self, asset, version_id, subject):
        generation = asset.generation + 1
        await self._connection.execute(
            "INSERT INTO prompt_publication (tenant_id, asset_id, generation, version_id, subject) VALUES (%s, %s, %s, %s, %s)",
            (asset.tenant_id, asset.asset_id, generation, version_id, subject))
        await self._connection.execute(
            "UPDATE prompt_asset SET generation=%s, published_version=%s WHERE tenant_id=%s AND asset_id=%s",
            (generation, version_id, asset.tenant_id, asset.asset_id))
        return PromptAsset(asset.tenant_id, asset.asset_id, generation, version_id)


class PostgresPromptUnitOfWork:
    def __init__(self, dsn):
        self._dsn = dsn

    @asynccontextmanager
    async def transaction(self):
        try:
            async with asyncio.timeout(5):
                async with await psycopg.AsyncConnection.connect(self._dsn, connect_timeout=5) as connection:
                    await connection.execute("SET LOCAL statement_timeout = '5s'")
                    yield PostgresPromptTransaction(connection)
        except psycopg.Error:
            # Never include SQL arguments, database diagnostics or template JSON.
            raise PromptPersistenceUnavailable() from None
