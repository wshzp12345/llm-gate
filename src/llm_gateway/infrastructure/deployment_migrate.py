"""Independent migration job. Wait for DB, migrate once, verify exactly."""

import argparse
import asyncio
from pathlib import Path

import psycopg

from llm_gateway.infrastructure.gateway_bootstrap import load_bootstrap, read_database_url
from llm_gateway.infrastructure.migrate import migrate, verify_schema


async def run(config):
    dsn = read_database_url(config)
    # Only readiness connections are retried; migration failures are terminal.
    async with asyncio.timeout(60):
        while True:
            try:
                async with await psycopg.AsyncConnection.connect(dsn, connect_timeout=3) as connection:
                    await connection.execute("SELECT 1")
                break
            except psycopg.OperationalError:
                await asyncio.sleep(1)
        async with await psycopg.AsyncConnection.connect(dsn, connect_timeout=3) as connection:
            await connection.execute("CREATE SCHEMA IF NOT EXISTS llm_gateway")
        await migrate(dsn)
        await verify_schema(dsn, expected_version=config.expected_schema_version, timeout_seconds=5)


def main():
    parser = argparse.ArgumentParser(description="Independent development deployment migration job")
    parser.add_argument("--bootstrap", type=Path, required=True)
    args = parser.parse_args()
    try:
        asyncio.run(run(load_bootstrap(args.bootstrap)), loop_factory=asyncio.SelectorEventLoop)
    except Exception:
        raise SystemExit("Migration job failed; Gateway admission must remain closed") from None
    print("Database Schema verified")


if __name__ == "__main__":
    main()
