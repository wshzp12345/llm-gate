"""Loopback-only development management launcher, not a complete Gateway.

Runtime configuration and lazy transport lifecycle are observed here; model
admission, production authorization and automatic publication remain absent.
"""

import argparse
import asyncio
from contextlib import asynccontextmanager
from pathlib import Path

import uvicorn

from llm_gateway.adapters.configuration_http import create_development_management_app
from llm_gateway.adapters.configuration_json import ParseLimits
from llm_gateway.application.configuration import ConfigurationCommands
from llm_gateway.application.active_configuration import ActiveConfigurationLoader
from llm_gateway.application.runtime_configuration import RuntimeConfiguration
from llm_gateway.adapters.active_configuration import CanonicalSnapshotDecoder
from llm_gateway.adapters.provider_transport_registry import (
    ProviderTransportFactory, ProviderTransportRegistry, ProviderTransportSnapshots,
)
from llm_gateway.infrastructure.configuration import PostgresConfigurationUnitOfWork
from llm_gateway.infrastructure.development_environment import load_development_environment, gateway_database_url
from llm_gateway.infrastructure.migrate import verify_schema
from llm_gateway.infrastructure.development_policy import development_validator, development_active_validator
from llm_gateway.infrastructure.provider_dns import BoundedDnsResolver
from llm_gateway.infrastructure.system_tls import system_tls_context


# Explicit ceilings for this partial dev launcher, not frozen Bootstrap defaults.
DEV_PARSE_LIMITS = ParseLimits(1048576, 64, 100000, 10000, 1048576)


def create_app(database_url: str):
    uow = PostgresConfigurationUnitOfWork(database_url)
    resolver = BoundedDnsResolver(max_concurrent_lookups=4)
    registry = ProviderTransportRegistry(factory=ProviderTransportFactory(resolver=resolver,
        system_context=system_tls_context, allow_plaintext=True))
    runtime = RuntimeConfiguration(loader=ActiveConfigurationLoader(uow, CanonicalSnapshotDecoder(DEV_PARSE_LIMITS),
        development_active_validator()), target=ProviderTransportSnapshots(registry), timeout_seconds=5)

    @asynccontextmanager
    async def lifespan(app):
        stop = asyncio.Event()
        watcher = None
        try:
            try:
                await runtime.refresh()
            except Exception:
                # Invalid/unavailable Active state must not remove recovery APIs.
                pass
            watcher = asyncio.create_task(runtime.watch(stop=stop, interval_seconds=5))
            yield
        finally:
            stop.set()
            if watcher is not None:
                watcher.cancel()
                await asyncio.gather(watcher, return_exceptions=True)
            try:
                await registry.aclose()
            finally:
                resolver.close()

    app = create_development_management_app(ConfigurationCommands(uow), uow, DEV_PARSE_LIMITS,
        command_timeout_seconds=5, validator=development_validator(),
        after_publication=runtime.after_publication, lifespan=lifespan)
    app.state.runtime_configuration = runtime
    app.state.provider_transports = registry
    # Readiness deliberately stays false until the Model execution backend exists.
    return app


def create_server(app, port: int = 8001) -> uvicorn.Server:
    if type(port) is not int or not 0 <= port <= 65535:
        raise ValueError("Invalid management port")
    return uvicorn.Server(uvicorn.Config(
        app, host="127.0.0.1", port=port, loop="asyncio", http="h11", ws="none",
        access_log=False, log_level="warning", proxy_headers=False, server_header=False,
        timeout_keep_alive=5, timeout_graceful_shutdown=10,
    ))


async def serve(env_file: Path, port: int = 8001):
    environment = load_development_environment(env_file)
    database_url = gateway_database_url(environment.database_url)
    await verify_schema(database_url, expected_version="0018_invocation_correlation.sql", timeout_seconds=5)
    # The key is not passed to management services or retained for Provider use.
    del environment
    server = create_server(create_app(database_url), port)
    await server.serve()


def main():
    parser = argparse.ArgumentParser(description="Partial dev management API on loopback only; no model calls")
    parser.add_argument("--env-file", type=Path, default=Path(".env.local"))
    parser.add_argument("--port", type=int, default=8001)
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("port must be between 1 and 65535")
    try:
        # psycopg async requires SelectorEventLoop on Windows.
        asyncio.run(serve(args.env_file, args.port), loop_factory=asyncio.SelectorEventLoop)
    except KeyboardInterrupt:
        pass
    except Exception:
        raise SystemExit("Development management failed; check local configuration and database Schema") from None


if __name__ == "__main__":
    main()
