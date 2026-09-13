"""Unified dev text service. Own dependencies once, serve two isolated APIs."""

import argparse
import asyncio
import json
import random
import signal
from contextlib import AsyncExitStack, asynccontextmanager, contextmanager
from datetime import datetime, timezone
from pathlib import Path

import uvicorn

from llm_gateway.adapters.active_configuration import CanonicalSnapshotDecoder
from llm_gateway.adapters.canonical_json import canonical_digest
from llm_gateway.adapters.configuration_http import create_development_management_app
from llm_gateway.adapters.fingerprint_material import FingerprintMaterialMetadata
from llm_gateway.adapters.model_http import create_development_model_app
from llm_gateway.adapters.prompt_http import register_prompt_routes
from llm_gateway.adapters.otlp_http import OtlpHttpOutput
from llm_gateway.application.prompts import Prompts
from llm_gateway.infrastructure.prompts import PostgresPromptUnitOfWork
from llm_gateway.adapters.provider_transport_registry import ProviderTransportFactory, ProviderTransportRegistry, ProviderTransportSnapshots
from llm_gateway.application.active_configuration import ActiveConfigurationLoader
from llm_gateway.application.configuration import ConfigurationCommands
from llm_gateway.application.model_api import ModelInvocationRejected
from llm_gateway.application.request_trace import current_trace_id
from llm_gateway.application.trace_export import BoundedTraceExporter
from llm_gateway.infrastructure.invocation_observation import PostgresInvocationObservation
from llm_gateway.application.runtime_configuration import RuntimeConfiguration
from llm_gateway.domain.invocation import AuthorizationContext
from llm_gateway.infrastructure.configuration import PostgresConfigurationUnitOfWork
from llm_gateway.infrastructure.credential_resolution import AsyncCredentialResolver
from llm_gateway.infrastructure.development_fingerprint_source import DevelopmentFingerprintSource, MountedFingerprintSecret
from llm_gateway.infrastructure.development_management import DEV_PARSE_LIMITS
from llm_gateway.infrastructure.development_policy import development_validator, development_active_validator, _development_policy
from llm_gateway.infrastructure.development_secrets import DevelopmentSecretSource
from llm_gateway.infrastructure.fingerprint_resolution import AsyncFingerprintResolver
from llm_gateway.infrastructure.full_service_text_process import FullServiceTextProcess
from llm_gateway.infrastructure.gateway_bootstrap import load_bootstrap, read_database_url
from llm_gateway.infrastructure.migrate import verify_schema
from llm_gateway.infrastructure.provider_dns import BoundedDnsResolver
from llm_gateway.infrastructure.system_tls import system_tls_context


DEV_AUTHORIZATION = AuthorizationContext("dev", "dev", frozenset({"dev:*"}),
    "urn:llm-gateway:dev", "llm-gateway", datetime(9999, 12, 31, 23, 59, 59, tzinfo=timezone.utc), "dev_bypass")


async def authorize_dev(query):
    return DEV_AUTHORIZATION


async def passive_health(admission, snapshot, binding):
    # Passive Attempt outcomes are sampled by the assembled CircuitCoordinator.
    # Never pretend an enabled, unscheduled active probe was successful.
    content = json.loads(snapshot.snapshot_json)
    provider = content["provider_model_bindings"][binding]["provider"]
    return content["providers"][provider]["health"]["active_probe"] is None


class Gateway:
    def __init__(self, config, dsn, *, enable_streaming=False, trace_sink=None, trace_output=None):
        if trace_sink is not None and trace_output is not None:
            raise ValueError("Choose one Trace sink or managed output")
        self.trace_exporter = (BoundedTraceExporter(PostgresInvocationObservation(dsn), trace_output,
            resource_reader=lambda: self.process.resource_metrics() if self.process is not None else {})
                               if trace_output is not None else None)
        self.config, self.dsn = config, dsn
        self.process = None
        self.stopping = False
        self.startup_failed = False
        self.resolver = BoundedDnsResolver(max_concurrent_lookups=4)
        self.transports = ProviderTransportRegistry(factory=ProviderTransportFactory(
            resolver=self.resolver, system_context=system_tls_context, allow_plaintext=True))
        self.credentials = AsyncCredentialResolver(DevelopmentSecretSource(startup_profile="dev",
            files={key: Path(value) for key, value in config.provider_secret_files.items()}), max_concurrent_reads=32)
        self.fingerprints = AsyncFingerprintResolver(DevelopmentFingerprintSource(startup_profile="dev",
            entries=tuple(MountedFingerprintSecret(FingerprintMaterialMetadata(item.key_id,
                item.key_version, item.secret_ref), Path(item.file)) for item in config.fingerprint_keys)))
        self.uow = PostgresConfigurationUnitOfWork(dsn)
        self.runtime = RuntimeConfiguration(loader=ActiveConfigurationLoader(self.uow,
            CanonicalSnapshotDecoder(DEV_PARSE_LIMITS), development_active_validator(known_secret_references=config.provider_secret_files, enable_streaming=enable_streaming)),
            target=ProviderTransportSnapshots(self.transports), timeout_seconds=5)
        self.management_app = create_development_management_app(ConfigurationCommands(self.uow), self.uow,
            DEV_PARSE_LIMITS, command_timeout_seconds=5, validator=development_validator(known_secret_references=config.provider_secret_files, enable_streaming=enable_streaming),
            readiness=lambda: self.ready, after_publication=self.runtime.after_publication)
        self.model_app = create_development_model_app(self, enable_streaming=enable_streaming,
            trace_sink=self.trace_exporter if self.trace_exporter is not None else trace_sink)
        self.prompts = Prompts(PostgresPromptUnitOfWork(dsn))
        register_prompt_routes(self.management_app, self.prompts, authorize=authorize_dev)

    @property
    def ready(self):
        return not self.stopping and self.process is not None and self.process.ready

    async def invoke(self, query):
        if not self.ready:
            raise ModelInvocationRejected("gateway_not_ready")
        return await self.process.invoke(query)

    async def _model_lifetime(self, stop):
        while not stop.is_set() and not self.stopping:
            if self.runtime.current is not None:
                try:
                    self.process = FullServiceTextProcess(self.dsn, fingerprint_ring=self.config.ring(),
                        fingerprint_source=self.fingerprints, configuration=self.runtime,
                        credential_source=self.credentials, transports=self.transports, health=passive_health,
                        authorize=authorize_dev, trace_id=current_trace_id,
                        resource_ceilings=_development_policy().resource_ceilings,
                        draw_jitter=lambda upper: random.randint(0, upper), allow_plaintext=True, prompts=self.prompts)
                    async with self.process.hold():
                        await stop.wait()
                except Exception:
                    # Management recovery remains available; never reclaim a lost
                    # execution epoch automatically. Restart after correcting it.
                    self.startup_failed = True
                    print("Model execution unavailable; correct local dependencies and restart", flush=True)
                return
            try:
                await asyncio.wait_for(stop.wait(), .1)
            except TimeoutError:
                pass

    @asynccontextmanager
    async def hold(self):
        async with AsyncExitStack() as exports:
            if self.trace_exporter is not None:
                await exports.enter_async_context(self.trace_exporter.hold())
            async with self._hold_execution():
                yield self

    @asynccontextmanager
    async def _hold_execution(self):
        stop = asyncio.Event()
        tasks = []
        try:
            await verify_schema(self.dsn, expected_version=self.config.expected_schema_version, timeout_seconds=5)
            try:
                await self.runtime.refresh()
            except Exception:
                pass
            tasks = [asyncio.create_task(self.runtime.watch(stop=stop, interval_seconds=5)),
                     asyncio.create_task(self._model_lifetime(stop))]
            yield self
        finally:
            self.stopping = True
            stop.set()
            try:
                if self.process is not None:
                    await self.process.shutdown()
            finally:
                await asyncio.gather(*tasks, return_exceptions=True)
                try:
                    await self.transports.aclose()
                finally:
                    self.credentials.close()
                    self.fingerprints.close()
                    self.resolver.close()


class Listener(uvicorn.Server):
    @contextmanager
    def capture_signals(self):
        yield  # The shared supervisor owns signals and the single drain window.


def create_servers(gateway):
    return tuple(Listener(uvicorn.Config(app, host=gateway.config.bind_host, port=port,
        loop="asyncio", http="h11", ws="none", access_log=False, log_level="warning",
        proxy_headers=False, server_header=False, timeout_keep_alive=5,
        timeout_graceful_shutdown=29.9)) for app, port in (
            (gateway.model_app, gateway.config.model_port),
            (gateway.management_app, gateway.config.management_port)))


async def _serve_listener(server):
    try:
        await server.serve()
    except SystemExit:
        # Keep Uvicorn bind failures inside supervision to join the sibling.
        raise RuntimeError("Gateway listener could not start") from None


@asynccontextmanager
async def configured_gateway(config, *, enable_streaming=False):
    # Output lifetime encloses Gateway's worker/drain lifetime, including failures.
    dsn = read_database_url(config)
    async with AsyncExitStack() as stack:
        output = None
        if config.telemetry is not None:
            output = await stack.enter_async_context(
                OtlpHttpOutput(config.telemetry.endpoint, allow_plaintext=True).hold())
        gateway = Gateway(config, dsn, enable_streaming=enable_streaming, trace_output=output)
        await stack.enter_async_context(gateway.hold())
        yield gateway


async def serve(config, *, stop=None, enable_streaming=False):
    stop = stop or asyncio.Event()
    loop = asyncio.get_running_loop()
    previous = {}
    for number in (signal.SIGINT, signal.SIGTERM):
        previous[number] = signal.signal(number, lambda *_: loop.call_soon_threadsafe(stop.set))
    try:
        async with configured_gateway(config, enable_streaming=enable_streaming) as gateway:
            servers = create_servers(gateway)
            tasks = [asyncio.create_task(_serve_listener(server)) for server in servers]
            waiter = asyncio.create_task(stop.wait())
            try:
                await asyncio.wait([*tasks, waiter], return_when=asyncio.FIRST_COMPLETED)
            finally:
                gateway.stopping = True
                for server in servers:
                    server.should_exit = True
                if gateway.process is not None:
                    await gateway.process.shutdown()
                waiter.cancel()
                await asyncio.gather(waiter, return_exceptions=True)
                await asyncio.gather(*tasks)
            if not stop.is_set():
                raise RuntimeError("Listener stopped unexpectedly")
    finally:
        for number, handler in previous.items():
            signal.signal(number, handler)


def main():
    parser = argparse.ArgumentParser(description="Unified development text Gateway (not production)")
    parser.add_argument("--bootstrap", type=Path, required=True)
    parser.add_argument("--enable-streaming", action="store_true", help="Enable the streaming API and capability validation together")
    args = parser.parse_args()
    try:
        config = load_bootstrap(args.bootstrap)
        print(json.dumps({"startup_profile": config.startup_profile, "authorization_mode": config.authorization.mode,
            "bootstrap_digest": canonical_digest(config.model_dump())}), flush=True)
        asyncio.run(serve(config, enable_streaming=args.enable_streaming), loop_factory=asyncio.SelectorEventLoop)
    except KeyboardInterrupt:
        pass
    except Exception:
        raise SystemExit("Gateway startup failed; check Bootstrap, mounted Secrets and database Schema") from None


if __name__ == "__main__":
    main()
