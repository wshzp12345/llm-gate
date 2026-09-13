"""Owned ASGI lifecycle for the implemented synchronous full-service text path.

The deployment supplies validated configuration, transport lifetimes, real
authorization/health and SecretSource ports. No environment, default authority,
listener, automatic migration, or claim of the full v0.1 surface is introduced.
All invocation writers share one fenced store. Key protection probes use a
separate read-only path; their admission effects still use the fenced store.
"""

import asyncio
import math
from contextlib import asynccontextmanager

import uvicorn

from llm_gateway.adapters.model_http import create_development_model_app
from llm_gateway.adapters.model_request_authorization import ModelRequestAuthorization
from llm_gateway.application.active_configuration import LoadedConfiguration
from llm_gateway.application.admitted_execution import _supervised_cleanup
from llm_gateway.application.fingerprint_keys import FingerprintKeys
from llm_gateway.application.model_api import ModelInvocationRejected
from llm_gateway.domain.invocation import InvocationPersistenceUnavailable
from llm_gateway.infrastructure.fingerprint_protection import PostgresFingerprintProtection
from llm_gateway.infrastructure.full_service_text_backend import FullServiceTextBackend
from llm_gateway.infrastructure.invocation import PostgresInvocationStore
from llm_gateway.infrastructure.invocation_recovery import PostgresInvocationRecovery
from llm_gateway.infrastructure.model_execution_owner import PostgresModelExecutionOwner, ModelOwnershipUnavailable
from llm_gateway.infrastructure.startup_recovery import PostgresStartupRecovery


class FullServiceTextProcess:
    def __init__(self, dsn, *, fingerprint_ring, fingerprint_source, configuration,
                 credential_source, transports, health, authorize=None, trace_id,
                 resource_ceilings, draw_jitter, allow_plaintext=False, shutdown_seconds=30,
                 request_authorization=None, prompts=None):
        if (type(shutdown_seconds) not in (int, float) or not math.isfinite(shutdown_seconds)
                or not 0 < shutdown_seconds <= 30):
            raise ValueError("Shutdown budget must be positive and at most 30 seconds")
        if request_authorization is not None:
            if not isinstance(request_authorization, ModelRequestAuthorization) or authorize is not None:
                raise ValueError("Select exactly one request authorization binding")
            authorize = request_authorization.authorize
        if not callable(authorize):
            raise ValueError("Explicit request authorization required")
        self._request_authorization = request_authorization
        self._configuration = configuration
        self._shutdown_seconds = shutdown_seconds
        self._shutdown_deadline = None
        self._shutdown_task = None
        self._entered = self._opened = self._lost = False
        self._releasing = False
        self._tasks = set()
        self._backend = None
        self._owner = PostgresModelExecutionOwner(dsn, on_loss=self._ownership_lost)
        self._store = PostgresInvocationStore(dsn, ownership=self._owner,
                                              lifetime_deadline=lambda: self._shutdown_deadline)
        self._keys = FingerprintKeys(fingerprint_ring, fingerprint_source,
                                    PostgresFingerprintProtection(PostgresInvocationStore(dsn)))
        self._dependencies = dict(configuration=configuration, credential_source=credential_source,
            transports=transports, health=health, authorize=authorize, trace_id=trace_id,
            resource_ceilings=resource_ceilings, draw_jitter=draw_jitter, allow_plaintext=allow_plaintext, prompts=prompts,
            recovery=PostgresInvocationRecovery(self._store))
        self.recovered_count = None

    @property
    def ready(self):
        """Execution readiness for this text slice, not all v0.1 dependencies."""
        return self._opened and not self._lost and self._keys.ready and self._configuration.current is not None

    @property
    def in_use(self):
        return self._backend.in_use if self._backend is not None else 0

    def resource_metrics(self):
        return self._backend.resource_metrics() if self._backend is not None else {}

    def _ownership_lost(self):
        if self._releasing:
            return  # Normal lease release after all local work has exited.
        self._lost = True
        self._opened = False
        if self._backend is not None:
            self._backend.cancel_pending(ownership_lost=True)
        for task in tuple(self._tasks):
            if not task.done() and not task.cancelling():
                task.cancel()

    async def _watch_owner(self, stop):
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=1)
            except TimeoutError:
                try:
                    await self._owner.verify()
                except ModelOwnershipUnavailable:
                    return  # The synchronous loss callback has closed admission.

    @asynccontextmanager
    async def hold(self):
        if self._entered or self._shutdown_task is not None:
            raise RuntimeError("Model process lifecycle cannot be reused")
        self._entered = True
        async with self._owner.hold():
            stop = asyncio.Event()
            watchers = []
            try:
                selected = self._configuration.current
                if not isinstance(selected, LoadedConfiguration):
                    raise ModelInvocationRejected("gateway_not_ready")
                if not await self._keys.validate_active():
                    raise InvocationPersistenceUnavailable()
                self.recovered_count = await PostgresStartupRecovery(self._store,
                    configuration_revision=int(selected.revision)).run()
                self._backend = FullServiceTextBackend(store=self._store, keys=self._keys, **self._dependencies)
                watchers = [asyncio.create_task(self._watch_owner(stop)), asyncio.create_task(self._keys.recover(stop))]
                self._opened = True
                yield self
            finally:
                self._opened = False
                async def close():
                    try:
                        await self.shutdown()
                    finally:
                        stop.set()
                        for task in watchers:
                            task.cancel()
                        await asyncio.gather(*watchers, return_exceptions=True)
                        self._releasing = True
                await _supervised_cleanup(close())

    async def shutdown(self):
        """One shared drain, also called before the HTTP server waits for tasks."""
        self._opened = False
        if self._backend is not None:
            self._backend.stop_admission()
        if self._shutdown_task is None:
            self._shutdown_task = asyncio.create_task(self._shutdown())

        async def join():
            await self._shutdown_task
        await _supervised_cleanup(join())

    async def _shutdown(self):
        if self._backend is None:
            return
        self._backend.stop_admission()
        loop = asyncio.get_running_loop()
        self._shutdown_deadline = loop.time() + self._shutdown_seconds
        tasks = set(self._tasks)
        if tasks and not self._lost:
            # Reserve up to two seconds *inside* the overall window for the
            # existing cancellation protocol; never add a second drain window.
            await asyncio.wait(tasks, timeout=max(0, self._shutdown_seconds - 2))
        self._backend.cancel_pending(shutdown_deadline=self._shutdown_deadline, ownership_lost=self._lost)
        for task in tasks:
            if not task.done() and not task.cancelling():
                task.cancel()
        # Ports must cooperate with cancellation. Do not detach tasks or release
        # process ownership while local execution still owns runtime leases.
        await asyncio.gather(*tasks, return_exceptions=True)

    async def invoke(self, query):
        if self._lost:
            raise ModelOwnershipUnavailable()
        if not self.ready:
            raise ModelInvocationRejected("gateway_not_ready")
        await self._owner.verify()
        # A shutdown can begin while verify awaits PostgreSQL.
        if not self.ready:
            raise ModelInvocationRejected("gateway_not_ready")
        task = asyncio.create_task(self._backend.invoke(query))
        self._tasks.add(task)
        try:
            result = await task
            if self._lost:
                raise ModelOwnershipUnavailable()
            return result
        except asyncio.CancelledError:
            if self._lost:
                raise ModelOwnershipUnavailable() from None
            raise
        finally:
            self._tasks.discard(task)


def create_owned_text_app(process, *, enable_streaming=False):
    """Actual ASGI lifespan binding, retaining the explicit text-only API."""
    if not isinstance(process, FullServiceTextProcess):
        raise ValueError("Owned text process required")

    @asynccontextmanager
    async def lifespan(app):
        async with process.hold():
            yield

    return create_development_model_app(process, lifespan=lifespan, request_authorization=process._request_authorization,
                                        enable_streaming=enable_streaming)


def create_local_jwt_text_process(dsn, *, authorization, **dependencies):
    """One startup-only local-JWT binding; no development or online fallback."""
    from llm_gateway.infrastructure.local_jwt_file import load_local_jwt
    if {"authorize", "request_authorization"}.intersection(dependencies):
        raise ValueError("Local JWT factory owns the complete authorization binding")
    adapter = load_local_jwt(authorization)
    return FullServiceTextProcess(dsn,
        request_authorization=ModelRequestAuthorization(adapter, authentication_method="local_jwt"), **dependencies)


class _OwnedTextServer(uvicorn.Server):
    def __init__(self, config, process):
        super().__init__(config)
        self._process = process

    async def shutdown(self, sockets=None):
        # ASGI lifespan shutdown alone is too late: Uvicorn first waits for
        # HTTP tasks. Start the same Model drain concurrently with that wait.
        # Otherwise it would cancel as client/context cancellation and then
        # start a second Model drain window in lifespan shutdown.
        drain = asyncio.create_task(self._process.shutdown())
        try:
            await super().shutdown(sockets)
        finally:
            await _supervised_cleanup(self._join_drain(drain))

    @staticmethod
    async def _join_drain(task):
        await task


def create_owned_text_server(process, port=8000, *, enable_streaming=False):
    """Explicit loopback server; deployment/production authentication is external."""
    if type(port) is not int or not 0 <= port <= 65535:
        raise ValueError("Invalid Model port")
    app = create_owned_text_app(process, enable_streaming=enable_streaming)
    return _OwnedTextServer(uvicorn.Config(app, host="127.0.0.1", port=port,
        loop="asyncio", http="h11", ws="none", access_log=False, log_level="warning",
        proxy_headers=False, server_header=False, timeout_keep_alive=5,
        timeout_graceful_shutdown=max(0, process._shutdown_seconds - .1)), process)
