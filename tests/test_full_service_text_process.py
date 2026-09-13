import asyncio
import socket
from dataclasses import replace
from types import SimpleNamespace

import httpx
import psycopg
import pytest

from llm_gateway.adapters.provider_transport_registry import ProviderTransportRegistry, project_provider_transports
from llm_gateway.domain.invocation import InvocationPersistenceUnavailable
from llm_gateway.infrastructure.full_service_text_process import FullServiceTextProcess, create_owned_text_app, create_owned_text_server
from llm_gateway.infrastructure.invocation import PostgresInvocationStore
from llm_gateway.infrastructure.model_execution_owner import ModelOwnershipBusy, ModelOwnershipUnavailable, PostgresModelExecutionOwner
from llm_gateway.infrastructure.startup_recovery import PostgresStartupRecovery
from tests.test_configuration import database, run, scalar
from tests.test_fingerprint_admission_postgres import prepare
from tests.test_fingerprint_leases import RING, Source
from tests.test_text_attempt_runtime import Harness
from tests.test_text_fingerprints import AUTH, LIMITS, QUERY, snapshot
from tests.test_completion import envelope
from tests.config_fixtures import bundle_submission
from tests.test_configuration_preparation import draft
from tests.test_provider_streaming import chunk, wire
from llm_gateway.application.active_configuration import LoadedConfiguration


VALIDATED_QUERY = replace(QUERY, body_bytes=100)


def process(database, selected, registry, *, source=None, shutdown_seconds=30):
    async def authorize(query):
        return AUTH  # Explicit isolated test authority, never a launcher default.

    async def health(admission, configuration, binding):
        return True  # Fixture observation; not a production health override.

    return FullServiceTextProcess(database, fingerprint_ring=RING, fingerprint_source=source or Source(),
        configuration=SimpleNamespace(current=selected), credential_source=Harness(), transports=registry,
        health=health, authorize=authorize, trace_id=lambda: "b" * 32,
        resource_ceilings=LIMITS, draw_jitter=lambda upper: 0, shutdown_seconds=shutdown_seconds)


@pytest.mark.parametrize("budget", [0, -1, 31, True, float("nan"), float("inf")])
def test_shutdown_budget_is_bounded_before_io(budget):
    with pytest.raises(ValueError):
        process("must not connect", None, None, shutdown_seconds=budget)


def test_expired_process_deadline_prevents_checkpoint_connection():
    async def scenario():
        store = PostgresInvocationStore("must not connect", lifetime_deadline=lambda: asyncio.get_running_loop().time() - 1)
        with pytest.raises(InvocationPersistenceUnavailable):
            async with store.transaction():
                raise AssertionError("expired checkpoint opened")
    run(scenario())


@pytest.mark.postgres
def test_asgi_lifespan_recovers_before_opening_and_new_http_execution_is_fenced(database):
    async def scenario():
        store, record = await prepare(database)
        await store.admit_unkeyed_shell(record)
        selected = snapshot(revision=record.configuration_revision)
        calls = []
        async def handler(request):
            calls.append(request)
            assert scalar(database, "SELECT count(*) FROM invocation_restart_recovery") == 1
            assert scalar(database, "SELECT count(*) FROM model_invocation WHERE state='running' AND execution_owner_id IS NOT NULL") == 1
            return httpx.Response(200, json=envelope())

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False) as upstream:
            registry = ProviderTransportRegistry(factory=lambda plan: upstream)
            await registry.install(selected.revision, project_provider_transports(selected))
            service = process(database, selected, registry)
            app = create_owned_text_app(service)
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gateway.test") as caller:
                payload = {"model": "general", "messages": [{"role": "user", "content": "private text"}]}
                assert (await caller.post("/v1/chat/completions", json=payload)).status_code == 503
                assert calls == [] and not service.ready
                async with app.router.lifespan_context(app):
                    assert service.ready and service.recovered_count == 1
                    response = await caller.post("/v1/chat/completions", json=payload)
                    assert response.status_code == 200, response.text
                    assert service.in_use == 0
                assert not service.ready and len(calls) == 1
            assert scalar(database, "SELECT count(*) FROM model_invocation WHERE state='uncertain'") == 1
            assert scalar(database, "SELECT count(*) FROM model_invocation WHERE state='completed'") == 1
            with pytest.raises(RuntimeError):
                async with service.hold():
                    pass
        new = PostgresModelExecutionOwner(database, on_loss=lambda: None)
        async with new.hold():
            await new.verify()  # Process exit actually released the lease.
    run(scenario())


@pytest.mark.postgres
@pytest.mark.parametrize("finish_during_drain", [True, False])
def test_shutdown_drains_or_records_system_cancel_before_upstream_abort(database, finish_during_drain):
    async def scenario():
        _, record = await prepare(database)
        selected = snapshot(revision=record.configuration_revision)
        entered, release, exited = asyncio.Event(), asyncio.Event(), asyncio.Event()
        async def handler(request):
            entered.set()
            try:
                await release.wait()
                return httpx.Response(200, json=envelope())
            except asyncio.CancelledError:
                assert scalar(database, "SELECT state FROM model_invocation") == "cancelling"
                assert scalar(database, "SELECT reason FROM invocation_local_cancellation") == "shutdown_drain_expired"
                raise
            finally:
                exited.set()

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False) as upstream:
            registry = ProviderTransportRegistry(factory=lambda plan: upstream)
            await registry.install(selected.revision, project_provider_transports(selected))
            service = process(database, selected, registry, shutdown_seconds=2.5 if finish_during_drain else 1)
            async with service.hold():
                task = asyncio.create_task(service.invoke(VALIDATED_QUERY))
                await asyncio.wait_for(entered.wait(), 3)
                if finish_during_drain:
                    asyncio.get_running_loop().call_later(.05, release.set)
            if finish_during_drain:
                await task
            else:
                with pytest.raises(asyncio.CancelledError):
                    await task
            assert exited.is_set() and service.in_use == 0 and not service.ready
            assert scalar(database, "SELECT state FROM model_invocation") == ("completed" if finish_during_drain else "cancelled")
            assert scalar(database, "SELECT count(*) FROM provider_attempt") == 1
    run(scenario())


@pytest.mark.postgres
def test_owner_loss_aborts_inflight_work_and_next_process_recovers_without_replay(database):
    async def scenario():
        _, record = await prepare(database)
        selected = snapshot(revision=record.configuration_revision)
        entered, exited = asyncio.Event(), asyncio.Event()
        async def handler(request):
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                exited.set()
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False) as upstream:
            registry = ProviderTransportRegistry(factory=lambda plan: upstream)
            await registry.install(selected.revision, project_provider_transports(selected))
            service = process(database, selected, registry)
            async with service.hold():
                task = asyncio.create_task(service.invoke(VALIDATED_QUERY))
                await asyncio.wait_for(entered.wait(), 3)
                with psycopg.connect(database) as connection:
                    # Kill only this fixture's exact owner connection.
                    assert connection.execute("SELECT pg_terminate_backend(%s,1000)", (service._owner._connection.info.backend_pid,)).fetchone()[0]
                await asyncio.wait_for(exited.wait(), 3)  # Real owner watcher, not an injected callback.
                with pytest.raises(ModelOwnershipUnavailable):
                    await task
                assert not service.ready and service.in_use == 0
                assert scalar(database, "SELECT state FROM model_invocation") == "running"
                assert scalar(database, "SELECT count(*) FROM invocation_local_cancellation") == 0
                with pytest.raises(ModelOwnershipUnavailable):
                    await service.invoke(QUERY)
                next_owner = PostgresModelExecutionOwner(database, on_loss=lambda: None)
                async with next_owner.hold():
                    assert await PostgresStartupRecovery(PostgresInvocationStore(database, ownership=next_owner),
                        configuration_revision=int(record.configuration_revision)).run() == 1
                assert scalar(database, "SELECT state FROM model_invocation") == "uncertain"
                assert scalar(database, "SELECT count(*) FROM provider_attempt") == 1
    run(scenario())


@pytest.mark.postgres
def test_failed_startup_does_not_open_admission_and_competitor_cannot_start(database):
    async def scenario():
        store, record = await prepare(database)
        await store.admit_unkeyed_shell(record)
        selected = snapshot(revision=record.configuration_revision)
        source = Source()
        source.fail.add(RING.active.identity)
        bad = process(database, selected, None, source=source)
        with pytest.raises(InvocationPersistenceUnavailable):
            async with bad.hold():
                raise AssertionError("bad key admitted startup")
        assert not bad.ready and scalar(database, "SELECT state FROM model_invocation") == "accepted"
        good = process(database, selected, None)
        async with good.hold():
            other_source = Source()
            other = process(database, selected, None, source=other_source)
            with pytest.raises(ModelOwnershipBusy):
                async with other.hold():
                    raise AssertionError("two process owners")
            assert other_source.calls == [] and not other.ready
            assert good.ready and good.recovered_count == 1
    run(scenario())


@pytest.mark.postgres
def test_owned_checkpoint_uses_remaining_shutdown_budget(database):
    async def scenario():
        owner = PostgresModelExecutionOwner(database, on_loss=lambda: None)
        async with owner.hold():
            loop = asyncio.get_running_loop()
            deadline = loop.time() + .1
            store = PostgresInvocationStore(database, ownership=owner, lifetime_deadline=lambda: deadline)
            with pytest.raises(InvocationPersistenceUnavailable):
                async with store.transaction() as connection:
                    await connection.execute("SELECT pg_sleep(5)")
            assert loop.time() < deadline + 1
            await owner.verify()
    run(scenario())


@pytest.mark.postgres
@pytest.mark.parametrize("complete", [True, False])
@pytest.mark.parametrize("streaming", [False, True])
def test_real_http_server_shares_model_drain_with_lifespan(database, complete, streaming):
    async def scenario():
        body = bundle_submission()
        body["bundle"]["provider_model_bindings"]["binding-a"]["capabilities"]["streaming"] = streaming
        _, record = await prepare(database, body=body)
        value = draft(body)
        selected = LoadedConfiguration(record.configuration_revision, value.snapshot_digest, value.snapshot_json, value.validation_resources)
        entered, release, exited = asyncio.Event(), asyncio.Event(), asyncio.Event()

        class StreamBody(httpx.AsyncByteStream):
            async def __aiter__(self):
                yield wire(chunk({"content": "hello"}))
                entered.set()
                await release.wait()
                yield wire(chunk(finish="stop"))
                yield wire("[DONE]")

            async def aclose(self):
                exited.set()

        async def handler(request):
            if streaming:
                return httpx.Response(200, stream=StreamBody(), headers={"content-type": "text/event-stream"})
            entered.set()
            try:
                await release.wait()
                return httpx.Response(200, json=envelope())
            finally:
                exited.set()

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False) as upstream:
            registry = ProviderTransportRegistry(factory=lambda plan: upstream)
            await registry.install(selected.revision, project_provider_transports(selected))
            service = process(database, selected, registry, shutdown_seconds=3 if complete else 1)
            server = create_owned_text_server(service, 0, enable_streaming=streaming)
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
                listener.bind(("127.0.0.1", 0))
                listener.listen(32)
                listener.setblocking(False)
                port = listener.getsockname()[1]
                serving = asyncio.create_task(server.serve(sockets=[listener]))
                request = None
                try:
                    async with asyncio.timeout(10):
                        while not server.started:
                            if serving.done():
                                await serving
                                raise AssertionError("Server exited before Model startup")
                            await asyncio.sleep(.01)
                        async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}", trust_env=False, timeout=5) as caller:
                            request = asyncio.create_task(caller.post("/v1/chat/completions", json={"model": "general",
                                "messages": [{"role": "user", "content": "test message"}], "stream": streaming}))
                            await asyncio.wait_for(entered.wait(), 3)
                            if complete:
                                asyncio.get_running_loop().call_later(.25, release.set)
                            server.should_exit = True
                            await serving
                            replies = await asyncio.gather(request, return_exceptions=True)
                            if complete:
                                assert isinstance(replies[0], httpx.Response) and replies[0].status_code == 200
                                if streaming:
                                    assert replies[0].text.endswith("data: [DONE]\n\n")
                            elif streaming:
                                assert not isinstance(replies[0], httpx.Response) or "[DONE]" not in replies[0].text
                            else:
                                assert not isinstance(replies[0], httpx.Response) or replies[0].status_code != 200
                finally:
                    server.should_exit = True
                    if not serving.done():
                        serving.cancel()
                    if request is not None and not request.done():
                        request.cancel()
                    await asyncio.gather(serving, *([request] if request is not None else []), return_exceptions=True)
            assert exited.is_set() and not service.ready and service.in_use == 0
            assert service._shutdown_task.done()
            assert scalar(database, "SELECT state FROM model_invocation") == ("completed" if complete else "cancelled")
            if not complete:
                assert scalar(database, "SELECT reason FROM invocation_local_cancellation") == "shutdown_drain_expired"
            owner = PostgresModelExecutionOwner(database, on_loss=lambda: None)
            async with owner.hold():
                await owner.verify()
    run(scenario())
