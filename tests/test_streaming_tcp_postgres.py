"""Real caller socket -> HTTP router -> real PostgreSQL, controlled Provider."""

import asyncio
from contextlib import asynccontextmanager
import json
import socket
from types import SimpleNamespace

import httpx
import pytest
import uvicorn

from llm_gateway.adapters.model_http import create_development_model_app
from llm_gateway.adapters.provider_transport_registry import ProviderTransportRegistry, project_provider_transports
from llm_gateway.application.active_configuration import LoadedConfiguration
from llm_gateway.application.fingerprint_keys import FingerprintKeys
from llm_gateway.infrastructure.fingerprint_protection import PostgresFingerprintProtection
from llm_gateway.infrastructure.full_service_text_backend import FullServiceTextBackend
from llm_gateway.infrastructure.invocation_observation import PostgresInvocationObservation
from tests.config_fixtures import bundle_submission
from tests.test_configuration import database, run, scalar
from tests.test_configuration_preparation import draft
from tests.test_fingerprint_admission_postgres import prepare
from tests.test_fingerprint_leases import RING, Source
from tests.test_provider_streaming import chunk, wire
from tests.test_text_attempt_runtime import Harness
from tests.test_text_fingerprints import AUTH, LIMITS


@asynccontextmanager
async def listening(app):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
        server = uvicorn.Server(uvicorn.Config(app, loop="asyncio", http="h11", ws="none",
            access_log=False, log_level="warning", proxy_headers=False, server_header=False,
            timeout_graceful_shutdown=5))
        serving = asyncio.create_task(server.serve(sockets=[listener]))
        try:
            async with asyncio.timeout(5):
                while not server.started:
                    if serving.done():
                        await serving
                        raise AssertionError("Server stopped before startup")
                    await asyncio.sleep(0.01)
            yield port
        finally:
            server.should_exit = True
            await asyncio.wait_for(serving, 10)


@pytest.mark.postgres
@pytest.mark.parametrize("mode", ["success", "retry", "refusal", "broken", "disconnect_before", "disconnect_after", "deadline"])
def test_real_tcp_stream_delivery_and_settlement(database, mode):
    async def scenario():
        body = bundle_submission()
        body["bundle"]["provider_model_bindings"]["binding-a"]["capabilities"]["streaming"] = True
        store, record = await prepare(database, body=body)
        value = draft(body)
        selected = LoadedConfiguration(record.configuration_revision, value.snapshot_digest, value.snapshot_json, value.validation_resources)
        keys = FingerprintKeys(RING, Source(), PostgresFingerprintProtection(store))
        assert await keys.validate_active()
        credentials = Harness()
        entered, release, closed = asyncio.Event(), asyncio.Event(), asyncio.Event()
        calls = []
        traces = []

        class TraceSink:
            def offer(self, trace):
                traces.append(trace)
                return True

        class Body(httpx.AsyncByteStream):
            async def __aiter__(self):
                entered.set()
                if mode == "disconnect_before":
                    await asyncio.Future()
                yield wire(chunk({"refusal" if mode == "refusal" else "content": "private-output"}))
                # The real client must see the first delta before allowing the
                # Provider to continue; whole-response buffering would deadlock.
                await release.wait()
                if mode == "broken":
                    return
                yield wire(chunk(finish="stop", usage={"prompt_tokens": 10, "completion_tokens": 6, "total_tokens": 16}))
                if mode in {"disconnect_after", "deadline"}:
                    await asyncio.Future()
                yield wire("[DONE]")

            async def aclose(self):
                closed.set()

        async def handler(request):
            calls.append(json.loads(request.content))
            if mode == "retry" and len(calls) == 1:
                return httpx.Response(503)
            return httpx.Response(200, stream=Body(), headers={"content-type": "text/event-stream"})

        async def authorize(query):
            return AUTH

        async def health(*args):
            return True

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False) as upstream:
            registry = ProviderTransportRegistry(factory=lambda plan: upstream)
            await registry.install(selected.revision, project_provider_transports(selected))
            backend = FullServiceTextBackend(store=store, keys=keys, configuration=SimpleNamespace(current=selected),
                credential_source=credentials, transports=registry, health=health, authorize=authorize,
                trace_id=lambda: "c" * 32, resource_ceilings=LIMITS | {"max_invocation_seconds": 5},
                draw_jitter=lambda _: 0)
            app = create_development_model_app(backend, enable_streaming=True, trace_sink=TraceSink())
            payload = {"model": "general", "messages": [{"role": "user", "content": "fixture"}], "stream": True}
            async with listening(app) as port:
                if mode == "disconnect_before":
                    reader, writer = await asyncio.open_connection("127.0.0.1", port)
                    data = json.dumps(payload).encode()
                    writer.write(b"POST /v1/chat/completions HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: "
                                 + str(len(data)).encode() + b"\r\n\r\n" + data)
                    await writer.drain()
                    try:
                        await asyncio.wait_for(entered.wait(), 3)
                        assert scalar(database, "SELECT count(*) FROM invocation_stream_commit") == 0
                        # No response headers have been committed yet.
                        with pytest.raises(TimeoutError):
                            await asyncio.wait_for(reader.read(1), 0.05)
                    finally:
                        writer.close()
                        await writer.wait_closed()
                else:
                    async with httpx.AsyncClient(trust_env=False, timeout=8) as caller:
                        async with caller.stream("POST", f"http://127.0.0.1:{port}/v1/chat/completions", json=payload) as response:
                            assert response.status_code == 200
                            lines = response.aiter_lines()
                            first = json.loads((await anext(lines))[6:])
                            assert first["choices"][0]["delta"] == {
                                "refusal" if mode == "refusal" else "content": "private-output"}
                            assert response.headers["x-gateway-call-id"] == first["id"][9:]
                            assert scalar(database, "SELECT count(*) FROM invocation_stream_commit") == 1
                            assert scalar(database, "SELECT count(*) FROM invocation_settlement") == 0
                            assert backend.in_use == 1
                            release.set()
                            if mode != "disconnect_after":
                                remaining = [line async for line in lines if line]
                                if mode in {"success", "retry", "refusal"}:
                                    assert remaining[-1] == "data: [DONE]"
                                    terminal = json.loads(remaining[-2][6:])
                                    assert terminal["choices"][0]["finish_reason"] == "stop"
                                    assert scalar(database, "SELECT state FROM model_invocation") == "completed"
                                else:
                                    assert all("[DONE]" not in line for line in remaining)
                                    assert json.loads(remaining[-1][6:])["error"]["code"] == (
                                        "deadline_exceeded" if mode == "deadline" else "provider_protocol_error")
                # Socket close returns before server-side durable cleanup. Wait
                # for authoritative backend ownership to be released.
                async with asyncio.timeout(8):
                    while backend.in_use or not traces:
                        await asyncio.sleep(0.01)
                assert closed.is_set() and keys.in_use == 0
                observation = await PostgresInvocationObservation(database).read(traces[0].call_id)
                assert observation.state == scalar(database, "SELECT state FROM model_invocation")
                assert len(observation.attempts) == len(calls)
                assert not observation.model_conflict
                if mode != "disconnect_before":
                    assert observation.actual_model == scalar(database, "SELECT resolved_model FROM invocation_stream_commit")
                root = next(span for span in traces[0].spans if span.stage == "http")
                assert root.outcome == ("cancelled" if mode.startswith("disconnect") else
                                        "error" if mode in {"broken", "deadline"} else "ok")
                assert all(not lease._material for lease in credentials.leases)
                assert len(calls) == (2 if mode == "retry" else 1)
                assert all(call == calls[0] for call in calls)
                assert scalar(database, "SELECT count(*) FROM invocation_settlement") == 1
                assert scalar(database, "SELECT state FROM model_invocation") == (
                    "completed" if mode in {"success", "retry", "refusal"} else
                    "cancelled" if mode.startswith("disconnect") else "failed")
    run(scenario())
