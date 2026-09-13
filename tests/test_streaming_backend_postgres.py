import asyncio
from dataclasses import replace
import json
from types import SimpleNamespace

import httpx
import psycopg
import pytest

from llm_gateway.adapters.provider_transport_registry import ProviderTransportRegistry, project_provider_transports
from llm_gateway.adapters.model_http import create_development_model_app
from llm_gateway.application.active_configuration import LoadedConfiguration
from llm_gateway.application.fingerprint_keys import FingerprintKeys
from llm_gateway.domain.model import ProviderFailure, ProviderResult
from llm_gateway.infrastructure.fingerprint_protection import PostgresFingerprintProtection
from llm_gateway.infrastructure.full_service_text_backend import FullServiceTextBackend
from tests.config_fixtures import bundle_submission
from tests.test_configuration import database, run, scalar
from tests.test_configuration_preparation import draft
from tests.test_fingerprint_admission_postgres import prepare
from tests.test_fingerprint_leases import RING, Source
from tests.test_provider_streaming import chunk, wire
from tests.test_text_attempt_runtime import Harness
from tests.test_text_fingerprints import AUTH, LIMITS, QUERY


@pytest.mark.postgres
@pytest.mark.parametrize("entry", ["direct", "http"])
@pytest.mark.parametrize("mode", ["success", "retry", "refusal", "broken", "cancel", "deadline", "unsupported"])
def test_stream_through_real_backend_admission_runtime_and_settlement(database, mode, entry):
    async def scenario():
        body = bundle_submission()
        body["bundle"]["provider_model_bindings"]["binding-a"]["capabilities"]["streaming"] = mode != "unsupported"
        store, record = await prepare(database, body=body)
        value = draft(body)
        selected = LoadedConfiguration(record.configuration_revision, value.snapshot_digest, value.snapshot_json, value.validation_resources)
        keys = FingerprintKeys(RING, Source(), PostgresFingerprintProtection(store))
        assert await keys.validate_active()
        credentials, calls, sent = Harness(), [], []
        traces = []

        class TraceSink:
            def offer(self, trace):
                traces.append(trace)
                return True
        entered, closed = asyncio.Event(), asyncio.Event()

        class Output:
            def admitted(self, call_id, accepted_at):
                self.call_id = call_id
                self.accepted_at = accepted_at

            async def delta(self, event):
                assert scalar(database, "SELECT count(*) FROM invocation_stream_commit") == 1
                assert scalar(database, "SELECT count(*) FROM invocation_settlement") == 0
                assert keys.in_use == 0
                sent.append(event)

        class Body(httpx.AsyncByteStream):
            async def __aiter__(self):
                yield wire(chunk({"refusal" if mode == "refusal" else "content": "private-output"}))
                if mode == "broken":
                    return  # Missing terminal is a failure after commitment.
                yield wire(chunk(finish="stop", usage={"prompt_tokens": 10, "completion_tokens": 6, "total_tokens": 16}))
                if mode in {"cancel", "deadline"}:
                    entered.set()
                    await asyncio.Event().wait()
                yield wire("[DONE]")

            async def aclose(self):
                closed.set()

        async def handler(request):
            calls.append(json.loads(request.content))
            assert calls[-1]["stream"] is True
            assert scalar(database, "SELECT count(*) FROM invocation_fingerprints") == 1
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
                trace_id=lambda: "b" * 32, resource_ceilings=LIMITS | ({"max_invocation_seconds": 2} if mode == "deadline" else {}),
                draw_jitter=lambda _: 0)
            payload = {"model": QUERY.requested_model, "messages": [
                {"role": item.role, "content": item.text} for item in QUERY.messages], "stream": True}
            async def invoke_http():
                app = create_development_model_app(backend, enable_streaming=True, trace_sink=TraceSink())
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gateway") as client:
                    return await client.post("/v1/chat/completions", json=payload)

            task = asyncio.create_task(invoke_http() if entry == "http" else backend.invoke(
                replace(QUERY, stream=True, stream_output=Output(), body_bytes=len(json.dumps(payload).encode()))))
            try:
                if mode == "cancel":
                    await asyncio.wait_for(entered.wait(), 5)
                    task.cancel()
                if mode == "cancel" or mode == "deadline" and entry == "direct":
                    with pytest.raises(asyncio.CancelledError if mode == "cancel" else TimeoutError):
                        await task
                else:
                    reply = await task
                    if entry == "http":
                        if mode == "unsupported":
                            assert reply.status_code == 422 and reply.json()["error"]["code"] == "unsupported_capability"
                        else:
                            assert reply.status_code == 200
                            assert reply.headers["x-gateway-call-id"]
                            frames = [json.loads(frame[6:]) for frame in reply.text.strip().split("\n\n")
                                      if frame != "data: [DONE]"]
                            sent.extend(frame for frame in frames if frame.get("choices", [{}])[0].get("delta"))
                            if mode in {"success", "retry", "refusal"}:
                                assert reply.text.endswith("data: [DONE]\n\n")
                                assert frames[-1]["model"] == "actual-model"
                            else:
                                assert "error" in frames[-1] and "[DONE]" not in reply.text
                    elif mode in {"success", "retry", "refusal"}:
                        assert isinstance(reply.result, ProviderResult)
                        assert reply.result.resolved_model == "actual-model"
                        assert reply.result.usage.input_tokens == (None if mode == "retry" else 10)
                    elif mode == "broken":
                        assert isinstance(reply.result, ProviderFailure)
            finally:
                if not task.done():
                    task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            assert backend.in_use == keys.in_use == 0
            if entry == "http":
                assert len(traces) == 1
                spans = traces[0].spans
                attempts = [span for span in spans if span.stage == "provider_attempt"]
                assert len(attempts) == len(calls)
                if mode in {"cancel", "deadline"}:
                    assert attempts[-1].outcome == "cancelled"
                    assert any(span.stage == ("cancellation" if mode == "cancel" else "settlement") for span in spans)
                elif mode == "broken":
                    assert attempts[-1].outcome == "error"
                    assert next(span for span in spans if span.stage == "http").outcome == "error"
            assert all(not lease._material for lease in credentials.leases)
            assert len(calls) == (0 if mode == "unsupported" else 2 if mode == "retry" else 1)
            assert len(sent) == (0 if entry == "http" and mode == "cancel" else int(mode != "unsupported"))
            if calls:
                assert closed.is_set()
                assert all(call == calls[0] for call in calls)
        with psycopg.connect(database) as connection:
            assert connection.execute("SELECT state FROM model_invocation").fetchone()[0] == (
                "completed" if mode in {"success", "retry", "refusal"} else "cancelled" if mode == "cancel" else "failed")
            assert connection.execute("SELECT count(*) FROM invocation_stream_commit").fetchone()[0] == int(mode != "unsupported")
            assert connection.execute("SELECT count(*) FROM invocation_settlement").fetchone() == (1,)
            assert connection.execute("SELECT safety_refused FROM invocation_settlement").fetchone()[0] == (mode == "refusal")
    run(scenario())
