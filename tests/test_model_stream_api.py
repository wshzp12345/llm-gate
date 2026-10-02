import asyncio
from contextlib import asynccontextmanager
from contextvars import ContextVar
import json

import httpx
import pytest

from llm_gateway.adapters.model_http import create_development_model_app
from llm_gateway.application.model_api import InvocationReply, ModelInvocationRejected
from llm_gateway.domain.model import FailureCode, ProviderFailure
from llm_gateway.domain.streaming import DeltaKind, StreamDelta
from tests.test_attempt_execution import SUCCESS
from tests.test_model_http import PAYLOAD
from tests.test_model_stream_http import CALL, ACCEPTED


class Service:
    def __init__(self, *, error=None, after_delta=False):
        self.queries = []
        self.error, self.after_delta = error, after_delta

    async def invoke(self, query):
        self.queries.append(query)
        query.stream_output.admitted(CALL, ACCEPTED)
        if self.error is None or self.after_delta:
            await query.stream_output.delta(StreamDelta(1, "model", DeltaKind.TEXT, "hello"))
        if self.error:
            raise self.error
        return InvocationReply(CALL, ACCEPTED, SUCCESS)


@pytest.mark.parametrize("accept,status", [("text/event-stream", 200), ("text/*", 200),
    ("*/*", 200), ("application/*", 406), ("application/json", 406),
    ("text/event-stream;q=0,*/*;q=1", 406)])
def test_stream_accept_negotiation_and_normalized_chunks(accept, status):
    async def scenario():
        service = Service()
        app = create_development_model_app(service, enable_streaming=True)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gateway") as client:
            response = await client.post("/v1/chat/completions", json=PAYLOAD | {"stream": True}, headers={"Accept": accept})
        assert response.status_code == status
        assert response.headers["cache-control"] == "no-store"
        assert response.headers["x-request-id"]
        if status == 406:
            assert not service.queries
            return
        assert response.headers["content-type"].startswith("text/event-stream")
        assert response.headers["x-gateway-call-id"] == str(CALL)
        frames = response.text.strip().split("\n\n")
        assert frames[-1] == "data: [DONE]" and len(frames) == 3
        first, terminal = [json.loads(frame[6:]) for frame in frames[:2]]
        assert first["choices"][0]["delta"] == {"content": "hello"}
        assert terminal["choices"][0] == {"index": 0, "delta": {}, "finish_reason": "stop"}
        assert first["id"] == terminal["id"] == "chatcmpl-" + str(CALL)
        assert service.queries[0].stream
    asyncio.run(scenario())


@pytest.mark.parametrize("after_delta", [False, True])
def test_stream_backend_exception_mapping(after_delta):
    async def scenario():
        service = Service(error=ModelInvocationRejected("rate_limited"), after_delta=after_delta)
        app = create_development_model_app(service, enable_streaming=True)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gateway") as client:
            response = await client.post("/v1/chat/completions", json=PAYLOAD | {"stream": True})
        assert response.status_code == (200 if after_delta else 429)
        assert "rate_limited" in response.text and "[DONE]" not in response.text
    asyncio.run(scenario())


def test_structured_failure_after_json_delta_has_error_frame_and_no_done():
    async def scenario():
        class StructuredFailure(Service):
            async def invoke(self, query):
                assert query.output_format.type == "json_object"
                query.stream_output.admitted(CALL, ACCEPTED)
                await query.stream_output.delta(StreamDelta(1, "model", DeltaKind.TEXT, '{"answer":'))
                return InvocationReply(CALL, ACCEPTED, ProviderFailure(
                    FailureCode.STRUCTURED_OUTPUT_INVALID, False, structured_reason="json_malformed"))

        app = create_development_model_app(StructuredFailure(), enable_streaming=True)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gateway") as client:
            response = await client.post("/v1/chat/completions", json=PAYLOAD | {
                "stream": True, "response_format": {"type": "json_object"}})
        assert response.status_code == 200
        frames = [json.loads(frame[6:]) for frame in response.text.strip().split("\n\n")]
        assert frames[0]["choices"][0]["delta"] == {"content": '{"answer":'}
        assert frames[1]["error"]["code"] == "structured_output_invalid"
        assert frames[1]["error"]["gateway"] == {"reason": "json_malformed"}
        assert "[DONE]" not in response.text
    asyncio.run(scenario())


def test_full_api_preserves_authorization_and_backpressure():
    async def scenario():
        tenant = ContextVar("stream-tenant", default=None)
        entered, release, advanced = asyncio.Event(), asyncio.Event(), asyncio.Event()
        messages = []

        class Authorization:
            @asynccontextmanager
            async def context(self, headers):
                token = tenant.set("approved")
                try:
                    yield
                finally:
                    tenant.reset(token)

        class Backend(Service):
            async def invoke(self, query):
                assert tenant.get() == "approved"
                result = await super().invoke(query)
                advanced.set()
                return result

        app = create_development_model_app(Backend(), request_authorization=Authorization(), enable_streaming=True)
        incoming = asyncio.Queue()
        await incoming.put({"type": "http.request", "body": json.dumps(PAYLOAD | {"stream": True}).encode(), "more_body": False})

        async def send(message):
            if message["type"] == "http.response.body" and not entered.is_set():
                entered.set()
                await release.wait()
            messages.append(message)

        scope = {"type": "http", "asgi": {"version": "3.0"}, "http_version": "1.1", "method": "POST",
            "scheme": "http", "path": "/v1/chat/completions", "raw_path": b"/v1/chat/completions",
            "query_string": b"", "headers": [(b"content-type", b"application/json")], "server": ("gateway", 80)}
        task = asyncio.create_task(app(scope, incoming.get, send))
        await asyncio.wait_for(entered.wait(), 1)
        assert not advanced.is_set() and len(messages) == 1
        release.set()
        await asyncio.wait_for(task, 1)
        assert advanced.is_set() and tenant.get() is None
        assert messages[-1]["body"] == b"data: [DONE]\n\n"
    asyncio.run(scenario())
