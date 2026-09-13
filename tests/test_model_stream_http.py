import asyncio
from contextvars import ContextVar
from datetime import datetime, timezone
from uuid import UUID

import pytest
from starlette.responses import JSONResponse

from llm_gateway.adapters.model_stream_http import ModelStreamResponse
from llm_gateway.adapters.model_http import _error
from llm_gateway.domain.streaming import DeltaKind, StreamDelta
from llm_gateway.application.request_trace import current_trace


CALL = UUID("7e4d080f-e4ca-4ab4-bd50-e7e28d3f29f5")
ACCEPTED = datetime(2026, 9, 13, tzinfo=timezone.utc)


def error(code):
    return JSONResponse({"error": {"code": code}}, status_code=502)


def success():
    return JSONResponse({"id": "chatcmpl-" + str(CALL), "model": "actual",
        "choices": [{"message": {"content": "你好"}, "finish_reason": "stop"}],
        "gateway": {"usage": {"state": "complete"}}},
        headers={"X-Gateway-Call-Id": str(CALL)})


def admitted(output):
    output.admitted(CALL, ACCEPTED)


async def delta(output):
    await output.delta(StreamDelta(1, "actual", DeltaKind.TEXT, "你好"))


class Connection:
    def __init__(self):
        self.messages = []
        self.incoming = asyncio.Queue()

    async def send(self, message):
        self.messages.append(message)

    async def receive(self):
        return await self.incoming.get()

    async def run(self, response, send=None):
        await response({"type": "http"}, self.receive, send or self.send)

    @property
    def body(self):
        return b"".join(message.get("body", b"") for message in self.messages)


@pytest.mark.parametrize("mode", ["text", "refusal", "empty", "failure", "post_failure", "write_error", "timeout", "disconnect"])
def test_http_trace_ttft_is_business_delta_send_boundary(mode):
    from llm_gateway.adapters.model_http import _ResponseIdentity

    async def scenario():
        records = []
        class Sink:
            def offer(self, record):
                records.append(record)
                return True
        connection = Connection()
        async def invoke(output):
            admitted(output)
            if mode == "failure":
                return error("unavailable")
            if mode != "empty":
                kind = DeltaKind.REFUSAL if mode == "refusal" else DeltaKind.TEXT
                await output.delta(StreamDelta(1, "actual", kind, "private-output"))
            if mode == "post_failure":
                return _error("provider_protocol_error")
            return success()
        async def app(scope, receive, send):
            await ModelStreamResponse(invoke, error_response=error,
                                      send_timeout=0.01 if mode == "timeout" else 5)(scope, receive, send)
        async def send(message):
            if b"private-output" in message.get("body", b""):
                trace = current_trace()
                assert trace._first_delta_ready is not None
                assert trace._first_delta_sent is None
                if mode == "write_error":
                    raise OSError("private-transport-error")
                if mode == "timeout":
                    await asyncio.Event().wait()
                if mode == "disconnect":
                    await connection.incoming.put({"type": "http.disconnect"})
                    await asyncio.Event().wait()
                await asyncio.sleep(0)
            await connection.send(message)
        middleware = _ResponseIdentity(app, Sink())
        if mode in {"write_error", "timeout"}:
            with pytest.raises(TimeoutError if mode == "timeout" else OSError):
                await connection.run(middleware, send)
        else:
            await asyncio.wait_for(connection.run(middleware, send), 1)
        record, = records
        assert record.http_send.calls >= 2
        assert record.http_send.errors == (mode == "write_error")
        assert record.http_send.cancellations == (mode in {"disconnect", "timeout"})
        if mode in {"empty", "failure"}:
            assert record.first_delta_ready_ns is None and record.first_delta_sent_ns is None
        elif mode in {"write_error", "disconnect", "timeout"}:
            assert record.first_delta_ready_ns is not None and record.first_delta_sent_ns is None
        else:
            assert 0 <= record.first_delta_ready_ns <= record.first_delta_sent_ns
        if mode == "post_failure":
            assert record.spans[-1].outcome == "error"
            assert b"[DONE]" not in connection.body
        assert "private" not in repr(record)
    asyncio.run(scenario())


def test_incremental_delivery_is_backpressured_and_keeps_context():
    async def scenario():
        tenant = ContextVar("tenant", default=None)
        tenant.set("authorized")
        connection = Connection()
        writing, release, advanced = asyncio.Event(), asyncio.Event(), asyncio.Event()

        async def invoke(output):
            assert tenant.get() == "authorized"
            admitted(output)
            await delta(output)
            advanced.set()
            return success()

        async def send(message):
            if message["type"] == "http.response.body" and not writing.is_set():
                writing.set()
                await release.wait()
            await connection.send(message)

        response = ModelStreamResponse(invoke, error_response=error)
        tenant.set(None)
        task = asyncio.create_task(connection.run(response, send))
        await asyncio.wait_for(writing.wait(), 1)
        assert not advanced.is_set()
        assert len(connection.messages) == 1
        release.set()
        await asyncio.wait_for(task, 1)
        assert advanced.is_set()
        assert "你好".encode() in connection.body
        assert connection.body.count(b"[DONE]") == 1
        assert connection.body.endswith(b"data: [DONE]\n\n")
        assert (b"x-gateway-call-id", str(CALL).encode()) in connection.messages[0]["headers"]
    asyncio.run(scenario())


@pytest.mark.parametrize("postcommit", [False, True])
@pytest.mark.parametrize("raises", [False, True])
def test_errors_are_safe_and_never_send_done(postcommit, raises):
    async def scenario():
        connection = Connection()

        async def invoke(output):
            admitted(output)
            if postcommit:
                await delta(output)
            if raises:
                raise RuntimeError("private provider material")
            return error("protocol_error")

        await asyncio.wait_for(connection.run(ModelStreamResponse(invoke, error_response=error)), 1)
        assert connection.messages[0]["status"] == (200 if postcommit else 502)
        assert b"[DONE]" not in connection.body and b"private" not in connection.body
        assert b"internal" in connection.body if raises else b"protocol_error" in connection.body
        assert not connection.messages[-1].get("more_body", False)
    asyncio.run(scenario())


@pytest.mark.parametrize("mode", ["disconnect", "write_failure", "write_timeout", "cancel_twice"])
def test_transport_failure_joins_invocation_cleanup(mode):
    async def scenario():
        connection = Connection()
        entered, cleaning, release, exited = (asyncio.Event() for _ in range(4))

        async def invoke(output):
            admitted(output)
            try:
                await delta(output)
                await asyncio.Future()
            finally:
                cleaning.set()
                await release.wait()
                exited.set()

        async def send(message):
            if message["type"] == "http.response.body":
                entered.set()
                if mode == "write_failure":
                    raise OSError("broken socket")
                await asyncio.Future()
            await connection.send(message)

        task = asyncio.create_task(connection.run(ModelStreamResponse(
            invoke, error_response=error, send_timeout=0.02 if mode == "write_timeout" else 5), send))
        await asyncio.wait_for(entered.wait(), 1)
        if mode == "disconnect":
            await connection.incoming.put({"type": "http.disconnect"})
        if mode == "cancel_twice":
            task.cancel()
        await asyncio.wait_for(cleaning.wait(), 1)
        if mode == "cancel_twice":
            task.cancel()
        await asyncio.sleep(0)
        assert not task.done() and not exited.is_set()
        release.set()
        result = (await asyncio.gather(task, return_exceptions=True))[0]
        assert exited.is_set()
        if mode == "disconnect":
            assert result is None
        elif mode == "cancel_twice":
            assert isinstance(result, asyncio.CancelledError)
        else:
            assert isinstance(result, OSError if mode == "write_failure" else TimeoutError)
        assert b"[DONE]" not in connection.body
        assert not [t for t in asyncio.all_tasks() if t is not asyncio.current_task() and not t.done()]
    asyncio.run(scenario())


def test_empty_success_has_call_identity_and_one_done():
    async def scenario():
        async def invoke(output):
            admitted(output)
            return success()
        connection = Connection()
        await connection.run(ModelStreamResponse(invoke, error_response=error))
        assert (b"x-gateway-call-id", str(CALL).encode()) in connection.messages[0]["headers"]
        assert connection.body.count(b"[DONE]") == 1
        assert b'"message"' not in connection.body
    asyncio.run(scenario())


def test_oversized_event_is_rejected_before_headers():
    async def scenario():
        async def invoke(output):
            admitted(output)
            await delta(output)
            return success()
        connection = Connection()
        await connection.run(ModelStreamResponse(invoke, error_response=error, max_event_bytes=20))
        assert connection.messages[0]["status"] == 502
        assert b"[DONE]" not in connection.body
    asyncio.run(scenario())


def test_unexpected_exception_uses_real_gateway_error_protocol():
    async def scenario():
        async def invoke(output):
            raise RuntimeError("credential must remain private")
        connection = Connection()
        await connection.run(ModelStreamResponse(invoke, error_response=_error))
        assert connection.messages[0]["status"] == 500
        assert b'"type":"gateway_error"' in connection.body
        assert b"credential" not in connection.body
    asyncio.run(scenario())


def test_disconnect_before_first_delta_waits_for_cleanup_without_headers():
    async def scenario():
        connection = Connection()
        entered, exited = asyncio.Event(), asyncio.Event()

        async def invoke(output):
            try:
                entered.set()
                await asyncio.Future()
            finally:
                await asyncio.sleep(0)
                exited.set()

        task = asyncio.create_task(connection.run(ModelStreamResponse(invoke, error_response=_error)))
        await asyncio.wait_for(entered.wait(), 1)
        await connection.incoming.put({"type": "http.disconnect"})
        await asyncio.wait_for(task, 1)
        assert exited.is_set() and not connection.messages
    asyncio.run(scenario())


def test_write_failure_does_not_cancel_shielded_work_before_owner_reservation():
    async def scenario():
        connection = Connection()
        reserved, child_exited = asyncio.Event(), asyncio.Event()

        async def invoke(output):
            admitted(output)

            async def work():
                try:
                    await delta(output)
                finally:
                    assert reserved.is_set()
                    child_exited.set()

            child = asyncio.create_task(work())
            try:
                await asyncio.shield(child)
            except asyncio.CancelledError:
                # Simulate the lifecycle's asynchronous durable reservation.
                await asyncio.sleep(0)
                assert not child.done() and not child_exited.is_set()
                reserved.set()
                raise
            finally:
                child.cancel()
                await asyncio.gather(child, return_exceptions=True)

        async def send(message):
            if message["type"] == "http.response.body":
                raise OSError("socket closed")
            await connection.send(message)

        with pytest.raises(OSError):
            await connection.run(ModelStreamResponse(invoke, error_response=_error), send)
        assert reserved.is_set() and child_exited.is_set()
    asyncio.run(scenario())
