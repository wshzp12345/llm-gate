import asyncio
from contextlib import aclosing
import json
from uuid import uuid4

import httpx
import pytest

from llm_gateway.adapters.openai_compatible import OpenAICompatibleCompletion
from llm_gateway.adapters.sse import SSEDecoder, SSEProtocolError
from llm_gateway.application.cancellation import LocalCancellationReason, ProviderCancelResult
from llm_gateway.application.cancellation_execution import CancellationAttemptHandle
from llm_gateway.application.provider_context import GatewayCancellationToken, ProviderInvocationContext
from llm_gateway.domain.model import FailureCode, RefusalOutput, Usage
from llm_gateway.domain.streaming import StreamDelta, StreamCompleted, StreamFailed, DeltaKind
from tests.test_completion import REQUEST


def chunk(delta=None, finish=None, **changes):
    return {"id": "provider-stream-id", "object": "chat.completion.chunk", "model": "actual-model",
            "choices": [{"index": 0, "delta": delta or {}, "finish_reason": finish}], **changes}


def wire(*values):
    return b"".join(b"data: " + (value.encode() if isinstance(value, str) else json.dumps(value, ensure_ascii=False).encode()) + b"\n\n" for value in values)


class Stream(httpx.AsyncByteStream):
    def __init__(self, chunks, *, error=None):
        self.chunks, self.error, self.closed = chunks, error, False
        self.read_count = 0

    async def __aiter__(self):
        for value in self.chunks:
            self.read_count += 1
            yield value
        if self.error is not None:
            raise self.error

    async def aclose(self):
        self.closed = True


def collect(body, *, pieces=None, error=None, status=200, headers=None, **options):
    async def scenario():
        stream = Stream(pieces if pieces is not None else [body], error=error)
        calls = []
        async def handler(request):
            calls.append(request)
            return httpx.Response(status, stream=stream, headers=headers or {"content-type": "text/event-stream"})
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            adapter = OpenAICompatibleCompletion(client, base_url="https://provider.invalid/v1", credential="synthetic", **options)
            async with aclosing(adapter.stream(REQUEST)) as events:
                result = [event async for event in events]
            assert stream.closed
            assert len(calls) == 1
            sent = json.loads(calls[0].content)
            assert sent["stream"] is True and sent["stream_options"] == {"include_usage": True}
            assert sent["messages"] == [{"role": "user", "content": "hello"}]
            return result
    return asyncio.run(scenario())


@pytest.mark.parametrize("fragment", [1, 2, 7, 10000])
def test_utf8_incremental_deltas_and_terminal_usage(fragment):
    data = wire(chunk({"role": "assistant", "content": ""}), chunk({"reasoning_content": "private-reasoning"}),
                chunk({"content": "你好"}), chunk({"content": "世界"}), chunk(finish="stop"),
                chunk(choices=[], usage={"prompt_tokens": 10, "completion_tokens": 6, "total_tokens": 16,
                    "prompt_tokens_details": {"cached_tokens": 4}, "completion_tokens_details": {"reasoning_tokens": 2}}), "[DONE]")
    events = collect(data, pieces=[data[i:i + fragment] for i in range(0, len(data), fragment)])
    assert [item.sequence for item in events] == [1, 2, 3]
    assert [item.text for item in events[:-1]] == ["你好", "世界"]
    assert all(item.kind == DeltaKind.TEXT for item in events[:-1])
    assert isinstance(events[-1], StreamCompleted)
    assert events[-1].result.output.text == "你好世界"
    assert events[-1].result.requested_model == "general"
    assert events[-1].result.resolved_model == "actual-model"
    assert events[-1].result.usage == Usage(10, 6, 4, 2, 16)
    assert "private-reasoning" not in repr(events)
    assert "你好" not in repr(events)


def test_first_delta_is_delivered_before_rest_of_body_is_read_and_early_close_releases_response():
    async def scenario():
        stream = Stream([wire(chunk({"content": "first"})), wire(chunk({"content": "second"}), chunk(finish="stop"), "[DONE]")])
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(200, stream=stream,
            headers={"content-type": "text/event-stream"}))) as client:
            adapter = OpenAICompatibleCompletion(client, base_url="https://provider.invalid", credential="synthetic")
            async with aclosing(adapter.stream(REQUEST)) as events:
                first = await anext(events)
                assert first.text == "first" and stream.read_count == 1
            assert stream.closed and not client.is_closed
    asyncio.run(scenario())


@pytest.mark.parametrize("tail", [b"", wire("[DONE]"), wire(chunk(finish="stop")),
    wire(chunk(finish="stop"), "[DONE]", "[DONE]"),
    wire(chunk(finish="stop"), chunk({"content": "late"}), "[DONE]"),
    wire(chunk(finish="stop"), chunk(finish="stop"), "[DONE]"),
    wire(chunk({"content": "changed"}, model="different")),
    wire(chunk({"content": "changed"}, id="different")),
    b"data: {bad-json}\n\n", b"data: \xff\n\n", b"data: {}",
    wire({"object": "chat.completion.chunk", "model": "actual-model", "choices": [{"index": 1, "delta": {"content": "bad"}}]})])
def test_failed_stream_never_emits_completed_and_has_contiguous_terminal(tail):
    events = collect(wire(chunk({"content": "visible"})) + tail)
    assert len(events) == 2
    assert isinstance(events[0], StreamDelta) and isinstance(events[-1], StreamFailed)
    assert events[-1].sequence == 2
    assert events[-1].failure.code == FailureCode.PROVIDER_PROTOCOL_ERROR
    assert events[-1].failure.retryable is False


@pytest.mark.parametrize("delta,finish", [({"content": "must-not-leak"}, "tool_calls"),
    ({"content": "must-not-leak", "reasoning_content": 123}, None),
    ({"content": "must-not-leak", "tool_calls": []}, None)])
def test_invalid_frame_is_validated_before_any_delta_is_released(delta, finish):
    events = collect(wire(chunk(delta, finish)))
    assert len(events) == 1 and isinstance(events[0], StreamFailed) and events[0].sequence == 1


def test_refusal_and_unknown_usage_are_not_fabricated_text_success():
    events = collect(wire(chunk({"refusal": "Cannot "}), chunk({"refusal": "help"}), chunk(finish="stop"), "[DONE]"))
    assert all(item.kind == DeltaKind.REFUSAL for item in events[:-1])
    assert events[-1].result.output == RefusalOutput(None, "Cannot help")
    assert events[-1].result.usage == Usage()
    filtered = collect(wire(chunk(finish="content_filter"), "[DONE]"))[-1]
    assert filtered.result.disposition == "safety_refused"


def test_partial_usage_is_preserved_even_when_done_is_missing():
    events = collect(wire(chunk(finish="length", usage={"prompt_tokens": 3, "completion_tokens": 8,
        "completion_tokens_details": {"reasoning_tokens": 8}})))
    assert isinstance(events[-1], StreamFailed)
    assert events[-1].usage == Usage(3, 8, None, 8)


@pytest.mark.parametrize("frames", [
    [chunk({"role": "assistant"}), chunk({"role": "assistant"})],
    [chunk({"content": "first"}), chunk({"role": "assistant"})],
    [chunk(choices=[])],
    [chunk({"content": "first"}, usage={"prompt_tokens": 10})],
    [chunk(finish="stop", usage={}), chunk(choices=[], usage={})],
])
def test_out_of_order_metadata_and_usage_cannot_be_success(frames):
    events = collect(wire(*frames, "[DONE]"))
    assert isinstance(events[-1], StreamFailed)
    assert not any(isinstance(event, StreamCompleted) for event in events)
    assert [event.sequence for event in events] == list(range(1, len(events) + 1))


def test_media_type_and_duplicate_json_members_fail_without_body_disclosure():
    assert isinstance(collect(b"private-body", headers={"content-type": "application/json"})[-1], StreamFailed)
    events = collect(b'data: {"model":"private-one","model":"private-two"}\n\n')
    assert len(events) == 1 and isinstance(events[0], StreamFailed)
    assert "private" not in repr(events)


@pytest.mark.parametrize("status,code,retry", [(401, FailureCode.PROVIDER_CREDENTIALS_UNAVAILABLE, False),
    (400, FailureCode.INVALID_REQUEST, False), (503, FailureCode.PROVIDER_UNAVAILABLE, True),
    (429, FailureCode.RATE_LIMITED, True)])
def test_http_failures_are_normalized_without_body_leak_or_retry(status, code, retry):
    events = collect(b"private-body", status=status, headers={"Retry-After": "2"})
    assert len(events) == 1 and isinstance(events[0], StreamFailed)
    assert events[0].failure.code == code and events[0].failure.retryable == retry
    assert events[0].failure.retry_after_ms == (2000 if retry else None)
    assert "private" not in repr(events)


@pytest.mark.parametrize("error,code", [(httpx.ConnectTimeout("private"), FailureCode.UPSTREAM_TIMEOUT),
    (httpx.ReadTimeout("private"), FailureCode.UNCERTAIN), (httpx.RemoteProtocolError("private"), FailureCode.UNCERTAIN)])
def test_network_failures_remain_uncertain_after_possible_execution(error, code):
    events = collect(wire(chunk({"content": "visible"})), error=error)
    assert isinstance(events[-1], StreamFailed) and events[-1].failure.code == code


@pytest.mark.parametrize("option", [{"max_event_bytes": 8}, {"max_response_bytes": 8}])
def test_event_and_aggregate_bounds_fail_closed(option):
    events = collect(wire(chunk({"content": "large"}), chunk(finish="stop"), "[DONE]"), **option)
    assert len(events) == 1 and isinstance(events[0], StreamFailed)


@pytest.mark.parametrize("separator", [b"\n", b"\r\n", b"\r"])
def test_sse_line_endings_multiline_bom_comments_and_exact_bound(separator):
    raw = b"\xef\xbb\xbf: keepalive" + separator + separator + b"event: message" + separator + b"data: first" + separator + b"data: second" + separator + separator
    decoder = SSEDecoder(len(raw))
    output = []
    for byte in raw:
        output.extend(decoder.feed(bytes([byte])))
    output.extend(decoder.finish())
    assert output == ["first\nsecond"]
    frame = b"data: x" + separator + separator
    decoder = SSEDecoder(len(frame))
    assert list(decoder.feed(frame)) + list(decoder.finish()) == ["x"]
    with pytest.raises(SSEProtocolError):
        list(SSEDecoder(len(frame) - 1).feed(frame))


def test_crlf_bound_is_checked_before_dispatch_even_when_last_lf_arrives_later():
    decoder = SSEDecoder(len(b"data: x\r\n\r\n") - 1)
    assert list(decoder.feed(b"data: x\r\n\r")) == []
    with pytest.raises(SSEProtocolError):
        list(decoder.feed(b"\n"))


def test_cancellation_aborts_only_its_stream_and_releases_context():
    async def scenario():
        entered = asyncio.Event()
        class Hanging(Stream):
            async def __aiter__(self):
                yield wire(chunk({"content": "first"}))
                entered.set()
                await asyncio.Future()
        stream = Hanging([])
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(200, stream=stream,
            headers={"content-type": "text/event-stream"}))) as client:
            adapter = OpenAICompatibleCompletion(client, base_url="https://provider.invalid", credential="synthetic")
            context = ProviderInvocationContext(CancellationAttemptHandle(uuid4(), 1, uuid4()), GatewayCancellationToken(),
                                                 asyncio.get_running_loop().time() + 5)
            seen = []
            async def consume():
                async with aclosing(adapter.stream(REQUEST, context=context)) as events:
                    async for event in events:
                        seen.append(event)
            task = asyncio.create_task(consume())
            await asyncio.wait_for(entered.wait(), 1)
            result = await adapter.cancel(context.handle, LocalCancellationReason.CONTEXT_CANCELLED,
                                          asyncio.get_running_loop().time() + 1)
            with pytest.raises(asyncio.CancelledError):
                await task
            assert result == ProviderCancelResult.NOT_SUPPORTED
            assert len(seen) == 1 and isinstance(seen[0], StreamDelta)
            assert stream.closed and not adapter._active and not client.is_closed
    asyncio.run(scenario())


def test_context_deadline_closes_transport_without_inventing_a_terminal_event():
    async def scenario():
        class Hanging(Stream):
            async def __aiter__(self):
                yield wire(chunk({"content": "first"}))
                await asyncio.Future()
        stream = Hanging([])
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(200, stream=stream,
            headers={"content-type": "text/event-stream"}))) as client:
            adapter = OpenAICompatibleCompletion(client, base_url="https://provider.invalid", credential="synthetic")
            context = ProviderInvocationContext(CancellationAttemptHandle(uuid4(), 1, uuid4()), GatewayCancellationToken(),
                                                 asyncio.get_running_loop().time() + .03)
            seen = []
            with pytest.raises(TimeoutError):
                async with aclosing(adapter.stream(REQUEST, context=context)) as events:
                    async for event in events:
                        seen.append(event)
            assert len(seen) == 1 and isinstance(seen[0], StreamDelta)
            assert stream.closed and not adapter._active
    asyncio.run(scenario())
