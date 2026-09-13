import asyncio
from contextlib import aclosing

import httpx
import pytest

from llm_gateway.adapters.openai_compatible import OpenAICompatibleCompletion
from llm_gateway.application.request_trace import RequestTrace, TraceStage
from llm_gateway.domain.streaming import StreamDelta
from tests.test_completion import REQUEST, envelope
from tests.test_provider_streaming import Stream, chunk, wire


@pytest.mark.parametrize("streaming,failed", [(False, False), (False, True), (True, False), (True, True)])
def test_adapter_is_child_of_attempt_and_normalized_failures_are_errors(streaming, failed):
    async def scenario():
        trace = RequestTrace()
        async def handler(request):
            if failed:
                return httpx.Response(503)
            if streaming:
                return httpx.Response(200, headers={"content-type": "text/event-stream"},
                    stream=Stream([wire(chunk({"content": "hello"}), chunk(finish="stop"), "[DONE]")]))
            return httpx.Response(200, json=envelope())
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            adapter = OpenAICompatibleCompletion(client, base_url="https://provider.invalid/v1", credential="private")
            with trace.activate(), trace.span(TraceStage.HTTP), trace.span(TraceStage.PROVIDER_ATTEMPT, attempt_number=1):
                if streaming:
                    async with aclosing(adapter.stream(REQUEST)) as events:
                        async for event in events:
                            if isinstance(event, StreamDelta):
                                with trace.span(TraceStage.SETTLEMENT):
                                    pass
                else:
                    await adapter.complete(REQUEST)
        spans = trace.finish().spans
        request, = [span for span in spans if span.stage == TraceStage.ADAPTER_REQUEST]
        attempt, = [span for span in spans if span.stage == TraceStage.PROVIDER_ATTEMPT]
        assert request.parent_span_id == attempt.span_id
        assert request.outcome == ("error" if failed else "ok")
        assert (request.first_delta_ns is not None) == (streaming and not failed)
        if request.first_delta_ns is not None:
            assert 0 <= request.first_delta_ns <= request.duration_ns
            downstream, = [span for span in spans if span.stage == TraceStage.SETTLEMENT]
            assert downstream.parent_span_id == attempt.span_id
        assert "private" not in repr(spans)
    asyncio.run(scenario())


def test_first_delta_uses_monotonic_clock_once_and_preserves_unknown_without_delta():
    ticks = iter([0, 10, 30, 80, 100, 130])
    trace = RequestTrace(clock=lambda: next(ticks))
    with trace.activate():
        with trace.span(TraceStage.ADAPTER_REQUEST) as observation:
            observation.mark_first_delta()
            observation.mark_first_delta()
        with trace.span(TraceStage.ADAPTER_REQUEST):
            pass
    first, second = trace.finish().spans
    assert first.first_delta_ns == 20 and first.duration_ns == 70
    assert second.first_delta_ns is None
