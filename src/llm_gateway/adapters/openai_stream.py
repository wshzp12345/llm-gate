"""Strict compatible text-stream normalization. No retry or outward commit."""

import json
from dataclasses import replace

import httpx

from llm_gateway.adapters.openai_compatible import _usage, _unique_object, _reject_constant, _network_retryable
from llm_gateway.adapters.retry_after import provider_retry_after_ms
from llm_gateway.adapters.sse import SSEDecoder, SSEProtocolError
from llm_gateway.domain.model import FailureCode, ProviderFailure, ProviderResult, TextOutput, RefusalOutput, Usage
from llm_gateway.domain.streaming import DeltaKind, StreamDelta, StreamCompleted, StreamFailed


class CompatibleStreamState:
    def __init__(self, request):
        self.request = request
        self.sequence = 0
        self.model = None
        self.identity = None
        self.role_seen = False
        self.finish_reason = None
        self.done = False
        self.usage = Usage()
        self.usage_seen = False
        self.text, self.refusal = [], []

    def accept(self, frame):
        if self.done:
            raise SSEProtocolError()
        if frame.strip() == "[DONE]":
            if self.finish_reason is None:
                raise SSEProtocolError()
            self.done = True
            return ()
        value = json.loads(frame, object_pairs_hook=_unique_object, parse_constant=_reject_constant)
        if not isinstance(value, dict) or value.get("object") != "chat.completion.chunk":
            raise SSEProtocolError()
        model = value.get("model")
        if type(model) is not str or not 1 <= len(model) <= 256:
            raise SSEProtocolError()
        model.encode("utf-8")
        if self.model is not None and self.model != model:
            raise SSEProtocolError()
        identity = self.identity
        if "id" in value:
            identity = value["id"]
            if type(identity) is not str or not 1 <= len(identity) <= 256 or self.identity is not None and identity != self.identity:
                raise SSEProtocolError()
        choices = value.get("choices")
        if type(choices) is not list or len(choices) > 1:
            raise SSEProtocolError()
        pending = []
        role_seen, finish_reason = self.role_seen, self.finish_reason
        if choices:
            if self.finish_reason is not None:
                raise SSEProtocolError()
            choice = choices[0]
            if not isinstance(choice, dict) or type(choice.get("index")) is not int or choice["index"] != 0:
                raise SSEProtocolError()
            delta = choice.get("delta")
            if not isinstance(delta, dict) or set(delta) - {"role", "content", "refusal", "reasoning_content"}:
                raise SSEProtocolError()
            if delta.get("role") is not None:
                if delta["role"] != "assistant" or self.role_seen or self.sequence:
                    raise SSEProtocolError()
                role_seen = True
            for key, kind, target in (("content", DeltaKind.TEXT, self.text), ("refusal", DeltaKind.REFUSAL, self.refusal)):
                text = delta.get(key)
                if text is not None and type(text) is not str:
                    raise SSEProtocolError()
                if text:
                    text.encode("utf-8")
                    pending.append((kind, target, text))
            reasoning = delta.get("reasoning_content")
            if reasoning is not None:
                if type(reasoning) is not str:
                    raise SSEProtocolError()
                reasoning.encode("utf-8")  # Never forward private reasoning text.
            finish = choice.get("finish_reason")
            if finish is not None:
                if finish not in {"stop", "length", "content_filter"}:
                    raise SSEProtocolError()
                finish_reason = finish
        elif self.finish_reason is None or value.get("usage") is None:
            raise SSEProtocolError()
        if value.get("usage") is not None:
            if finish_reason is None or self.usage_seen or not isinstance(value["usage"], dict):
                raise SSEProtocolError()
            self.usage = _usage(value["usage"])
            self.usage_seen = True
        self.model, self.identity = model, identity
        self.role_seen, self.finish_reason = role_seen, finish_reason
        events = []
        for kind, target, text in pending:
            target.append(text)
            self.sequence += 1
            events.append(StreamDelta(self.sequence, model, kind, text))
        return tuple(events)

    def complete(self):
        if not self.done or self.finish_reason is None or self.model is None:
            raise SSEProtocolError()
        text = "".join(self.text)
        output = (RefusalOutput(text or None, "".join(self.refusal) or None)
                  if self.refusal or self.finish_reason == "content_filter" else TextOutput(text))
        result = ProviderResult(self.request.requested_model, self.model, output, self.usage, self.finish_reason)
        return StreamCompleted(self.sequence + 1, result)

    def failed(self, failure):
        return StreamFailed(self.sequence + 1, failure, self.usage, self.model)


async def stream_completion(adapter, request, *, context=None):
    from contextlib import aclosing
    from llm_gateway.application.request_trace import trace_stage, TraceStage
    with trace_stage(TraceStage.ADAPTER_REQUEST) as observation:
        async with aclosing(_stream_completion(adapter, request, context=context)) as events:
            async for event in events:
                if isinstance(event, StreamDelta):
                    observation.mark_first_delta()
                elif isinstance(event, StreamFailed):
                    observation.mark_failed()
                with observation.yield_to_caller():
                    yield event


async def _stream_completion(adapter, request, *, context=None):
    state = CompatibleStreamState(request)
    decoder = SSEDecoder(min(adapter._max_event_bytes, adapter._max_response_bytes))
    payload = {"model": request.resolved_model,
        "messages": [{"role": item.role, "content": item.text} for item in request.messages],
        "max_tokens": request.max_output_tokens, "stream": True, "stream_options": {"include_usage": True}}
    if request.temperature is not None:
        payload["temperature"] = request.temperature
    if request.top_p is not None:
        payload["top_p"] = request.top_p
    try:
        async with adapter._client.stream("POST", adapter._url, json=payload,
            headers={"Authorization": "Bearer " + adapter._credential}, follow_redirects=False) as response:
            failure = adapter._http_failure(response.status_code)
            if failure is not None:
                if failure.retryable:
                    failure = replace(failure, retry_after_ms=provider_retry_after_ms(response.headers, now=adapter._clock))
                yield state.failed(failure)
                return
            if response.headers.get("content-type", "").split(";", 1)[0].strip().lower() != "text/event-stream":
                raise SSEProtocolError()
            size = 0
            async for chunk in response.aiter_bytes():
                size += len(chunk)
                if size > adapter._max_response_bytes:
                    raise SSEProtocolError()
                for frame in decoder.feed(chunk):
                    events = state.accept(frame)
                    if context is not None:
                        context.observed_stream(state.usage, state.model)
                    for event in events:
                        yield event
            for frame in decoder.finish():
                events = state.accept(frame)
                if context is not None:
                    context.observed_stream(state.usage, state.model)
                for event in events:
                    yield event
            # Completion is withheld until framing, finish_reason, DONE and EOF
            # agree. Duplicate/trailing business events cannot become success.
            completed = state.complete()
        yield completed
        return
    except httpx.PoolTimeout:
        failure = ProviderFailure(FailureCode.PROVIDER_UNAVAILABLE, False)
    except httpx.ConnectTimeout:
        failure = ProviderFailure(FailureCode.UPSTREAM_TIMEOUT, True)
    except httpx.TimeoutException:
        failure = ProviderFailure(FailureCode.UNCERTAIN, False)
    except (httpx.LocalProtocolError, httpx.UnsupportedProtocol):
        failure = ProviderFailure(FailureCode.PROVIDER_PROTOCOL_ERROR, False)
    except httpx.ConnectError as error:
        failure = ProviderFailure(FailureCode.PROVIDER_UNAVAILABLE, _network_retryable(error))
    except httpx.TransportError:
        failure = ProviderFailure(FailureCode.UNCERTAIN, False)
    except httpx.DecodingError:
        failure = ProviderFailure(FailureCode.PROVIDER_PROTOCOL_ERROR, False)
    except (ValueError, KeyError, TypeError, IndexError, RecursionError):
        failure = ProviderFailure(FailureCode.PROVIDER_PROTOCOL_ERROR, False)
    yield state.failed(failure)
