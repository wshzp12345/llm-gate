"""Text-only completion slice; not yet a production-ready transport.

The composition root owns the client, TLS/egress policy and credential lease.
No SDK retry loop or Provider payload crosses the application boundary.
"""

import json
import asyncio
import math
import socket
import ssl
from dataclasses import replace
from datetime import datetime, timezone
from collections.abc import Mapping
from contextlib import aclosing

import httpx

from llm_gateway.adapters.provider_dns import DnsResolutionFailure
from llm_gateway.adapters.retry_after import provider_retry_after_ms
from llm_gateway.application.cancellation import LocalCancellationReason, ProviderCancelResult
from llm_gateway.application.cancellation_execution import CancellationAttemptHandle
from llm_gateway.application.provider_context import ProviderInvocationContext
from llm_gateway.application.request_trace import trace_stage, TraceStage

from llm_gateway.domain.model import (
    CompletionRequest, FailureCode, ProviderFailure, ProviderResult, RefusalOutput, TextOutput, Usage,
)


def _count(value: object) -> int | None:
    return value if type(value) is int and value >= 0 else None


def _usage(value: object) -> Usage:
    if not isinstance(value, dict):
        return Usage()
    parents = (_count(value.get("prompt_tokens")), _count(value.get("completion_tokens")))
    subsets: list[int | None] = []
    for key, field, parent in (
        ("prompt_tokens_details", "cached_tokens", parents[0]),
        ("completion_tokens_details", "reasoning_tokens", parents[1]),
    ):
        details = value.get(key)
        count = _count(details.get(field)) if isinstance(details, dict) else None
        subsets.append(count if parent is not None and count is not None and count <= parent else None)
    return Usage(*parents, *subsets, provider_reported_total=_count(value.get("total_tokens")))


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON member")
        result[key] = value
    return result


def _reject_constant(value: str) -> None:
    raise ValueError("Non-finite JSON number")


def _network_retryable(error: BaseException) -> bool:
    """Inspect typed causes, never endpoint-bearing exception messages.

    HTTP libraries may wrap certificate or local permission failures as network
    errors. Ambiguous/cyclic/deep chains fail closed instead of granting retry.
    SSL EOF remains a transient interruption rather than a certificate defect.
    """
    seen = set()
    for _ in range(16):
        if id(error) in seen:
            return False
        seen.add(id(error))
        if isinstance(error, DnsResolutionFailure):
            return error.retryable is True
        if isinstance(error, (PermissionError, ssl.SSLCertVerificationError)):
            return False
        if isinstance(error, ssl.SSLError) and not isinstance(error, ssl.SSLEOFError):
            return False
        if isinstance(error, socket.gaierror) and error.errno != socket.EAI_AGAIN:
            return False
        cause = error.__cause__
        if cause is None:
            # httpcore can re-raise from None while retaining a typed wrapped
            # exception in args/context. Suppression controls display, not the
            # retry classification of a certificate or policy failure.
            wrapped = tuple(value for value in error.args if isinstance(value, BaseException))
            if len(wrapped) > 1:
                return False
            cause = wrapped[0] if wrapped else error.__context__
        if cause is None:
            return True
        error = cause
    return False


class OpenAICompatibleCompletion:
    supports_remote_cancellation = False

    def __init__(
        self, client: httpx.AsyncClient, *, base_url: str,
        credential: str, max_response_bytes: int = 4 * 1024 * 1024,
        clock=lambda: datetime.now(timezone.utc), max_event_bytes: int = 262144,
    ) -> None:
        if not credential or "\r" in credential or "\n" in credential:
            raise ValueError("Invalid credential")
        if type(max_response_bytes) is not int or max_response_bytes < 1:
            raise ValueError("Response bound must be positive")
        if type(max_event_bytes) is not int or max_event_bytes < 1:
            raise ValueError("Event bound must be positive")
        url = httpx.URL(base_url)
        if (url.scheme not in {"https", "http"} or not url.host or url.userinfo
                or url.query or url.fragment or base_url.endswith("/")):
            raise ValueError("Invalid Provider API root")
        self._client = client
        self._url = base_url.rstrip("/") + "/chat/completions"
        self._credential = credential
        self._max_response_bytes = max_response_bytes
        self._max_event_bytes = max_event_bytes
        self._clock = clock
        self._active = {}

    async def stream(self, request: CompletionRequest, *, context: ProviderInvocationContext | None = None):
        from llm_gateway.adapters.openai_stream import stream_completion
        if context is None:
            async with aclosing(stream_completion(self, request)) as events:
                async for event in events:
                    yield event
            return
        if not isinstance(context, ProviderInvocationContext):
            raise ValueError("Typed Provider invocation context required")
        context.cancellation.raise_if_cancelled()
        if context.handle in self._active:
            raise ValueError("Attempt handle is already active")
        if asyncio.get_running_loop().time() >= context.deadline:
            raise TimeoutError()
        task = asyncio.current_task()
        self._active[context.handle] = (task, context.cancellation)
        try:
            with context.cancellation.abort_on_cancel(task.cancel):
                async with asyncio.timeout_at(context.deadline):
                    async with aclosing(stream_completion(self, request, context=context)) as events:
                        async for event in events:
                            context.cancellation.raise_if_cancelled()
                            yield event
        finally:
            self._active.pop(context.handle, None)

    async def complete(self, request: CompletionRequest, *, context: ProviderInvocationContext | None = None) -> ProviderResult | ProviderFailure:
        if context is None:
            return await self._complete(request)
        if not isinstance(context, ProviderInvocationContext):
            raise ValueError("Typed Provider invocation context required")
        context.cancellation.raise_if_cancelled()
        if context.handle in self._active:
            raise ValueError("Attempt handle is already active")
        if asyncio.get_running_loop().time() >= context.deadline:
            raise TimeoutError()
        task = asyncio.current_task()
        self._active[context.handle] = (task, context.cancellation)
        try:
            with context.cancellation.abort_on_cancel(task.cancel):
                async with asyncio.timeout_at(context.deadline):
                    result = await self._complete(request)
                if context.cancellation.reason is not None:
                    await context.record_late(result)
                context.cancellation.raise_if_cancelled()
                return result
        finally:
            self._active.pop(context.handle, None)

    async def cancel(self, handle: CancellationAttemptHandle, reason: LocalCancellationReason,
                     deadline: float) -> ProviderCancelResult:
        if (not isinstance(handle, CancellationAttemptHandle) or not isinstance(reason, LocalCancellationReason)
                or type(deadline) not in (int, float) or not math.isfinite(deadline)):
            raise ValueError("Typed cancellation handle, reason and finite deadline required")
        active = self._active.get(handle)
        if active is None:
            return ProviderCancelResult.UNKNOWN
        task, token = active
        if task is asyncio.current_task():
            raise ValueError("Cancellation requires an independent lifecycle task")
        # Request once even when stop() already signalled the same token. A
        # repeated task.cancel() could interrupt the first abort's cleanup.
        token.request(reason)
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining > 0 and not task.done():
            await asyncio.wait({task}, timeout=remaining)
        # The generic protocol has no trusted remote cancellation endpoint.
        # Do not close the shared client or another Invocation's connections.
        return ProviderCancelResult.NOT_SUPPORTED if task.done() else ProviderCancelResult.UNKNOWN

    async def _complete(self, request: CompletionRequest) -> ProviderResult | ProviderFailure:
        with trace_stage(TraceStage.ADAPTER_REQUEST) as observation:
            result = await self._complete_request(request)
            if isinstance(result, ProviderFailure):
                observation.mark_failed()
            return result

    async def _complete_request(self, request: CompletionRequest) -> ProviderResult | ProviderFailure:
        payload = {
            "model": request.resolved_model,
            "messages": [{"role": message.role, "content": message.text} for message in request.messages],
            "max_tokens": request.max_output_tokens,
            "stream": False,
        }
        if request.temperature is not None:
            payload["temperature"] = request.temperature
        if request.top_p is not None:
            payload["top_p"] = request.top_p
        try:
            async with self._client.stream(
                "POST", self._url, json=payload,
                headers={"Authorization": "Bearer " + self._credential},
                follow_redirects=False,
            ) as response:
                failure = self._http_failure(response.status_code)
                if failure is not None:
                    if failure.retryable:
                        failure = replace(failure, retry_after_ms=provider_retry_after_ms(response.headers, now=self._clock))
                    return failure
                if response.headers.get("content-type", "").split(";", 1)[0].strip().lower() != "application/json":
                    return ProviderFailure(FailureCode.PROVIDER_PROTOCOL_ERROR, False)
                body = bytearray()
                async for chunk in response.aiter_bytes():
                    if len(body) + len(chunk) > self._max_response_bytes:
                        return ProviderFailure(FailureCode.PROVIDER_PROTOCOL_ERROR, False)
                    body.extend(chunk)
                data = json.loads(
                    body.decode("utf-8"), object_pairs_hook=_unique_object,
                    parse_constant=_reject_constant,
                )
                return self._translate(data, request)
        except httpx.PoolTimeout:
            return ProviderFailure(FailureCode.PROVIDER_UNAVAILABLE, False)
        except httpx.ConnectTimeout:
            return ProviderFailure(FailureCode.UPSTREAM_TIMEOUT, True)
        except httpx.TimeoutException:
            return ProviderFailure(FailureCode.UNCERTAIN, False)
        except (httpx.LocalProtocolError, httpx.UnsupportedProtocol):
            return ProviderFailure(FailureCode.PROVIDER_PROTOCOL_ERROR, False)
        except httpx.ConnectError as error:
            return ProviderFailure(FailureCode.PROVIDER_UNAVAILABLE, _network_retryable(error))
        except httpx.TransportError:
            return ProviderFailure(FailureCode.UNCERTAIN, False)
        except httpx.DecodingError:
            return ProviderFailure(FailureCode.PROVIDER_PROTOCOL_ERROR, False)
        except (ValueError, KeyError, TypeError, IndexError, RecursionError):
            return ProviderFailure(FailureCode.PROVIDER_PROTOCOL_ERROR, False)

    @staticmethod
    def _http_failure(status: int) -> ProviderFailure | None:
        if status == 200:
            return None
        if status in {401, 403}:
            return ProviderFailure(FailureCode.PROVIDER_CREDENTIALS_UNAVAILABLE, False)
        if status == 429:
            return ProviderFailure(FailureCode.RATE_LIMITED, True)
        if status == 408:
            return ProviderFailure(FailureCode.UPSTREAM_TIMEOUT, True)
        if 500 <= status <= 599:
            return ProviderFailure(FailureCode.PROVIDER_UNAVAILABLE, status in {500, 502, 503, 504})
        if status == 400:
            return ProviderFailure(FailureCode.INVALID_REQUEST, False)
        return ProviderFailure(FailureCode.PROVIDER_PROTOCOL_ERROR, False)

    @staticmethod
    def _translate(data: object, request: CompletionRequest) -> ProviderResult:
        if not isinstance(data, Mapping) or data.get("object") != "chat.completion":
            raise ValueError("Invalid completion envelope")
        model = data["model"]
        choices = data["choices"]
        if not isinstance(model, str) or not model or not isinstance(choices, list) or len(choices) != 1:
            raise ValueError("Invalid completion fields")
        choice = choices[0]
        if not isinstance(choice, dict) or not isinstance(choice.get("message"), dict):
            raise ValueError("Invalid choice/message envelope")
        message = choice["message"]
        if type(choice.get("index")) is not int or choice["index"] != 0:
            raise ValueError("Invalid choice index")
        if (message.get("role") != "assistant" or message.get("tool_calls")
                or message.get("function_call")):
            raise ValueError("Output not supported by text-only slice")
        content, finish = message["content"], choice["finish_reason"]
        refusal = message.get("refusal")
        if refusal is not None or finish == "content_filter":
            return ProviderResult(request.requested_model, model, RefusalOutput(content, refusal),
                                  _usage(data.get("usage")), finish)
        if not isinstance(content, str) or finish not in {"stop", "length"}:
            raise ValueError("Invalid text completion")
        content.encode("utf-8")
        return ProviderResult(request.requested_model, model, TextOutput(content), _usage(data.get("usage")), finish)
