"""Injectable dev text API with opt-in streaming; not the full compatibility profile.

No Provider client, credentials, database, routing policy or implicit retries
live here. No default backend or readiness is invented for missing wiring.
"""

from typing import Annotated, Literal
from dataclasses import replace
from uuid import UUID

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictInt, ValidationError
from llm_gateway.adapters.model_stream_http import ModelStreamResponse
from llm_gateway.adapters.structured_output import InvalidOutputFormat, parse_output_format
from llm_gateway.application.request_trace import RequestTrace, TraceStage, current_trace
from llm_gateway.adapters.trace_context import incoming_trace, business_correlation

from llm_gateway.adapters.configuration_json import ParseLimits, ConfigurationStructureError, parse_strict_json
from llm_gateway.application.model_api import ModelInvocationPort, ModelInvocationRejected, TextInvocationQuery
from llm_gateway.application.authorization import Unauthorized, AuthorizationUnavailable, Forbidden
from llm_gateway.adapters.configuration_http import _accepts
from llm_gateway.domain.invocation import InvocationAuthorizationExpired, InvocationPersistenceUnavailable, InvocationTerminalConflict
from llm_gateway.domain.model import Message, ProviderFailure, RefusalOutput
from llm_gateway.domain.routing_failure import UnattemptedRoutingFailure
from llm_gateway.domain.prompts import PromptReference, PromptSelection, PromptInvalid
from llm_gateway.application.prompts import PromptNotFound, PromptPersistenceUnavailable


class TextMessage(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    role: Literal["system", "developer", "user", "assistant", "tool"]
    content: str


class PromptSelectionBody(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    asset_id: str
    version_id: str
    variables: dict[str, str]


class TextRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    model: Annotated[str, Field(pattern=r"^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$", max_length=128)]
    messages: Annotated[list[TextMessage], Field(min_length=1, max_length=512)] | None = None
    prompt: PromptSelectionBody | None = None
    max_completion_tokens: Annotated[StrictInt, Field(gt=0)] | None = None
    max_tokens: Annotated[StrictInt, Field(gt=0)] | None = None
    stream: StrictBool = False
    n: Annotated[StrictInt, Field(ge=1)] = 1
    store: StrictBool = False
    temperature: Annotated[float, Field(ge=0, le=2)] | None = None
    top_p: Annotated[float, Field(gt=0, le=1)] | None = None
    response_format: dict[str, object] | None = None


_UNIMPLEMENTED_FIELDS = {
    "stop", "presence_penalty", "frequency_penalty", "reasoning_effort", "verbosity",
    "stream_options", "tools", "tool_choice", "parallel_tool_calls", "functions", "function_call", "seed",
    "modalities", "audio", "logprobs", "top_logprobs", "logit_bias", "prediction", "service_tier", "web_search_options",
    "metadata", "safety_identifier", "user", "prompt_cache_key", "prompt_cache_retention", "gateway",
}
_STATUS = {
    "invalid_request": 400, "structured_output_invalid": 400, "request_too_large": 413, "unsupported_media_type": 415,
    "unsupported_capability": 422, "rate_limited": 429, "provider_protocol_error": 502,
    "gateway_not_ready": 503, "provider_unavailable": 503, "provider_credentials_unavailable": 503,
    "persistence_unavailable": 503, "upstream_timeout": 504, "deadline_exceeded": 504,
    "already_terminal": 409, "internal": 500, "uncertain": 504, "authorization_unavailable": 503,
    "no_route": 503, "forbidden": 403, "unauthorized": 401, "not_acceptable": 406,
    "prompt_not_found": 404, "invalid_template": 422, "invalid_variables": 422,
}


class ApiFailure(Exception):
    def __init__(self, code):
        self.code = code


def _error(code, *, reason=None, path=None):
    trace = current_trace()
    if trace is not None:
        trace.mark_http_failed()
    detail = {"code": code, "type": "gateway_error", "param": None,
              "message": "Gateway could not complete the request."}
    if reason is not None:
        detail["gateway"] = {"reason": reason, **({"path": path} if path is not None else {})}
    return JSONResponse({"error": detail}, status_code=_STATUS[code],
                        headers={"WWW-Authenticate": "Bearer"} if code == "unauthorized" else None)


def _response(reply):
    result = reply.result
    if isinstance(result, (ProviderFailure, UnattemptedRoutingFailure)):
        response = _error(result.code, reason=getattr(result, "structured_reason", None),
                          path=getattr(result, "structured_path", None))
    else:
        usage = result.usage
        known = (usage.input_tokens, usage.output_tokens, usage.cached_tokens, usage.reasoning_tokens)
        state = "complete" if all(value is not None for value in known) else "partial" if any(value is not None for value in known) or usage.provider_reported_total is not None else "unavailable"
        body = {
            "id": "chatcmpl-" + str(reply.call_id), "object": "chat.completion", "created": int(reply.accepted_at.timestamp()),
            "model": result.resolved_model,
            "choices": [{"index": 0, "message": {"role": "assistant", "content": result.output.text,
                **({"refusal": result.output.refusal} if isinstance(result.output, RefusalOutput) and result.output.refusal is not None else {})}, "finish_reason": result.finish_reason}],
            "gateway": {"requested_model": result.requested_model, "resolved_model": result.resolved_model, "usage": {"state": state}},
        }
        if usage.total_tokens is not None:
            body["usage"] = {"prompt_tokens": usage.input_tokens, "completion_tokens": usage.output_tokens, "total_tokens": usage.total_tokens}
            if usage.cached_tokens is not None:
                body["usage"]["prompt_tokens_details"] = {"cached_tokens": usage.cached_tokens}
            if usage.reasoning_tokens is not None:
                body["usage"]["completion_tokens_details"] = {"reasoning_tokens": usage.reasoning_tokens}
        if reply.prompt_reference is not None:
            body["gateway"]["prompt"] = {"asset_id": str(reply.prompt_reference.asset_id),
                                         "version_id": str(reply.prompt_reference.version_id)}
        if reply.recovery is not None:
            body["gateway"]["recovery"] = {"attempts": reply.recovery.attempts,
                "retries": reply.recovery.retries, "fallback_used": reply.recovery.fallback_used}
        response = JSONResponse(body)
    response.headers["X-Gateway-Call-Id"] = str(reply.call_id)
    if reply.recovery is not None:
        response.headers["X-Gateway-Attempts"] = str(reply.recovery.attempts)
        response.headers["X-Gateway-Retries"] = str(reply.recovery.retries)
        response.headers["X-Gateway-Fallback-Used"] = str(reply.recovery.fallback_used).lower()
    return response


def _exception_response(error):
    if isinstance(error, (ApiFailure, ModelInvocationRejected)):
        return _error(error.code)
    if isinstance(error, PromptInvalid):
        return _error("request_too_large" if error.code == "prompt_too_large" else "invalid_request" if error.code == "invalid_identity" else error.code)
    for types, code in (
        ((ConfigurationStructureError, ValidationError), "invalid_request"),
        ((PromptNotFound,), "prompt_not_found"),
        ((PromptPersistenceUnavailable, InvocationPersistenceUnavailable), "persistence_unavailable"),
        ((Unauthorized, InvocationAuthorizationExpired), "unauthorized"),
        ((Forbidden,), "forbidden"),
        ((AuthorizationUnavailable,), "authorization_unavailable"),
        ((InvocationTerminalConflict,), "already_terminal"),
        ((TimeoutError,), "deadline_exceeded"),
    ):
        if isinstance(error, types):
            return _error(code)
    return _error("internal")


class _ResponseIdentity:
    """Inline ASGI header transform: no task or buffer between sends."""

    def __init__(self, app, trace_sink=None):
        self.app = app
        self.trace_sink = trace_sink

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        headers = scope.get("headers", [])
        oversized = sum(len(name) + len(value) + 4 for name, value in headers) > 32768
        trace = RequestTrace(incoming=None if oversized else incoming_trace(headers))
        request_id = str(trace.request_id).encode("ascii")

        async def identified(message):
            if message["type"] == "http.response.start":
                if message["status"] >= 400:
                    trace.mark_http_failed()
                headers = [(key, value) for key, value in message["headers"]
                           if key.lower() not in {b"x-request-id", b"cache-control"}]
                message = {**message, "headers": headers + [(b"x-request-id", request_id), (b"cache-control", b"no-store")]}
            with trace.http_send():
                await send(message)

        try:
            with trace.activate(), trace.span(TraceStage.HTTP):
                if oversized:
                    await _error("request_too_large")(scope, receive, identified)
                else:
                    try:
                        trace.correlation = business_correlation(headers)
                    except ValueError:
                        await _error("invalid_request")(scope, receive, identified)
                    else:
                        await self.app(scope, receive, identified)
        finally:
            record = trace.finish()
            if self.trace_sink is not None:
                try:
                    # Sink must accept/drop synchronously without I/O; export
                    # owns separate bounded resources, never request permits.
                    self.trace_sink.offer(record)
                except Exception:
                    pass  # Telemetry cannot rewrite a completed response.


def create_development_model_app(service: ModelInvocationPort | None = None, *,
                                 limits: ParseLimits = ParseLimits(4 * 1024 * 1024, 64, 100000, 10000, 1024 * 1024),
                                 lifespan=None, request_authorization=None, enable_streaming=False, trace_sink=None):
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None, lifespan=lifespan)

    app.add_middleware(_ResponseIdentity, trace_sink=trace_sink)

    async def invoke(query):
        try:
            return _response(await service.invoke(query))
        except Exception as error:
            return _exception_response(error)

    async def complete_request(request: Request):
        try:
            if request.url.query:
                raise ApiFailure("invalid_request")
            types = request.headers.getlist("content-type")
            if len(types) != 1 or types[0].lower().replace(" ", "") not in {"application/json", "application/json;charset=utf-8", 'application/json;charset="utf-8"'}:
                raise ApiFailure("unsupported_media_type")
            accepts = request.headers.getlist("accept")
            if len(accepts) > 1:
                raise ApiFailure("not_acceptable")
            if request.headers.get("content-encoding") is not None:
                raise ApiFailure("unsupported_media_type")
            if "idempotency-key" in request.headers:
                raise ApiFailure("unsupported_capability")
            body = bytearray()
            async for chunk in request.stream():
                if len(body) + len(chunk) > limits.max_bytes:
                    raise ApiFailure("request_too_large")
                body.extend(chunk)
            value = parse_strict_json(bytes(body), replace(limits, max_string_bytes=limits.max_bytes))
            if not isinstance(value, dict):
                raise ApiFailure("invalid_request")
            if _UNIMPLEMENTED_FIELDS.intersection(value):
                raise ApiFailure("unsupported_capability")
            try:
                output_format = parse_output_format(value["response_format"]) if "response_format" in value else None
            except InvalidOutputFormat:
                raise ApiFailure("invalid_request") from None
            if isinstance(value.get("messages"), list):
                for message in value["messages"]:
                    if isinstance(message, dict):
                        if isinstance(message.get("content"), str) and len(message["content"].encode("utf-8")) > limits.max_string_bytes:
                            raise ApiFailure("request_too_large")
                        if isinstance(message.get("content"), list) or {"tool_calls", "tool_call_id", "function_call", "refusal", "name"}.intersection(message):
                            raise ApiFailure("unsupported_capability")
            payload = TextRequest.model_validate(value)
            representation = "text/event-stream" if payload.stream and enable_streaming else "application/json"
            if not _accepts(accepts[0] if accepts else None, representation):
                raise ApiFailure("not_acceptable")
            if ("messages" in value) == ("prompt" in value) or (payload.messages is None and payload.prompt is None):
                raise ApiFailure("invalid_request")
            selection = None
            if payload.prompt is not None:
                try:
                    asset, version = UUID(payload.prompt.asset_id), UUID(payload.prompt.version_id)
                    if str(asset) != payload.prompt.asset_id or str(version) != payload.prompt.version_id:
                        raise ValueError()
                except ValueError:
                    raise ApiFailure("invalid_request") from None
                selection = PromptSelection(PromptReference(asset, version), payload.prompt.variables)
            if "max_completion_tokens" in value and "max_tokens" in value:
                raise ApiFailure("invalid_request")
            if any(value.get(key, 1) is None for key in ("max_completion_tokens", "max_tokens", "temperature", "top_p")):
                raise ApiFailure("invalid_request")
            if (payload.stream and not enable_streaming) or payload.n != 1 or payload.store or any(message.role == "tool" for message in (payload.messages or [])):
                raise ApiFailure("unsupported_capability")
            if payload.stream and output_format is not None and output_format.type != "json_object":
                raise ApiFailure("unsupported_capability")
            if service is None:
                raise ApiFailure("gateway_not_ready")
            query = TextInvocationQuery(payload.model, tuple(Message(item.role, item.content) for item in (payload.messages or [])),
                                        payload.max_completion_tokens or payload.max_tokens,
                                        "max_tokens" in value, "store" in value, payload.temperature, payload.top_p, len(body),
                                        prompt=selection, output_format=output_format)
            if payload.stream:
                return ModelStreamResponse(lambda output: invoke(replace(query, stream=True, stream_output=output)),
                                           error_response=_error)
            return await invoke(query)
        except Exception as error:
            return _exception_response(error)

    @app.post("/v1/chat/completions")
    async def complete(request: Request):
        if request_authorization is None:
            return await complete_request(request)
        try:
            async with request_authorization.context(request.scope["headers"]):
                return await complete_request(request)
        except Unauthorized:
            return _error("unauthorized")
        except Forbidden:
            return _error("forbidden")
        except AuthorizationUnavailable:
            return _error("authorization_unavailable")

    return app
