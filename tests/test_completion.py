import asyncio
import json

import httpx
import pytest

from llm_gateway.adapters.openai_compatible import OpenAICompatibleCompletion
from llm_gateway.domain.model import CompletionRequest, FailureCode, Message, OutputFormat, ProviderFailure, Usage


REQUEST = CompletionRequest("general", "upstream-model", (Message("user", "hello"),), 32)


def envelope(**changes):
    result = {
        "object": "chat.completion", "model": "actual-model",
        "choices": [{"index": 0, "message": {"role": "assistant", "content": "你好"}, "finish_reason": "stop"}],
    }
    return result | changes


def invoke(handler, **kwargs):
    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False) as client:
            adapter = OpenAICompatibleCompletion(client, base_url="https://provider.invalid/v1", credential="test-only", **kwargs)
            return await adapter.complete(REQUEST)
    return asyncio.run(run())


def test_request_translation_and_model_pair():
    def handler(request):
        assert str(request.url) == "https://provider.invalid/v1/chat/completions"
        assert request.headers["authorization"] == "Bearer test-only"
        assert json.loads(request.content) == {
            "model": "upstream-model", "messages": [{"role": "user", "content": "hello"}],
            "max_tokens": 32, "stream": False,
        }
        return httpx.Response(200, json=envelope())
    result = invoke(handler)
    assert result.requested_model == "general"
    assert result.resolved_model == "actual-model"
    assert result.output.text == "你好"
    assert result.usage == Usage()  # FR-719: no invented counts.


def test_json_object_mode_is_forwarded_without_modifying_messages():
    def handler(request):
        payload = json.loads(request.content)
        assert payload["response_format"] == {"type": "json_object"}
        assert payload["messages"] == [{"role": "user", "content": "hello"}]
        return httpx.Response(200, json=envelope())

    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False) as client:
            return await OpenAICompatibleCompletion(client, base_url="https://provider.invalid/v1", credential="test-only").complete(
                CompletionRequest("general", "upstream-model", (Message("user", "hello"),), 32,
                                  output_format=OutputFormat("json_object")))
    assert asyncio.run(run()).output.text == "你好"


def test_usage_parent_total_excludes_subsets():
    usage = {"prompt_tokens": 10, "completion_tokens": 6, "total_tokens": 999,
             "prompt_tokens_details": {"cached_tokens": 4},
             "completion_tokens_details": {"reasoning_tokens": 2}}
    result = invoke(lambda _: httpx.Response(200, json=envelope(usage=usage)))
    assert result.usage == Usage(10, 6, 4, 2, 999)
    assert result.usage.total_tokens == 16
    assert result.usage.provenance == "provider_reported"


@pytest.mark.parametrize("usage", [None, {}, {"total_tokens": 99}, {"prompt_tokens": True, "completion_tokens": -2}])
def test_untrustworthy_usage_is_unknown(usage):
    result = invoke(lambda _: httpx.Response(200, json=envelope(usage=usage)))
    assert result.usage.total_tokens is None


@pytest.mark.parametrize("status,code,retryable", [
    (400, FailureCode.INVALID_REQUEST, False),
    (401, FailureCode.PROVIDER_CREDENTIALS_UNAVAILABLE, False),
    (403, FailureCode.PROVIDER_CREDENTIALS_UNAVAILABLE, False),
    (429, FailureCode.RATE_LIMITED, True),
    (503, FailureCode.PROVIDER_UNAVAILABLE, True),
    (302, FailureCode.PROVIDER_PROTOCOL_ERROR, False),
])
def test_errors_are_safe_and_never_retried_by_adapter(status, code, retryable):
    calls = []
    def handler(request):
        calls.append(request)
        return httpx.Response(status, text="secret-provider-body", headers={"Location": "https://other.invalid"})
    result = invoke(handler)
    assert result == ProviderFailure(code, retryable)
    assert len(calls) == 1
    assert "secret" not in repr(result)


@pytest.mark.parametrize("error,code", [
    (httpx.ConnectTimeout, FailureCode.UPSTREAM_TIMEOUT),
    (httpx.ConnectError, FailureCode.PROVIDER_UNAVAILABLE),
])
def test_transport_failure_does_not_escape(error, code):
    def handler(request):
        raise error("sensitive endpoint", request=request)
    assert invoke(handler) == ProviderFailure(code, True)


@pytest.mark.parametrize("error,code", [
    (httpx.LocalProtocolError, FailureCode.PROVIDER_PROTOCOL_ERROR),
    (httpx.UnsupportedProtocol, FailureCode.PROVIDER_PROTOCOL_ERROR),
    (httpx.PoolTimeout, FailureCode.PROVIDER_UNAVAILABLE),
    (httpx.ProxyError, FailureCode.UNCERTAIN),
    (httpx.RemoteProtocolError, FailureCode.UNCERTAIN),
    (httpx.TransportError, FailureCode.UNCERTAIN),
])
def test_unclassified_or_local_transport_failure_does_not_authorize_retry(error, code):
    calls = []
    def handler(request):
        calls.append(request)
        raise error("sensitive endpoint", request=request)
    assert invoke(handler) == ProviderFailure(code, False)
    assert len(calls) == 1


@pytest.mark.parametrize("data", [[], {}, envelope(choices=[]), envelope(choices=[None]),
    envelope(choices=[{"index": 0, "message": None}]), envelope(model=None)])
def test_malformed_envelope_is_protocol_failure(data):
    assert invoke(lambda _: httpx.Response(200, json=data)).code == FailureCode.PROVIDER_PROTOCOL_ERROR


@pytest.mark.parametrize("body", [b'{"a":1,"a":2}', b'{"a":NaN}', b'\xff', b'{'])
def test_invalid_json_is_protocol_failure(body):
    result = invoke(lambda _: httpx.Response(200, content=body, headers={"content-type": "application/json"}))
    assert result.code == FailureCode.PROVIDER_PROTOCOL_ERROR


def test_response_bound():
    result = invoke(lambda _: httpx.Response(200, json=envelope()), max_response_bytes=8)
    assert result.code == FailureCode.PROVIDER_PROTOCOL_ERROR
    assert result.retryable is False


@pytest.mark.parametrize("status,retryable", [(500, True), (502, True), (503, True), (504, True), (501, False), (505, False), (599, False)])
def test_only_explicit_transient_5xx_are_retryable(status, retryable):
    result = invoke(lambda _: httpx.Response(status))
    assert result.code == FailureCode.PROVIDER_UNAVAILABLE and result.retryable is retryable


def test_malformed_provider_envelope_is_not_a_json_structure_retry():
    result = invoke(lambda _: httpx.Response(200, json={}))
    assert result.code == FailureCode.PROVIDER_PROTOCOL_ERROR and result.retryable is False


def test_cancellation_propagates():
    def handler(_):
        raise asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        invoke(handler)


def test_domain_has_no_framework_dependencies():
    import ast
    from pathlib import Path
    root = Path(__file__).parents[1] / "src" / "llm_gateway"
    for layer in ("domain", "application"):
        for source in (root / layer).glob("*.py"):
            for node in ast.walk(ast.parse(source.read_text(encoding="utf-8"))):
                if isinstance(node, ast.Import):
                    imports = [alias.name for alias in node.names]
                elif isinstance(node, ast.ImportFrom):
                    imports = [node.module or ""]
                else:
                    continue
                assert not any(name.startswith(("httpx", "fastapi", "pydantic", "psycopg", "rfc8785", "llm_gateway.adapters", "llm_gateway.infrastructure")) for name in imports)


def test_partial_total_is_retained_without_inventing_parents():
    result = invoke(lambda _: httpx.Response(200, json=envelope(usage={"total_tokens": 99})))
    assert result.usage.provider_reported_total == 99
    assert result.usage.total_tokens is None
    assert result.usage.input_tokens is None
    assert result.usage.output_tokens is None


def test_missing_usage_provenance():
    assert Usage().provenance == "unavailable"
    assert Usage(0, 0).provenance == "provider_reported"


@pytest.mark.parametrize("data", [
    envelope(choices=[{"index": 0, "message": {"role": "assistant", "content": "", "tool_calls": [{"id": "a"}]}, "finish_reason": "tool_calls"}]),
])
def test_unimplemented_output_kinds_are_not_silently_dropped(data):
    assert invoke(lambda _: httpx.Response(200, json=data)).code == FailureCode.PROVIDER_PROTOCOL_ERROR


def test_incorrect_media_type_is_rejected():
    assert invoke(lambda _: httpx.Response(200, text="not JSON")).code == FailureCode.PROVIDER_PROTOCOL_ERROR


@pytest.mark.parametrize("kwargs", [
    {"input_tokens": -1}, {"output_tokens": True},
    {"input_tokens": 3, "cached_tokens": 4}, {"reasoning_tokens": 1},
])
def test_usage_invariants(kwargs):
    with pytest.raises(ValueError):
        Usage(**kwargs)
