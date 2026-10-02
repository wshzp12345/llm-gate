import asyncio
from copy import deepcopy
import json
from dataclasses import replace

import httpx

from llm_gateway.adapters.anthropic_messages import AnthropicMessagesCompletion
from llm_gateway.adapters.credentialed_runtime import CredentialBinding, CredentialedCandidateRuntime
from llm_gateway.adapters.text_routing_plan import prepare_text_routing
from llm_gateway.adapters.transport_credential_projection import project_transport_credentials
from llm_gateway.adapters.openai_compatible import OpenAICompatibleCompletion
from llm_gateway.domain.model import CompletionRequest, FailureCode, Message, OutputFormat, ProviderFailure
from tests.test_credentialed_runtime import Harness
from tests.test_text_fingerprints import QUERY, snapshot


REQUEST = CompletionRequest("general", "claude-test", (Message("system", "Be concise."), Message("user", "Hello")), 32)


def test_anthropic_messages_uses_distinct_wire_protocol_and_normalizes_result():
    def handler(request):
        assert request.url.path == "/v1/messages"
        assert request.headers["x-api-key"] == "test-secret"
        assert request.headers["anthropic-version"] == "2023-06-01"
        assert "authorization" not in request.headers
        payload = __import__("json").loads(request.content)
        assert payload == {"model": "claude-test", "max_tokens": 32, "messages": [{"role": "user", "content": "Hello"}],
                           "system": "Be concise.", "stream": False}
        return httpx.Response(200, json={"type": "message", "id": "msg_test", "role": "assistant",
            "model": "claude-test", "content": [{"type": "text", "text": "Hello "}, {"type": "text", "text": "there"}],
            "stop_reason": "end_turn", "usage": {"input_tokens": 9, "output_tokens": 3,
                "cache_creation_input_tokens": 4, "cache_read_input_tokens": 2}})

    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False) as client:
            return await AnthropicMessagesCompletion(client, base_url="https://provider.invalid/v1",
                                                     credential="test-secret").complete(REQUEST)
    result = asyncio.run(run())
    assert result.output.text == "Hello there" and result.finish_reason == "stop"
    assert result.usage.input_tokens == 15 and result.usage.output_tokens == 3
    assert result.usage.cached_tokens == 2


def test_both_protocols_remain_independent_for_same_gateway_request():
    seen = []

    def handler(request):
        seen.append((request.url.path, dict(request.headers)))
        if request.url.path == "/v1/chat/completions":
            return httpx.Response(200, json={"object": "chat.completion", "model": "openai-test",
                "choices": [{"index": 0, "message": {"role": "assistant", "content": "A"}, "finish_reason": "stop"}]})
        return httpx.Response(200, json={"type": "message", "role": "assistant", "model": "claude-test",
            "content": [{"type": "text", "text": "B"}], "stop_reason": "end_turn"})

    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False) as client:
            a = await OpenAICompatibleCompletion(client, base_url="https://provider.invalid/v1", credential="one").complete(
                CompletionRequest("general", "openai-test", (Message("user", "Hello"),), 32))
            b = await AnthropicMessagesCompletion(client, base_url="https://provider.invalid/v1", credential="two").complete(
                CompletionRequest("general", "claude-test", (Message("user", "Hello"),), 32))
            return a, b
    a, b = asyncio.run(run())
    assert not isinstance(a, ProviderFailure) and not isinstance(b, ProviderFailure)
    assert (a.output.text, b.output.text) == ("A", "B")
    assert [path for path, _ in seen] == ["/v1/chat/completions", "/v1/messages"]
    assert "authorization" in seen[0][1] and "x-api-key" in seen[1][1]


def test_anthropic_schema_is_translated_without_prompt_mutation():
    def handler(request):
        payload = __import__("json").loads(request.content)
        assert payload["messages"] == [{"role": "user", "content": "Hello"}]
        assert payload["output_config"] == {"format": {"type": "json_schema", "schema": {
            "type": "object", "properties": {"answer": {"type": "integer"}}}}}
        return httpx.Response(200, json={"type": "message", "role": "assistant", "model": "claude-test",
            "content": [{"type": "text", "text": '{"answer": 1}'}], "stop_reason": "end_turn"})

    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False) as client:
            return await AnthropicMessagesCompletion(client, base_url="https://provider.invalid/v1", credential="secret").complete(
                CompletionRequest("general", "claude-test", (Message("user", "Hello"),), 32,
                    output_format=OutputFormat("json_schema", "answer", '{"type":"object","properties":{"answer":{"type":"integer"}}}')))
    assert asyncio.run(run()).output.text == '{"answer": 1}'


def test_binding_selects_messages_adapter_without_exposing_credential():
    async def run():
        harness = Harness(statuses=(200,))
        def handler(request):
            assert request.url.path == "/v1/messages"
            assert request.headers["x-api-key"] == harness.leases[-1].bearer_value()
            return httpx.Response(200, json={"type": "message", "role": "assistant", "model": "claude-test",
                "content": [{"type": "text", "text": "OK"}], "stop_reason": "end_turn"})
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False) as client:
            runtime = CredentialedCandidateRuntime(source=harness, gates=harness.gates, reject=harness.reject,
                bindings={"claude": CredentialBinding("provider-key", "https://provider.invalid/v1", client,
                                                        "anthropic_messages", "v1")})
            async with runtime.acquire("claude") as provider:
                result = await provider.complete(REQUEST)
        assert all(not lease._material for lease in harness.leases)
        return result
    assert asyncio.run(run()).output.text == "OK"


def test_messages_schema_failure_is_a_gateway_validation_error_not_provider_success():
    async def run():
        harness = Harness(statuses=(200,))
        def handler(request):
            return httpx.Response(200, json={"type": "message", "role": "assistant", "model": "claude-test",
                "content": [{"type": "text", "text": '{"answer":"wrong"}'}], "stop_reason": "end_turn",
                "usage": {"input_tokens": 5, "output_tokens": 3}})
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False) as client:
            runtime = CredentialedCandidateRuntime(source=harness, gates=harness.gates, reject=harness.reject,
                bindings={"claude": CredentialBinding("provider-key", "https://provider.invalid/v1", client,
                                                        "anthropic_messages", "v1")})
            async with runtime.acquire("claude") as provider:
                return await provider.complete(CompletionRequest("claude", "claude-test", (Message("user", "Return JSON"),), 32,
                    output_format=OutputFormat("json_schema", "answer", '{"type":"object","properties":{"answer":{"type":"integer"}}}')))
    result = asyncio.run(run())
    assert result.code == FailureCode.STRUCTURED_OUTPUT_INVALID
    assert result.structured_reason == "schema_mismatch" and result.structured_path == "/answer"
    assert result.observed_usage.input_tokens == 5


def test_two_model_aliases_route_to_different_protocols_with_synthetic_call_evidence():
    content = json.loads(snapshot().snapshot_json)
    provider = deepcopy(content["providers"]["provider-a"])
    provider["adapter"] = {"type": "anthropic_messages", "version": "v1"}
    provider["credential"]["secret_ref"] = "provider-key"
    content["providers"]["provider-a"]["credential"]["secret_ref"] = "provider-key"
    content["providers"]["provider-b"] = provider
    binding = deepcopy(content["provider_model_bindings"]["binding-a"])
    binding.update(provider="provider-b", upstream_model="claude-test")
    binding["capabilities"]["structured_output"] = "json_schema"
    content["provider_model_bindings"]["binding-b"] = binding
    alias = deepcopy(content["model_aliases"]["general"])
    alias["candidates"] = [{**alias["candidates"][0], "binding": "binding-b"}]
    content["model_aliases"]["claude"] = alias
    selected = snapshot(content)
    first = prepare_text_routing(selected, QUERY, seed_hex="a" * 64, initial_rejections={"binding-a": ()})
    second = prepare_text_routing(selected, replace(QUERY, requested_model="claude"),
                                  seed_hex="b" * 64, initial_rejections={"binding-b": ()})
    assert first.full[0].binding_id == "binding-a" and second.full[0].binding_id == "binding-b"
    seen = []

    def handler(request):
        payload = json.loads(request.content)
        seen.append((request.url.path, payload["model"]))
        if request.url.path.endswith("/chat/completions"):
            return httpx.Response(200, json={"object": "chat.completion", "model": payload["model"],
                "choices": [{"index": 0, "message": {"role": "assistant", "content": "A"}, "finish_reason": "stop"}]})
        return httpx.Response(200, json={"type": "message", "role": "assistant", "model": payload["model"],
            "content": [{"type": "text", "text": "B"}], "stop_reason": "end_turn"})

    async def run():
        harness = Harness(statuses=(200, 200))
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False) as client:
            class Registry:
                def acquire(self, plan):
                    return client
            bindings = project_transport_credentials(selected, ("binding-a", "binding-b"), Registry())
            runtime = CredentialedCandidateRuntime(source=harness, gates=harness.gates, reject=harness.reject,
                                                    bindings=bindings)
            results = []
            for candidate in (first.full[0], second.full[0]):
                async with runtime.acquire(candidate.binding_id) as provider:
                    results.append(await provider.complete(candidate.request))
            return results
    results = asyncio.run(run())
    assert [(item.requested_model, item.output.text) for item in results] == [("general", "A"), ("claude", "B")]
    assert [path for path, _ in seen] == ["/v1/chat/completions", "/v1/messages"]
