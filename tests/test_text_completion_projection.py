import asyncio
import json
from dataclasses import replace

import httpx
import pytest

from llm_gateway.adapters.text_completion_projection import prepare_text_completion
from llm_gateway.adapters.openai_compatible import OpenAICompatibleCompletion
from llm_gateway.application.active_configuration import LoadedConfiguration
from llm_gateway.application.model_api import ModelInvocationRejected, TextInvocationQuery
from llm_gateway.domain.configuration_changes import SnapshotResources
from llm_gateway.domain.model import CompletionRequest, Message
from tests.test_completion import envelope
from tests.test_model_http import Service, request, PAYLOAD


QUERY = TextInvocationQuery("general", (Message("developer", "instruction"), Message("user", "hello")), None)


def snapshot(binding_limit=4096):
    content = {"model_aliases": {"general": {"generation_defaults": {"temperature": 1.0, "top_p": 1.0, "max_output_tokens": 4096},
                                          "candidates": [{"binding": "a"}]}},
               "provider_model_bindings": {"a": {"upstream_model": "model", "limits": {"max_output_tokens": binding_limit}}}}
    return LoadedConfiguration("1", "fixture", json.dumps(content).encode(), SnapshotResources({}))


def test_alias_defaults_are_explicit_and_caller_values_override_without_clamping():
    prepared = prepare_text_completion(snapshot(), QUERY, "a")
    assert (prepared.request.max_output_tokens, prepared.request.temperature, prepared.request.top_p) == (4096, 1, 1)
    assert all(source == "alias_default" for _, source in prepared.parameter_sources)
    overridden = prepare_text_completion(snapshot(), replace(QUERY, max_output_tokens=128, temperature=0, top_p=0.5), "a")
    assert (overridden.request.max_output_tokens, overridden.request.temperature, overridden.request.top_p) == (128, 0, 0.5)
    assert all(source == "caller" for _, source in overridden.parameter_sources)
    assert overridden.effective_parameters_digest != prepared.effective_parameters_digest


def test_same_effective_values_have_same_digest_but_different_sources():
    omitted = prepare_text_completion(snapshot(), QUERY, "a")
    explicit = prepare_text_completion(snapshot(), replace(QUERY, temperature=1, top_p=1, max_output_tokens=4096), "a")
    assert omitted.effective_parameters_digest == explicit.effective_parameters_digest
    assert omitted.parameter_sources != explicit.parameter_sources


@pytest.mark.parametrize("limit,query,code", [
    (4096, replace(QUERY, max_output_tokens=4097), "invalid_request"),
    (1024, QUERY, "unsupported_capability"),
])
def test_output_ceiling_is_rejected_not_silently_clamped(limit, query, code):
    with pytest.raises(ModelInvocationRejected) as error:
        prepare_text_completion(snapshot(limit), query, "a")
    assert error.value.code == code


def test_selected_binding_cannot_escape_requested_alias():
    with pytest.raises(ValueError):
        prepare_text_completion(snapshot(), QUERY, "unrelated")


@pytest.mark.parametrize("field,value", [
    ("temperature", True), ("temperature", float("nan")), ("temperature", -1),
    ("temperature", 3), ("top_p", 0), ("top_p", 1.1), ("top_p", float("inf")),
])
def test_internal_request_enforces_sampling_bounds(field, value):
    with pytest.raises(ValueError):
        CompletionRequest("general", "model", QUERY.messages, 32, **{field: value})


@pytest.mark.parametrize("field,value", [("temperature", True), ("temperature", "1"), ("temperature", None),
                                         ("top_p", 0), ("top_p", None), ("top_p", 2)])
def test_http_sampling_validation_prevents_application_entry(field, value):
    service = Service()
    assert request(service, json=PAYLOAD | {field: value}).status_code == 400
    assert not service.queries


def test_http_query_and_adapter_preserve_developer_order_and_effective_parameters():
    service = Service()
    payload = PAYLOAD | {"temperature": 0.25, "top_p": 0.8,
                         "messages": [{"role": "developer", "content": "instruction"}, {"role": "user", "content": "hello"}]}
    assert request(service, json=payload).status_code == 200
    prepared = prepare_text_completion(snapshot(), service.queries[0], "a")
    async def run():
        def handler(request):
            body = json.loads(request.content)
            assert body["messages"] == payload["messages"]
            assert (body["temperature"], body["top_p"], body["max_tokens"]) == (0.25, 0.8, 32)
            return httpx.Response(200, json=envelope())
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False) as client:
            await OpenAICompatibleCompletion(client, base_url="https://provider.invalid/v1", credential="test-only").complete(prepared.request)
    asyncio.run(run())
