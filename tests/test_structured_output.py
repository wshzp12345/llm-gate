from dataclasses import replace
import asyncio

import httpx

import pytest

from llm_gateway.adapters.structured_output import InvalidOutputFormat, parse_output_format, schema_feature_ids, validate_structured_result, validate_structured_stream
from llm_gateway.application.text_failure_recovery import classify_text_failure
from llm_gateway.adapters.credentialed_runtime import CredentialBinding, CredentialedCandidateRuntime
from llm_gateway.domain.model import CompletionRequest, FailureCode, Message, OutputFormat, ProviderFailure, TextOutput
from llm_gateway.domain.streaming import DeltaKind, StreamCompleted, StreamDelta, StreamFailed
from tests.test_attempt_execution import SUCCESS
from tests.test_credentialed_runtime import Harness


SCHEMA = {"type": "object", "properties": {"answer": {"type": "integer"}},
          "required": ["answer"], "additionalProperties": False}


def test_json_object_requires_one_object_and_fails_closed_on_bad_content():
    requested = parse_output_format({"type": "json_object"})
    assert validate_structured_result(replace(SUCCESS, output=TextOutput('{"answer":1}')), requested).output.text == '{"answer":1}'
    for content, reason in (("[]", "json_wrong_root"), ("", "json_missing"),
                            ('{"answer":1} trailing', "json_malformed"), ('{"a":1,"a":2}', "json_duplicate_key")):
        result = validate_structured_result(replace(SUCCESS, output=TextOutput(content)), requested)
        assert isinstance(result, ProviderFailure) and result.code == FailureCode.STRUCTURED_OUTPUT_INVALID
        assert result.structured_reason == reason
    truncated = validate_structured_result(replace(SUCCESS, output=TextOutput('{"a":'), finish_reason="length"), requested)
    assert truncated.structured_reason == "output_truncated"
    recovery = classify_text_failure(truncated)
    assert not recovery.retryable and not recovery.failover_eligible


def test_json_schema_is_preflighted_and_independently_validated():
    requested = parse_output_format({"type": "json_schema", "json_schema": {"name": "answer", "strict": True, "schema": SCHEMA}})
    assert validate_structured_result(replace(SUCCESS, output=TextOutput('{"answer":1}')), requested).output.text == '{"answer":1}'
    bad = validate_structured_result(replace(SUCCESS, output=TextOutput('{"answer":"one"}')), requested)
    assert bad.code == FailureCode.STRUCTURED_OUTPUT_INVALID
    assert bad.structured_reason == "schema_mismatch" and bad.structured_path == "/answer"
    assert bad.observed_usage == SUCCESS.usage and bad.observed_model == SUCCESS.resolved_model


def test_configured_local_extraction_returns_only_one_json_candidate():
    requested = OutputFormat("json_object", local_extraction=True)
    for content in ('```json\n{"answer":1}\n```', 'The answer is {"answer":1}.'):
        result = validate_structured_result(replace(SUCCESS, output=TextOutput(content)), requested)
        assert result.output.text == '{"answer":1}'
    bad = validate_structured_result(replace(SUCCESS, output=TextOutput('{"a":1} and {"b":2}')), requested)
    assert bad.code == FailureCode.STRUCTURED_OUTPUT_INVALID and bad.structured_reason == "json_ambiguous"


@pytest.mark.parametrize("shape", [None, {}, {"type": "unsupported"}, {"type": "json_object", "schema": {}},
    {"type": "json_schema", "json_schema": {"name": "a", "schema": {"$ref": "https://example.invalid/schema"}}},
    {"type": "json_schema", "json_schema": {"name": "a", "schema": {"type": "object", "format": "date"}}},
    {"type": "json_schema", "json_schema": {"name": "a", "schema": {"$ref": "#"}}}])
def test_unsupported_or_invalid_format_is_rejected_before_execution(shape):
    with pytest.raises(InvalidOutputFormat):
        parse_output_format(shape)


def test_schema_preflight_bounds_members_refs_depth_and_unicode_without_provider_io():
    chain = {f"n{index}": {"$ref": f"#/$defs/n{index + 1}"} for index in range(33)}
    chain["n33"] = {"type": "object"}
    nested = "leaf"
    for _ in range(65):
        nested = [nested]
    invalid = (
        {"type": "object", "properties": {"a": {}}, "required": ["a", "a"]},
        {"type": "object", "properties": {f"p{index}": {} for index in range(257)}},
        {"$defs": chain, "$ref": "#/$defs/n0"},
        {"$ref": "#/$defs/~2", "$defs": {"~2": {"type": "object"}}},
        {"const": nested},
        {"const": "\ud800"},
        {"properties": {"\ud800": {"type": "string"}}},
    )
    for schema in invalid:
        with pytest.raises(InvalidOutputFormat):
            parse_output_format({"type": "json_schema", "json_schema": {"name": "bounded", "schema": schema}})


def test_schema_member_and_reference_limits_accept_exact_boundary():
    properties = {f"p{index}": {"type": "integer"} for index in range(256)}
    schema = {"type": "object", "properties": properties,
              "required": list(properties), "additionalProperties": False}
    assert parse_output_format({"type": "json_schema", "json_schema": {"name": "bounded", "schema": schema}})
    chain = {f"n{index}": {"$ref": f"#/$defs/n{index + 1}"} for index in range(31)}
    chain["n31"] = {"type": "object"}
    assert parse_output_format({"type": "json_schema", "json_schema": {"name": "chain", "schema": {
        "$defs": chain, "$ref": "#/$defs/n0"}}})


def test_schema_feature_scan_treats_literals_and_property_names_as_data():
    schema = {"type": "object", "additionalProperties": False,
              "properties": {"additionalProperties": {"const": {"type": "object", "properties": {}}}}}
    parsed = parse_output_format({"type": "json_schema", "json_schema": {"name": "literal", "schema": schema}})
    assert schema_feature_ids(parsed.schema_json) == ("const", "object-closed")


@pytest.mark.parametrize("content,reason", [
    ('{"n":' + '9' * 257 + '}', "structured_resource_limit"),
    ('{"n":1e1001}', "structured_resource_limit"),
    ('{"n":1e-1001}', "structured_resource_limit"),
    ('{"text":"' + 'x' * 262145 + '"}', "structured_resource_limit"),
    ('{"' + 'x' * 262145 + '":1}', "structured_resource_limit"),
    ('{"text":"\\ud800"}', "json_invalid_unicode"),
], ids=["number-token", "positive-exponent", "negative-exponent", "string-codepoints",
        "member-codepoints", "invalid-surrogate"])
def test_structured_instance_numeric_string_and_unicode_limits(content, reason):
    result = validate_structured_result(replace(SUCCESS, output=TextOutput(content)), OutputFormat("json_object"))
    assert isinstance(result, ProviderFailure)
    assert result.structured_reason == reason


def test_structured_instance_numeric_and_string_boundaries_remain_valid():
    content = '{"n":' + '9' * 256 + ',"large":1e1000,"text":"' + 'x' * 262144 + '"}'
    result = validate_structured_result(replace(SUCCESS, output=TextOutput(content)), OutputFormat("json_object"))
    assert result.output.text == content


def test_credentialed_runtime_validates_output_before_attempt_result_is_returned():
    async def run():
        harness = Harness(statuses=(200,))
        def handler(request):
            return httpx.Response(200, json={"object": "chat.completion", "model": "actual",
                "choices": [{"index": 0, "message": {"role": "assistant", "content": "not json"},
                             "finish_reason": "stop"}],
                "usage": {"prompt_tokens": 4, "completion_tokens": 2}})
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False) as client:
            runtime = CredentialedCandidateRuntime(source=harness, gates=harness.gates, reject=harness.reject,
                bindings={"a": CredentialBinding("provider-key", "https://provider.invalid/v1", client)})
            async with runtime.acquire("a") as provider:
                return await provider.complete(CompletionRequest("general", "actual", (Message("user", "JSON"),), 32,
                    output_format=OutputFormat("json_object")))
    result = asyncio.run(run())
    assert result.code == FailureCode.STRUCTURED_OUTPUT_INVALID
    assert result.observed_usage.input_tokens == 4 and result.observed_usage.output_tokens == 2


@pytest.mark.parametrize("pieces,content,reason", [
    (("  ", "{", '"answer":1}'), '  {"answer":1}', None),
    (("no json",), "no json", "json_wrong_root"),
    (("  ",), "  ", "json_malformed"),
    (("{", '"answer":'), '{"answer":', "json_malformed"),
    (("{", '"answer":1} trailing'), '{"answer":1} trailing', "json_extraneous_content"),
    (('{"answer":1}', ' {"second":2}'), '{"answer":1} {"second":2}', "json_extraneous_content"),
    (('{"answer":1}', '\u00a0'), '{"answer":1}\u00a0', "json_extraneous_content"),
    (("{", '"items":' + '[' * 65), '{"items":' + '[' * 65, "structured_resource_limit"),
    (('{"answer":"' + 'x' * 5000 + '"}',), '{"answer":"' + 'x' * 5000 + '"}', None),
])
def test_structured_stream_guards_first_object_and_terminal(pieces, content, reason):
    async def run():
        async def source():
            for number, piece in enumerate(pieces, 1):
                yield StreamDelta(number, "model", DeltaKind.TEXT, piece)
            yield StreamCompleted(len(pieces) + 1, replace(SUCCESS, output=TextOutput(content)))
        return [event async for event in validate_structured_stream(source(), OutputFormat("json_object"))]
    events = asyncio.run(run())
    assert [event.sequence for event in events] == list(range(1, len(events) + 1))
    if reason:
        assert isinstance(events[-1], StreamFailed)
        assert events[-1].failure.code == FailureCode.STRUCTURED_OUTPUT_INVALID
        assert events[-1].failure.structured_reason == reason
    else:
        assert isinstance(events[-1], StreamCompleted)
        assert "".join(event.text for event in events[:-1]) == content
