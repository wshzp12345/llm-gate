import asyncio
import json
from copy import deepcopy
from dataclasses import fields, replace
from datetime import datetime, timedelta, timezone

import pytest

from llm_gateway.adapters.text_fingerprints import request_fingerprint, execution_fingerprint, REQUEST_PROFILE
from llm_gateway.application.active_configuration import LoadedConfiguration
from llm_gateway.application.model_api import TextInvocationQuery
from llm_gateway.domain.invocation import AuthorizationContext
from llm_gateway.domain.model import Message, OutputFormat
from tests.test_fingerprint_leases import setup, deadline
from tests.test_configuration_preparation import draft


QUERY = TextInvocationQuery("general", (Message("user", "private text é"),), None)
AUTH = AuthorizationContext("tenant", "subject", frozenset({"model.invoke", "model.read"}),
                            "issuer", "audience", datetime(2030, 1, 1, tzinfo=timezone.utc), "local_jwt")
LIMITS = dict(max_request_bytes=4194304, max_messages=512, max_content_item_bytes=1048576,
              max_output_schema_bytes=262144, max_sse_event_bytes=1048576, max_invocation_seconds=120)


def snapshot(content=None, revision="1", digest="fixture"):
    prepared = draft()
    return LoadedConfiguration(revision, digest, prepared.snapshot_json if content is None else json.dumps(content).encode(),
                               prepared.validation_resources)


def test_request_fingerprint_excludes_auth_expiry_and_spelling_flags_but_not_intent():
    async def run():
        keys, source, _ = setup()
        assert await keys.validate_active()
        async with keys.active(deadline=deadline()) as lease:
            original = request_fingerprint(QUERY, AUTH, lease)
            assert original.digest == "2300a9c2de65da5bd216eb25e662c8c9fa3501d440053bff625a1d57c08fc269"
            equivalent = request_fingerprint(replace(QUERY, deprecated_max_tokens=True, store_false_requested=True, body_bytes=9876),
                replace(AUTH, expires_at=AUTH.expires_at+timedelta(hours=1), authentication_method="online_authority"), lease)
            assert original == equivalent
            assert original.canonicalization == REQUEST_PROFILE
            assert "private text" not in repr(original)
            variants = [replace(QUERY, requested_model="other"),
                        replace(QUERY, messages=(Message("developer", "private text é"),)),
                        replace(QUERY, messages=(Message("user", "private text e\u0301"),)),
                        replace(QUERY, max_output_tokens=100), replace(QUERY, temperature=1),
                        replace(QUERY, top_p=.5)]
            assert all(request_fingerprint(query, AUTH, lease) != original for query in variants)
            for changed in (replace(AUTH, tenant_id="other"), replace(AUTH, subject="other"),
                            replace(AUTH, issuer="other"), replace(AUTH, audience="other"),
                            replace(AUTH, scopes=frozenset({"model.invoke"}))):
                assert request_fingerprint(QUERY, changed, lease) != original
            assert len(source.calls) == 2
    asyncio.run(run())


def test_numeric_forms_are_canonical_but_omitted_sampling_is_distinct():
    async def run():
        keys, _, _ = setup()
        assert await keys.validate_active()
        async with keys.active(deadline=deadline()) as lease:
            integer = request_fingerprint(replace(QUERY, temperature=1), AUTH, lease)
            floating = request_fingerprint(replace(QUERY, temperature=1.0), AUTH, lease)
            assert integer == floating
            assert integer != request_fingerprint(QUERY, AUTH, lease)
            assert request_fingerprint(replace(QUERY, temperature=0), AUTH, lease) == request_fingerprint(
                replace(QUERY, temperature=-0.0), AUTH, lease)
    asyncio.run(run())


def test_new_query_fields_require_an_explicit_fingerprint_profile_decision():
    assert {field.name for field in fields(TextInvocationQuery)} == {
        "requested_model", "messages", "max_output_tokens", "temperature", "top_p",
        "deprecated_max_tokens", "store_false_requested",
        "body_bytes", "prompt", "prompt_reference", "stream", "stream_output", "output_format",
    }


def test_structured_mode_and_schema_change_both_fingerprints():
    async def run():
        keys, _, _ = setup()
        assert await keys.validate_active()
        async with keys.active(deadline=deadline()) as lease:
            variants = (QUERY,
                replace(QUERY, output_format=OutputFormat("json_object")),
                replace(QUERY, output_format=OutputFormat("json_schema", "answer", '{"type":"object"}')),
                replace(QUERY, output_format=OutputFormat("json_schema", "answer", '{"type":"array"}')))
            requests = [request_fingerprint(item, AUTH, lease) for item in variants]
            executions = [execution_fingerprint(item, snapshot(), lease, effective_limits=LIMITS) for item in variants]
            assert len(set(requests)) == len(variants)
            assert len(set(executions)) == len(variants)
    asyncio.run(run())


def test_extraction_policy_changes_structured_execution_identity_only():
    async def run():
        keys, _, _ = setup()
        assert await keys.validate_active()
        before = json.loads(snapshot().snapshot_json)
        after = deepcopy(before)
        after["resource_policies"]["structured_output"]["local_extraction_enabled"] = False
        structured = replace(QUERY, output_format=OutputFormat("json_object"))
        async with keys.active(deadline=deadline()) as lease:
            assert execution_fingerprint(structured, snapshot(before), lease, effective_limits=LIMITS) != \
                execution_fingerprint(structured, snapshot(after), lease, effective_limits=LIMITS)
            assert execution_fingerprint(QUERY, snapshot(before), lease, effective_limits=LIMITS) == \
                execution_fingerprint(QUERY, snapshot(after), lease, effective_limits=LIMITS)
    asyncio.run(run())


def test_execution_excludes_revision_unrelated_resources_prices_and_nonsemantic_tls_label():
    async def run():
        keys, _, _ = setup()
        assert await keys.validate_active()
        content = json.loads(snapshot().snapshot_json)
        content["providers"]["provider-a"]["transport"]["tls"] = {
            "trust": "bundle", "trust_bundle": {"identity": "sha256:"+"a"*64, "pem": "fixture", "label": "old"}}
        changed = deepcopy(content)
        changed["providers"]["provider-a"]["transport"]["tls"]["trust_bundle"]["label"] = "new"
        changed["providers"]["unrelated"] = deepcopy(changed["providers"]["provider-a"])
        changed["pricing_tables"]["price-a"]["rates"]["input"] = "99"
        changed["resource_policies"]["routing_evidence_ttl_seconds"] = 123456
        async with keys.active(deadline=deadline()) as lease:
            original = execution_fingerprint(QUERY, snapshot(content), lease, effective_limits=LIMITS)
            assert original == execution_fingerprint(QUERY, snapshot(changed, "2", "different"), lease, effective_limits=LIMITS)
            assert "private text" not in repr(original)
            explicit = replace(QUERY, max_output_tokens=4096, temperature=1, top_p=1)
            assert execution_fingerprint(explicit, snapshot(content), lease, effective_limits=LIMITS) == original
            assert request_fingerprint(explicit, AUTH, lease) != request_fingerprint(QUERY, AUTH, lease)
    asyncio.run(run())


@pytest.mark.parametrize("change", ["upstream", "weight", "sampling", "endpoint", "limits", "retry"])
def test_resolved_behavior_changes_execution_but_not_request(change):
    async def run():
        keys, _, _ = setup()
        assert await keys.validate_active()
        content = json.loads(snapshot().snapshot_json)
        if change == "upstream":
            content["provider_model_bindings"]["binding-a"]["upstream_model"] = "Other"
        elif change == "weight":
            content["model_aliases"]["general"]["candidates"][0]["weight"] = 2
        elif change == "sampling":
            content["model_aliases"]["general"]["generation_defaults"]["temperature"] = .5
        elif change == "endpoint":
            content["providers"]["provider-a"]["endpoint"]["base_url"] = "https://other.invalid/v1"
        elif change == "limits":
            content["provider_model_bindings"]["binding-a"]["limits"]["context_tokens"] = 4096
        else:
            content["routing_policies"]["route-a"]["retry"]["max_attempts"] = 2
        async with keys.active(deadline=deadline()) as lease:
            request_before = request_fingerprint(QUERY, AUTH, lease)
            assert execution_fingerprint(QUERY, snapshot(), lease, effective_limits=LIMITS) != execution_fingerprint(
                QUERY, snapshot(content), lease, effective_limits=LIMITS)
            assert request_before == request_fingerprint(QUERY, AUTH, lease)
    asyncio.run(run())


def test_execution_requires_resolved_limits_and_preserves_candidate_order_independence():
    async def run():
        keys, _, _ = setup()
        assert await keys.validate_active()
        content = json.loads(snapshot().snapshot_json)
        content["provider_model_bindings"]["binding-b"] = deepcopy(content["provider_model_bindings"]["binding-a"])
        candidates = content["model_aliases"]["general"]["candidates"]
        candidates.append({"binding": "binding-b", "service_level": "full", "priority": 0, "weight": 2})
        async with keys.active(deadline=deadline()) as lease:
            first = execution_fingerprint(QUERY, snapshot(content), lease, effective_limits=LIMITS)
            candidates.reverse()
            assert first == execution_fingerprint(QUERY, snapshot(content), lease, effective_limits=LIMITS)
            with pytest.raises(ValueError):
                execution_fingerprint(QUERY, snapshot(content), lease, effective_limits={**LIMITS, "max_messages": "inherit"})
    asyncio.run(run())
