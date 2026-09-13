import asyncio
from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest

from llm_gateway.adapters.text_fingerprints import request_fingerprint, execution_fingerprint
from llm_gateway.application.model_api import ModelInvocationRejected
from llm_gateway.domain.prompts import PromptReference, PromptSelection, PromptVersion
from llm_gateway.domain.model import Message
from tests.test_fingerprint_leases import setup, deadline
from tests.test_text_fingerprints import QUERY, AUTH, LIMITS, snapshot
from tests.test_fingerprinted_text_invocation import backend, RESULT
from tests.test_model_http import Service, request


def test_profile_separates_pinned_versions_and_never_hashes_unresolved_selection():
    async def scenario():
        keys, _, _ = setup()
        assert await keys.validate_active()
        reference = PromptReference(uuid4(), uuid4())
        first = replace(QUERY, prompt_reference=reference)
        second = replace(first, prompt_reference=replace(reference, version_id=uuid4()))
        async with keys.active(deadline=deadline()) as lease:
            for fingerprint in (lambda q: request_fingerprint(q, AUTH, lease),
                                lambda q: execution_fingerprint(q, snapshot(), lease, effective_limits=LIMITS)):
                a, b, plain = fingerprint(first), fingerprint(second), fingerprint(QUERY)
                assert a.digest != b.digest and a.digest != plain.digest
                assert a.canonicalization.endswith("/prompt-text-v1")
                with pytest.raises(ValueError, match="pinned and rendered"):
                    fingerprint(replace(QUERY, prompt=PromptSelection(reference, {})))
    asyncio.run(scenario())


def test_render_once_before_fingerprints_and_execution_keeps_immutable_messages():
    async def scenario():
        keys, _, _ = setup()
        assert await keys.validate_active()
        reference = PromptReference(uuid4(), uuid4())
        original = PromptVersion(AUTH.tenant_id, reference.asset_id, reference.version_id, (Message("user", "Hello {{name}}"),))
        events = []
        class Prompts:
            async def render(self, auth, asset_id, version_id, values):
                events.append("render")
                assert keys.in_use == 0 and auth is AUTH
                return original.render(values)
        class Admission:
            async def admit(self, record, *args, **kwargs):
                events.append("admit")
                assert record.prompt_reference == reference
                return datetime.now(timezone.utc)
        class Execution:
            async def execute(self, record, query, *args, **kwargs):
                events.append("execute")
                assert query.prompt is None and query.prompt_reference == reference
                assert query.messages == (Message("user", "Hello World"),)
                return RESULT
        query = replace(QUERY, messages=(), body_bytes=200, prompt=PromptSelection(reference, {"name": "World"}))
        service = backend(keys, SimpleNamespace(current=snapshot()), Admission(), Execution(), prompts=Prompts())
        result = await service.invoke_authorized(query, AUTH)
        assert result.prompt_reference == reference
        assert events == ["render", "admit", "execute"]
    asyncio.run(scenario())


def test_prompt_selection_is_immutable_and_query_repr_omits_values():
    values = {"name": "secret-value"}
    selection = PromptSelection(PromptReference(uuid4(), uuid4()), values)
    values["name"] = "changed"
    assert selection.variables["name"] == "secret-value"
    with pytest.raises(TypeError):
        selection.variables["name"] = "changed"
    assert "secret-value" not in repr(replace(QUERY, messages=(), prompt=selection))
    assert "private text" not in repr(QUERY)


@pytest.mark.parametrize("changes", [{}, {"prompt": None}, {"messages": None},
    {"messages": [], "prompt": {"asset_id": str(uuid4()), "version_id": str(uuid4()), "variables": {}}},
    {"prompt": {"asset_id": "bad", "version_id": "bad", "variables": {}}}])
def test_invalid_prompt_shape_never_enters_backend(changes):
    service = Service()
    response = request(service, json={"model": "general", **changes})
    assert response.status_code == 400
    assert not service.queries
