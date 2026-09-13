import asyncio
import json
from dataclasses import replace
from uuid import uuid4

import pytest

from llm_gateway.adapters.text_fingerprints import request_fingerprint, execution_fingerprint
from llm_gateway.adapters.text_routing_plan import text_routing_requirements
from llm_gateway.domain.prompts import PromptReference
from tests.test_fingerprint_leases import setup, deadline
from tests.test_text_fingerprints import QUERY, AUTH, LIMITS, snapshot


class Output:
    def admitted(self, call_id, accepted_at):
        pass

    async def delta(self, event):
        pass


@pytest.mark.parametrize("changes", [dict(stream=True), dict(stream=1), dict(stream_output=Output()),
    dict(stream=True, stream_output=object())])
def test_stream_query_requires_explicit_paired_output(changes):
    with pytest.raises(ValueError):
        replace(QUERY, **changes)


@pytest.mark.parametrize("prompt", [False, True])
def test_stream_fingerprints_are_separate_and_ignore_sink_identity(prompt):
    async def scenario():
        keys, _, _ = setup()
        assert await keys.validate_active()
        sync = replace(QUERY, prompt_reference=PromptReference(uuid4(), uuid4()) if prompt else None)
        query = replace(sync, stream=True, stream_output=Output())
        other_sink = replace(query, stream_output=Output())
        assert "stream_output" not in repr(query)
        async with keys.active(deadline=deadline()) as lease:
            for fingerprint in (lambda value: request_fingerprint(value, AUTH, lease),
                                lambda value: execution_fingerprint(value, snapshot(), lease, effective_limits=LIMITS)):
                value = fingerprint(query)
                assert value != fingerprint(sync) and value == fingerprint(other_sink)
                assert value.canonicalization.endswith("/prompt-stream-text-v1" if prompt else "/stream-text-v1")
    asyncio.run(scenario())


def test_stream_candidate_requires_explicit_capability():
    query = replace(QUERY, stream=True, stream_output=Output())
    selected = snapshot()
    requirement, assessed = text_routing_requirements(selected, query)
    assert requirement.streaming and not assessed[0].statically_eligible
    content = json.loads(selected.snapshot_json)
    content["provider_model_bindings"]["binding-a"]["capabilities"]["streaming"] = True
    _, assessed = text_routing_requirements(snapshot(content), query)
    assert assessed[0].statically_eligible
