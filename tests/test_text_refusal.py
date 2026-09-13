import asyncio
import json
from datetime import datetime, timezone
from uuid import uuid4

import httpx
import pytest

from llm_gateway.adapters.model_http import _response
from llm_gateway.application.model_api import InvocationReply
from llm_gateway.application.provider_circuits import circuit_observation
from llm_gateway.domain.model import ProviderResult, ProviderFailure, RefusalOutput, Usage
from llm_gateway.domain.recovery import RecoveryPlan
from llm_gateway.infrastructure.invocation import PostgresAttemptJournal
from llm_gateway.infrastructure.settlement import PostgresInvocationSettlement
from tests.test_completion import envelope, invoke
from tests.test_attempt_execution import Harness, SUCCESS


@pytest.mark.parametrize("text,refusal,finish", [(None, "Cannot help.", "stop"),
    ("", None, "content_filter"), (None, None, "content_filter"),
    ("Safe explanation", "Cannot help.", "content_filter"), (None, "", "stop"), (None, "Cannot", "length")])
def test_valid_refusal_keeps_representation_and_never_recovers(text, refusal, finish):
    payload = envelope(choices=[{"index": 0, "message": {"role": "assistant", "content": text,
        **({"refusal": refusal} if refusal is not None else {})}, "finish_reason": finish}],
        usage={"prompt_tokens": 7, "completion_tokens": 2})
    result = invoke(lambda request: httpx.Response(200, json=payload))
    assert isinstance(result, ProviderResult) and isinstance(result.output, RefusalOutput)
    assert result.disposition == "safety_refused"
    assert result.usage == Usage(7, 2) and circuit_observation(result) is None
    harness = Harness([result, SUCCESS])
    assert asyncio.run(harness.run()) is result
    assert harness.events == [("gate", "a"), ("start", 1, "a", 1), ("provider",),
                              ("finish", 1, "stop"), ("release", "a")]
    response = _response(InvocationReply(uuid4(), datetime.now(timezone.utc), result))
    assert response.status_code == 200 and "retry-after" not in response.headers
    choice = json.loads(response.body)["choices"][0]
    assert choice["finish_reason"] == finish and choice["message"]["content"] == text
    if refusal is not None:
        assert choice["message"]["refusal"] == refusal


@pytest.mark.parametrize("text,refusal,finish", [(None, 7, "stop"), ({}, "no", "stop"),
    (None, "no", "tool_calls"), (None, False, "stop"), (None, "\ud800", "stop")])
def test_invalid_refusal_envelope_is_not_accepted(text, refusal, finish):
    payload = envelope(choices=[{"index": 0, "message": {"role": "assistant", "content": text,
                                "refusal": refusal}, "finish_reason": finish}])
    result = invoke(lambda request: httpx.Response(200, content=json.dumps(payload).encode(),
                                                headers={"content-type": "application/json"}))
    assert isinstance(result, ProviderFailure) and result.code == "provider_protocol_error"


def test_refusal_cannot_be_recorded_with_recovery():
    result = ProviderResult("general", "model", RefusalOutput(None, "no"), Usage(), "stop")
    async def run():
        for action in ("retry", "advance"):
            with pytest.raises(ValueError, match="cannot recover"):
                await PostgresAttemptJournal(None, uuid4()).finished(1, result, RecoveryPlan(action))
    asyncio.run(run())
