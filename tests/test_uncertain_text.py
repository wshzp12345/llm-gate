import asyncio
import json
from datetime import datetime, timezone
from uuid import uuid4

import httpx
import pytest

from llm_gateway.adapters.model_http import _response
from llm_gateway.application.model_api import InvocationReply
from llm_gateway.application.provider_circuits import circuit_observation
from llm_gateway.application.text_failure_recovery import classify_text_failure
from llm_gateway.domain.model import FailureCode, ProviderFailure
from llm_gateway.domain.recovery import RecoveryPlan
from llm_gateway.infrastructure.invocation import PostgresAttemptJournal
from tests.test_completion import invoke
from tests.test_attempt_execution import Harness, SUCCESS


@pytest.mark.parametrize("error", [httpx.ReadTimeout, httpx.WriteTimeout, httpx.TimeoutException,
    httpx.ReadError, httpx.WriteError, httpx.NetworkError, httpx.RemoteProtocolError, httpx.TransportError])
def test_possible_sent_request_without_terminal_evidence_is_uncertain_and_never_replayed(error):
    def handler(request):
        raise error("private upstream detail", request=request)
    result = invoke(handler)
    assert result == ProviderFailure(FailureCode.UNCERTAIN, False)
    assert circuit_observation(result) is None
    flags = classify_text_failure(result)
    assert not flags.retryable and not flags.failover_eligible
    harness = Harness([result, SUCCESS], failover=True)
    assert asyncio.run(harness.run()) is result
    assert harness.events.count(("provider",)) == 1
    assert ("finish", 1, "stop") in harness.events
    response = _response(InvocationReply(uuid4(), datetime.now(timezone.utc), result))
    assert response.status_code == 504 and json.loads(response.body)["error"]["code"] == "uncertain"
    assert "private" not in response.body.decode() and "retry-after" not in response.headers


def test_even_incorrect_retryable_uncertainty_cannot_authorize_recovery():
    result = ProviderFailure(FailureCode.UNCERTAIN, True)
    harness = Harness([result, SUCCESS])
    assert asyncio.run(harness.run()) is result
    assert harness.events.count(("provider",)) == 1
    assert circuit_observation(result) is None


@pytest.mark.parametrize("action", ["retry", "advance"])
def test_uncertain_journal_rejects_recovery_before_database_access(action):
    async def run():
        with pytest.raises(ValueError, match="Uncertain Attempt cannot recover"):
            await PostgresAttemptJournal(None, uuid4()).finished(1,
                ProviderFailure(FailureCode.UNCERTAIN, False), RecoveryPlan(action))
    asyncio.run(run())
