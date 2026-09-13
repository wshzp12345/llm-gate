import asyncio
from dataclasses import replace
from datetime import datetime, timezone
from uuid import uuid4

import httpx
import pytest

from llm_gateway.adapters.configuration_json import ParseLimits
from llm_gateway.adapters.model_http import create_development_model_app
from llm_gateway.application.model_api import InvocationReply, ModelInvocationRejected
from llm_gateway.domain.invocation import InvocationPersistenceUnavailable
from llm_gateway.domain.model import Usage
from tests.test_attempt_execution import SUCCESS, FAILURE


PAYLOAD = {"model": "general", "messages": [{"role": "user", "content": "hello"}], "max_completion_tokens": 32}


class Service:
    def __init__(self, result=SUCCESS, error=None):
        self.queries = []
        self.result, self.error = result, error
        self.call_id = uuid4()

    async def invoke(self, query):
        self.queries.append(query)
        if self.error:
            raise self.error
        return InvocationReply(self.call_id, datetime(2026, 1, 1, tzinfo=timezone.utc), self.result)


def request(service=None, *, limits=None, **kwargs):
    async def run():
        app = create_development_model_app(service, **({"limits": limits} if limits else {}))
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gateway") as client:
            return await client.post("/v1/chat/completions", **(kwargs or {"json": PAYLOAD}))
    return asyncio.run(run())


def test_request_translation_and_compatible_response_identity():
    service = Service(replace(SUCCESS, usage=Usage(10, 6, 4, 2)))
    response = request(service, json=PAYLOAD, headers={"Authorization": "ignored in dev", "X-Request-Id": "caller-controlled"})
    assert response.status_code == 200
    query = service.queries[0]
    assert query.requested_model == "general" and query.max_output_tokens == 32
    assert query.messages[0].text == "hello"
    body = response.json()
    assert body["model"] == "model" and body["gateway"]["requested_model"] == "general"
    assert body["usage"] == {"prompt_tokens": 10, "completion_tokens": 6, "total_tokens": 16,
                             "prompt_tokens_details": {"cached_tokens": 4}, "completion_tokens_details": {"reasoning_tokens": 2}}
    assert body["id"] == "chatcmpl-" + str(service.call_id)
    assert response.headers["X-Gateway-Call-Id"] == str(service.call_id)
    assert response.headers["X-Request-Id"] not in {"caller-controlled", str(service.call_id)}
    assert response.headers["Cache-Control"] == "no-store"


@pytest.mark.parametrize("usage,state", [(Usage(), "unavailable"), (Usage(provider_reported_total=99), "partial"), (Usage(3), "partial")])
def test_ineligible_standard_usage_is_omitted_without_numeric_total_leak(usage, state):
    body = request(Service(replace(SUCCESS, usage=usage))).json()
    assert "usage" not in body
    assert body["gateway"]["usage"] == {"state": state}


def test_deprecated_token_alias_and_store_false_are_preserved_for_evidence():
    service = Service()
    payload = dict(PAYLOAD, max_tokens=32, store=False)
    del payload["max_completion_tokens"]
    assert request(service, json=payload).status_code == 200
    assert service.queries[0].deprecated_max_tokens and service.queries[0].store_false_requested


@pytest.mark.parametrize("changes,status", [
    ({"max_tokens": 32}, 400), ({"max_completion_tokens": None}, 400), ({"max_completion_tokens": True}, 400),
    ({"n": 0}, 400), ({"n": 2}, 422), ({"stream": True}, 422), ({"stream": "true"}, 400),
    ({"store": True}, 422), ({"unknown": 1}, 400), ({"tools": []}, 422), ({"metadata": {}}, 422),
    ({"temperature": 3}, 400), ({"messages": [{"role": "tool", "content": "hello"}]}, 422),
    ({"messages": [{"role": "user", "content": [{"type": "image_url"}]}]}, 422),
])
def test_rejected_inputs_do_not_enter_application(changes, status):
    service = Service()
    response = request(service, json=PAYLOAD | changes)
    assert response.status_code == status and not service.queries
    assert "X-Gateway-Call-Id" not in response.headers


@pytest.mark.parametrize("body", [b'{"model":"a","model":"b"}', b'\xef\xbb\xbf{}', b'{"x":NaN}', b'\xff'])
def test_strict_json_rejects_ambiguous_content(body):
    service = Service()
    assert request(service, content=body, headers={"Content-Type": "application/json"}).status_code == 400
    assert not service.queries


def test_body_and_content_byte_limits_are_413():
    service = Service()
    assert request(service, limits=ParseLimits(16, 8, 100, 100, 16), json=PAYLOAD).status_code == 413
    assert request(service, limits=ParseLimits(1000, 8, 100, 100, 4), json=PAYLOAD).status_code == 413
    assert not service.queries


def test_unwired_backend_and_transport_negotiation():
    assert request().json()["error"]["code"] == "gateway_not_ready"
    assert request(Service(), json=PAYLOAD, headers={"Accept": "text/plain"}).status_code == 406
    assert request(Service(), json=PAYLOAD, headers={"Idempotency-Key": "key"}).status_code == 422


@pytest.mark.parametrize("error,code,status", [
    (InvocationPersistenceUnavailable(), "persistence_unavailable", 503),
    (ModelInvocationRejected("no_route"), "no_route", 503),
    (RuntimeError("private credential"), "internal", 500),
])
def test_failures_are_safe_and_do_not_disclose_usage(error, code, status):
    response = request(Service(error=error))
    assert response.status_code == status and response.json()["error"]["code"] == code
    assert "private" not in response.text and "usage" not in response.text
    assert "X-Gateway-Call-Id" not in response.headers


def test_committed_failure_has_call_identity_but_no_usage_or_retry_details():
    response = request(Service(FAILURE))
    assert response.status_code == 503 and "X-Gateway-Call-Id" in response.headers
    assert set(response.json()) == {"error"}
    assert "usage" not in response.text and "retry" not in response.text
