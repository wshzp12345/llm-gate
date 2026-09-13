import asyncio
from datetime import datetime, timezone
from uuid import uuid4

import httpx
import pytest

from llm_gateway.adapters.model_http import create_development_model_app
from llm_gateway.adapters.model_request_authorization import ModelRequestAuthorization
from llm_gateway.application.authorization import Unauthorized, AuthorizationUnavailable
from llm_gateway.application.model_api import InvocationReply
from tests.test_attempt_execution import SUCCESS
from tests.test_local_jwt import signing_key, verifier, token, CLAIMS


@pytest.mark.parametrize("failure", ["missing", "duplicate", "combined", "wrong_signature", "scope", "invalid_scope"])
def test_http_authentication_precedes_body_parsing_and_backend(signing_key, failure):
    async def scenario():
        calls = []
        binding = ModelRequestAuthorization(verifier(signing_key), authentication_method="local_jwt")
        class Service:
            async def invoke(self, query):
                calls.append(query)
                raise AssertionError("Rejected authentication reached execution")
        app = create_development_model_app(Service(), request_authorization=binding)
        credential = token(signing_key, CLAIMS | ({"scope": []} if failure == "scope" else
                                                 {"scope": "dev:*"} if failure == "invalid_scope" else {}))
        headers = [("Authorization", "Bearer " + credential)]
        if failure == "missing":
            headers = []
        elif failure == "duplicate":
            headers += [("authorization", "Bearer " + credential)]
        elif failure == "combined":
            headers = [("Authorization", "Bearer " + credential + ",Bearer other")]
        elif failure == "wrong_signature":
            first, second, signature = credential.split(".")
            headers = [("Authorization", "Bearer " + first + "." + second + "." + signature[:-4] + "AAAA")]
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gateway.test") as caller:
            response = await caller.post("/v1/chat/completions", content=b"not-json", headers=headers)
            assert response.status_code == (403 if failure == "scope" else 401)
            if response.status_code == 401:
                assert response.headers.get_list("www-authenticate") == ["Bearer"]
            else:
                assert "www-authenticate" not in response.headers
            assert response.headers["cache-control"] == "no-store"
            assert "retry-after" not in response.headers and "x-gateway-call-id" not in response.headers
            assert credential not in response.text and calls == []
        with pytest.raises(Unauthorized):
            await binding.authorize(None)
    asyncio.run(scenario())


def test_concurrent_requests_keep_their_own_verified_identity_and_reset_after_response(signing_key):
    async def scenario():
        binding = ModelRequestAuthorization(verifier(signing_key), authentication_method="local_jwt")
        entered, all_entered, observed = 0, asyncio.Event(), []
        class Service:
            async def invoke(self, query):
                nonlocal entered
                first = await binding.authorize(query)
                entered += 1
                if entered == 2:
                    all_entered.set()
                await all_entered.wait()
                second = await binding.authorize(query)
                assert first is second
                observed.append((first.tenant_id, first.subject))
                return InvocationReply(uuid4(), datetime.now(timezone.utc), SUCCESS)
        app = create_development_model_app(Service(), request_authorization=binding)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gateway.test") as caller:
            async def call(identity):
                credential = token(signing_key, CLAIMS | {"tenant_id": identity, "sub": identity})
                return await caller.post("/v1/chat/completions", headers={"Authorization": "Bearer " + credential},
                    json={"model": "general", "messages": [{"role": "user", "content": "hello"}]})
            replies = await asyncio.wait_for(asyncio.gather(call("alice"), call("bob")), 3)
            assert all(reply.status_code == 200 for reply in replies)
        assert set(observed) == {("alice", "alice"), ("bob", "bob")}
        with pytest.raises(Unauthorized):
            await binding.authorize(None)
    asyncio.run(scenario())


def test_request_context_is_released_even_on_cancellation(signing_key):
    async def scenario():
        binding = ModelRequestAuthorization(verifier(signing_key), authentication_method="local_jwt")
        with pytest.raises(asyncio.CancelledError):
            async with binding.context([(b"authorization", ("Bearer " + token(signing_key)).encode())]):
                assert (await binding.authorize(None)).subject == "principal"
                raise asyncio.CancelledError()
        with pytest.raises(Unauthorized):
            await binding.authorize(None)
    asyncio.run(scenario())


def test_unexpected_adapter_failure_is_safe_unavailable_not_unauthorized():
    class Adapter:
        async def authenticate(self, credential):
            raise RuntimeError("private upstream content")
    async def scenario():
        binding = ModelRequestAuthorization(Adapter(), authentication_method="online_authority")
        app = create_development_model_app(request_authorization=binding)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gateway.test") as caller:
            response = await caller.post("/v1/chat/completions", headers={"Authorization": "Bearer abc"})
            assert response.status_code == 503 and response.json()["error"]["code"] == "authorization_unavailable"
            assert "private" not in response.text and "www-authenticate" not in response.headers
            assert "retry-after" not in response.headers
    asyncio.run(scenario())
