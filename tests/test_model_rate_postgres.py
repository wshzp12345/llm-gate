"""HTTP JSON/SSE -> model RPM -> normal durable execution; synthetic Provider."""

import json
from copy import deepcopy
from types import SimpleNamespace

import httpx
import pytest

from llm_gateway.adapters.model_http import create_development_model_app
from llm_gateway.adapters.provider_transport_registry import ProviderTransportRegistry, project_provider_transports
from llm_gateway.application.active_configuration import LoadedConfiguration
from llm_gateway.application.fingerprint_keys import FingerprintKeys
from llm_gateway.infrastructure.fingerprint_protection import PostgresFingerprintProtection
from llm_gateway.infrastructure.full_service_text_backend import FullServiceTextBackend
from tests.config_fixtures import bundle_submission
from tests.test_configuration import database, run, scalar
from tests.test_configuration_preparation import draft
from tests.test_fingerprint_admission_postgres import prepare
from tests.test_fingerprint_leases import RING, Source
from tests.test_provider_streaming import chunk, wire
from tests.test_completion import envelope
from tests.test_text_attempt_runtime import Harness
from tests.test_text_fingerprints import AUTH, LIMITS


@pytest.mark.postgres
def test_model_default_and_override_share_sync_sse_quota_not_attempts(database):
    async def scenario():
        body = bundle_submission()
        body["bundle"]["providers"]["provider-a"]["rate_limit"].update(qps=100, burst=100)
        body["bundle"]["provider_model_bindings"]["binding-a"]["capabilities"]["streaming"] = True
        other = deepcopy(body["bundle"]["model_aliases"]["general"])
        other["requests_per_minute"] = 2
        body["bundle"]["model_aliases"]["other"] = other
        store, record = await prepare(database, body=body)
        value = draft(body)
        selected = LoadedConfiguration(record.configuration_revision, value.snapshot_digest, value.snapshot_json, value.validation_resources)
        keys = FingerprintKeys(RING, Source(), PostgresFingerprintProtection(store))
        assert await keys.validate_active()
        calls = []

        async def handler(request):
            payload = json.loads(request.content)
            calls.append(payload)
            if len(calls) == 1:
                return httpx.Response(503)
            if payload.get("stream"):
                return httpx.Response(200, content=wire(chunk({"content": "OK"}),
                    chunk(finish="stop", usage={"prompt_tokens": 2, "completion_tokens": 1, "total_tokens": 3}),
                    "[DONE]"), headers={"content-type": "text/event-stream"})
            return httpx.Response(200, json=envelope())

        async def authorize(query):
            return AUTH

        async def health(*args):
            return True

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as upstream:
            registry = ProviderTransportRegistry(factory=lambda plan: upstream)
            await registry.install(selected.revision, project_provider_transports(selected))
            backend = FullServiceTextBackend(store=store, keys=keys, configuration=SimpleNamespace(current=selected),
                credential_source=Harness(), transports=registry, health=health, authorize=authorize,
                trace_id=lambda: "c" * 32, resource_ceilings=LIMITS, draw_jitter=lambda _: 0)
            app = create_development_model_app(backend, enable_streaming=True)
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gateway.test") as client:
                for model, limit in (("general", 5), ("other", 2)):
                    for index in range(limit):
                        stream = index % 2 == 1
                        payload = {"model": model, "messages": [{"role": "user", "content": "test"}], "stream": stream}
                        response = await client.post("/v1/chat/completions", json=payload)
                        assert response.status_code == 200, response.text
                        if stream:
                            assert "data: [DONE]" in response.text
                    before = len(calls)
                    for stream in (False, True):
                        payload["stream"] = stream
                        response = await client.post("/v1/chat/completions", json=payload)
                        assert response.status_code == 429, response.text
                        assert response.json()["error"]["code"] == "rate_limited"
                        assert "x-gateway-call-id" not in response.headers
                        assert "text/event-stream" not in response.headers["content-type"]
                        assert len(calls) == before
                # Callers cannot override model policy in a generation request.
                payload["requests_per_minute"] = 100
                assert (await client.post("/v1/chat/completions", json=payload)).status_code == 400
        assert len(calls) == 8  # Seven requests plus one internal retry.
        assert scalar(database, "SELECT count(*) FROM model_invocation") == 7
        assert scalar(database, "SELECT count(*) FROM model_invocation WHERE state = 'completed'") == 7
    run(scenario())
