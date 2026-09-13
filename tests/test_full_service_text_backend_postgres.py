import json
import asyncio
from types import SimpleNamespace

import httpx
import psycopg
import pytest

from llm_gateway.adapters.model_http import create_development_model_app
from llm_gateway.adapters.provider_transport_registry import ProviderTransportRegistry, project_provider_transports
from llm_gateway.application.fingerprint_keys import FingerprintKeys
from llm_gateway.infrastructure.fingerprint_protection import PostgresFingerprintProtection
from llm_gateway.infrastructure.full_service_text_backend import FullServiceTextBackend
from tests.test_configuration import database, run, scalar
from tests.test_fingerprint_admission_postgres import prepare
from tests.test_fingerprint_leases import RING, Source
from tests.test_text_attempt_runtime import Harness
from tests.test_text_fingerprints import AUTH, LIMITS, snapshot
from tests.test_completion import envelope


@pytest.mark.postgres
@pytest.mark.parametrize("outcome", ["success", "refusal", "uncertain", "cancel", "late_cancel", "deadline"])
def test_http_through_real_fingerprints_admission_gates_and_settlement(database, outcome):
    refusal, uncertain = outcome == "refusal", outcome == "uncertain"
    cancelled, expired = outcome in {"cancel", "late_cancel"}, outcome == "deadline"
    async def scenario():
        store, record = await prepare(database)
        selected = snapshot(revision=record.configuration_revision)
        keys = FingerprintKeys(RING, Source(), PostgresFingerprintProtection(store))
        assert await keys.validate_active()
        credentials = Harness()
        calls = []
        entered, aborted = asyncio.Event(), asyncio.Event()

        async def handler(request):
            assert scalar(database, "SELECT count(*) FROM provider_attempt") == 1
            assert scalar(database, "SELECT count(*) FROM invocation_fingerprints") == 1
            assert scalar(database, "SELECT count(*) FROM invocation_safety_policy") == 1
            assert keys.in_use == 0
            calls.append(request)
            if cancelled or expired:
                entered.set()
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    if outcome != "late_cancel":
                        raise
                    return httpx.Response(200, json=envelope(usage={"prompt_tokens": 10, "completion_tokens": 6}))
                finally:
                    aborted.set()
            if uncertain:
                raise httpx.ReadTimeout("private upstream detail", request=request)
            body = envelope()
            if refusal:
                body["choices"][0]["message"] = {"role": "assistant", "content": None, "refusal": "No."}
                body["choices"][0]["finish_reason"] = "content_filter"
            return httpx.Response(200, json=body)

        async def authorize(query):
            return AUTH  # Explicit test authority; not installed in a live app.

        async def health(admission, config, binding):
            return True  # Isolated external observation fixture.

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False) as upstream:
            registry = ProviderTransportRegistry(factory=lambda plan: upstream)
            await registry.install(selected.revision, project_provider_transports(selected))
            backend = FullServiceTextBackend(store=store, keys=keys, configuration=SimpleNamespace(current=selected),
                credential_source=credentials, transports=registry, health=health, authorize=authorize,
                trace_id=lambda: "b" * 32, resource_ceilings=LIMITS | ({"max_invocation_seconds": 1} if expired else {}),
                draw_jitter=lambda upper: 0)
            app = create_development_model_app(backend)
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gateway.test") as caller:
                request = asyncio.create_task(caller.post("/v1/chat/completions", json={"model": "general",
                    "messages": [{"role": "user", "content": "private caller text"}]}))
                try:
                    if cancelled:
                        await asyncio.wait_for(entered.wait(), 2)
                        request.cancel()
                        with pytest.raises(asyncio.CancelledError):
                            await request
                        response = None
                    else:
                        response = await request
                finally:
                    if not request.done():
                        request.cancel()
                    await asyncio.gather(request, return_exceptions=True)
            if not cancelled:
                assert response.status_code == (504 if uncertain or expired else 200), response.text
            assert len(calls) == 1 and backend.in_use == keys.in_use == 0
            assert all(not lease._material for lease in credentials.leases)
            assert scalar(database, "SELECT state FROM model_invocation") == ("cancelled" if cancelled else "failed" if expired else "uncertain" if uncertain else "completed")
            assert scalar(database, "SELECT safety_refused FROM invocation_settlement") is refusal
            assert scalar(database, "SELECT count(*) FROM invocation_cost_summary") == 1
            if uncertain:
                assert response.json()["error"]["code"] == "uncertain"
                assert scalar(database, "SELECT outcome FROM provider_attempt_outcome") == "uncertain"
                assert scalar(database, "SELECT error_code FROM invocation_settlement") == "uncertain"
                assert scalar(database, "SELECT recovery_action FROM provider_attempt_outcome") == "stop"
            if refusal:
                assert response.json()["choices"][0]["message"]["refusal"] == "No."
                assert scalar(database, "SELECT count(*) FROM safety_refusal") == 1
            if cancelled or expired:
                assert entered.is_set() and aborted.is_set()
                assert scalar(database, "SELECT outcome FROM provider_attempt_outcome") == "uncertain"
                assert scalar(database, "SELECT error_code FROM invocation_settlement") == ("cancelled" if cancelled else "deadline_exceeded")
                assert scalar(database, "SELECT count(*) FROM invocation_local_cancellation") == int(cancelled)
                assert scalar(database, "SELECT count(*) FROM late_provider_outcome") == int(outcome == "late_cancel")
                if expired:
                    assert response.json()["error"]["code"] == "deadline_exceeded"
            with psycopg.connect(database) as connection:
                stored = connection.execute("SELECT to_jsonb(i) FROM model_invocation i").fetchone()[0]
                assert "private caller text" not in json.dumps(stored, default=str)
    run(scenario())
