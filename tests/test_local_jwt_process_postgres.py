import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

import httpx
import psycopg
import pytest
from cryptography.hazmat.primitives import serialization

from llm_gateway.adapters.provider_transport_registry import ProviderTransportRegistry, project_provider_transports
from llm_gateway.infrastructure.full_service_text_process import create_local_jwt_text_process, create_owned_text_app
from tests.test_configuration import database, run, scalar
from tests.test_fingerprint_admission_postgres import prepare
from tests.test_fingerprint_leases import RING, Source
from tests.test_text_attempt_runtime import Harness
from tests.test_text_fingerprints import LIMITS, snapshot
from tests.test_completion import envelope
from tests.test_local_jwt import signing_key, token, CLAIMS, file_config


@pytest.mark.postgres
def test_local_jwt_factory_through_http_fenced_admission_and_persisted_identity(database, tmp_path, signing_key):
    path = tmp_path / "public.pem"
    path.write_bytes(signing_key.public_key().public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo))
    async def scenario():
        _, record = await prepare(database)
        selected = snapshot(revision=record.configuration_revision)
        calls = []
        async def handler(request):
            calls.append(request)
            return httpx.Response(200, json=envelope())
        async def health(admission, configuration, binding):
            return True  # Isolated Provider observation fixture.
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False) as upstream:
            registry = ProviderTransportRegistry(factory=lambda plan: upstream)
            await registry.install(selected.revision, project_provider_transports(selected))
            process = create_local_jwt_text_process(database, authorization=file_config(path),
                fingerprint_ring=RING, fingerprint_source=Source(), configuration=SimpleNamespace(current=selected),
                credential_source=Harness(), transports=registry, health=health, trace_id=lambda: "b"*32,
                resource_ceilings=LIMITS, draw_jitter=lambda upper: 0)
            app = create_owned_text_app(process)
            async with app.router.lifespan_context(app):
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gateway.test") as caller:
                    body = {"model": "general", "messages": [{"role": "user", "content": "hello"}]}
                    claims = CLAIMS | {"exp": int(datetime.now(timezone.utc).timestamp()) + 120}
                    response = await caller.post("/v1/chat/completions", json=body)
                    assert response.status_code == 401 and response.headers["www-authenticate"] == "Bearer"
                    response = await caller.post("/v1/chat/completions", json=body,
                        headers={"Authorization": "Bearer " + token(signing_key, claims | {"scope": []})})
                    assert response.status_code == 403
                    assert scalar(database, "SELECT count(*) FROM model_invocation") == 0 and calls == []
                    async def call(identity):
                        return await caller.post("/v1/chat/completions", json=body,
                            headers={"Authorization": "Bearer " + token(signing_key, claims | {"sub": identity, "tenant_id": identity})})
                    replies = await asyncio.gather(call("alice"), call("bob"))
                    assert all(response.status_code == 200 for response in replies), [r.text for r in replies]
            assert len(calls) == 2 and process.in_use == 0
            with psycopg.connect(database) as connection:
                rows = connection.execute("SELECT subject,tenant_id,scopes,authentication_method,state FROM model_invocation ORDER BY subject").fetchall()
                assert rows == [(name, name, ["gateway.model.invoke"], "local_jwt", "completed") for name in ("alice", "bob")]
                assert connection.execute("SELECT count(*) FROM model_invocation WHERE execution_owner_id IS NOT NULL").fetchone()[0] == 2
    run(scenario())
