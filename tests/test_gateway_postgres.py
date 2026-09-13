import asyncio
import json
from pathlib import Path
from uuid import uuid4

import httpx
import pytest

from llm_gateway.infrastructure.gateway import Gateway
from llm_gateway.infrastructure.gateway_bootstrap import load_bootstrap
from tests.test_gateway_bootstrap import bootstrap, write
from tests.test_configuration import database, run, scalar
from tests.config_fixtures import bundle_submission
from tests.test_completion import envelope


@pytest.mark.postgres
def test_empty_start_api_publication_execution_and_restart(database, tmp_path):
    body = bootstrap(tmp_path)
    Path(body["fingerprint_keys"][0]["file"]).write_bytes(b"f" * 32)
    Path(body["provider_secret_files"]["deepseek-api-key"]).write_text(json.dumps({
        "secret_version": str(uuid4()), "value": "test-only", "revoked": False,
        "valid_until": "9999-12-31T23:59:59Z"}))
    config = load_bootstrap(write(tmp_path, body))

    async def scenario():
        calls = []
        entered, release = asyncio.Event(), asyncio.Event()
        async def handler(request):
            calls.append(request)
            if len(calls) == 2:
                entered.set()
                await release.wait()
            return httpx.Response(200, json=envelope())
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as upstream:
            upstream.stop_acquiring = lambda: None
            gateway = Gateway(config, database)
            gateway.transports._factory = lambda plan: upstream
            async with gateway.hold():
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway.management_app),
                    base_url="http://management.test") as control, httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=gateway.model_app), base_url="http://model.test") as model:
                    assert (await control.get("/healthz")).status_code == 200
                    assert (await control.get("/readyz")).status_code == 503
                    assert (await model.get("/healthz")).status_code == 404
                    assert (await control.post("/v1/chat/completions", json={})).status_code == 404
                    query = {"model": "general", "messages": [{"role": "user", "content": "Hello"}]}
                    assert (await model.post("/v1/chat/completions", json=query)).status_code == 503
                    submission = bundle_submission()
                    submission["bundle"]["providers"]["provider-a"]["credential"]["secret_ref"] = "deepseek-api-key"
                    created = await control.post("/gateway/v1/config/revisions", json=submission,
                        headers={"X-Gateway-Command-Id": str(uuid4())})
                    assert created.status_code == 201, created.text
                    revision = created.json()["revision"]
                    assert not gateway.ready and not calls
                    response = await control.post(f"/gateway/v1/config/revisions/{revision['revision']}/publish",
                        json={"expected_active_revision": "0", "candidate_snapshot_digest": revision["snapshot_digest"],
                              "description": "integration"}, headers={"X-Gateway-Command-Id": str(uuid4())})
                    assert response.status_code == 200, response.text
                    async with asyncio.timeout(5):
                        while not gateway.ready:
                            await asyncio.sleep(.02)
                    assert not calls  # Publication and readiness made no Provider request.
                    response = await model.post("/v1/chat/completions", json=query,
                        headers=[("Authorization", "invalid"), ("Authorization", "also invalid")])
                    assert response.status_code == 200, response.text
                    assert len(calls) == 1
                    assert scalar(database, "SELECT subject FROM model_invocation") == "dev"
                    assert scalar(database, "SELECT state FROM model_invocation") == "completed"
                    created_prompt = await control.post("/gateway/v1/prompts", json={"messages": [{"role": "user", "content": "你好 {{name}}"}]})
                    assert created_prompt.status_code == 201, created_prompt.text
                    prompt = created_prompt.json()
                    prompt_query = {"model": "general", "prompt": {"asset_id": prompt["asset_id"],
                        "version_id": prompt["version_id"], "variables": {"name": "世界"}}}
                    assert (await model.post("/v1/chat/completions", json=prompt_query)).status_code == 404
                    assert len(calls) == 1
                    published_prompt = await control.post(f"/gateway/v1/prompts/{prompt['asset_id']}/publish",
                        json={"version_id": prompt["version_id"], "expected_generation": 0})
                    assert published_prompt.status_code == 200, published_prompt.text
                    task = asyncio.create_task(model.post("/v1/chat/completions", json=prompt_query))
                    try:
                        await asyncio.wait_for(entered.wait(), 5)
                        replacement = await control.post(f"/gateway/v1/prompts/{prompt['asset_id']}/versions",
                            json={"messages": [{"role": "user", "content": "Different instructions"}]})
                        assert replacement.status_code == 201
                        republished = await control.post(f"/gateway/v1/prompts/{prompt['asset_id']}/publish",
                            json={"version_id": replacement.json()["version_id"], "expected_generation": 1})
                        assert republished.status_code == 200
                    finally:
                        release.set()
                        response = await task
                    assert response.status_code == 200, response.text
                    assert response.json()["gateway"]["prompt"]["version_id"] == prompt["version_id"]
                    assert json.loads(calls[-1].content)["messages"] == [{"role": "user", "content": "你好 世界"}]
                    assert "prompt" not in json.loads(calls[-1].content)
                    assert str(scalar(database, "SELECT version_id FROM invocation_prompt")) == prompt["version_id"]
                    assert scalar(database, "SELECT count(*) FROM invocation_fingerprints WHERE request_profile='gateway.request-fingerprint/prompt-text-v1'") == 1
                    prompt_query["prompt"]["variables"] = {}
                    assert (await model.post("/v1/chat/completions", json=prompt_query)).status_code == 422
                    assert len(calls) == 2
                    assert scalar(database, "SELECT count(*) FROM model_invocation") == 2
            assert not gateway.ready
            # Restart reuses protected key version, does not replay completed work.
            again = Gateway(config, database)
            async with again.hold():
                async with asyncio.timeout(5):
                    while not again.ready:
                        await asyncio.sleep(.02)
                assert again.process.recovered_count == 0 and len(calls) == 2
    run(scenario())
