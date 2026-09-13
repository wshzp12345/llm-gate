import asyncio
import json
from uuid import uuid4

import httpx
import pytest

from llm_gateway.adapters.configuration_http import create_development_management_app
from llm_gateway.adapters.active_configuration import CanonicalSnapshotDecoder
from llm_gateway.application.active_configuration import ActiveConfigurationLoader
from llm_gateway.application.configuration import ConfigurationCommands
from llm_gateway.infrastructure.configuration import PostgresConfigurationUnitOfWork
from tests.config_fixtures import bundle_submission
from tests.test_configuration import database, scalar
from tests.test_configuration_dto import LIMITS
from tests.test_configuration_http import local_validator


@pytest.mark.postgres
def test_http_validate_create_publish_and_cross_format_replay(database):
    async def scenario():
        uow = PostgresConfigurationUnitOfWork(database)
        loader = ActiveConfigurationLoader(uow, CanonicalSnapshotDecoder(LIMITS), local_validator())
        assert await loader.load(timeout_seconds=5) is None
        app = create_development_management_app(ConfigurationCommands(uow), uow, LIMITS,
                                                command_timeout_seconds=5, validator=local_validator())
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://management.test") as client:
            body = bundle_submission()
            validated = await client.post("/gateway/v1/config/validate", json=body)
            assert validated.status_code == 200 and validated.json()["valid"]
            assert scalar(database, "SELECT count(*) FROM config_revision") == 0
            assert scalar(database, "SELECT count(*) FROM config_command_index") == 0
            command = str(uuid4())
            created = await client.post("/gateway/v1/config/revisions", json=body, headers={"X-Gateway-Command-Id": command})
            assert created.status_code == 201
            revision = created.json()["revision"]
            assert created.headers["location"].endswith("/" + revision["revision"])
            assert revision["base_revision"] is None
            published = await client.post(f"/gateway/v1/config/revisions/{revision['revision']}/publish",
                                          json={"expected_active_revision": "0", "candidate_snapshot_digest": revision["snapshot_digest"], "description": "publish"},
                                          headers={"X-Gateway-Command-Id": str(uuid4())})
            assert published.status_code == 200
            loaded = await loader.load(timeout_seconds=5)
            assert loaded.revision == revision["revision"]
            assert loaded.snapshot_digest == revision["snapshot_digest"]
            active = await client.get("/gateway/v1/config/active")
            assert active.json() == published.json()
            # YAML shares the semantic command identity; replay precedes the now-invalid base.
            replay = await client.post("/gateway/v1/config/revisions", content=json.dumps(body["bundle"]),
                                      headers={"X-Gateway-Command-Id": command, "Content-Type": "application/yaml"})
            assert replay.status_code == 201
            assert replay.json() == created.json()
            assert scalar(database, "SELECT count(*) FROM config_revision") == 1
            change = {"kind": "change_set", "change_set": {"schema_version": "gateway.config-change-set/v1", "base_revision": revision["revision"], "operations": []}}
            same = await client.post("/gateway/v1/config/revisions", json=change, headers={"X-Gateway-Command-Id": str(uuid4())})
            assert same.status_code == 201
            assert same.json()["revision"]["snapshot_digest"] == revision["snapshot_digest"]
            assert same.json()["revision"]["revision"] != revision["revision"]
    asyncio.run(scenario(), loop_factory=asyncio.SelectorEventLoop)


@pytest.mark.postgres
def test_export_and_rollback_require_explicit_publication(database):
    async def scenario():
        uow = PostgresConfigurationUnitOfWork(database)
        app = create_development_management_app(ConfigurationCommands(uow), uow, LIMITS,
                                                command_timeout_seconds=5, validator=local_validator())
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://management.test") as client:
            async def post(path, body, command=None):
                return await client.post("/gateway/v1/config/" + path, json=body,
                                         headers={"X-Gateway-Command-Id": command or str(uuid4())})

            async def publish(revision, active):
                response = await post(f"revisions/{revision['revision']}/publish", {
                    "expected_active_revision": active, "candidate_snapshot_digest": revision["snapshot_digest"],
                    "description": "publish candidate"})
                assert response.status_code == 200, response.text

            body = bundle_submission()
            body["bundle"]["metadata"] = {"description": "original creation"}
            first = (await post("revisions", body)).json()["revision"]
            await publish(first, "0")
            body["bundle"]["base_revision"] = first["revision"]
            body["bundle"]["metadata"] = {"description": "second creation"}
            second = (await post("revisions", body)).json()["revision"]
            await publish(second, first["revision"])
            exported = await client.get(f"/gateway/v1/config/revisions/{first['revision']}/export",
                                        headers={"Accept": "application/yaml"})
            assert exported.status_code == 200
            assert "original creation" in exported.text and "second creation" not in exported.text
            assert exported.headers["X-Gateway-Snapshot-Digest"] == first["snapshot_digest"]
            request = {"expected_active_revision": second["revision"], "description": "restore original"}
            assert (await post(f"revisions/{second['revision']}/rollback", request)).status_code == 409
            command = str(uuid4())
            path = f"revisions/{first['revision']}/rollback"
            restored = await post(path, request, command)
            assert restored.status_code == 201, restored.text
            candidate = restored.json()["revision"]
            assert candidate["state"] == "candidate"
            assert candidate["rollback_of"] == first["revision"]
            assert candidate["base_revision"] == second["revision"]
            assert candidate["snapshot_digest"] == first["snapshot_digest"]
            active = (await client.get("/gateway/v1/config/active")).json()["active"]
            assert active["revision"] == second["revision"]
            await publish(candidate, second["revision"])
            replay = await post(path, request, command)
            assert replay.status_code == 201 and replay.json() == restored.json()
            changed = await post(path, {**request, "description": "different command"}, command)
            assert changed.status_code == 409
            assert scalar(database, "SELECT count(*) FROM config_revision") == 3
            active = (await client.get("/gateway/v1/config/active")).json()["active"]
            assert active["revision"] == candidate["revision"]
    asyncio.run(scenario(), loop_factory=asyncio.SelectorEventLoop)
