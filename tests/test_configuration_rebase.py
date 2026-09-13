import asyncio
from copy import deepcopy
from uuid import uuid4

import httpx
import pytest

from llm_gateway.adapters.configuration_http import create_development_management_app
from llm_gateway.application.configuration import ConfigurationCommands
from llm_gateway.domain.configuration import ConfigurationDiagnostic
from llm_gateway.infrastructure.configuration import PostgresConfigurationUnitOfWork
from tests.config_fixtures import bundle_submission
from tests.test_configuration import database, scalar
from tests.test_configuration_dto import LIMITS
from tests.test_configuration_http import access, local_validator


@pytest.mark.parametrize("revision,body", [
    ("01", {"expected_active_revision": "1", "description": "rebase"}),
    ("1", {"expected_active_revision": "0", "description": "rebase"}),
    ("1", {"expected_active_revision": "1"}),
])
def test_rebase_protocol_failures_precede_commands(revision, body):
    response = access("POST", f"/gateway/v1/config/revisions/{revision}/rebase", json=body,
                      validator=local_validator(), headers={"X-Gateway-Command-Id": str(uuid4())})
    assert response.status_code == 400


@pytest.mark.postgres
@pytest.mark.parametrize("mode", ["disjoint", "conflict", "validation", "initial_convergence"])
def test_rebase_transaction_and_replay(database, mode):
    async def scenario():
        uow = PostgresConfigurationUnitOfWork(database)
        reject = False
        ordinary_validator = local_validator()

        class Validator:
            async def validate(self, candidate):
                if reject:
                    return (ConfigurationDiagnostic("reference_not_found", "/bundle/providers/provider-a"),)
                return await ordinary_validator.validate(candidate)

        app = create_development_management_app(ConfigurationCommands(uow), uow, LIMITS,
                                                command_timeout_seconds=5, validator=Validator())
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://management.test") as client:
            async def post(path, body, command=None):
                return await client.post("/gateway/v1/config/" + path, json=body,
                                         headers={"X-Gateway-Command-Id": command or str(uuid4())})

            async def create(body):
                response = await post("revisions", body)
                assert response.status_code == 201, response.text
                return response.json()["revision"]

            async def publish(revision, active):
                response = await post(f"revisions/{revision['revision']}/publish", {
                    "expected_active_revision": active, "candidate_snapshot_digest": revision["snapshot_digest"],
                    "description": "publish"})
                assert response.status_code == 200, response.text

            body = bundle_submission()
            first = await create(body)
            if mode == "initial_convergence":
                stale = await create(body)
                await publish(first, "0")
                winner = first
            else:
                await publish(first, "0")
                body["bundle"]["base_revision"] = first["revision"]
                proposed = deepcopy(body)
                proposed["bundle"]["pricing_tables"]["price-a"]["rates"]["input"] = "3"
                stale = await create(proposed)
                if mode == "conflict":
                    # Distinct fields on the same resource must not merge.
                    body["bundle"]["pricing_tables"]["price-a"]["rates"]["output"] = "4"
                else:
                    body["bundle"]["providers"]["provider-a"]["rate_limit"]["max_concurrency"] = 3
                winner = await create(body)
                await publish(winner, first["revision"])
            reject = mode == "validation"
            request = {"expected_active_revision": winner["revision"], "description": "rebase stale"}
            command = str(uuid4())
            count = scalar(database, "SELECT count(*) FROM config_revision")
            path = f"revisions/{stale['revision']}/rebase"
            response = await post(path, request, command)
            expected = 409 if mode == "conflict" else 422 if mode == "validation" else 201
            assert response.status_code == expected, response.text
            active = (await client.get("/gateway/v1/config/active")).json()["active"]
            assert active["revision"] == winner["revision"]
            if expected == 201:
                candidate = response.json()["revision"]
                assert candidate["state"] == "candidate" and candidate["base_revision"] == winner["revision"]
                assert candidate["rollback_of"] is None
                assert response.headers["location"].endswith("/" + candidate["revision"])
                if mode == "initial_convergence":
                    assert candidate["snapshot_digest"] == winner["snapshot_digest"]
                else:
                    async with uow.transaction(5) as tx:
                        import json
                        merged = json.loads(await tx.get_snapshot(candidate["revision"]))
                    assert merged["pricing_tables"]["price-a"]["rates"]["input"] == "3"
                    assert merged["providers"]["provider-a"]["rate_limit"]["max_concurrency"] == 3
                await publish(candidate, winner["revision"])
            else:
                assert scalar(database, "SELECT count(*) FROM config_revision") == count
                assert ("resource_changed" if mode == "conflict" else "reference_not_found") in response.text
            reject = False
            replay = await post(path, request, command)
            assert replay.status_code == expected and replay.json() == response.json()
            assert scalar(database, "SELECT count(*) FROM config_revision") == count + (expected == 201)
            async with uow.transaction(5) as tx:
                assert (await tx.get_revision(stale["revision"])).state.value == "stale"
    asyncio.run(scenario(), loop_factory=asyncio.SelectorEventLoop)
