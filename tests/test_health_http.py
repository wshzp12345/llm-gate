import asyncio
from uuid import uuid4

import httpx
import pytest

from llm_gateway.adapters.configuration_http import create_development_management_app
from tests.test_configuration_dto import LIMITS
from tests.test_configuration_http import BODY, REVISION, Commands, UnitOfWork


@pytest.mark.parametrize("observation,expected", [(True, 200), (False, 503), (None, 503), (1, 503), ("ready", 503)])
def test_readiness_is_status_only_and_reads_no_database(observation, expected):
    async def scenario():
        uow = UnitOfWork()
        app = create_development_management_app(Commands(), uow, LIMITS,
                                                command_timeout_seconds=3, readiness=lambda: observation)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://internal.test") as client:
            response = await client.get("/readyz", headers={"Authorization": "Bearer invalid", "Accept": "text/plain"})
            assert response.status_code == expected
            assert response.content == b""
            assert response.headers["cache-control"] == "no-store"
            assert uow.reads == 0
    asyncio.run(scenario())


def test_default_non_ready_does_not_block_liveness_or_configuration_recovery():
    async def scenario():
        uow, commands = UnitOfWork(), Commands()
        app = create_development_management_app(commands, uow, LIMITS, command_timeout_seconds=3)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://internal.test") as client:
            assert (await client.get("/readyz")).status_code == 503
            live = await client.get("/healthz")
            assert live.status_code == 200 and live.content == b""
            assert (await client.get("/gateway/v1/config/active")).status_code == 200
            published = await client.post(f"/gateway/v1/config/revisions/{REVISION.revision}/publish",
                                          json=BODY, headers={"X-Gateway-Command-Id": str(uuid4())})
            assert published.status_code == 200 and len(commands.calls) == 1
    asyncio.run(scenario())


def test_local_readiness_fault_is_not_disclosed_and_liveness_is_independent():
    def broken():
        raise RuntimeError("sensitive dependency detail")

    async def scenario():
        app = create_development_management_app(Commands(), UnitOfWork(), LIMITS,
                                                command_timeout_seconds=3, readiness=broken)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://internal.test") as client:
            response = await client.get("/readyz")
            assert response.status_code == 503 and response.content == b""
            assert (await client.get("/healthz")).status_code == 200
    asyncio.run(scenario())


def test_readiness_observes_recovery_and_loss_without_storing_probe_results():
    async def scenario():
        current = False
        app = create_development_management_app(Commands(), UnitOfWork(), LIMITS,
                                                command_timeout_seconds=3, readiness=lambda: current)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://internal.test") as client:
            assert (await client.get("/readyz")).status_code == 503
            current = True
            assert (await client.get("/readyz")).status_code == 200
            current = False
            assert (await client.get("/readyz")).status_code == 503
    asyncio.run(scenario())
