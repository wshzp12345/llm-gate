import asyncio
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import httpx
import psycopg
import pytest
from fastapi import FastAPI

from llm_gateway.adapters.prompt_http import register_prompt_routes
from llm_gateway.application.authorization import Forbidden, Unauthorized
from llm_gateway.application.prompts import Prompts, PromptNotFound, PromptConflict
from llm_gateway.domain.model import Message
from llm_gateway.infrastructure.gateway import DEV_AUTHORIZATION
from llm_gateway.infrastructure.prompts import PostgresPromptUnitOfWork
from tests.test_configuration import database, run


TEXT = (Message("user", "你好 {{name}}"),)


@pytest.mark.postgres
def test_version_publication_pinning_and_tenant_isolation(database):
    async def scenario():
        service = Prompts(PostgresPromptUnitOfWork(database))
        first = await service.create(DEV_AUTHORIZATION, TEXT)
        asset = await service.asset(DEV_AUTHORIZATION, first.asset_id)
        assert asset.generation == 0 and asset.published_version is None
        with pytest.raises(PromptNotFound):
            await service.render(DEV_AUTHORIZATION, first.asset_id, first.version_id, {"name": "世界"})
        published = await service.publish(DEV_AUTHORIZATION, first.asset_id, first.version_id, 0)
        assert published.generation == 1
        pinned = await service.render(DEV_AUTHORIZATION, first.asset_id, first.version_id, {"name": "世界"})
        second = await service.add_version(DEV_AUTHORIZATION, first.asset_id, (Message("user", "Changed"),))
        await service.publish(DEV_AUTHORIZATION, first.asset_id, second.version_id, 1)
        assert pinned.messages == (Message("user", "你好 世界"),)
        assert (await service.render(DEV_AUTHORIZATION, first.asset_id, first.version_id, {"name": "世界"})) == pinned
        assert await service.version(DEV_AUTHORIZATION, first.asset_id, first.version_id) == first
        other = replace(DEV_AUTHORIZATION, tenant_id="another")
        for operation in (
            service.asset(other, first.asset_id),
            service.version(other, first.asset_id, first.version_id),
            service.add_version(other, first.asset_id, TEXT),
            service.publish(other, first.asset_id, first.version_id, 2),
            service.render(other, first.asset_id, first.version_id, {"name": "private"}),
        ):
            with pytest.raises(PromptNotFound):
                await operation
        return first
    first = run(scenario())
    for table in ("prompt_version", "prompt_publication"):
        for action in (f"DELETE FROM {table}", f"UPDATE {table} SET tenant_id=tenant_id"):
            with psycopg.connect(database) as connection:
                with pytest.raises(psycopg.errors.RaiseException, match="Immutable Prompt record"):
                    connection.execute(action)
    with psycopg.connect(database) as connection:
        assert connection.execute("SELECT count(*) FROM prompt_publication").fetchone()[0] == 2
        assert connection.execute("SELECT count(*) FROM prompt_version").fetchone()[0] == 2


@pytest.mark.postgres
def test_concurrent_publication_is_compare_and_swap(database):
    async def scenario():
        service = Prompts(PostgresPromptUnitOfWork(database))
        first = await service.create(DEV_AUTHORIZATION, TEXT)
        second = await service.add_version(DEV_AUTHORIZATION, first.asset_id, TEXT)
        results = await asyncio.gather(*(
            service.publish(DEV_AUTHORIZATION, item.asset_id, item.version_id, 0)
            for item in (first, second)), return_exceptions=True)
        assert sum(isinstance(item, PromptConflict) for item in results) == 1
        asset = await service.asset(DEV_AUTHORIZATION, first.asset_id)
        assert asset.generation == 1
        assert asset.published_version in {first.version_id, second.version_id}
    run(scenario())


@pytest.mark.postgres
def test_asset_and_first_version_are_one_transaction(database):
    async def scenario():
        uow = PostgresPromptUnitOfWork(database)
        asset_id = uuid4()
        with pytest.raises(RuntimeError):
            async with uow.transaction() as tx:
                await tx.create_asset("dev", asset_id)
                raise RuntimeError("injected before version")
        async with uow.transaction() as tx:
            assert await tx.asset("dev", asset_id) is None
    run(scenario())


def test_permission_and_expiry_fail_before_storage():
    class NoStorage:
        def transaction(self):
            raise AssertionError("Storage must not be touched")
    async def scenario():
        service = Prompts(NoStorage())
        context = replace(DEV_AUTHORIZATION, authentication_method="local_jwt", scopes=frozenset())
        for permission, operation in (
            ("gateway.prompts.write", lambda c: service.create(c, TEXT)),
            ("gateway.prompts.read", lambda c: service.asset(c, uuid4())),
            ("gateway.prompts.publish", lambda c: service.publish(c, uuid4(), uuid4(), 0)),
            ("gateway.prompts.use", lambda c: service.render(c, uuid4(), uuid4(), {})),
        ):
            with pytest.raises(Forbidden):
                await operation(context)
            expired = replace(context, scopes=frozenset({permission}), expires_at=datetime.now(timezone.utc) - timedelta(seconds=1))
            with pytest.raises(Unauthorized):
                await operation(expired)
    run(scenario())


@pytest.mark.postgres
def test_http_prompt_lifecycle_and_safe_protocol(database):
    async def scenario():
        context = DEV_AUTHORIZATION
        async def authorize(request):
            return context
        app = FastAPI()
        register_prompt_routes(app, Prompts(PostgresPromptUnitOfWork(database)), authorize=authorize)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            created = await client.post("/gateway/v1/prompts", json={"messages": [{"role": "user", "content": "secret {{name}}"}]})
            assert created.status_code == 201
            ids = created.json()
            path = "/gateway/v1/prompts/" + ids["asset_id"]
            version_path = path + "/versions/" + ids["version_id"]
            assert (await client.get(version_path)).json() == ids
            assert (await client.post(version_path + "/render", json={"variables": {"name": "value"}})).status_code == 404
            published = await client.post(path + "/publish", json={"version_id": ids["version_id"], "expected_generation": 0})
            assert published.status_code == 200 and published.json()["generation"] == 1
            assert (await client.post(path + "/publish", json={"version_id": ids["version_id"], "expected_generation": 0})).status_code == 409
            rendered = await client.post(version_path + "/render", json={"variables": {"name": "value"}})
            assert rendered.json()["messages"] == [{"role": "user", "content": "secret value"}]
            assert rendered.headers["cache-control"] == "no-store"
            invalid = await client.post(version_path + "/render", json={"variables": {"secret-value": "private"}})
            assert invalid.status_code == 422
            assert "private" not in invalid.text and "secret-value" not in invalid.text
            for body in [b'{"messages":[],"messages":[]}', b'{"messages":"secret-body"}', b'{"unknown":"private"}']:
                failed = await client.post("/gateway/v1/prompts", content=body, headers={"content-type": "application/json"})
                assert failed.status_code == 400 and "secret-body" not in failed.text
            assert (await client.post(path + "/publish", json={"version_id": ids["version_id"], "expected_generation": True})).status_code == 400
            context = replace(context, tenant_id="foreign")
            assert (await client.get(version_path)).status_code == 404
    run(scenario())
