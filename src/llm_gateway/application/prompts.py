"""Tenant-scoped Prompt use cases and transaction port."""

from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol
from uuid import UUID, uuid4

from llm_gateway.application.authorization import require_permission, Unauthorized
from llm_gateway.domain.invocation import AuthorizationContext
from llm_gateway.domain.model import Message
from llm_gateway.domain.prompts import PromptVersion, RenderedPrompt


class PromptNotFound(Exception):
    pass


class PromptConflict(Exception):
    pass


class PromptPersistenceUnavailable(Exception):
    pass


@dataclass(frozen=True)
class PromptAsset:
    tenant_id: str
    asset_id: UUID
    generation: int
    published_version: UUID | None


class PromptTransaction(Protocol):
    async def create_asset(self, tenant_id: str, asset_id: UUID) -> None: ...
    async def asset(self, tenant_id: str, asset_id: UUID, *, lock: bool = False) -> PromptAsset | None: ...
    async def insert_version(self, version: PromptVersion) -> None: ...
    async def version(self, tenant_id: str, asset_id: UUID, version_id: UUID,
                      *, published_only: bool = False) -> PromptVersion | None: ...
    async def publish(self, asset: PromptAsset, version_id: UUID, subject: str) -> PromptAsset: ...


class PromptUnitOfWork(Protocol):
    def transaction(self) -> AbstractAsyncContextManager[PromptTransaction]: ...


class Prompts:
    def __init__(self, uow: PromptUnitOfWork, *, utcnow=lambda: datetime.now(timezone.utc)):
        self._uow, self._utcnow = uow, utcnow

    def _authorize(self, context, permission):
        require_permission(context, permission)
        if context.expires_at <= self._utcnow():
            raise Unauthorized()

    async def create(self, context: AuthorizationContext, messages: tuple[Message, ...]) -> PromptVersion:
        self._authorize(context, "gateway.prompts.write")
        version = PromptVersion(context.tenant_id, uuid4(), uuid4(), messages)
        async with self._uow.transaction() as tx:
            await tx.create_asset(context.tenant_id, version.asset_id)
            await tx.insert_version(version)
        return version

    async def add_version(self, context, asset_id, messages) -> PromptVersion:
        self._authorize(context, "gateway.prompts.write")
        version = PromptVersion(context.tenant_id, asset_id, uuid4(), messages)
        async with self._uow.transaction() as tx:
            if await tx.asset(context.tenant_id, asset_id) is None:
                raise PromptNotFound()
            await tx.insert_version(version)
        return version

    async def asset(self, context, asset_id) -> PromptAsset:
        self._authorize(context, "gateway.prompts.read")
        async with self._uow.transaction() as tx:
            asset = await tx.asset(context.tenant_id, asset_id)
            if asset is None:
                raise PromptNotFound()
            return asset

    async def version(self, context, asset_id, version_id) -> PromptVersion:
        self._authorize(context, "gateway.prompts.read")
        async with self._uow.transaction() as tx:
            version = await tx.version(context.tenant_id, asset_id, version_id)
            if version is None:
                raise PromptNotFound()
            return version

    async def publish(self, context, asset_id, version_id, expected_generation) -> PromptAsset:
        self._authorize(context, "gateway.prompts.publish")
        if type(expected_generation) is not int or not 0 <= expected_generation < 2**63 - 1:
            raise PromptConflict()
        async with self._uow.transaction() as tx:
            asset = await tx.asset(context.tenant_id, asset_id, lock=True)
            if asset is None or await tx.version(context.tenant_id, asset_id, version_id) is None:
                raise PromptNotFound()
            if asset.generation != expected_generation:
                raise PromptConflict()
            # Recheck after potentially waiting on a concurrent publisher.
            self._authorize(context, "gateway.prompts.publish")
            return await tx.publish(asset, version_id, context.subject)

    async def render(self, context, asset_id, version_id, values) -> RenderedPrompt:
        self._authorize(context, "gateway.prompts.use")
        async with self._uow.transaction() as tx:
            version = await tx.version(context.tenant_id, asset_id, version_id, published_only=True)
            if version is None:
                raise PromptNotFound()
        self._authorize(context, "gateway.prompts.use")
        return version.render(values)
