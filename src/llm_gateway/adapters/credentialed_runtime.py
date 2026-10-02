"""Credential stage for an Invocation's Candidate runtime, not a full Router.

The caller supplies validated endpoint clients and mandatory remaining gates.
No default allow gate, endpoint creation, health updates or credential logging.
"""

from collections.abc import Awaitable, Callable, Mapping
from contextlib import AbstractAsyncContextManager, aclosing, asynccontextmanager
from dataclasses import dataclass, field, replace
from typing import Protocol

import httpx

from llm_gateway.adapters.openai_compatible import OpenAICompatibleCompletion
from llm_gateway.adapters.anthropic_messages import AnthropicMessagesCompletion
from llm_gateway.adapters.structured_output import validate_structured_result, validate_structured_stream
from llm_gateway.adapters.egress_pool import ProviderTransportRetired
from llm_gateway.adapters.provider_credentials import (
    CredentialMetadata, CredentialUnavailable, ProviderCredentialLease,
)
from llm_gateway.domain.model import FailureCode, ProviderFailure
from llm_gateway.domain.streaming import StreamFailed
from llm_gateway.application.cancellation import ProviderCancelResult


class AsyncCredentialSource(Protocol):
    async def resolve(self, secret_ref: str) -> ProviderCredentialLease: ...


@dataclass(frozen=True)
class CredentialBinding:
    secret_ref: str
    base_url: str = field(repr=False)
    client: httpx.AsyncClient | Callable[[], httpx.AsyncClient] = field(repr=False)
    adapter_type: str = "compatible"
    adapter_version: str = "v1"


class CredentialedCandidateRuntime:
    def __init__(self, *, bindings: Mapping[str, CredentialBinding], source: AsyncCredentialSource,
                 gates: Callable[[str, CredentialMetadata], AbstractAsyncContextManager[bool]],
                 reject: Callable[[str, str], Awaitable[None]]):
        self._bindings = dict(bindings)
        self._source = source
        self._gates = gates
        self._reject = reject
        # Invocation-local only; process-wide credential health is separate.
        self._rejected_versions = set()

    @asynccontextmanager
    async def acquire(self, binding_id: str):
        binding = self._bindings[binding_id]
        try:
            lease = await self._source.resolve(binding.secret_ref)
        except CredentialUnavailable:
            await self._reject(binding_id, "provider_credentials_unavailable")
            yield None
            return
        with lease:
            identity = (binding.secret_ref, lease.metadata.secret_version)
            if lease.metadata.secret_ref != binding.secret_ref or lease.metadata.revoked or identity in self._rejected_versions:
                await self._reject(binding_id, "provider_credentials_unavailable")
                yield None
                return
            async with self._gates(binding_id, lease.metadata) as allowed:
                if allowed is not True:
                    yield None
                    return
                try:
                    client = binding.client() if callable(binding.client) else binding.client
                except ProviderTransportRetired:
                    await self._reject(binding_id, "security_invalidated")
                    yield None
                    return
                yield _LeasedCompletion(replace(binding, client=client), lease, self._rejected_versions, identity)


class _LeasedCompletion:
    supports_remote_cancellation = False

    def __init__(self, binding, lease, rejected_versions, identity):
        self._binding = binding
        self._lease = lease
        self._rejected_versions = rejected_versions
        self._identity = identity
        self._used = False
        self._adapter = None

    def _new_adapter(self):
        adapters = {("compatible", "v1"): OpenAICompatibleCompletion,
                    ("anthropic_messages", "v1"): AnthropicMessagesCompletion}
        adapter = adapters.get((self._binding.adapter_type, self._binding.adapter_version))
        if adapter is None:
            raise ValueError("Unregistered Provider Adapter contract")
        return adapter(self._binding.client, base_url=self._binding.base_url,
                       credential=self._lease.bearer_value())

    async def complete(self, request, *, context=None):
        if self._used:
            raise RuntimeError("Provider Attempt cannot be reused")
        self._used = True
        adapter = self._new_adapter()
        self._adapter = adapter
        try:
            result = validate_structured_result(await adapter.complete(request, context=context), request.output_format)
        finally:
            # The cancellation handle must not retain the credential-bearing
            # Adapter after its Attempt scope has ended.
            self._adapter = None
        if isinstance(result, ProviderFailure) and result.code == FailureCode.PROVIDER_CREDENTIALS_UNAVAILABLE:
            self._rejected_versions.add(self._identity)
        return result

    async def stream(self, request, *, context=None):
        if self._used:
            raise RuntimeError("Provider Attempt cannot be reused")
        self._used = True
        adapter = self._new_adapter()
        self._adapter = adapter
        try:
            source = adapter.stream(request, context=context)
            if request.output_format is not None:
                source = validate_structured_stream(source, request.output_format)
            async with aclosing(source) as events:
                async for event in events:
                    if isinstance(event, StreamFailed) and event.failure.code == FailureCode.PROVIDER_CREDENTIALS_UNAVAILABLE:
                        self._rejected_versions.add(self._identity)
                    yield event
        finally:
            self._adapter = None

    async def cancel(self, handle, reason, deadline):
        adapter = self._adapter
        if adapter is None:
            return ProviderCancelResult.UNKNOWN
        return await adapter.cancel(handle, reason, deadline)
