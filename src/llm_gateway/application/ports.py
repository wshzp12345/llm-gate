from typing import Protocol
from collections.abc import AsyncIterator

from llm_gateway.domain.model import CompletionRequest, ProviderFailure, ProviderResult
from llm_gateway.application.provider_context import ProviderInvocationContext
from llm_gateway.domain.streaming import ProviderStreamEvent


class CompletionPort(Protocol):
    """Synchronous-completion slice of the Provider Port; owns no retry policy."""

    async def complete(
        self, request: CompletionRequest, *, context: ProviderInvocationContext | None = None,
    ) -> ProviderResult | ProviderFailure: ...


class StreamingCompletionPort(Protocol):
    def stream(self, request: CompletionRequest, *, context: ProviderInvocationContext | None = None) -> AsyncIterator[ProviderStreamEvent]:
        """Ordered deltas then exactly one terminal; caller closes on early exit.

        A completed Provider stream is not a committed Gateway Invocation.
        Recovery, outward commitment and final settlement belong to the caller.
        """
        ...
