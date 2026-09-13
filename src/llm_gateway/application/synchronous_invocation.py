"""Complete admitted synchronous execution only after final durable settlement."""

import asyncio
import math
from dataclasses import replace
from typing import Protocol

from llm_gateway.application.attempt_execution import ExecutionCandidate, NoEligibleCandidate
from llm_gateway.domain.model import ProviderFailure, ProviderResult, Usage
from llm_gateway.domain.routing_failure import UnattemptedRoutingFailure
from llm_gateway.domain.recovery import RetryPolicy
from llm_gateway.application.request_trace import trace_stage, TraceStage


class InvocationExecutionPort(Protocol):
    async def execute(self, candidates: tuple[ExecutionCandidate, ...], *, policy: RetryPolicy,
                      deadline: float) -> ProviderResult | ProviderFailure: ...


class InvocationSettlementPort(Protocol):
    """Bound to this Invocation; return only after the terminal transaction commits."""

    async def settle(self, result: ProviderResult | ProviderFailure) -> Usage: ...

    async def settle_exhausted(self) -> UnattemptedRoutingFailure:
        """Derive failure from complete durable rejections and commit zero-Attempt terminal."""
        ...


class SynchronousInvocation:
    def __init__(self, execution: InvocationExecutionPort, settlement: InvocationSettlementPort):
        self._execution = execution
        self._settlement = settlement

    async def execute(self, candidates: tuple[ExecutionCandidate, ...], *, policy: RetryPolicy,
                      deadline: float) -> ProviderResult | ProviderFailure | UnattemptedRoutingFailure:
        # Admission and the outer Invocation permit are caller-owned. This
        # wrapper neither replays uncertain work nor catches cancellation.
        if type(deadline) not in (int, float) or not math.isfinite(deadline):
            raise ValueError("Finite monotonic deadline required")
        loop = asyncio.get_running_loop()
        if loop.time() >= deadline:
            raise TimeoutError()
        async with asyncio.timeout_at(deadline):
            try:
                result = await self._execution.execute(candidates, policy=policy, deadline=deadline)
            except NoEligibleCandidate:
                if loop.time() >= deadline:
                    raise TimeoutError()
                with trace_stage(TraceStage.SETTLEMENT):
                    result = await self._settlement.settle_exhausted()
                if not isinstance(result, UnattemptedRoutingFailure):
                    raise TypeError("Exhausted route requires committed routing failure")
                if loop.time() >= deadline:
                    raise TimeoutError()
                return result
            if loop.time() >= deadline:
                raise TimeoutError()
            with trace_stage(TraceStage.SETTLEMENT):
                aggregate_usage = await self._settlement.settle(result)
            if not isinstance(aggregate_usage, Usage):
                raise TypeError("Settlement must return committed Invocation Usage")
            if loop.time() >= deadline:
                raise TimeoutError()
            return replace(result, usage=aggregate_usage) if isinstance(result, ProviderResult) and result.usage != aggregate_usage else result
