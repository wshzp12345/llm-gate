"""Synchronous Attempt execution for an admitted, durably preselected Invocation.

The caller owns Invocation permits and final Usage/Cost/terminal persistence.
Returning here is not permission to send a successful HTTP response. Journal
failures and cancellation propagate, leaving unfinished durable work for the
uncertain-execution reconciliation path, never automatic Provider replay.
"""

import asyncio
import math
from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from typing import Protocol

from llm_gateway.application.ports import CompletionPort
from llm_gateway.application.request_trace import trace_stage, TraceStage
from llm_gateway.application.attempt_control import AttemptExecutionControl
from llm_gateway.domain.model import CompletionRequest, FailureCode, ProviderFailure, ProviderResult
from llm_gateway.domain.recovery import RecoveryPlan, RetryPolicy, plan_recovery


@dataclass(frozen=True)
class ExecutionCandidate:
    binding_id: str
    request: CompletionRequest


@dataclass(frozen=True)
class FailureRecovery:
    retryable: bool
    failover_eligible: bool
    retry_after_ms: int | None = None


class AttemptJournal(Protocol):
    """Bound to one admitted Invocation; each method returns only after commit."""

    async def started(self, number: int, binding_id: str, candidate_attempt: int) -> None: ...

    async def finished(self, number: int, result: ProviderResult | ProviderFailure,
                       recovery: RecoveryPlan) -> None: ...


class CandidateRuntime(Protocol):
    def acquire(self, binding_id: str) -> AbstractAsyncContextManager[CompletionPort | None]:
        """Recheck dynamic gates, persist rejections, lease secrets and capacity.

        None means skip without an Attempt. Leases remain valid until exit;
        cancellation and errors must release them. No ordering or hidden retry.
        """
        ...


class NoEligibleCandidate(Exception):
    """No Provider result exists; the outer Router maps its recorded reasons."""


class SynchronousAttemptExecutor:
    def __init__(self, *, runtime: CandidateRuntime, journal: AttemptJournal,
                 classify: Callable[[ProviderFailure], FailureRecovery],
                 draw_jitter: Callable[[int], int],
                 sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
                 control: AttemptExecutionControl | None = None):
        if control is not None and not isinstance(control, AttemptExecutionControl):
            raise ValueError("Typed Attempt execution control required")
        self._runtime = runtime
        self._journal = journal
        self._classify = classify
        self._draw_jitter = draw_jitter
        self._sleep = sleep
        self._control = control

    async def execute(self, candidates: tuple[ExecutionCandidate, ...], *,
                      policy: RetryPolicy, deadline: float) -> ProviderResult | ProviderFailure:
        if self._control is None:
            return await self._execute(candidates, policy=policy, deadline=deadline)
        with self._control.scope():
            try:
                return await self._execute(candidates, policy=policy, deadline=deadline)
            except asyncio.CancelledError:
                await self._control.record_late()
                raise

    async def _execute(self, candidates: tuple[ExecutionCandidate, ...], *,
                       policy: RetryPolicy, deadline: float) -> ProviderResult | ProviderFailure:
        """Use a single absolute event-loop monotonic deadline, never reset it.

        Candidate order is already locked and persisted. This text-only path
        exposes no business output until its caller commits the final result.
        """
        if len({candidate.binding_id for candidate in candidates}) != len(candidates):
            raise ValueError("Duplicate execution Candidate")
        if type(deadline) not in (int, float) or not math.isfinite(deadline):
            raise ValueError("Finite monotonic deadline required")
        loop = asyncio.get_running_loop()
        attempts = 0
        last_failure = None
        async with asyncio.timeout_at(deadline):
            self._check_deadline(loop.time(), deadline)
            for candidate in candidates:
                candidate_attempts = 0
                while True:
                    self._check_deadline(loop.time(), deadline)
                    async with self._runtime.acquire(candidate.binding_id) as provider:
                        if provider is None:
                            break
                        self._check_deadline(loop.time(), deadline)
                        attempts += 1
                        candidate_attempts += 1
                        await self._journal.started(attempts, candidate.binding_id, candidate_attempts)
                        self._check_deadline(loop.time(), deadline)
                        with trace_stage(TraceStage.PROVIDER_ATTEMPT, attempt_number=attempts) as observation:
                            result = await self._complete(provider, candidate.request, attempts, deadline)
                            if isinstance(result, ProviderFailure):
                                observation.mark_failed()
                        if isinstance(result, ProviderResult):
                            await self._finish(attempts, result, RecoveryPlan("stop"))
                            self._check_deadline(loop.time(), deadline)
                            return result
                        if not isinstance(result, ProviderFailure):
                            raise TypeError("Invalid Provider Port result")
                        last_failure = result
                        if result.code == FailureCode.UNCERTAIN:
                            # A caller-supplied classifier cannot authorize
                            # regeneration of a possibly executed request.
                            await self._finish(attempts, result, RecoveryPlan("stop"))
                            self._check_deadline(loop.time(), deadline)
                            return result
                        flags = self._classify(result)
                        recovery = plan_recovery(
                            policy, attempts_started=attempts, candidate_attempts=candidate_attempts,
                            remaining_ms=int((deadline - loop.time()) * 1000),
                            retryable=flags.retryable, failover_eligible=flags.failover_eligible,
                            committed=False, retry_after_ms=flags.retry_after_ms,
                            draw_jitter=self._draw_jitter,
                        )
                        await self._finish(attempts, result, recovery)
                        self._check_deadline(loop.time(), deadline)
                    if recovery.action == "stop":
                        return result
                    if recovery.action == "advance":
                        break
                    self._check_deadline(loop.time(), deadline)
                    with trace_stage(TraceStage.BACKOFF):
                        await self._sleep(recovery.delay_ms / 1000)
            if last_failure is not None:
                return last_failure
            raise NoEligibleCandidate()

    async def _complete(self, provider, request, number, deadline):
        if self._control is None:
            return await provider.complete(request)
        with self._control.attempt(number, provider, deadline) as context:
            result = await provider.complete(request, context=context)
            self._control.observed(context, result)
            self._control.token.raise_if_cancelled()
            return result

    async def _finish(self, number, result, recovery):
        await self._journal.finished(number, result, recovery)
        if self._control is not None:
            self._control.checkpointed(number)

    def _check_deadline(self, now: float, deadline: float) -> None:
        if self._control is not None:
            self._control.token.raise_if_cancelled()
        if now >= deadline:
            raise TimeoutError()
