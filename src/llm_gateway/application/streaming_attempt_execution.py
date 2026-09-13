"""Stream Attempts with one irreversible delivery boundary and no hidden replay.

The owner still supplies durable journal/commit implementations and terminal
Invocation settlement. A returned Provider terminal is not HTTP success.
"""

import asyncio
from contextlib import aclosing, nullcontext
from dataclasses import replace
import math
from typing import Protocol

from llm_gateway.application.attempt_execution import AttemptJournal, NoEligibleCandidate
from llm_gateway.application.attempt_control import AttemptExecutionControl
from llm_gateway.application.request_trace import trace_stage, TraceStage
from llm_gateway.domain.model import FailureCode, ProviderFailure, RefusalOutput, Usage
from llm_gateway.domain.recovery import RecoveryPlan, plan_recovery
from llm_gateway.domain.streaming import DeltaKind, StreamDelta, StreamCompleted, StreamFailed


class StreamAttemptJournal(Protocol):
    async def started(self, number: int, binding_id: str, candidate_attempt: int) -> None: ...

    async def finished(self, number: int, terminal: StreamCompleted | StreamFailed, recovery: RecoveryPlan) -> None:
        """Commit outcome and all reported Usage, including partial failed Usage."""
        ...


class StreamOutputPort(Protocol):
    def admitted(self, call_id, accepted_at) -> None:
        """Assign trusted admission metadata locally; no I/O or delivery here."""
        ...

    async def delta(self, event: StreamDelta) -> None:
        """Deliver data only after the executor's durable commit reservation.

        An exception can mean uncertain delivery, never permission to switch
        models. This owner must not emit success before Invocation settlement.
        """
        ...


class StreamCommitPort(Protocol):
    async def first_delta(self, number: int, event: StreamDelta) -> None:
        """Durably forbid recovery before any business output can be delivered."""
        ...


class SharedStreamAttemptJournal:
    """Use the same durable Attempt/Cost checkpoint as non-streaming calls."""

    def __init__(self, journal: AttemptJournal):
        self._journal = journal

    async def started(self, number, binding_id, candidate_attempt):
        await self._journal.started(number, binding_id, candidate_attempt)

    async def finished(self, number, terminal, recovery):
        if not isinstance(terminal, (StreamCompleted, StreamFailed)):
            raise ValueError("Typed stream terminal required")
        result = terminal.result if isinstance(terminal, StreamCompleted) else terminal.as_result()
        await self._journal.finished(number, result, recovery)


class StreamingAttemptExecutor:
    def __init__(self, *, runtime, journal: StreamAttemptJournal, output: StreamOutputPort,
                 classify, draw_jitter, commit: StreamCommitPort, control: AttemptExecutionControl | None = None,
                 sleep=asyncio.sleep, max_output_bytes=4 * 1024 * 1024):
        if type(max_output_bytes) is not int or max_output_bytes < 1:
            raise ValueError("Positive stream output bound required")
        if not callable(getattr(commit, "first_delta", None)):
            raise ValueError("Explicit stream commitment port required")
        self._runtime, self._journal, self._output = runtime, journal, output
        self._classify, self._draw_jitter, self._sleep = classify, draw_jitter, sleep
        if control is not None and not isinstance(control, AttemptExecutionControl):
            raise ValueError("Typed Attempt execution control required")
        self._control = control
        self._commit = commit
        self._maximum = max_output_bytes
        self._committed = False
        self._used = False

    @property
    def committed(self):
        return self._committed

    async def execute(self, candidates, *, policy, deadline):
        if self._control is None:
            return await self._execute(candidates, policy=policy, deadline=deadline)
        with self._control.scope():
            try:
                return await self._execute(candidates, policy=policy, deadline=deadline)
            except asyncio.CancelledError:
                await self._control.record_late()
                raise

    async def _execute(self, candidates, *, policy, deadline):
        if self._used:
            raise RuntimeError("Stream executor cannot be reused")
        self._used = True
        if type(deadline) not in (int, float) or not math.isfinite(deadline):
            raise ValueError("Finite monotonic deadline required")
        if len({candidate.binding_id for candidate in candidates}) != len(candidates):
            raise ValueError("Duplicate execution Candidate")
        attempts, last_failure = 0, None
        loop = asyncio.get_running_loop()
        async with asyncio.timeout_at(deadline):
            for candidate in candidates:
                candidate_attempts = 0
                while True:
                    if loop.time() >= deadline:
                        raise TimeoutError()
                    async with self._runtime.acquire(candidate.binding_id) as provider:
                        if provider is None:
                            break
                        attempts += 1
                        candidate_attempts += 1
                        await self._journal.started(attempts, candidate.binding_id, candidate_attempts)
                        if loop.time() >= deadline:
                            raise TimeoutError()
                        scope = self._control.attempt(attempts, provider, deadline) if self._control else nullcontext(None)
                        with scope as context:
                            with trace_stage(TraceStage.PROVIDER_ATTEMPT, attempt_number=attempts) as observation:
                                terminal = await self._consume(provider, candidate.request, context, attempts)
                                if isinstance(terminal, StreamFailed):
                                    observation.mark_failed()
                            if self._control is not None:
                                result = terminal.result if isinstance(terminal, StreamCompleted) else terminal.as_result()
                                self._control.observed(context, result)
                                self._control.token.raise_if_cancelled()
                        if isinstance(terminal, StreamCompleted):
                            recovery = RecoveryPlan("stop")
                        else:
                            last_failure = terminal
                            if terminal.failure.code == FailureCode.UNCERTAIN:
                                recovery = RecoveryPlan("stop")
                            else:
                                flags = self._classify(terminal.failure)
                                recovery = plan_recovery(policy, attempts_started=attempts, candidate_attempts=candidate_attempts,
                                    remaining_ms=int((deadline - loop.time()) * 1000), retryable=flags.retryable,
                                    failover_eligible=flags.failover_eligible, committed=self._committed,
                                    retry_after_ms=flags.retry_after_ms, draw_jitter=self._draw_jitter)
                        await self._journal.finished(attempts, terminal, recovery)
                        if self._control is not None:
                            self._control.checkpointed(attempts)
                        if loop.time() >= deadline:
                            raise TimeoutError()
                    if recovery.action == "stop":
                        return terminal
                    if recovery.action == "advance":
                        break
                    with trace_stage(TraceStage.BACKOFF):
                        await self._sleep(recovery.delay_ms / 1000)
            if last_failure is not None:
                return last_failure
            raise NoEligibleCandidate()

    async def _consume(self, provider, request, context, number):
        sequence, size, model, terminal = 1, 0, None, None
        text, refusal = [], []

        def invalid():
            usage = terminal.result.usage if isinstance(terminal, StreamCompleted) else terminal.usage if isinstance(terminal, StreamFailed) else Usage()
            return StreamFailed(sequence, ProviderFailure(FailureCode.PROVIDER_PROTOCOL_ERROR, False), usage, model)

        async with aclosing(provider.stream(request, context=context)) as events:
            async for event in events:
                if self._control is not None:
                    self._control.token.raise_if_cancelled()
                if terminal is not None or not isinstance(event, (StreamDelta, StreamCompleted, StreamFailed)) or event.sequence != sequence:
                    return invalid()
                if isinstance(event, StreamDelta):
                    if model is not None and event.resolved_model != model:
                        return invalid()
                    model = event.resolved_model
                    size += len(event.text.encode("utf-8"))
                    if size > self._maximum:
                        return invalid()
                    (text if event.kind == DeltaKind.TEXT else refusal).append(event.text)
                    # Sticky before awaiting the sink: a failed write may have
                    # already exposed bytes. Only the lifecycle may settle it.
                    first = not self._committed
                    self._committed = True
                    if first:
                        await self._commit.first_delta(number, event)
                    if self._control is not None:
                        self._control.token.raise_if_cancelled()
                    await self._output.delta(event)
                    sequence += 1
                else:
                    terminal = event
                    if isinstance(event, StreamCompleted):
                        result = event.result
                        if (result.requested_model != request.requested_model
                                or model is not None and result.resolved_model != model
                                or (result.output.text or "") != "".join(text)
                                or (result.output.refusal or "" if isinstance(result.output, RefusalOutput) else "") != "".join(refusal)):
                            return invalid()
                    elif event.resolved_model is not None and model is not None and event.resolved_model != model:
                        return invalid()
        if isinstance(terminal, StreamFailed) and terminal.resolved_model is None and model is not None:
            terminal = replace(terminal, resolved_model=model)
        return terminal if terminal is not None else invalid()
