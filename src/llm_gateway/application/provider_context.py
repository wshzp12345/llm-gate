"""Gateway-owned, event-loop-local cancellation context for Provider work."""

import asyncio
import math
from contextlib import contextmanager
from dataclasses import dataclass, field
from collections.abc import Callable

from llm_gateway.application.cancellation import LocalCancellationReason
from llm_gateway.application.cancellation_execution import CancellationAttemptHandle
from llm_gateway.application.late_provider_outcome import LateProviderOutcomePort, LateTextOutcome
from llm_gateway.domain.model import FailureCode, ProviderFailure, Usage


class GatewayCancellationToken:
    """One-way signal; first reason wins and abort callbacks are scope-owned.

    Ordinary cancellation follows a won durable reservation. Irrevocable process
    ownership loss may also abort local work; that emergency does not assert a
    cancelled terminal. This signal has no database or authorization authority.
    """

    def __init__(self):
        self._reason = None
        self._aborts = {}

    @property
    def reason(self):
        return self._reason

    def raise_if_cancelled(self):
        if self._reason is not None:
            raise asyncio.CancelledError()

    def request(self, reason: LocalCancellationReason):
        if not isinstance(reason, LocalCancellationReason):
            raise ValueError("Typed cancellation reason required")
        if self._reason is not None:
            return
        self._reason = reason
        callbacks, self._aborts = tuple(self._aborts), {}
        errors = []
        for abort in callbacks:
            try:
                abort()
            except Exception as error:
                errors.append(error)
        if errors:
            raise ExceptionGroup("Local cancellation abort failed", errors)

    @contextmanager
    def abort_on_cancel(self, abort):
        """Register a synchronous nonblocking local abort for this scope only."""
        self.raise_if_cancelled()
        self._aborts[abort] = self._aborts.get(abort, 0) + 1
        try:
            yield
        finally:
            count = self._aborts.get(abort, 0)
            if count <= 1:
                self._aborts.pop(abort, None)
            else:
                self._aborts[abort] = count - 1


@dataclass(frozen=True)
class ProviderInvocationContext:
    handle: CancellationAttemptHandle
    cancellation: GatewayCancellationToken
    deadline: float
    late_outcomes: LateProviderOutcomePort | None = None
    observe_partial: Callable[[ProviderFailure], None] | None = field(default=None, repr=False, compare=False)

    def __post_init__(self):
        if (not isinstance(self.handle, CancellationAttemptHandle)
                or not isinstance(self.cancellation, GatewayCancellationToken)
                or type(self.deadline) not in (int, float) or not math.isfinite(self.deadline)):
            raise ValueError("Typed Provider invocation context required")
        if self.observe_partial is not None and not callable(self.observe_partial):
            raise ValueError("Partial observation sink must be callable")

    def observed_stream(self, usage: Usage, model: str | None) -> None:
        """In-memory snapshot, not a completion claim or a database write."""
        if self.observe_partial is not None:
            self.observe_partial(ProviderFailure(FailureCode.UNCERTAIN, False,
                observed_usage=usage, observed_model=model))

    async def record_late(self, result):
        if self.cancellation.reason is None:
            raise ValueError("Late result requires cancellation ownership")
        if self.late_outcomes is not None:
            await self.late_outcomes.record(LateTextOutcome.from_result(self.handle.reference, self.handle.number, result))
