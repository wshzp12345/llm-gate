"""Invocation-local ownership of synchronous Attempt execution and cancellation.

This owns the executor's leases, not admission or an HTTP task. A higher-level
lifecycle owner must still supervise admission/settlement and initiate durable
cancellation before calling stop.
"""

import asyncio
from contextlib import contextmanager
from uuid import UUID, uuid4

from llm_gateway.application.cancellation import ProviderCancelResult
from llm_gateway.application.cancellation_execution import CancellationAttemptHandle
from llm_gateway.application.provider_context import GatewayCancellationToken, ProviderInvocationContext
from llm_gateway.application.late_provider_outcome import LateTextOutcome


class AttemptExecutionControl:
    def __init__(self, call_id: UUID, *, late_outcomes=None):
        if not isinstance(call_id, UUID) or call_id.version != 4:
            raise ValueError("Admitted Invocation UUID required")
        self._call_id = call_id
        self.token = GatewayCancellationToken()
        self._started = False
        self._done = asyncio.Event()
        self._active = None
        self._cancel_target = None
        self._cancel_capability = None
        self._late_outcomes = late_outcomes
        self._observation = None
        self._stopped_observation = None

    @contextmanager
    def scope(self):
        if self._started:
            raise RuntimeError("Invocation execution control cannot be reused")
        self._started = True
        try:
            with self.token.abort_on_cancel(asyncio.current_task().cancel):
                yield
        finally:
            # scope encloses the entire executor, including runtime __aexit__,
            # so this is not merely the Provider response having finished.
            self._done.set()
            self._stopped_observation = self._observation if self.token.reason is None else None
            self._observation = None

    @contextmanager
    def attempt(self, number, provider, deadline):
        self.token.raise_if_cancelled()
        if not self._started or self.stopped or self._active is not None:
            raise RuntimeError("Attempt requires one live execution scope")
        context = ProviderInvocationContext(CancellationAttemptHandle(self._call_id, number, uuid4()),
                                            self.token, deadline, self._late_outcomes,
                                            lambda result: self.observed(context, result))
        self._observation = None
        self._active = (context.handle, provider)
        try:
            yield context
        finally:
            self._active = None

    def observed(self, context, result):
        self._observation = LateTextOutcome.from_result(context.handle.reference, context.handle.number, result)

    @property
    def stopped_observation(self):
        """Content-free, uncheckpointed facts available only after local exit."""
        if self._started and not self._done.is_set():
            raise RuntimeError("Observation requires stopped execution")
        return self._stopped_observation

    def checkpointed(self, number):
        if self._observation is not None and self._observation.attempt_number == number:
            self._observation = None

    async def record_late(self):
        if self.token.reason is not None and self._observation is not None and self._late_outcomes is not None:
            await self._late_outcomes.record(self._observation)
            self._observation = None

    def stop(self, reason):
        if self.token.reason is None:
            self._cancel_target = self._active
            self._cancel_capability = self._active[1].supports_remote_cancellation if self._active else False
        self.token.request(reason)
        return self._cancel_target[0] if self._cancel_target else None

    @property
    def stopped(self):
        return self._done.is_set() or not self._started

    async def wait_stopped(self):
        if self._started:
            await self._done.wait()

    @property
    def supports_remote_cancellation(self):
        if self._cancel_capability is not None:
            return self._cancel_capability
        target = self._cancel_target or self._active
        return target[1].supports_remote_cancellation if target else False

    async def cancel(self, handle, reason, deadline):
        target = self._cancel_target
        if target is None or target[0] != handle:
            return ProviderCancelResult.UNKNOWN
        try:
            return await target[1].cancel(handle, reason, deadline)
        finally:
            self._cancel_target = None
