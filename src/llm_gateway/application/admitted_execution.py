"""Own admitted execution through result settlement, cancel and deadline exit.

Admission permits stay with the caller until this method finishes. Child work
must cooperate with cancellation, as required by the Provider and storage ports.
"""

import asyncio

from llm_gateway.application.cancellation import LocalCancellationReason
from llm_gateway.application.cancellation_execution import LocalCancellationCoordinator
from llm_gateway.domain.invocation import InvocationTerminalConflict
from llm_gateway.application.request_trace import trace_stage, TraceStage


class _InvocationTask:
    def __init__(self, task, control):
        self._task, self._control = task, control

    def stop(self, reason):
        before = self._task.cancelling()
        handle = self._control.stop(reason)
        # Before the Attempt executor and after it returns, its subscription
        # is absent. Still stop initial checks or terminal settlement work.
        if not self._task.done() and self._task.cancelling() == before:
            self._task.cancel()
        return handle

    @property
    def stopped(self):
        return self._task.done()

    async def wait_stopped(self):
        await asyncio.wait({self._task})


async def _supervised_cleanup(operation):
    """Keep ownership of one cleanup task through repeated parent cancellation.

    No repeated cleanup dispatch and no detached/unobserved result. Storage and
    Provider ports supply their own finite operation deadlines.
    """
    task = asyncio.create_task(operation)
    while True:
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            if task.done():
                return task.result()


class AdmittedExecutionLifecycle:
    def __init__(self, *, cancellation_store, settle_deadline):
        self._cancellation = LocalCancellationCoordinator(cancellation_store)
        self._settle_deadline = settle_deadline

    async def run(self, admission, *, control, work, deadline, cancellation_context=None):
        task = asyncio.create_task(work())
        execution = _InvocationTask(task, control)

        async def observe_exit():
            await asyncio.gather(task, return_exceptions=True)

        async def deadline_exit():
            if not task.done() and not task.cancelling():
                task.cancel()
            await observe_exit()
            try:
                with trace_stage(TraceStage.SETTLEMENT):
                    await self._settle_deadline(admission, deadline=deadline)
            except InvocationTerminalConflict:
                # Completion or a cancellation reservation already committed.
                # A timed-out response may not rewrite that winning terminal.
                pass

        async def cancel_exit():
            try:
                reason, cancel_deadline = (cancellation_context(deadline) if cancellation_context is not None
                                           else (LocalCancellationReason.CONTEXT_CANCELLED, deadline))
                with trace_stage(TraceStage.CANCELLATION):
                    await self._cancellation.cancel(admission, reason,
                        execution=execution, provider=control, deadline=min(deadline, cancel_deadline), downstream_writable=False)
            except BaseException:
                # Persistence/cleanup failure must not leak a running Provider
                # task. This local abort does not claim durable cancellation or
                # dispatch another remote cancellation request.
                if not task.done() and not task.cancelling():
                    task.cancel()
                await observe_exit()
                raise
            await observe_exit()

        try:
            # Shield only so parent cancellation cannot close transport before
            # the PostgreSQL cancellation/normal-completion race is decided.
            return await asyncio.shield(task)
        except TimeoutError:
            if asyncio.get_running_loop().time() >= deadline:
                await _supervised_cleanup(deadline_exit())
            raise
        except asyncio.CancelledError:
            if asyncio.get_running_loop().time() >= deadline and control.token.reason is None:
                await _supervised_cleanup(deadline_exit())
            else:
                await _supervised_cleanup(cancel_exit())
            raise
