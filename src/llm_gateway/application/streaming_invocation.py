"""Normalize stream terminals into the shared committed-result lifecycle."""

from llm_gateway.application.synchronous_invocation import SynchronousInvocation
from llm_gateway.domain.streaming import StreamCompleted, StreamFailed


class _StreamTerminalExecution:
    def __init__(self, executor):
        self._executor = executor

    async def execute(self, candidates, *, policy, deadline):
        terminal = await self._executor.execute(candidates, policy=policy, deadline=deadline)
        if isinstance(terminal, StreamCompleted):
            return terminal.result
        if isinstance(terminal, StreamFailed):
            return terminal.as_result()
        raise TypeError("Typed stream terminal required before settlement")


class StreamingInvocation:
    def __init__(self, executor, settlement):
        # The existing wrapper owns exhausted routes, aggregate Usage, deadline
        # checks and the final durable transaction. Deltas alone are not success.
        self._settled = SynchronousInvocation(_StreamTerminalExecution(executor), settlement)

    async def execute(self, candidates, *, policy, deadline):
        return await self._settled.execute(candidates, policy=policy, deadline=deadline)
