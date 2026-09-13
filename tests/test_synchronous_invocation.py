import asyncio

import pytest

from llm_gateway.application.synchronous_invocation import SynchronousInvocation
from llm_gateway.domain.invocation import InvocationPersistenceUnavailable
from llm_gateway.domain.recovery import RetryPolicy
from tests.test_attempt_execution import CANDIDATES, SUCCESS


def test_settlement_must_finish_before_result_is_returned():
    events = []
    class Execution:
        async def execute(self, candidates, *, policy, deadline):
            events.append(("execute", deadline))
            return SUCCESS
    class Settlement:
        async def settle(self, result):
            assert result is SUCCESS
            events.append(("settled",))
            return result.usage
    async def run():
        deadline = asyncio.get_running_loop().time() + 10
        result = await SynchronousInvocation(Execution(), Settlement()).execute(CANDIDATES, policy=RetryPolicy(), deadline=deadline)
        events.append(("returned",))
        assert result is SUCCESS
        assert events == [("execute", deadline), ("settled",), ("returned",)]
    asyncio.run(run())


def test_settlement_failure_never_returns_or_reexecutes_provider_result():
    events = []
    class Execution:
        async def execute(self, *args, **kwargs):
            events.append("executed")
            return SUCCESS
    class Settlement:
        async def settle(self, result):
            raise InvocationPersistenceUnavailable()
    async def run():
        with pytest.raises(InvocationPersistenceUnavailable):
            await SynchronousInvocation(Execution(), Settlement()).execute(CANDIDATES, policy=RetryPolicy(),
                deadline=asyncio.get_running_loop().time() + 10)
    asyncio.run(run())
    assert events == ["executed"]


def test_cancellation_during_settlement_propagates_without_retry():
    async def run():
        entered = asyncio.Event()
        class Execution:
            async def execute(self, *args, **kwargs):
                return SUCCESS
        class Settlement:
            async def settle(self, result):
                entered.set()
                await asyncio.Future()
        task = asyncio.create_task(SynchronousInvocation(Execution(), Settlement()).execute(CANDIDATES,
            policy=RetryPolicy(), deadline=asyncio.get_running_loop().time() + 10))
        await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    asyncio.run(run())


@pytest.mark.parametrize("deadline", [0, float("nan"), float("inf"), True])
def test_invalid_or_expired_deadline_never_executes(deadline):
    with pytest.raises((ValueError, TimeoutError)):
        asyncio.run(SynchronousInvocation(None, None).execute(CANDIDATES, policy=RetryPolicy(), deadline=deadline))
