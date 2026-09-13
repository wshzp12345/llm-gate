import asyncio

import pytest

from llm_gateway.application.attempt_execution import NoEligibleCandidate
from llm_gateway.application.synchronous_invocation import SynchronousInvocation
from llm_gateway.domain.invocation import InvocationPersistenceUnavailable
from llm_gateway.domain.recovery import RetryPolicy
from llm_gateway.domain.routing_failure import UnattemptedRoutingFailure, classify_unattempted_rejections


@pytest.mark.parametrize("static,runtime,code", [
    ((), (), "no_route"),
    (({"provider_disabled"},), (), "no_route"),
    (({"capability_mismatch"}, {"parameter_unsupported"}), (), "unsupported_capability"),
    ((), ({"concurrency_exhausted"}, {"qps_exhausted"}), "rate_limited"),
    (({"provider_disabled"},), ({"concurrency_exhausted"},), "rate_limited"),
    ((), ({"provider_credentials_unavailable"},), "provider_credentials_unavailable"),
    ((), ({"provider_credentials_unavailable"}, {"concurrency_exhausted"}), "no_route"),
    ((), ({"circuit_open"}, {"circuit_half_open_busy"}), "provider_unavailable"),
    ((), ({"circuit_open"}, {"concurrency_exhausted"}), "no_route"),
])
def test_only_complete_supported_rejection_families_are_classified(static, runtime, code):
    assert classify_unattempted_rejections(static=tuple(map(frozenset, static)), runtime=tuple(map(frozenset, runtime))) == UnattemptedRoutingFailure(code)


@pytest.mark.parametrize("runtime", [(frozenset(),), (frozenset({"unknown"}),), (frozenset({"commitment_reached"}),)])
def test_missing_or_not_yet_supported_gate_policy_is_not_guessed(runtime):
    with pytest.raises(ValueError):
        classify_unattempted_rejections(static=(), runtime=runtime)


@pytest.mark.parametrize("fail", [False, True])
def test_unattempted_terminal_is_committed_before_return_and_never_reexecuted(fail):
    events = []
    class Execution:
        async def execute(self, *args, **kwargs):
            events.append("exhausted")
            raise NoEligibleCandidate()
    class Settlement:
        async def settle_exhausted(self):
            events.append("settle")
            if fail:
                raise InvocationPersistenceUnavailable()
            return UnattemptedRoutingFailure("rate_limited")
    async def run():
        result = await SynchronousInvocation(Execution(), Settlement()).execute((), policy=RetryPolicy(),
            deadline=asyncio.get_running_loop().time() + 5)
        events.append("return")
        assert result.code == "rate_limited"
    if fail:
        with pytest.raises(InvocationPersistenceUnavailable):
            asyncio.run(run())
        assert events == ["exhausted", "settle"]
    else:
        asyncio.run(run())
        assert events == ["exhausted", "settle", "return"]


def test_cancel_during_unattempted_settlement_propagates():
    async def run():
        entered = asyncio.Event()
        class Execution:
            async def execute(self, *args, **kwargs):
                raise NoEligibleCandidate()
        class Settlement:
            async def settle_exhausted(self):
                entered.set()
                await asyncio.Future()
        task = asyncio.create_task(SynchronousInvocation(Execution(), Settlement()).execute((), policy=RetryPolicy(),
            deadline=asyncio.get_running_loop().time() + 5))
        await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    asyncio.run(run())
