import asyncio

import pytest

from llm_gateway.application.admitted_execution import AdmittedExecutionLifecycle
from llm_gateway.application.attempt_control import AttemptExecutionControl
from llm_gateway.application.cancellation import CancellationReservation
from llm_gateway.domain.invocation import InvocationPersistenceUnavailable
from tests.test_invocation import admission


@pytest.mark.parametrize("failure", [False, True])
def test_parent_cancel_is_shielded_until_reservation_and_cleanup_is_not_restarted(failure):
    async def scenario():
        record = admission()
        control = AttemptExecutionControl(record.call_id)
        entered, reserving, reserve_allowed, exited = (asyncio.Event() for _ in range(4))
        events = []
        class Store:
            async def reserve(self, admission, reason):
                events.append("reserve")
                reserving.set()
                await reserve_allowed.wait()
                if failure:
                    raise InvocationPersistenceUnavailable()
                return CancellationReservation.RESERVED

            async def finalize(self, admission, cleanup):
                assert exited.is_set()
                events.append("finalize")
        async def deadline_settlement(*args, **kwargs):
            raise AssertionError("not a deadline")
        async def work():
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                exited.set()
        lifecycle = AdmittedExecutionLifecycle(cancellation_store=Store(), settle_deadline=deadline_settlement)
        task = asyncio.create_task(lifecycle.run(record, control=control, work=work,
            deadline=asyncio.get_running_loop().time() + 5))
        await entered.wait()
        task.cancel()
        await reserving.wait()
        assert not exited.is_set()
        task.cancel()  # Must not cancel or duplicate the already owned reservation.
        reserve_allowed.set()
        with pytest.raises(InvocationPersistenceUnavailable if failure else asyncio.CancelledError):
            await task
        assert exited.is_set()
        assert events == (["reserve"] if failure else ["reserve", "finalize"])
    asyncio.run(scenario())


def test_expired_parent_uses_deadline_settlement_without_cancel_dispatch():
    async def scenario():
        record = admission()
        entered, exited, settled = asyncio.Event(), asyncio.Event(), []
        async def work():
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                exited.set()
        async def deadline_settlement(admission, *, deadline):
            assert exited.is_set()
            settled.append(admission.call_id)
        lifecycle = AdmittedExecutionLifecycle(cancellation_store=None, settle_deadline=deadline_settlement)
        task = asyncio.create_task(lifecycle.run(record, control=AttemptExecutionControl(record.call_id),
            work=work, deadline=asyncio.get_running_loop().time() - 1))
        await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert settled == [record.call_id]
    asyncio.run(scenario())
