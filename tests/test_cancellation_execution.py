import asyncio
from uuid import uuid4

import pytest

from llm_gateway.application.cancellation import CancellationReservation as Reservation, LocalCancellationReason as Reason, ProviderCancelResult as Result
from llm_gateway.application.cancellation_execution import CancellationAttemptHandle, CancellationCleanupIncomplete, LocalCancellationCoordinator
from tests.test_invocation import admission


class Harness:
    supports_remote_cancellation = False

    def __init__(self):
        self.admission = admission()
        self.handle = CancellationAttemptHandle(self.admission.call_id, 1, uuid4())
        self.events = []
        self.reservation = Reservation.RESERVED
        self.response = Result.NOT_SUPPORTED
        self.stopped = True
        self.cleanup = None

    async def reserve(self, admission, reason):
        self.events.append("reserve_committed")
        return self.reservation

    def stop(self, reason):
        self.events.append("stop")
        return self.handle

    async def cancel(self, handle, reason, deadline):
        assert handle == self.handle
        self.events.append("dispatch")
        self.deadline = deadline
        return self.response

    async def wait_stopped(self):
        self.events.append("wait_stopped")
        self.stopped = True

    async def finalize(self, admission, cleanup):
        assert self.stopped is True
        self.events.append("finalize_committed")
        self.cleanup = cleanup

    async def run(self, *, remaining=10):
        self.original_deadline = asyncio.get_running_loop().time() + remaining
        return await LocalCancellationCoordinator(self).cancel(
            self.admission, Reason.CONTEXT_CANCELLED, execution=self, provider=self,
            deadline=self.original_deadline, downstream_writable=False)


@pytest.mark.parametrize("response", list(Result))
def test_ordering_single_dispatch_and_trusted_capability(response):
    async def scenario():
        harness = Harness()
        harness.response = response
        harness.supports_remote_cancellation = True
        before = asyncio.get_running_loop().time()
        assert await harness.run() == Reservation.CANCELLED
        assert harness.events == ["reserve_committed", "stop", "dispatch", "finalize_committed"]
        assert harness.cleanup.result == response
        assert before + 2 <= harness.deadline <= asyncio.get_running_loop().time() + 2
        assert harness.deadline < harness.original_deadline
    asyncio.run(scenario())


def test_local_only_provider_cannot_claim_remote_acknowledgement():
    async def scenario():
        harness = Harness()
        harness.response = Result.ACKNOWLEDGED
        await harness.run(remaining=1)
        assert harness.deadline == harness.original_deadline
        assert harness.cleanup.result == Result.UNKNOWN
    asyncio.run(scenario())


@pytest.mark.parametrize("reservation", [Reservation.CANCELLING, Reservation.CANCELLED, Reservation.ALREADY_TERMINAL])
def test_losing_or_repeated_cancellation_never_dispatches(reservation):
    async def scenario():
        harness = Harness()
        harness.reservation = reservation
        assert await harness.run() == reservation
        assert harness.events == ["reserve_committed"]
    asyncio.run(scenario())


@pytest.mark.parametrize("active", [True, False])
def test_expired_budget_still_stops_local_work_but_does_not_dispatch(active):
    async def scenario():
        harness = Harness()
        if not active:
            harness.handle = None
        await harness.run(remaining=-1)
        assert harness.events == ["reserve_committed", "stop", "finalize_committed"]
        assert not harness.cleanup.dispatched and harness.cleanup.result is None
    asyncio.run(scenario())


def test_pre_attempt_cancellation_has_no_cancel_side_effect():
    async def scenario():
        harness = Harness()
        harness.handle = None
        harness.stopped = False
        await harness.run()
        assert harness.events == ["reserve_committed", "stop", "wait_stopped", "finalize_committed"]
    asyncio.run(scenario())


@pytest.mark.parametrize("failure", ["timeout", "exception", "invalid", "late_ack"])
def test_cancel_failure_is_safe_and_never_retried(failure):
    async def scenario():
        harness = Harness()
        harness.supports_remote_cancellation = True
        async def cancel(*args):
            harness.events.append("dispatch")
            if failure == "invalid":
                return "acknowledged"
            if failure == "exception":
                raise RuntimeError("must never become persisted detail")
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                if failure == "late_ack":
                    return Result.ACKNOWLEDGED
                raise
        harness.cancel = cancel
        await harness.run(remaining=.02)
        assert harness.events.count("dispatch") == 1
        assert harness.cleanup.result == (Result.FAILED if failure in {"exception", "invalid"} else Result.UNKNOWN)
    asyncio.run(scenario())


@pytest.mark.parametrize("case", ["expired", "wait_timeout", "false_stop", "late_stop"])
def test_local_work_still_live_never_finalizes_cancelled(case):
    async def scenario():
        harness = Harness()
        harness.stopped = False
        harness.handle = None
        async def wait_stopped():
            if case in {"wait_timeout", "late_stop"}:
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    if case == "late_stop":
                        harness.stopped = True
                        return
                    raise
        harness.wait_stopped = wait_stopped
        with pytest.raises(CancellationCleanupIncomplete):
            await harness.run(remaining=-1 if case == "expired" else .02)
        assert harness.cleanup is None
    asyncio.run(scenario())


def test_reservation_failure_has_no_local_or_remote_effect():
    async def scenario():
        harness = Harness()
        async def fail(*args):
            raise RuntimeError("commit failed")
        harness.reserve = fail
        with pytest.raises(RuntimeError):
            await harness.run()
        assert not harness.events
    asyncio.run(scenario())


def test_coordinator_cancellation_propagates_without_fabricated_finalization():
    async def scenario():
        harness = Harness()
        async def cancel(*args):
            raise asyncio.CancelledError()
        harness.cancel = cancel
        with pytest.raises(asyncio.CancelledError):
            await harness.run()
        assert harness.cleanup is None
    asyncio.run(scenario())


def test_foreign_attempt_handle_never_reaches_provider():
    async def scenario():
        harness = Harness()
        harness.handle = CancellationAttemptHandle(uuid4(), 1, uuid4())
        with pytest.raises(ValueError):
            await harness.run()
        assert harness.events == ["reserve_committed", "stop"]
    asyncio.run(scenario())
