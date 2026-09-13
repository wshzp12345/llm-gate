"""Single-dispatch cancellation after a durable ownership reservation.

Run under the lifecycle owner's supervision, not in the already-cancelled
Provider task. This does not authorize an explicit public cancellation request.
"""

import asyncio
import math
from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

from llm_gateway.application.cancellation import (
    CancellationCleanup, CancellationReservation, LocalCancellationReason,
    LocalCancellationStore, ProviderCancelResult,
)
from llm_gateway.domain.invocation import InvocationAdmission


@dataclass(frozen=True)
class CancellationAttemptHandle:
    """Gateway identity and Adapter-owned opaque reference, without SDK types."""

    call_id: UUID
    number: int
    reference: UUID

    def __post_init__(self):
        if (not isinstance(self.call_id, UUID) or self.call_id.version != 4
                or not isinstance(self.reference, UUID) or self.reference.version != 4
                or type(self.number) is not int or not 1 <= self.number <= 3):
            raise ValueError("Valid opaque Attempt handle required")


class ProviderCancellationPort(Protocol):
    supports_remote_cancellation: bool

    async def cancel(self, handle: CancellationAttemptHandle, reason: LocalCancellationReason,
                     deadline: float) -> ProviderCancelResult:
        """Abort local transport and optionally request trusted remote cancel.

        Exactly one call, using the supplied absolute monotonic deadline.
        Local socket/task closure alone cannot acknowledge remote cancellation.
        Implementations must cooperate with task cancellation; the lifecycle
        owner remains responsible for supervising stuck Adapter tasks.
        """
        ...


class LocalCancellationExecution(Protocol):
    def stop(self, reason: LocalCancellationReason) -> CancellationAttemptHandle | None:
        """Synchronously suppress output/new Attempts and initiate local abort.

        Return the active durable Attempt handle, or None when there is none.
        The handle stays valid through cleanup even if local work exits first.
        """
        ...

    @property
    def stopped(self) -> bool:
        """True only after local execution and its leases have actually ended."""
        ...

    async def wait_stopped(self) -> None: ...


class CancellationCleanupIncomplete(Exception):
    """Local work did not stop in budget; retain cancelling for reconciliation."""


class LocalCancellationCoordinator:
    def __init__(self, store: LocalCancellationStore):
        self._store = store

    async def cancel(self, admission: InvocationAdmission, reason: LocalCancellationReason, *,
                     execution: LocalCancellationExecution, provider: ProviderCancellationPort,
                     deadline: float, downstream_writable: bool) -> CancellationReservation:
        if (not isinstance(admission, InvocationAdmission) or not isinstance(reason, LocalCancellationReason)
                or type(deadline) not in (int, float) or not math.isfinite(deadline)
                or type(downstream_writable) is not bool):
            raise ValueError("Typed local cancellation context and finite deadline required")
        if type(provider.supports_remote_cancellation) is not bool:
            raise ValueError("Explicit Provider cancellation capability required")
        reservation = await self._store.reserve(admission, reason)
        if not isinstance(reservation, CancellationReservation):
            raise TypeError("Typed cancellation reservation required")
        if reservation != CancellationReservation.RESERVED:
            return reservation
        loop = asyncio.get_running_loop()
        cleanup_deadline = min(deadline, loop.time() + 2)
        handle = execution.stop(reason)
        if handle is not None and (not isinstance(handle, CancellationAttemptHandle) or handle.call_id != admission.call_id):
            raise ValueError("Cancellation handle must belong to the admitted Invocation")
        dispatched, result = False, None
        if handle is not None and loop.time() < cleanup_deadline:
            dispatched = True
            try:
                async with asyncio.timeout_at(cleanup_deadline):
                    result = await provider.cancel(handle, reason, cleanup_deadline)
                # A misbehaving Adapter may swallow cancellation and return
                # late. It cannot turn expired cleanup into an acknowledgement.
                if loop.time() >= cleanup_deadline:
                    result = ProviderCancelResult.UNKNOWN
                elif not isinstance(result, ProviderCancelResult):
                    result = ProviderCancelResult.FAILED
                elif result == ProviderCancelResult.ACKNOWLEDGED and not provider.supports_remote_cancellation:
                    result = ProviderCancelResult.UNKNOWN
            except TimeoutError:
                result = ProviderCancelResult.UNKNOWN
            except Exception:
                # Safe bounded cancellation evidence only; never model retry,
                # failover, Circuit observation or raw Adapter exception text.
                result = ProviderCancelResult.FAILED
        if execution.stopped is not True:
            if loop.time() >= cleanup_deadline:
                raise CancellationCleanupIncomplete()
            try:
                async with asyncio.timeout_at(cleanup_deadline):
                    await execution.wait_stopped()
            except TimeoutError:
                raise CancellationCleanupIncomplete() from None
            if loop.time() >= cleanup_deadline or execution.stopped is not True:
                raise CancellationCleanupIncomplete()
        await self._store.finalize(admission, CancellationCleanup(dispatched, result, downstream_writable))
        return CancellationReservation.CANCELLED
