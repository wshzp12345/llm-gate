"""Durable cancellation ownership for an already admitted local execution.

This is not a public lookup or authorization port. Explicit API cancellation
must resolve and authorize its target separately before it can be implemented.
"""

from enum import StrEnum
from dataclasses import dataclass
from typing import Protocol

from llm_gateway.domain.invocation import InvocationAdmission


class LocalCancellationReason(StrEnum):
    CONTEXT_CANCELLED = "context_cancelled"
    CLIENT_DISCONNECTED = "client_disconnected"
    SHUTDOWN_DRAIN_EXPIRED = "shutdown_drain_expired"


class CancellationReservation(StrEnum):
    RESERVED = "reserved"
    CANCELLING = "cancelling"
    CANCELLED = "cancelled"
    ALREADY_TERMINAL = "already_terminal"


class ProviderCancelResult(StrEnum):
    ACKNOWLEDGED = "acknowledged"
    ALREADY_FINISHED = "already_finished"
    NOT_SUPPORTED = "not_supported"
    UNKNOWN = "unknown"
    FAILED = "failed"


@dataclass(frozen=True)
class CancellationCleanup:
    """Observed cleanup facts, never an inference from local socket closure.

    ACKNOWLEDGED requires a trusted remote-cancel capability at the caller.
    No dispatch (including an exhausted budget) has no Provider response.
    """

    dispatched: bool
    result: ProviderCancelResult | None
    downstream_writable: bool

    def __post_init__(self):
        if type(self.dispatched) is not bool or type(self.downstream_writable) is not bool:
            raise ValueError("Boolean cancellation observations required")
        if (self.dispatched and not isinstance(self.result, ProviderCancelResult)
                or not self.dispatched and self.result is not None):
            raise ValueError("Cancellation dispatch/result mismatch")


class LocalCancellationStore(Protocol):
    async def reserve(self, admission: InvocationAdmission,
                      reason: LocalCancellationReason) -> CancellationReservation:
        """Commit cancelling and its initiator atomically before transport cleanup.

        Only RESERVED owns cleanup/finalization. Other results perform neither.
        This does not acknowledge remote cancellation or finalize billing.
        """
        ...

    async def finalize(self, admission: InvocationAdmission, cleanup: CancellationCleanup) -> None:
        """Commit cancelled, cleanup facts and accounting after local work stops.

        Requires a won local reservation. Never used for restarted cancelling
        work, which needs uncertainty reconciliation, not assumed cancellation.
        """
        ...
