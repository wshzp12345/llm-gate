"""Process-local Circuit ownership and conservative text-outcome classification.

Registry instances belong to the process, never one Invocation or Revision.
The runtime owner must attach Binding/Revision identity to transition evidence.
"""

from threading import Lock

from llm_gateway.domain.model import FailureCode, ProviderFailure, ProviderResult
from llm_gateway.domain.provider_circuit import ProviderCircuit


def circuit_observation(result: ProviderResult | ProviderFailure) -> bool | None:
    if isinstance(result, ProviderResult):
        return None if result.disposition == "safety_refused" else True
    if not isinstance(result, ProviderFailure):
        raise TypeError("Typed Provider outcome required")
    if result.retryable is True and result.code in {
        FailureCode.RATE_LIMITED, FailureCode.UPSTREAM_TIMEOUT, FailureCode.PROVIDER_UNAVAILABLE,
    }:
        return False
    return None


class ProviderCircuits:
    def __init__(self):
        self._lock = Lock()
        self._circuits = {}

    def rejection(self, binding_id: str, now: float) -> str | None:
        if not isinstance(binding_id, str) or not binding_id:
            raise ValueError("Binding identity required")
        with self._lock:
            # A temporary closed Circuit validates the clock without registering
            # an unused Binding or acquiring a trial permit.
            return self._circuits.get(binding_id, ProviderCircuit()).rejection(now)

    def acquire(self, binding_id: str, now: float):
        if not isinstance(binding_id, str) or not binding_id:
            raise ValueError("Binding identity required")
        with self._lock:
            circuit = self._circuits.setdefault(binding_id, ProviderCircuit())
            return circuit.acquire(now)

    def finish(self, binding_id: str, permit, success: bool | None, now: float):
        with self._lock:
            return self._circuits[binding_id].finish(permit, success, now)
