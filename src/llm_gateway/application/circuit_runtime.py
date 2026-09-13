"""Circuit gates and post-checkpoint observations with durable transition ACKs.

One coordinator is process-owned. Each Invocation owns its stage/journal pair.
Pending transition facts retain one event identity across later delivery; no
Provider retry or background retry task is created by a persistence failure.
"""

import re
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from threading import Lock
from time import monotonic
from uuid import UUID, uuid4

from llm_gateway.application.circuit_evidence import CircuitEvidencePort, CircuitTransitionEvidence
from llm_gateway.application.provider_circuits import ProviderCircuits, circuit_observation
from llm_gateway.domain.configuration import revision_number
from llm_gateway.domain.invocation import InvocationPersistenceUnavailable
from llm_gateway.domain.routing_eligibility import StaticRoutingReason


@dataclass(frozen=True, eq=False)
class _Lease:
    binding: str
    revision: str
    call_id: UUID
    permit: object


class CircuitCoordinator:
    def __init__(self, *, circuits: ProviderCircuits, evidence: CircuitEvidencePort,
                 clock=monotonic, utcnow=lambda: datetime.now(timezone.utc)):
        self._circuits, self._evidence = circuits, evidence
        self._clock, self._utcnow = clock, utcnow
        self._lock = Lock()
        self._pending = {}
        self._leases = set()

    def _wall_time(self):
        value = self._utcnow()
        if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() != timedelta(0):
            raise ValueError("UTC Circuit event time required")
        return value

    def _record(self, lease, transition, at, attempt_number=None):
        if transition is not None:
            if lease.binding in self._pending:
                raise InvocationPersistenceUnavailable()
            self._pending[lease.binding] = CircuitTransitionEvidence(uuid4(), lease.binding, lease.revision,
                at, transition, lease.call_id, attempt_number)

    async def _flush(self, binding):
        with self._lock:
            event = self._pending.get(binding)
        if event is None:
            return
        await self._evidence.append(event)
        with self._lock:
            if self._pending.get(binding) is event:
                del self._pending[binding]

    async def acquire(self, binding: str, revision: str, call_id: UUID):
        if not isinstance(binding, str) or len(binding) > 128 or not re.fullmatch(r"[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*", binding):
            raise ValueError("Binding resource identity required")
        revision_number(revision)
        if not isinstance(call_id, UUID) or call_id.version != 4:
            raise ValueError("Invocation UUIDv4 required")
        await self._flush(binding)
        at = self._wall_time()
        with self._lock:
            if binding in self._pending:
                raise InvocationPersistenceUnavailable()
            admission = self._circuits.acquire(binding, self._clock())
            lease = _Lease(binding, revision, call_id, admission.permit)
            self._record(lease, admission.transition, at)
            if admission.permit is not None:
                self._leases.add(lease)
        try:
            await self._flush(binding)
        except BaseException:
            if admission.permit is not None:
                with self._lock:
                    self._circuits.finish(binding, admission.permit, None, self._clock())
                    self._leases.remove(lease)
            raise
        return (lease if admission.permit is not None else None), admission.rejection

    def rejection(self, binding: str) -> str | None:
        """Read-only initial check; unacknowledged transitions still fail closed."""
        with self._lock:
            if binding in self._pending:
                raise InvocationPersistenceUnavailable()
            return self._circuits.rejection(binding, self._clock())

    async def finish(self, lease, observation, attempt_number=None):
        if observation is not None and (type(attempt_number) is not int or not 1 <= attempt_number <= 3):
            raise ValueError("Eligible Circuit observation requires its Attempt number")
        at = self._wall_time()
        with self._lock:
            if lease not in self._leases:
                raise ValueError("Unknown Circuit runtime lease")
            transition = self._circuits.finish(lease.binding, lease.permit, observation, self._clock())
            self._leases.remove(lease)
            self._record(lease, transition, at, attempt_number)
        await self._flush(lease.binding)


@dataclass
class _Flight:
    lease: _Lease
    number: int | None = None
    observation: bool | None = None
    completed: bool = False


class CircuitInvocationStage:
    def __init__(self, *, coordinator: CircuitCoordinator, revision: str, call_id: UUID, reject):
        self._coordinator, self._revision, self._call_id = coordinator, revision, call_id
        self._reject = reject
        self._active = {}

    @asynccontextmanager
    async def acquire(self, binding):
        if self._active:
            raise RuntimeError("Invocation Circuit stages must not overlap")
        lease, reason = await self._coordinator.acquire(binding, self._revision, self._call_id)
        if lease is None:
            await self._reject(binding, (StaticRoutingReason(reason),))
            yield False
            return
        flight = _Flight(lease)
        self._active[binding] = flight
        try:
            yield True
        except BaseException:
            self._active.pop(binding)
            try:
                await self._coordinator.finish(lease, flight.observation, flight.number)
            except BaseException:
                # Preserve the primary cancellation/checkpoint error. An event
                # whose ACK failed remains pending and gates subsequent work.
                pass
            raise
        else:
            self._active.pop(binding)
            await self._coordinator.finish(lease, flight.observation, flight.number)

    def journal(self, delegate):
        return _CircuitJournal(self, delegate)


class _CircuitJournal:
    def __init__(self, stage, delegate):
        self._stage, self._delegate = stage, delegate

    async def started(self, number, binding_id, candidate_attempt):
        flight = self._stage._active.get(binding_id)
        if flight is None or flight.number is not None:
            raise RuntimeError("Attempt requires a fresh Circuit stage")
        await self._delegate.started(number, binding_id, candidate_attempt)
        flight.number = number

    async def finished(self, number, result, recovery):
        matches = [flight for flight in self._stage._active.values() if flight.number == number]
        if len(matches) != 1 or matches[0].completed:
            raise RuntimeError("Attempt outcome requires its active Circuit stage")
        observation = circuit_observation(result)
        await self._delegate.finished(number, result, recovery)
        matches[0].observation = observation
        matches[0].completed = True
