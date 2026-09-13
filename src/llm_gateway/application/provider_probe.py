"""Optional operational probes, isolated from Model Invocation accounting.

No generation request, user Attempt, live Circuit mutation or automatic retry
is performed here. A lifecycle owner consumes returned observations and owns
durable transition evidence and any authorized half-open recovery integration.
"""

import asyncio
import math
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from threading import Lock
from time import monotonic
from typing import Protocol

from llm_gateway.domain.configuration import revision_number


@dataclass(frozen=True)
class ProbePolicy:
    provider_id: str
    configuration_revision: str
    interval_seconds: int
    timeout_seconds: int

    def __post_init__(self):
        if not isinstance(self.provider_id, str) or len(self.provider_id) > 128 or not re.fullmatch(r"[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*", self.provider_id):
            raise ValueError("Provider resource identity required")
        revision_number(self.configuration_revision)
        if (type(self.interval_seconds) is not int or not 10 <= self.interval_seconds <= 3600
                or type(self.timeout_seconds) is not int or not 1 <= self.timeout_seconds <= 10
                or self.timeout_seconds >= self.interval_seconds):
            raise ValueError("Published probe interval and timeout required")


@dataclass(frozen=True)
class ProbeOutcome:
    code: str

    def __post_init__(self):
        if self.code not in {"available", "upstream_timeout", "rate_limited", "provider_unavailable",
                             "provider_protocol_error", "provider_credentials_unavailable", "transport_nonretryable", "internal"}:
            raise ValueError("Registered probe outcome required")

    @property
    def availability_sample(self) -> bool | None:
        if self.code == "available":
            return True
        if self.code in {"upstream_timeout", "rate_limited", "provider_unavailable"}:
            return False
        return None


@dataclass(frozen=True)
class ProbeObservation:
    policy: ProbePolicy
    observed_at: datetime
    outcome: ProbeOutcome
    signal: str = "active_probe"

    def __post_init__(self):
        if (not isinstance(self.policy, ProbePolicy) or not isinstance(self.outcome, ProbeOutcome)
                or self.signal != "active_probe" or not isinstance(self.observed_at, datetime)
                or self.observed_at.tzinfo is None or self.observed_at.utcoffset() != timedelta(0)):
            raise ValueError("Typed content-free UTC probe observation required")


@dataclass
class ProbeContext:
    deadline: float
    _phase: str = field(default="credentials", init=False, repr=False)

    @property
    def timeout_code(self):
        return {"credentials": "provider_credentials_unavailable", "transport": "upstream_timeout", "cleanup": "internal"}[self._phase]

    def begin_transport(self):
        if self._phase != "credentials":
            raise RuntimeError("A probe may start only one transport operation")
        self._phase = "transport"

    def finish_transport(self):
        if self._phase != "transport":
            raise RuntimeError("Probe transport must be active")
        self._phase = "cleanup"


class ProviderProbePort(Protocol):
    async def probe(self, context: ProbeContext) -> ProbeOutcome:
        """One configured safe GET with a fresh isolated credential lease."""
        ...


class ProviderProbeRunner:
    def __init__(self, *, clock=monotonic, utcnow=lambda: datetime.now(timezone.utc)):
        self._clock, self._utcnow = clock, utcnow
        self._lock = Lock()
        self._busy = set()
        self._next = {}
        self._last_time = None
        self._stopped = False

    def stop(self):
        """Stop scheduling; the caller still owns active task cancellation."""
        with self._lock:
            self._stopped = True

    def _wall_time(self):
        now = self._utcnow()
        if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() != timedelta(0):
            raise ValueError("UTC probe observation time required")
        return now

    async def run_due(self, policy: ProbePolicy, port: ProviderProbePort) -> ProbeObservation | None:
        if not isinstance(policy, ProbePolicy):
            raise ValueError("Published probe policy required")
        self._wall_time()
        with self._lock:
            now = self._clock()
            if type(now) not in (int, float) or not math.isfinite(now) or now < 0 or self._last_time is not None and now < self._last_time:
                raise ValueError("Nondecreasing monotonic probe time required")
            self._last_time = now
            if self._stopped or policy.provider_id in self._busy or now < self._next.get(policy.provider_id, 0):
                return None
            self._busy.add(policy.provider_id)
            self._next[policy.provider_id] = now + policy.interval_seconds
        try:
            context = ProbeContext(asyncio.get_running_loop().time() + policy.timeout_seconds)
            try:
                async with asyncio.timeout_at(context.deadline):
                    outcome = await port.probe(context)
                if not isinstance(outcome, ProbeOutcome):
                    outcome = ProbeOutcome("internal")
            except TimeoutError:
                outcome = ProbeOutcome(context.timeout_code)
            except Exception:
                outcome = ProbeOutcome("internal")
            return ProbeObservation(policy, self._wall_time(), outcome)
        finally:
            with self._lock:
                self._busy.remove(policy.provider_id)
