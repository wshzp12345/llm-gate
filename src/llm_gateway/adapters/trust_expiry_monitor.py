"""Network-free FR-129 expiry metrics and deduplicated non-content events.

One process-owned monitor is refreshed by the lifecycle owner. This component
does not change readiness, Circuit state, pools, or active TLS connections.
"""

import asyncio
import json
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

from llm_gateway.adapters.trust_bundle import TrustBundleIntegrityValidator


@dataclass(frozen=True)
class TrustExpiryObservation:
    event_id: UUID
    provider_id: str
    identity: str
    configuration_revision: str
    observed_at: datetime
    days_until_expiry: float
    kind: str


class TrustExpiryMonitor:
    def __init__(self, *, metrics, events, clock=lambda: datetime.now(timezone.utc)):
        """metrics.replace receives the complete bounded Provider->days gauge.

        events.append must deduplicate by event_id and receives no certificates
        or endpoint data. A failed ACK retains the same event for later delivery.
        """
        self._metrics, self._events, self._clock = metrics, events, clock
        self._warnings, self._expired = {}, set()
        self._pending = {}
        self._lock = asyncio.Lock()
        self._last_time = None

    async def run(self, configuration, *, interval_seconds):
        """Lifecycle-owned task; cancellation propagates, no Provider probing.

        The owner supplies cadence and supervises failures instead of this task
        hiding metric/event persistence errors or changing readiness itself.
        """
        if type(interval_seconds) not in (int, float) or not math.isfinite(interval_seconds) or interval_seconds <= 0:
            raise ValueError("Positive expiry observation interval required")
        while True:
            snapshot = configuration.current
            if snapshot is not None:
                await self.refresh(snapshot)
            await asyncio.sleep(interval_seconds)

    async def refresh(self, snapshot):
        async with self._lock:
            now = self._clock()
            if (not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() != timedelta(0)
                    or self._last_time is not None and now < self._last_time):
                raise ValueError("Nondecreasing UTC expiry observation time required")
            self._last_time = now
            gauges, observations = {}, []
            content = json.loads(snapshot.snapshot_json)
            validator = TrustBundleIntegrityValidator()
            for provider_id, provider in sorted(content["providers"].items()):
                bundle = provider["transport"]["tls"]["trust_bundle"]
                if bundle is None:
                    continue
                certificates = validator.certificates(bundle["identity"], bundle["pem"])
                if certificates is None:
                    raise ValueError("Invalid published Trust Bundle")
                expires_at = min(certificate.not_valid_after_utc for certificate in certificates)
                days = (expires_at - now).total_seconds() / 86400
                gauges[provider_id] = days
                identity = provider_id, bundle["identity"]
                if now > expires_at:
                    if identity not in self._expired:
                        observations.append(TrustExpiryObservation(uuid4(), provider_id, bundle["identity"],
                            snapshot.revision, now, days, "expired"))
                elif days <= 30:
                    previous = self._warnings.get(identity)
                    if previous is None or now - previous >= timedelta(hours=24):
                        observations.append(TrustExpiryObservation(uuid4(), provider_id, bundle["identity"],
                            snapshot.revision, now, days, "expiring"))
            # Replacement clears gauges for removed Providers/system trust,
            # rather than retaining stale metric series or identity labels.
            await self._metrics.replace(gauges)
            for observation in observations:
                self._pending.setdefault((observation.provider_id, observation.identity, observation.kind), observation)
            for key, observation in tuple(self._pending.items()):
                await self._events.append(observation)
                identity = observation.provider_id, observation.identity
                if observation.kind == "expired":
                    self._expired.add(identity)
                else:
                    self._warnings[identity] = now
                del self._pending[key]
