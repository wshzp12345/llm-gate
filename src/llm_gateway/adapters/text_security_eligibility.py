"""Network-free text routing security checks against one admitted Snapshot.

This context grants no socket or permanent security lease. The runtime's lazy
registry acquisition rechecks publication, and EgressNetworkBackend rechecks
whole DNS answers, peer addresses and Trust dates on each new connection.
Health is an explicit observation dependency, never a default allow decision.
"""

import json
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from llm_gateway.adapters.provider_transport_registry import project_provider_transports
from llm_gateway.adapters.trust_bundle import TrustBundleIntegrityValidator
from llm_gateway.domain.routing_eligibility import StaticRoutingReason


class TextSecurityEligibility:
    def __init__(self, *, transports, health, allow_plaintext=False, clock=lambda: datetime.now(timezone.utc)):
        if type(allow_plaintext) is not bool:
            raise ValueError("Explicit plaintext deployment policy required")
        self._transports, self._health = transports, health
        self._allow_plaintext, self._clock = allow_plaintext, clock

    @asynccontextmanager
    async def __call__(self, admission, snapshot, binding, metadata):
        if admission.configuration_revision != snapshot.revision:
            raise ValueError("Security checks must use the admitted Snapshot")
        content = json.loads(snapshot.snapshot_json)
        provider_id = content["provider_model_bindings"][binding]["provider"]
        provider = content["providers"][provider_id]
        plan = project_provider_transports(snapshot)[provider_id]
        certificates = None
        bundle = provider["transport"]["tls"]["trust_bundle"]
        if bundle is not None:
            certificates = TrustBundleIntegrityValidator().certificates(bundle["identity"], bundle["pem"])
            if certificates is None:
                raise ValueError("Invalid published Trust Bundle")

        def local_reasons():
            now = self._clock()
            if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() is None:
                raise ValueError("Aware security observation time required")
            reasons = set()
            if plan.host not in plan.policy.allowed_hosts or plan.scheme == "http" and not self._allow_plaintext:
                reasons.add("egress_policy_violation")
            if not self._transports.eligible(plan):
                reasons.add("security_invalidated")
            if certificates is not None:
                if any(now > cert.not_valid_after_utc for cert in certificates):
                    reasons.add("trust_bundle_expired")
                if any(now < cert.not_valid_before_utc for cert in certificates):
                    reasons.add("security_invalidated")
            if (metadata.secret_ref != provider["credential"]["secret_ref"] or metadata.revoked
                    or metadata.valid_until <= now):
                reasons.add("provider_credentials_unavailable")
            return reasons

        reasons = local_reasons()
        if not reasons:
            healthy = await self._health(admission, snapshot, binding)
            if type(healthy) is not bool:
                raise TypeError("Explicit health eligibility observation required")
            # Time and publication may change while a health observation awaits.
            reasons = local_reasons()
            if not healthy:
                reasons.add("health_unavailable")
        # Downstream Evidence orders the combined reason stages canonically.
        yield tuple(StaticRoutingReason(code) for code in sorted(reasons))
