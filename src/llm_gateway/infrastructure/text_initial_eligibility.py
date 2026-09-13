"""Initial text eligibility without consuming Attempt resource allowances.

Credential and security leases are closed before ordering. These observations
are not reservations: the Invocation runtime must recheck every Attempt.
"""

import asyncio
import json
import math

from llm_gateway.adapters.provider_credentials import CredentialUnavailable
from llm_gateway.adapters.provider_rate_projection import project_provider_rate
from llm_gateway.domain.routing_evidence import ordered_reasons
from llm_gateway.domain.routing_eligibility import StaticRoutingReason


class TextInitialEligibility:
    def __init__(self, *, credential_source, security_gates, circuits, capacity, rate):
        self._source, self._security_gates = credential_source, security_gates
        self._circuits, self._capacity, self._rate = circuits, capacity, rate

    async def __call__(self, admission, snapshot, requirement, assessed, *, deadline):
        if admission.configuration_revision != snapshot.revision:
            raise ValueError("Initial eligibility must use its admitted Snapshot")
        if type(deadline) not in (int, float) or not math.isfinite(deadline):
            raise ValueError("Finite monotonic deadline required")
        loop = asyncio.get_running_loop()

        def check_deadline():
            if loop.time() >= deadline:
                raise TimeoutError()

        async with asyncio.timeout_at(deadline):
            check_deadline()
            bindings = tuple(item.candidate.routing.binding for item in assessed if item.statically_eligible)
            limits = project_provider_rate(snapshot, bindings)
            content = json.loads(snapshot.snapshot_json)
            results = {}
            for binding in bindings:
                check_deadline()
                provider = content["providers"][limits[binding].capacity.provider]
                reference = provider["credential"]["secret_ref"]
                try:
                    lease = await self._source.resolve(reference)
                except CredentialUnavailable:
                    results[binding] = (StaticRoutingReason("provider_credentials_unavailable"),)
                    continue
                with lease:
                    check_deadline()
                    if lease.metadata.secret_ref != reference or lease.metadata.revoked:
                        results[binding] = (StaticRoutingReason("provider_credentials_unavailable"),)
                        continue
                    async with self._security_gates(admission, snapshot, binding, lease.metadata) as security:
                        reasons = list(ordered_reasons(security, requirement=requirement))
                        check_deadline()
                        circuit = self._circuits.rejection(binding)
                        if circuit is not None:
                            reasons.append(StaticRoutingReason(circuit))
                        if not self._capacity.available(limits[binding].capacity):
                            reasons.append(StaticRoutingReason("concurrency_exhausted"))
                        if not self._rate.available(limits[binding]):
                            reasons.append(StaticRoutingReason("qps_exhausted"))
                        results[binding] = ordered_reasons(tuple(reasons), requirement=requirement)
                check_deadline()
            check_deadline()
            return results
