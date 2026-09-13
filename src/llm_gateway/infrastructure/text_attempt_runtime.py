"""Invocation runtime assembly with process-owned circuit/capacity/rate state.

Security gates remain an explicit deployment dependency: they must check and
lease the locked Binding's health, Egress, Trust and security eligibility using
the resolved credential metadata. This module never substitutes an allow gate.
The executor owns allowed Evidence; every rejection here uses its bound sink.
"""

import json
from contextlib import asynccontextmanager

from llm_gateway.adapters.credentialed_runtime import CredentialedCandidateRuntime
from llm_gateway.adapters.provider_rate_projection import project_provider_rate
from llm_gateway.adapters.transport_credential_projection import project_transport_credentials
from llm_gateway.application.circuit_runtime import CircuitInvocationStage
from llm_gateway.application.provider_rate import ProviderRateStage
from llm_gateway.domain.routing_eligibility import StaticRoutingReason
from llm_gateway.infrastructure.full_service_text_execution import TextAttemptResources


class TextAttemptRuntimeFactory:
    def __init__(self, *, credential_source, transports, circuits, capacity, rate, security_gates):
        self._source, self._transports = credential_source, transports
        self._circuits, self._capacity, self._rate = circuits, capacity, rate
        self._security_gates = security_gates

    def __call__(self, admission, snapshot, *, journal, reject):
        if admission.configuration_revision != snapshot.revision:
            raise ValueError("Runtime must use its admitted Snapshot")
        content = json.loads(snapshot.snapshot_json)
        binding_ids = tuple(candidate["binding"] for candidate in
                            content["model_aliases"][admission.requested_model]["candidates"]
                            if content["providers"][content["provider_model_bindings"][candidate["binding"]]["provider"]]["status"] == "enabled")
        bindings = project_transport_credentials(snapshot, binding_ids, self._transports)
        circuit = CircuitInvocationStage(coordinator=self._circuits, revision=snapshot.revision,
                                         call_id=admission.call_id, reject=reject)
        rate = ProviderRateStage(capacity=self._capacity, rate=self._rate,
                                 limits=project_provider_rate(snapshot, binding_ids), reject=reject)

        @asynccontextmanager
        async def gates(binding, metadata):
            async with self._security_gates(admission, snapshot, binding, metadata) as reasons:
                if not isinstance(reasons, tuple) or any(not isinstance(reason, StaticRoutingReason) for reason in reasons):
                    raise TypeError("Explicit security gate reason tuple required")
                if reasons:
                    await reject(binding, reasons)
                    yield False
                    return
                async with circuit.acquire(binding) as circuit_allowed:
                    if not circuit_allowed:
                        yield False
                        return
                    async with rate.acquire(binding) as rate_allowed:
                        yield rate_allowed

        async def credential_rejection(binding, code):
            await reject(binding, (StaticRoutingReason(code),))

        runtime = CredentialedCandidateRuntime(bindings=bindings, source=self._source,
                                               gates=gates, reject=credential_rejection)
        # Both wrappers share precisely the same Invocation-local stage. Only
        # durably recorded outcomes reach the process-owned Circuit observer.
        return TextAttemptResources(runtime, circuit.journal(journal))
