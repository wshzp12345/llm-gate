"""One-instance assembly of the synchronous full-service text backend.

Not the full v0.1 launcher: the owner supplies validated configuration/key and
transport lifecycles, request-context authorization/trace, and real health
observations. Construction performs no I/O, reads no environment and starts
no listener. Reduced/cache policy is rejected before durable admission.
"""

import asyncio

from llm_gateway.adapters.text_fingerprint_projection import TextFingerprintProjection
from llm_gateway.adapters.text_security_eligibility import TextSecurityEligibility
from llm_gateway.application.admission_capacity import AdmissionCapacity
from llm_gateway.application.admission_controlled_invocation import AdmissionControlledInvocation
from llm_gateway.application.api_rate import ApiRate
from llm_gateway.application.attempt_capacity import AttemptCapacity
from llm_gateway.application.circuit_runtime import CircuitCoordinator
from llm_gateway.application.cancellation import LocalCancellationReason
from llm_gateway.application.fingerprinted_text_invocation import FingerprintedTextInvocation
from llm_gateway.application.provider_circuits import ProviderCircuits
from llm_gateway.application.provider_rate import ProviderRate
from llm_gateway.application.text_failure_recovery import classify_text_failure
from llm_gateway.infrastructure.circuit_evidence import PostgresCircuitEvidence
from llm_gateway.infrastructure.fingerprint_admission import PostgresFingerprintAdmission
from llm_gateway.infrastructure.full_service_text_execution import PostgresFullServiceTextExecution, require_full_service_text_policy
from llm_gateway.infrastructure.text_attempt_runtime import TextAttemptRuntimeFactory
from llm_gateway.infrastructure.text_initial_eligibility import TextInitialEligibility


class _FullServiceFingerprints(TextFingerprintProjection):
    def execution(self, query, snapshot, lease):
        resolved = super().execution(query, snapshot, lease)
        require_full_service_text_policy(snapshot, query)
        return resolved


class FullServiceTextBackend:
    def __init__(self, *, store, keys, configuration, credential_source, transports,
                 health, authorize, trace_id, resource_ceilings, draw_jitter, allow_plaintext=False, prompts=None, recovery=None):
        self._capacity = AdmissionCapacity()
        self._tasks = set()
        self._shutdown_deadline = None
        attempt_capacity, rate = AttemptCapacity(), ProviderRate()
        self._attempt_capacity = attempt_capacity
        circuits = CircuitCoordinator(circuits=ProviderCircuits(), evidence=PostgresCircuitEvidence(store))
        security = TextSecurityEligibility(transports=transports, health=health, allow_plaintext=allow_plaintext)
        initial = TextInitialEligibility(credential_source=credential_source, security_gates=security,
                                         circuits=circuits, capacity=attempt_capacity, rate=rate)
        runtime = TextAttemptRuntimeFactory(credential_source=credential_source, transports=transports,
            circuits=circuits, capacity=attempt_capacity, rate=rate, security_gates=security)
        execution = PostgresFullServiceTextExecution(store=store, initial_gates=initial,
            runtime_factory=runtime, classify=classify_text_failure, draw_jitter=draw_jitter,
            cancellation_context=self._cancellation_context)
        self._execution = execution
        workflow = FingerprintedTextInvocation(keys=keys, configuration=configuration,
            fingerprints=_FullServiceFingerprints(resource_ceilings=resource_ceilings),
            admission=PostgresFingerprintAdmission(store), execution=execution,
            max_invocation_seconds=resource_ceilings["max_invocation_seconds"], trace_id=trace_id, prompts=prompts, recovery=recovery)
        self._service = AdmissionControlledInvocation(capacity=self._capacity, api_rate=ApiRate(),
                                                      backend=workflow, authorize=authorize)

    @property
    def in_use(self):
        return self._capacity.in_use

    def resource_metrics(self):
        return {**self._capacity.metrics(), **self._attempt_capacity.metrics()}

    def stop_admission(self):
        self._capacity.stop_admission()

    def _cancellation_context(self, deadline):
        if self._shutdown_deadline is not None:
            return LocalCancellationReason.SHUTDOWN_DRAIN_EXPIRED, min(deadline, self._shutdown_deadline)
        return LocalCancellationReason.CONTEXT_CANCELLED, deadline

    def cancel_pending(self, *, shutdown_deadline=None, ownership_lost=False):
        self.stop_admission()
        if shutdown_deadline is not None:
            self._shutdown_deadline = shutdown_deadline
        if ownership_lost:
            self._execution.abort_local()
        for task in tuple(self._tasks):
            if not task.done() and not task.cancelling():
                task.cancel()

    async def invoke(self, query):
        task = asyncio.current_task()
        self._tasks.add(task)
        try:
            return await self._service.invoke(query)
        finally:
            self._tasks.remove(task)
