"""Authorized new text-call workflow; no key bytes cross into execution.

Wrap this with AdmissionControlledInvocation. Idempotency/replay and the complete
request surface are separate paths, never inferred from this text-only backend.
"""

import asyncio
import math
from dataclasses import dataclass, replace
from typing import Protocol
from uuid import uuid4

from llm_gateway.application.active_configuration import LoadedConfiguration
from llm_gateway.application.fingerprint_keys import FingerprintKeys, FingerprintLease
from llm_gateway.application.model_api import InvocationReply, ModelInvocationRejected, TextInvocationQuery
from llm_gateway.application.model_rate import ModelRate
from llm_gateway.application.request_trace import current_trace, trace_stage, TraceStage
from llm_gateway.domain.fingerprints import FingerprintIdentity
from llm_gateway.domain.invocation import AuthorizationContext, InvocationAdmission
from llm_gateway.domain.correlation import BusinessCorrelation
from llm_gateway.domain.model import ProviderFailure, ProviderResult
from llm_gateway.domain.routing_failure import UnattemptedRoutingFailure


@dataclass(frozen=True)
class TextExecutionIdentity:
    fingerprint: FingerprintIdentity
    routing_policy: str
    max_invocation_seconds: int
    requests_per_minute: int = 5


class TextFingerprintPort(Protocol):
    def request(self, query: TextInvocationQuery, authorization: AuthorizationContext,
                lease: FingerprintLease) -> FingerprintIdentity: ...

    def execution(self, query: TextInvocationQuery, snapshot: LoadedConfiguration,
                  lease: FingerprintLease) -> TextExecutionIdentity: ...


class SettledTextExecutionPort(Protocol):
    async def execute(self, admission: InvocationAdmission, query: TextInvocationQuery,
                      snapshot: LoadedConfiguration, *, routing_seed: str, deadline: float):
        """Prepare durable routing, execute all gates/Attempts, commit settlement.

        Return only the committed aggregate result; do not acquire Fingerprint
        material, refresh the Snapshot, or reset the supplied absolute deadline.
        """
        ...


class FingerprintedTextInvocation:
    def __init__(self, *, keys: FingerprintKeys, configuration, fingerprints: TextFingerprintPort,
                 admission, execution: SettledTextExecutionPort, max_invocation_seconds: float,
                 trace_id, call_id_factory=uuid4, prompts=None, recovery=None, model_rate=None):
        if (type(max_invocation_seconds) not in (int, float)
                or not math.isfinite(max_invocation_seconds) or max_invocation_seconds <= 0):
            raise ValueError("Positive invocation deadline ceiling required")
        self._keys, self._configuration, self._fingerprints = keys, configuration, fingerprints
        self._admission, self._execution = admission, execution
        self._maximum = max_invocation_seconds
        self._trace_id, self._call_id = trace_id, call_id_factory
        self._prompts = prompts
        self._recovery = recovery
        self._model_rate = model_rate if model_rate is not None else ModelRate()

    async def invoke_authorized(self, query: TextInvocationQuery, authorization: AuthorizationContext) -> InvocationReply:
        loop = asyncio.get_running_loop()
        started = loop.time()
        deadline = started + self._maximum
        async with asyncio.timeout_at(deadline) as timeout:
            if query.prompt is not None:
                if self._prompts is None or query.messages or query.prompt_reference is not None:
                    raise ModelInvocationRejected("invalid_request")
                reference = query.prompt.reference
                with trace_stage(TraceStage.PROMPT):
                    rendered = await self._prompts.render(authorization, reference.asset_id,
                        reference.version_id, dict(query.prompt.variables))
                query = replace(query, messages=rendered.messages, prompt=None, prompt_reference=reference)
            async with self._keys.active(deadline=deadline) as lease:
                with trace_stage(TraceStage.FINGERPRINT):
                    request = self._fingerprints.request(query, authorization, lease)
                snapshot = self._configuration.current
                if snapshot is None:
                    raise ModelInvocationRejected("gateway_not_ready")
                with trace_stage(TraceStage.FINGERPRINT):
                    resolved = self._fingerprints.execution(query, snapshot, lease)
                deadline = min(deadline, started + resolved.max_invocation_seconds)
                timeout.reschedule(deadline)
                if loop.time() >= deadline:
                    raise TimeoutError()
                # Locked model definition, once before identity/admission and
                # before any SSE headers or Provider I/O. No refund on failure.
                if not self._model_rate.try_consume(query.requested_model, resolved.requests_per_minute):
                    raise ModelInvocationRejected("rate_limited")
                trace = current_trace()
                record = InvocationAdmission(self._call_id(), self._trace_id(), snapshot.revision,
                                             query.requested_model, authorization, query.prompt_reference,
                                             trace.correlation if trace is not None else BusinessCorrelation())
                seed = lease.routing_seed(record.call_id, resolved.routing_policy, snapshot.revision)
                with trace_stage(TraceStage.ADMISSION):
                    accepted_at = await self._admission.admit(record, request, resolved.fingerprint, lease.fence,
                        routing_policy=resolved.routing_policy, routing_seed=seed, security_scope=self._keys.security_scope)
                trace = current_trace()
                if trace is not None:
                    trace.admitted(record.call_id)
            # Neither material nor the Fingerprint permit survives into routing I/O.
            if query.stream:
                query.stream_output.admitted(record.call_id, accepted_at)
            result = await self._execution.execute(record, query, snapshot, routing_seed=seed, deadline=deadline)
            if not isinstance(result, (ProviderResult, ProviderFailure, UnattemptedRoutingFailure)):
                raise TypeError("Execution must return a committed model result")
            if loop.time() >= deadline:
                raise TimeoutError()
            recovery = await self._recovery(record.call_id) if self._recovery is not None else None
            return InvocationReply(record.call_id, accepted_at, result, query.prompt_reference, recovery)
