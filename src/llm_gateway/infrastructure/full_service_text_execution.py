"""PostgreSQL-backed execution for the full-service-only synchronous text slice.

Not a production Model-service composition root. A caller must preflight the
supported policy before admission; execute repeats that guard defensively.
Runtime construction and initial checks are mandatory deployment dependencies.
No cache/degradation policy is silently discarded, and no gate is allow-all.
"""

import asyncio
import json
import math
from contextlib import asynccontextmanager
from dataclasses import dataclass

from llm_gateway.adapters.text_routing_plan import prepare_text_routing, text_routing_requirements
from llm_gateway.application.attempt_execution import AttemptJournal, CandidateRuntime, SynchronousAttemptExecutor
from llm_gateway.application.attempt_control import AttemptExecutionControl
from llm_gateway.application.request_trace import trace_stage, TraceStage
from llm_gateway.application.admitted_execution import AdmittedExecutionLifecycle
from llm_gateway.application.cancellation import LocalCancellationReason
from llm_gateway.application.synchronous_invocation import SynchronousInvocation
from llm_gateway.application.streaming_invocation import StreamingInvocation
from llm_gateway.application.streaming_attempt_execution import StreamingAttemptExecutor, SharedStreamAttemptJournal
from llm_gateway.infrastructure.stream_commit import PostgresStreamCommit
from llm_gateway.infrastructure.routing_evidence import PostgresRoutingEvidence
from llm_gateway.infrastructure.settlement import PostgresInvocationSettlement
from llm_gateway.infrastructure.local_cancellation import PostgresLocalCancellationStore
from llm_gateway.infrastructure.late_provider_outcome import PostgresLateProviderOutcome
from llm_gateway.infrastructure.cancellation_settlement import settle_deadline_exceeded


def require_full_service_text_policy(snapshot, query):
    """Call before admission when selecting this deliberately limited backend."""
    content = json.loads(snapshot.snapshot_json)
    alias = content["model_aliases"][query.requested_model]
    degradation = content["routing_policies"][alias["routing_policy"]]["degradation"]
    if degradation["reduced_service"]["enabled"] or degradation["cache"]["enabled"]:
        raise ValueError("Full-service text backend does not support degradation or cache")


@dataclass(frozen=True)
class TextAttemptResources:
    """One Invocation's runtime and journal, including required stage wrappers.

    The factory receives the durable journal and bound rejection callback.
    It must wrap that journal for stages such as Circuit observation, not
    replace it with an in-memory journal. Runtime leases span each Attempt.
    """

    runtime: CandidateRuntime
    journal: AttemptJournal


class _CheckpointedRuntime:
    def __init__(self, runtime, evidence, call_id):
        self._runtime, self._evidence, self._call_id = runtime, evidence, call_id

    @asynccontextmanager
    async def acquire(self, binding):
        async with self._runtime.acquire(binding) as provider:
            if provider is not None:
                # Includes lazy transport selection: a retired pool must not
                # leave an allowed checkpoint immediately before Attempt start.
                await self._evidence.gate(self._call_id, binding, ())
            yield provider


class PostgresFullServiceTextExecution:
    def __init__(self, *, store, initial_gates, runtime_factory, classify, draw_jitter, cancellation_context=None):
        self._store, self._initial_gates = store, initial_gates
        self._runtime_factory, self._classify, self._draw_jitter = runtime_factory, classify, draw_jitter
        self._cancellation_context = cancellation_context
        self._controls = set()

    def abort_local(self):
        """Ownership-loss emergency: stop local I/O, not a durable cancel claim."""
        for control in tuple(self._controls):
            try:
                control.stop(LocalCancellationReason.CONTEXT_CANCELLED)
            except Exception:
                # Continue aborting other invocations. The process also cancels
                # every owning task and remains permanently non-ready.
                pass

    async def execute(self, admission, query, snapshot, *, routing_seed, deadline):
        if type(deadline) not in (int, float) or not math.isfinite(deadline):
            raise ValueError("Finite monotonic deadline required")
        if admission.configuration_revision != snapshot.revision or admission.requested_model != query.requested_model:
            raise ValueError("Execution must use its admitted Snapshot and Alias")
        require_full_service_text_policy(snapshot, query)
        control = AttemptExecutionControl(admission.call_id,
            late_outcomes=PostgresLateProviderOutcome(self._store, admission.call_id))

        async def settle_deadline(record, *, deadline):
            await settle_deadline_exceeded(self._store, record, deadline=deadline,
                observation=control.stopped_observation)

        lifecycle = AdmittedExecutionLifecycle(cancellation_store=PostgresLocalCancellationStore(self._store),
                                                settle_deadline=settle_deadline)
        self._controls.add(control)
        try:
            return await lifecycle.run(admission, control=control, deadline=deadline,
                cancellation_context=self._cancellation_context,
                work=lambda: self._execute(admission, query, snapshot, routing_seed=routing_seed, deadline=deadline, control=control))
        finally:
            self._controls.remove(control)

    async def _execute(self, admission, query, snapshot, *, routing_seed, deadline, control):
        loop = asyncio.get_running_loop()

        def check_deadline():
            if loop.time() >= deadline:
                raise TimeoutError()

        async with asyncio.timeout_at(deadline):
            check_deadline()
            with trace_stage(TraceStage.ROUTING):
                requirement, assessed = text_routing_requirements(snapshot, query)
                # Read-only initial eligibility, not Attempt leases or QPS spending.
                # The plan validates exact coverage of all statically eligible IDs.
                rejections = await self._initial_gates(admission, snapshot, requirement, assessed, deadline=deadline)
                check_deadline()
                plan = prepare_text_routing(snapshot, query, seed_hex=routing_seed, initial_rejections=rejections)
                evidence = PostgresRoutingEvidence(self._store)
                await evidence.preselect(admission.call_id, plan.preselection)
                check_deadline()

            async def reject(binding, reasons):
                if not reasons:
                    raise ValueError("Rejected Candidate requires actual reasons")
                await evidence.gate(admission.call_id, binding, reasons)

            resources = self._runtime_factory(admission, snapshot,
                journal=self._store.journal(admission.call_id), reject=reject)
            if not isinstance(resources, TextAttemptResources):
                raise TypeError("Invocation-bound text Attempt resources required")
            if query.stream:
                executor = StreamingAttemptExecutor(
                    runtime=_CheckpointedRuntime(resources.runtime, evidence, admission.call_id),
                    journal=SharedStreamAttemptJournal(resources.journal), output=query.stream_output,
                    commit=PostgresStreamCommit(self._store, admission.call_id),
                    classify=self._classify, draw_jitter=self._draw_jitter, control=control)
                settlement = (PostgresInvocationSettlement(self._store, admission.call_id)
                              if query.output_format is None else
                              PostgresInvocationSettlement(self._store, admission.call_id, structured_output=True))
                return await StreamingInvocation(executor, settlement).execute(
                        plan.full, policy=plan.retry, deadline=deadline)
            executor = SynchronousAttemptExecutor(
                runtime=_CheckpointedRuntime(resources.runtime, evidence, admission.call_id),
                journal=resources.journal, classify=self._classify, draw_jitter=self._draw_jitter, control=control)
            settlement = (PostgresInvocationSettlement(self._store, admission.call_id)
                          if query.output_format is None else
                          PostgresInvocationSettlement(self._store, admission.call_id, structured_output=True))
            return await SynchronousInvocation(executor, settlement).execute(
                    plan.full, policy=plan.retry, deadline=deadline)
