import asyncio
import json
from contextlib import asynccontextmanager, closing, ExitStack
from dataclasses import replace

import httpx
import psycopg
import pytest

from llm_gateway.adapters.model_http import create_development_model_app
from llm_gateway.adapters.credentialed_runtime import CredentialBinding, CredentialedCandidateRuntime
from llm_gateway.adapters.text_completion_projection import prepare_text_completion
from llm_gateway.adapters.capacity_projection import project_attempt_capacity
from llm_gateway.application.attempt_capacity import AttemptCapacity
from llm_gateway.application.admission_capacity import AdmissionCapacity
from llm_gateway.application.api_rate import ApiRate
from llm_gateway.application.admission_controlled_invocation import AdmissionControlledInvocation
from llm_gateway.adapters.provider_rate_projection import project_provider_rate
from llm_gateway.application.provider_rate import ProviderRate, ProviderRateStage
from llm_gateway.application.circuit_runtime import CircuitCoordinator, CircuitInvocationStage
from llm_gateway.application.provider_circuits import ProviderCircuits
from llm_gateway.application.active_configuration import LoadedConfiguration
from llm_gateway.application.attempt_execution import ExecutionCandidate
from llm_gateway.application.model_api import InvocationReply
from llm_gateway.application.synchronous_invocation import SynchronousInvocation
from llm_gateway.domain.configuration_changes import SnapshotResources
from llm_gateway.domain.recovery import RetryPolicy
from llm_gateway.domain.routing_eligibility import StaticRoutingReason
from llm_gateway.infrastructure.settlement import PostgresInvocationSettlement
from llm_gateway.infrastructure.credential_resolution import AsyncCredentialResolver
from llm_gateway.infrastructure.routing_evidence import PostgresRoutingEvidence
from llm_gateway.infrastructure.circuit_evidence import PostgresCircuitEvidence
from tests.test_attempt_execution import Harness
from tests.test_configuration import database, run, scalar
from tests.test_invocation_postgres import setup, install_runtime_gate
from tests.test_model_http import PAYLOAD
from tests.test_development_secrets import environment_source, record as credential_record
from tests.test_routing_evidence import preselection


@pytest.mark.postgres
@pytest.mark.parametrize("retry,fail_settlement,busy", [(False, False, False), (True, False, False),
    (False, True, False), (False, False, True), (False, False, "all"),
    (False, True, "all"), (False, False, "credentials"), (False, False, "circuit"),
    (False, False, "circuit_all"), (False, False, "half_open"),
    (False, False, "half_open_ack_failure"), (False, False, "qps"),
    (False, False, "qps_all"), (True, False, "qps_retry"),
    (False, False, "tenant_admission"), (False, False, "instance_admission"),
    (False, False, "admission_write_failure"), (False, False, "tenant_qps"),
    (False, False, "instance_qps")])
def test_http_adapter_execution_and_durable_settlement_pipeline(database, retry, fail_settlement, busy):
    async def scenario():
        store, record = await setup(database, admit=False)
        admission_capacity = AdmissionCapacity()
        api_rate = ApiRate(clock=lambda: 0)
        if busy in {"tenant_qps", "instance_qps"}:
            for n in range(100 if busy == "instance_qps" else 50):
                context = replace(record.authorization, tenant_id=f"tenant-{n}") if busy == "instance_qps" else record.authorization
                assert api_rate.try_consume(context)
        with psycopg.connect(database) as connection:
            stored = connection.execute("SELECT snapshot FROM config_revision WHERE revision=%s", (int(record.configuration_revision),)).fetchone()[0]
        configuration = LoadedConfiguration(record.configuration_revision, "fixture", json.dumps(stored).encode(), SnapshotResources({}))
        capacity = AttemptCapacity()
        capacity_limits = project_attempt_capacity(configuration, ("a", "b", "c"))
        async def capacity_rejected(binding, reasons):
            await PostgresRoutingEvidence(store).gate(record.call_id, binding, reasons)
        rate = ProviderRate(clock=lambda: 0)
        rate_limits = project_provider_rate(configuration, ("a", "b", "c"))
        if busy in {"qps", "qps_all", "qps_retry"}:
            for name in (("a", "b") if busy == "qps_all" else ("a",)):
                for _ in range(1 if busy == "qps_retry" else 2):
                    assert rate.try_consume(rate_limits[name])
        capacity_stage = ProviderRateStage(capacity=capacity, rate=rate, limits=rate_limits, reject=capacity_rejected)
        circuits = ProviderCircuits()
        # Historical Circuit state is a fixture; subsequent gate transitions
        # and current Attempt observations use the real coordinator and store.
        if busy in {"circuit", "circuit_all", "half_open", "half_open_ack_failure"}:
            for name in (("a", "b", "c") if busy == "circuit_all" else ("a",)):
                for _ in range(5):
                    circuits.finish(name, circuits.acquire(name, 0).permit, False, 0)
        coordinator = CircuitCoordinator(circuits=circuits, evidence=PostgresCircuitEvidence(store),
            clock=lambda: 30 if busy in {"half_open", "half_open_ack_failure"} else 0)
        circuit_stage = CircuitInvocationStage(coordinator=coordinator, revision=record.configuration_revision,
            call_id=record.call_id, reject=capacity_rejected)
        upstream = []
        def handler(request):
            assert admission_capacity.in_use == 1
            assert api_rate._instance.credit == 99_000_000_000
            upstream.append(request)
            assert request.headers["Authorization"] == "Bearer test-only"
            assert json.loads(request.content)["messages"] == PAYLOAD["messages"]
            assert json.loads(request.content)["temperature"] == 0.25
            assert json.loads(request.content)["top_p"] == 0.8
            assert scalar(database, "SELECT count(*) FROM provider_attempt") == len(upstream)
            binding = scalar(database, "SELECT binding_id FROM provider_attempt ORDER BY number DESC LIMIT 1")
            assert capacity._bindings[binding] == 1
            assert binding == ("b" if busy is True or busy in {"circuit", "qps"}
                               or busy == "qps_retry" and len(upstream) == 2 else "a")
            if busy == "half_open":
                assert scalar(database, "SELECT transition->>'new_state' FROM circuit_transition_evidence") == "half_open"
            if retry and len(upstream) == 1:
                return httpx.Response(503)
            return httpx.Response(200, json={"object": "chat.completion", "model": "model",
                "choices": [{"index": 0, "message": {"role": "assistant", "content": "done"}, "finish_reason": "stop"}],
                "usage": {"prompt_tokens": 3, "completion_tokens": 2, "total_tokens": 5}})
        if fail_settlement:
            with psycopg.connect(database) as connection:
                connection.execute("ALTER TABLE invocation_settlement ADD CONSTRAINT reject_http_test_settlement CHECK(false)")
        if busy == "admission_write_failure":
            with psycopg.connect(database) as connection:
                connection.execute("ALTER TABLE model_invocation ADD CONSTRAINT reject_test_admission CHECK(false)")
        if busy == "half_open_ack_failure":
            with psycopg.connect(database) as connection:
                connection.execute("ALTER TABLE circuit_transition_evidence ADD CONSTRAINT reject_test_transition CHECK(false)")
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False) as provider_client:
            harness = Harness([])
            install_runtime_gate(harness, store, record)
            executor = harness.executor()
            executor._journal = circuit_stage.journal(store.journal(record.call_id))
            async def backoff(seconds):
                assert not capacity._leases
                assert admission_capacity.in_use == 1
                assert api_rate._instance.credit == 99_000_000_000
                await harness.sleep(seconds)
            executor._sleep = backoff
            @asynccontextmanager
            async def gates(binding_id, metadata):
                assert metadata.secret_ref == "provider-key"
                async with circuit_stage.acquire(binding_id) as circuit_allowed:
                    if not circuit_allowed:
                        yield False
                        return
                    async with capacity_stage.acquire(binding_id) as available:
                        if not available:
                            yield False
                            return
                        async with harness.acquire(binding_id) as candidate:
                            yield candidate is not None
            async def reject(binding_id, code):
                await PostgresRoutingEvidence(store).gate(record.call_id, binding_id, (StaticRoutingReason(code),))
            source = environment_source({} if busy == "credentials" else {"EXPLICIT_RECORD": credential_record(value="test-only")})
            with closing(AsyncCredentialResolver(source, max_concurrent_reads=1)) as resolver:
                executor._runtime = CredentialedCandidateRuntime(source=resolver, gates=gates, reject=reject,
                    bindings={name: CredentialBinding("provider-key", "https://provider.invalid/v1", provider_client) for name in ("a", "b", "c")})
                invocation = SynchronousInvocation(executor, PostgresInvocationSettlement(store, record.call_id))
                class Service:
                    async def invoke_authorized(self, query, authorization):
                        assert admission_capacity.in_use == 1
                        assert authorization is record.authorization
                        await store.admit_unkeyed_shell(record)
                        await PostgresRoutingEvidence(store).preselect(record.call_id, preselection())
                        candidates = tuple(ExecutionCandidate(name, prepare_text_completion(configuration, query, name).request)
                                           for name in ("a", "b", "c"))
                        result = await invocation.execute(candidates, policy=RetryPolicy(),
                                                          deadline=asyncio.get_running_loop().time() + 10)
                        assert admission_capacity.in_use == 1
                        assert scalar(database, "SELECT state FROM model_invocation") in {"completed", "failed"}
                        accepted_at = scalar(database, "SELECT accepted_at FROM model_invocation")
                        return InvocationReply(record.call_id, accepted_at, result)
                async def authorize(query):
                    return record.authorization  # Trusted fixture, not HTTP credential authentication.
                service = AdmissionControlledInvocation(capacity=admission_capacity, api_rate=api_rate, backend=Service(), authorize=authorize)
                with ExitStack() as occupied:
                    if busy in {"tenant_admission", "instance_admission"}:
                        for tenant in range(4 if busy == "instance_admission" else 1):
                            context = replace(record.authorization, tenant_id=f"other-{tenant}") if busy == "instance_admission" else record.authorization
                            for _ in range(50):
                                occupied.enter_context(admission_capacity.acquire(context))
                    if busy is True or busy == "all":
                        occupied.enter_context(capacity.try_acquire(capacity_limits["a"]))
                    if busy == "all":
                        occupied.enter_context(capacity.try_acquire(capacity_limits["b"]))
                    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_development_model_app(service)),
                                                 base_url="http://gateway") as client:
                        response = await client.post("/v1/chat/completions", json=PAYLOAD | {"temperature": 0.25, "top_p": 0.8},
                                                     headers={"Authorization": "caller-not-forwarded"})
        assert not harness.leased
        assert admission_capacity.in_use == 0 and not admission_capacity._tenants
        if busy in {"tenant_admission", "instance_admission"}:
            assert api_rate._instance is None
        elif busy == "tenant_qps":
            assert api_rate._instance.credit == 50_000_000_000
        elif busy == "instance_qps":
            assert api_rate._instance.credit == 0
        else:
            assert api_rate._instance.credit == 99_000_000_000
        assert not coordinator._leases and not circuit_stage._active
        assert not capacity._leases and not capacity._providers and not capacity._bindings
        if busy in {"tenant_admission", "instance_admission", "admission_write_failure", "tenant_qps", "instance_qps"}:
            assert not upstream
            assert response.status_code == (503 if busy == "admission_write_failure" else 429)
            assert response.json()["error"]["code"] == ("persistence_unavailable" if busy == "admission_write_failure" else "rate_limited")
            assert "X-Gateway-Call-Id" not in response.headers
            assert scalar(database, "SELECT count(*) FROM model_invocation") == 0
            assert scalar(database, "SELECT count(*) FROM provider_attempt") == 0
            assert scalar(database, "SELECT count(*) FROM cost_accrual") == 0
            return
        if busy == "half_open_ack_failure":
            assert coordinator._pending and not upstream
            assert response.status_code == 503
            assert response.json()["error"]["code"] == "persistence_unavailable"
            assert scalar(database, "SELECT count(*) FROM provider_attempt") == 0
            assert scalar(database, "SELECT count(*) FROM circuit_transition_evidence") == 0
            assert scalar(database, "SELECT state FROM model_invocation") == "accepted"
            return
        assert not coordinator._pending
        if busy in {"all", "credentials", "circuit_all", "qps_all"}:
            assert not upstream
            expected = "provider_credentials_unavailable" if busy == "credentials" else "provider_unavailable" if busy == "circuit_all" else "rate_limited"
            assert response.json()["error"]["code"] == ("persistence_unavailable" if fail_settlement else expected)
            assert response.status_code == (503 if fail_settlement or busy in {"credentials", "circuit_all"} else 429)
            assert "usage" not in response.json()
            assert scalar(database, "SELECT count(*) FROM provider_attempt") == 0
            assert scalar(database, "SELECT count(*) FROM cost_accrual") == 0
            assert scalar(database, "SELECT count(*) FROM invocation_cost_summary") == 0
            assert scalar(database, "SELECT state FROM model_invocation") == ("accepted" if fail_settlement else "failed")
            assert scalar(database, "SELECT count(*) FROM routing_terminal") == (0 if fail_settlement else 1)
            if not fail_settlement:
                assert scalar(database, "SELECT attempt_count FROM invocation_settlement") == 0
                assert scalar(database, "SELECT input_tokens FROM invocation_settlement") is None
            return
        assert len(upstream) == (2 if retry else 1)
        if busy is True or busy in {"circuit", "qps", "qps_retry"}:
            assert scalar(database, "SELECT detail FROM routing_event WHERE kind='candidate_skipped'") == {
                "decision": "skipped", "reasons": [{"code": "circuit_open" if busy == "circuit" else
                    "qps_exhausted" if busy in {"qps", "qps_retry"} else "concurrency_exhausted", "subject": None}]}
        if fail_settlement:
            assert response.status_code == 503
            assert response.json()["error"]["code"] == "persistence_unavailable"
            assert "done" not in response.text and "usage" not in response.text
            assert scalar(database, "SELECT state FROM model_invocation") == "running"
        else:
            assert response.status_code == 200
            assert response.json()["choices"][0]["message"]["content"] == "done"
            assert scalar(database, "SELECT state FROM model_invocation") == "completed"
            if retry:
                # Unknown first-Attempt Usage cannot be hidden behind last-Attempt counts.
                assert "usage" not in response.json()
                assert scalar(database, "SELECT input_tokens FROM invocation_settlement") is None
            else:
                assert response.json()["usage"] == {"prompt_tokens": 3, "completion_tokens": 2, "total_tokens": 5}
    run(scenario())
