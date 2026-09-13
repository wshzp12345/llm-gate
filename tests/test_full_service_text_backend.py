import asyncio
import json
from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace

import httpx
import pytest

from llm_gateway.adapters.provider_transport_registry import ProviderTransportRegistry, project_provider_transports
from llm_gateway.application.model_api import ModelInvocationRejected
from llm_gateway.infrastructure.full_service_text_backend import FullServiceTextBackend
from llm_gateway.infrastructure import full_service_text_backend as assembly
from llm_gateway.infrastructure import full_service_text_execution as execution
from tests.test_fingerprint_leases import setup as keys_setup
from tests.test_text_attempt_runtime import Harness
from tests.test_text_fingerprints import QUERY, AUTH, LIMITS, snapshot
from tests.test_completion import envelope


class Store:
    def __init__(self):
        self.events = []
        self.call_id = None

    def journal(self, call_id):
        assert call_id == self.call_id
        return self

    async def started(self, number, binding, candidate_attempt):
        self.events.append(("start", number))

    async def finished(self, number, result, recovery):
        self.events.append(("finish", recovery.action, result.disposition if hasattr(result, "disposition") else "failed"))


def persistence_doubles(monkeypatch):
    class Admission:
        def __init__(self, store):
            self.store = store

        async def admit(self, record, request, resolved, fence, **kwargs):
            self.store.events.append(("admit",))
            self.store.call_id = record.call_id
            assert request.key_id == resolved.key_id == fence.key_id
            return datetime.now(timezone.utc)

    class Evidence:
        def __init__(self, store):
            self.store = store

        async def preselect(self, call_id, plan):
            self.store.events.append(("preselect",))
            assert len(plan.candidates) == 1

        async def gate(self, call_id, binding, reasons):
            self.store.events.append(("gate", tuple(reason.code for reason in reasons)))

    class Settlement:
        def __init__(self, store, call_id):
            self.store = store

        async def settle(self, result):
            self.store.events.append(("settled",))
            return result.usage

    monkeypatch.setattr(assembly, "PostgresFingerprintAdmission", Admission)
    monkeypatch.setattr(execution, "PostgresRoutingEvidence", Evidence)
    monkeypatch.setattr(execution, "PostgresInvocationSettlement", Settlement)


@pytest.mark.parametrize("mode", ["success", "retry", "refusal", "unsupported", "unauthorized"])
def test_composed_backend_runs_ordered_workflow_and_preflight_guards(monkeypatch, mode):
    persistence_doubles(monkeypatch)

    async def run():
        store, source = Store(), Harness()
        keys, _, _ = keys_setup()
        assert await keys.validate_active()
        selected = snapshot()
        if mode == "unsupported":
            content = json.loads(selected.snapshot_json)
            content["routing_policies"]["route-a"]["degradation"]["cache"]["enabled"] = True
            selected = snapshot(content)
        calls = []

        async def handler(request):
            assert store.events[-1][0] == "start"
            calls.append(request)
            if mode == "retry" and len(calls) == 1:
                return httpx.Response(503)
            body = envelope()
            if mode == "refusal":
                body["choices"][0]["message"] = {"role": "assistant", "content": None, "refusal": "No."}
            return httpx.Response(200, json=body)

        async def authorize(query):
            assert store.events == [] or store.events[-1] == ("settled",)
            if mode == "unauthorized":
                raise ModelInvocationRejected("forbidden")
            return AUTH

        async def health(admission, config, binding):
            assert config is selected and binding == "binding-a"
            return True

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False) as client:
            registry = ProviderTransportRegistry(factory=lambda plan: client)
            await registry.install(selected.revision, project_provider_transports(selected))
            backend = FullServiceTextBackend(store=store, keys=keys, configuration=SimpleNamespace(current=selected),
                credential_source=source, transports=registry, health=health, authorize=authorize,
                trace_id=lambda: "a" * 32, resource_ceilings=LIMITS, draw_jitter=lambda upper: 0)
            query = replace(QUERY, body_bytes=128)
            if mode in {"unsupported", "unauthorized"}:
                with pytest.raises(ValueError if mode == "unsupported" else ModelInvocationRejected):
                    await backend.invoke(query)
                assert store.events == [] and calls == [] and source.events == []
            else:
                reply = await backend.invoke(query)
                assert reply.call_id == store.call_id
                assert reply.result.disposition == ("safety_refused" if mode == "refusal" else "succeeded")
                assert store.events[0:2] == [("admit",), ("preselect",)]
                assert store.events[-1] == ("settled",)
                expected = 2 if mode == "retry" else 1
                assert len(calls) == expected and source.events.count("resolve") == expected + 1
                assert all(not lease._material for lease in source.leases)
                backend.stop_admission()
                with pytest.raises(ModelInvocationRejected, match="gateway_not_ready"):
                    await backend.invoke(query)
            assert backend.in_use == 0

    asyncio.run(run())
