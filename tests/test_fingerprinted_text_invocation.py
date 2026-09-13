import asyncio
from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest

from llm_gateway.adapters.text_fingerprint_projection import TextFingerprintProjection
from llm_gateway.application.fingerprinted_text_invocation import FingerprintedTextInvocation
from llm_gateway.application.model_api import ModelInvocationRejected
from llm_gateway.application.admission_controlled_invocation import AdmissionControlledInvocation
from llm_gateway.application.admission_capacity import AdmissionCapacity
from llm_gateway.application.api_rate import ApiRate
from llm_gateway.domain.invocation import InvocationPersistenceUnavailable
from llm_gateway.domain.model import ProviderResult, TextOutput, Usage
from tests.test_fingerprint_leases import setup
from tests.test_text_fingerprints import QUERY, AUTH, LIMITS, snapshot


QUERY = replace(QUERY, body_bytes=100)
RESULT = ProviderResult("general", "model", TextOutput("result"), Usage(2, 3), "stop")


def backend(keys, configuration, admission, execution, **changes):
    arguments = dict(keys=keys, configuration=configuration,
        fingerprints=TextFingerprintProjection(resource_ceilings=LIMITS), admission=admission,
        execution=execution, max_invocation_seconds=120, trace_id=lambda: "1"*32)
    return FingerprintedTextInvocation(**(arguments | changes))


def test_request_fingerprint_precedes_snapshot_and_material_is_closed_before_execution():
    async def run():
        keys, source, _ = setup()
        assert await keys.validate_active()
        selected = snapshot()
        phases = []
        class Configuration:
            @property
            def current(self):
                assert phases == ["request"]
                phases.append("snapshot")
                return selected
        projection = TextFingerprintProjection(resource_ceilings=LIMITS)
        class Fingerprints:
            def request(self, *args):
                phases.append("request")
                return projection.request(*args)
            def execution(self, *args):
                phases.append("execution_identity")
                return projection.execution(*args)
        class Admission:
            async def admit(self, record, request, execution, fence, **kwargs):
                phases.append("admit")
                assert keys.in_use == 1 and not source.materials[-1]._closed
                assert request.key_version == execution.key_version == fence.key_version
                async with kwargs["security_scope"](fence) as check:
                    check(fence)
                return datetime(2026, 1, 1, tzinfo=timezone.utc)
        class Execution:
            async def execute(self, record, query, received, *, routing_seed, deadline):
                phases.append("execute_settle")
                assert received is selected and keys.in_use == 0
                assert all(material._closed for material in source.materials)
                assert len(routing_seed) == 64 and deadline > asyncio.get_running_loop().time()
                return RESULT
        result = await backend(keys, Configuration(), Admission(), Execution(), fingerprints=Fingerprints()).invoke_authorized(QUERY, AUTH)
        assert result.result == RESULT
        assert phases == ["request", "snapshot", "execution_identity", "admit", "execute_settle"]
        assert len(source.calls) == 2
    asyncio.run(run())


@pytest.mark.parametrize("fault", ["key", "snapshot", "admission", "execution"])
def test_failures_do_not_repeat_admission_or_leak_material(fault):
    async def run():
        keys, source, _ = setup()
        assert await keys.validate_active()
        calls = []
        class Admission:
            async def admit(self, *args, **kwargs):
                calls.append("admit")
                if fault == "admission":
                    raise InvocationPersistenceUnavailable()
                return datetime.now(timezone.utc)
        class Execution:
            async def execute(self, *args, **kwargs):
                calls.append("execute")
                raise InvocationPersistenceUnavailable()
        def call_id():
            calls.append("call_id")
            return uuid4()
        if fault == "key":
            source.fail.add(source.calls[0].identity)
        configuration = SimpleNamespace(current=None if fault == "snapshot" else snapshot())
        with pytest.raises((InvocationPersistenceUnavailable, ModelInvocationRejected)):
            await backend(keys, configuration, Admission(), Execution(), call_id_factory=call_id).invoke_authorized(QUERY, AUTH)
        assert calls == ([] if fault in ("key", "snapshot") else ["call_id", "admit"] if fault == "admission" else ["call_id", "admit", "execute"])
        assert keys.in_use == 0 and all(material._closed for material in source.materials)
    asyncio.run(run())


@pytest.mark.parametrize("field,value,code", [("body_bytes", 4194305, "request_too_large"),
    ("requested_model", "absent", "invalid_request")])
def test_locked_limits_and_alias_validation_prevent_call_creation(field, value, code):
    async def run():
        keys, _, _ = setup()
        assert await keys.validate_active()
        def must_not_allocate():
            pytest.fail("preflight failure allocated Invocation identity")
        service = backend(keys, SimpleNamespace(current=snapshot()), None, None, call_id_factory=must_not_allocate)
        with pytest.raises(ModelInvocationRejected) as error:
            await service.invoke_authorized(replace(QUERY, **{field: value}), AUTH)
        assert error.value.code == code and keys.in_use == 0
    asyncio.run(run())


@pytest.mark.parametrize("cancel", [False, True])
def test_outer_permit_spans_execution_but_key_material_does_not_and_deadline_is_not_reset(cancel):
    async def run():
        keys, source, _ = setup()
        assert await keys.validate_active()
        capacity, entered = AdmissionCapacity(), asyncio.Event()
        selected = snapshot()
        configuration = SimpleNamespace(current=selected)
        class Admission:
            async def admit(self, *args, **kwargs):
                assert capacity.in_use == 1
                configuration.current = None  # concurrent publication/recovery does not change the locked value
                return datetime.now(timezone.utc)
        class Execution:
            async def execute(self, record, query, snapshot, *, routing_seed, deadline):
                assert snapshot is selected and capacity.in_use == 1 and keys.in_use == 0
                assert all(material._closed for material in source.materials)
                assert 0 < deadline-asyncio.get_running_loop().time() <= .05
                entered.set()
                await asyncio.Future()
        async def authorize(query):
            assert capacity.in_use == 0
            return AUTH
        inner = backend(keys, configuration, Admission(), Execution(), max_invocation_seconds=.05)
        service = AdmissionControlledInvocation(capacity=capacity, api_rate=ApiRate(), backend=inner, authorize=authorize)
        task = asyncio.create_task(service.invoke(QUERY))
        await entered.wait()
        if cancel:
            task.cancel()
        with pytest.raises(asyncio.CancelledError if cancel else TimeoutError):
            await task
        assert capacity.in_use == keys.in_use == 0
    asyncio.run(run())
