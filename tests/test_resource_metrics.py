import asyncio
from contextlib import ExitStack
import json

import pytest

from llm_gateway.application.admission_capacity import AdmissionCapacity
from llm_gateway.application.attempt_capacity import AttemptCapacity, AttemptCapacityLimits
from llm_gateway.application.model_api import ModelInvocationRejected
from llm_gateway.application.trace_export import BoundedTraceExporter
from llm_gateway.adapters.otlp_json import payloads
from tests.test_invocation import authorization
from tests.test_trace_export import Reader, Output, access


def test_admission_actual_occupancy_peak_and_rejections_survive_release():
    pool = AdmissionCapacity()
    with ExitStack() as stack:
        for tenant in range(4):
            for _ in range(50):
                stack.enter_context(pool.acquire(authorization(tenant_id=f"private-{tenant}")))
            with pytest.raises(ModelInvocationRejected):
                pool.acquire(authorization(tenant_id=f"private-{tenant}"))
        assert pool.metrics() == {"admission.in_use": 200, "admission.limit": 200, "admission.peak": 200,
            "admission.instance_rejected": 1, "admission.tenant_rejected": 3}
    snapshot = pool.metrics()
    assert snapshot["admission.in_use"] == 0 and snapshot["admission.peak"] == 200
    assert "private" not in repr(snapshot)


def test_attempt_rejection_is_measured_not_inferred_from_availability_check():
    pool = AttemptCapacity()
    limits = AttemptCapacityLimits("private-binding", "private-provider", 1)
    with pool.try_acquire(limits):
        assert not pool.available(limits)
        assert pool.metrics()["attempt.provider_rejected"] == 0
        assert pool.try_acquire(limits) is None
        assert pool.metrics()["attempt.provider_rejected"] == 1
    assert pool.metrics()["attempt.in_use"] == 0 and pool.metrics()["attempt.peak"] == 1
    with ExitStack() as stack:
        limits = AttemptCapacityLimits("b", "p", 100)
        for _ in range(20):
            stack.enter_context(pool.try_acquire(limits))
        assert pool.try_acquire(limits) is None
        assert pool.metrics()["attempt.binding_rejected"] == 1
    assert pool.metrics()["attempt.active_providers"] == pool.metrics()["attempt.active_bindings"] == 0


@pytest.mark.parametrize("broken", [False, True])
def test_export_resource_gauges_do_not_change_model_or_export_ownership(broken):
    async def scenario():
        pool, output = AdmissionCapacity(), Output()
        def read():
            if broken:
                raise RuntimeError("private diagnostic")
            return pool.metrics()
        worker = BoundedTraceExporter(Reader(), output, resource_reader=read)
        with pool.acquire(authorization()):
            async with worker.hold():
                assert worker.offer(access())
            record = output.records[0]
            gauges = dict(record.resource_metrics)
            assert gauges["export.queue_limit"] == 256
            assert gauges["export.queue_used"] == 0
            if broken:
                assert "admission.in_use" not in gauges
            else:
                assert gauges["admission.in_use"] == 1
            metrics = payloads(record, instance_id="test", observed_unix_ns=record.access.started_unix_ns)["metrics"]
            assert "private diagnostic" not in json.dumps(metrics)
            series = metrics["resourceMetrics"][0]["scopeMetrics"][0]["metrics"]
            assert all(item["unit"] == "{item}" for item in series if item["name"].startswith("gateway.resource."))
            assert worker.counters.exported == 1 and pool.in_use == 1
    asyncio.run(scenario())
