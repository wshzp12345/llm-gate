import asyncio
from contextvars import ContextVar
from dataclasses import replace
from uuid import uuid4

import pytest

from llm_gateway.application.invocation_observation import InvocationObservation
from llm_gateway.application.request_trace import RequestTrace, TraceStage, current_trace
from llm_gateway.application.trace_export import BoundedTraceExporter
from llm_gateway.domain.model import Usage


def access(admitted=False):
    trace = RequestTrace()
    if admitted:
        trace.admitted(uuid4())
    with trace.activate(), trace.span(TraceStage.HTTP):
        pass
    return trace.finish()


def observation(record):
    return InvocationObservation(record.call_id, record.trace_id, "completed", "completed", None,
        "actual", False, (), Usage(), (), record.correlation)


class Reader:
    def __init__(self, result=None):
        self.result, self.calls = result, []
    async def read(self, call_id):
        self.calls.append(call_id)
        return self.result


class Output:
    def __init__(self):
        self.records = []
    async def write(self, record):
        self.records.append(record)


@pytest.mark.parametrize("changes", [{"capacity": 0}, {"capacity": True}, {"capacity": 4097},
    {"operation_timeout": 0}, {"operation_timeout": float("inf")}, {"drain_timeout": float("nan")}])
def test_limits_are_explicit_and_finite(changes):
    with pytest.raises(ValueError):
        BoundedTraceExporter(Reader(), Output(), **changes)


def test_offer_is_bounded_synchronous_and_outside_lifecycle_drops():
    async def scenario():
        reader, output = Reader(), Output()
        exporter = BoundedTraceExporter(reader, output, capacity=2)
        record = access()
        assert not exporter.offer(record)
        async with exporter.hold():
            assert exporter.offer(record) and exporter.offer(record)
            assert not exporter.offer(record)
            assert output.records == [] and reader.calls == []
        assert not exporter.offer(record)
        assert exporter.counters.accepted == 2 and exporter.counters.dropped == 3
        assert exporter.counters.exported == 2 and reader.calls == []
        assert all(item.enrichment == "not_admitted" for item in output.records)
        with pytest.raises(RuntimeError):
            async with exporter.hold():
                pass
    asyncio.run(scenario())


@pytest.mark.parametrize("mode", ["available", "missing", "error", "timeout", "call_id", "trace_id", "correlation"])
def test_enrichment_failure_never_exports_mismatched_invocation(mode):
    async def scenario():
        record = access(True)
        result = observation(record)
        if mode == "call_id":
            result = replace(result, call_id=uuid4())
        if mode == "trace_id":
            result = replace(result, trace_id="a" * 32)
        if mode == "correlation":
            result = replace(result, correlation=replace(record.correlation, task_id="other"))
        class Source:
            async def read(self, call_id):
                assert call_id == record.call_id
                if mode == "error":
                    raise RuntimeError("private-database-error")
                if mode == "timeout":
                    await asyncio.Event().wait()
                return None if mode == "missing" else result
        output = Output()
        exporter = BoundedTraceExporter(Source(), output, operation_timeout=0.01)
        async with exporter.hold():
            assert exporter.offer(record)
        exported, = output.records
        assert exported.access == record
        expected = "available" if mode == "available" else "missing" if mode == "missing" else (
            "unavailable" if mode in {"error", "timeout"} else "identity_mismatch")
        assert exported.enrichment == expected
        assert (exported.invocation is not None) == (mode == "available")
        assert exporter.counters.enrichment_failed == (mode != "available")
        assert "private-database-error" not in repr(exported)
    asyncio.run(scenario())


@pytest.mark.parametrize("timeout", [False, True])
def test_export_error_or_timeout_does_not_retry_or_stop_later_records(timeout):
    async def scenario():
        calls = []
        class FailingOutput:
            async def write(self, record):
                calls.append(record)
                if len(calls) == 1:
                    if timeout:
                        await asyncio.Event().wait()
                    raise RuntimeError("private-export-error")
        exporter = BoundedTraceExporter(Reader(), FailingOutput(), operation_timeout=0.01)
        first, second = access(), access()
        async with exporter.hold():
            assert exporter.offer(first) and exporter.offer(second)
        assert [item.access for item in calls] == [first, second]
        assert exporter.counters.failed == 1 and exporter.counters.exported == 1
    asyncio.run(scenario())


def test_worker_context_is_empty_and_drain_timeout_owns_cancellation():
    async def scenario():
        secret = ContextVar("private-auth", default=None)
        secret.set("private-token")
        entered, stopped = asyncio.Event(), asyncio.Event()
        class HangingOutput:
            async def write(self, record):
                assert secret.get() is None and current_trace() is None
                entered.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    stopped.set()
        exporter = BoundedTraceExporter(Reader(), HangingOutput(), operation_timeout=5, drain_timeout=0.01)
        trace = RequestTrace()
        with trace.activate():
            async with exporter.hold():
                assert exporter.offer(access())
                await asyncio.wait_for(entered.wait(), 1)
                assert exporter.offer(access())
        assert stopped.is_set() and exporter._worker.done() and exporter._queue.empty()
        assert exporter.counters.abandoned == 2
        assert exporter.counters.exported == 0 and exporter.counters.failed == 0
    asyncio.run(scenario())


def test_repeated_shutdown_cancellation_does_not_detach_io():
    async def scenario():
        entered, cleaning, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
        class OutputWithCleanup:
            async def write(self, record):
                entered.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    cleaning.set()
                    await release.wait()
        exporter = BoundedTraceExporter(Reader(), OutputWithCleanup(), drain_timeout=0.01)
        async def owner():
            async with exporter.hold():
                exporter.offer(access())
                await asyncio.Event().wait()
        task = asyncio.create_task(owner())
        await asyncio.wait_for(entered.wait(), 1)
        task.cancel()
        await asyncio.wait_for(cleaning.wait(), 1)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 1)
        assert exporter._worker.done() and exporter.counters.abandoned == 1
    asyncio.run(scenario())


def test_saturated_exporter_does_not_block_or_rewrite_http_errors():
    import httpx
    from llm_gateway.adapters.model_http import create_development_model_app

    async def scenario():
        entered = asyncio.Event()
        class BlockedOutput:
            async def write(self, record):
                entered.set()
                await asyncio.Event().wait()
        reader = Reader()
        exporter = BoundedTraceExporter(reader, BlockedOutput(), capacity=1, drain_timeout=0.01)
        app = create_development_model_app(trace_sink=exporter)
        async with exporter.hold():
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gateway") as client:
                first = await asyncio.wait_for(client.post("/v1/chat/completions", json={}), 1)
                await asyncio.wait_for(entered.wait(), 1)
                responses = [first]
                for _ in range(3):
                    responses.append(await asyncio.wait_for(client.post("/v1/chat/completions", json={}), 1))
                assert all(response.status_code == 400 for response in responses)
                assert len({response.headers["x-request-id"] for response in responses}) == 4
                assert all("x-gateway-call-id" not in response.headers for response in responses)
        assert reader.calls == []
        assert exporter.counters.accepted == 2 and exporter.counters.dropped == 2
        assert exporter.counters.abandoned == 2
    asyncio.run(scenario())
