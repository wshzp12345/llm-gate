import asyncio
from dataclasses import asdict
from uuid import uuid4

import pytest

from llm_gateway.application.request_trace import RequestTrace, TraceStage, current_trace, current_trace_id, trace_stage


def test_exact_monotonic_stage_durations_and_identity():
    times = iter([100, 110, 120, 145, 160])
    trace = RequestTrace(clock=lambda: next(times))
    call_id = uuid4()
    with trace.activate(), trace_stage(TraceStage.HTTP):
        trace.admitted(call_id)
        with trace_stage(TraceStage.ADMISSION):
            assert current_trace_id() == trace.trace_id
    record = trace.finish()
    assert record.call_id == call_id and record.request_id != call_id
    admission, http = record.spans
    assert (admission.start_offset_ns, admission.duration_ns) == (20, 25)
    assert (http.start_offset_ns, http.duration_ns) == (10, 50)
    assert admission.parent_span_id == http.span_id and http.parent_span_id is None
    assert current_trace() is None


def test_parallel_stages_are_siblings_not_shared_stack():
    async def scenario():
        trace = RequestTrace()
        arrived, release = asyncio.Event(), asyncio.Event()
        async def first():
            with trace_stage(TraceStage.PROMPT):
                arrived.set()
                await release.wait()
        async def second():
            await arrived.wait()
            with trace_stage(TraceStage.ROUTING):
                release.set()
        with trace.activate(), trace_stage(TraceStage.HTTP):
            await asyncio.gather(first(), second())
        record = trace.finish()
        root = next(span for span in record.spans if span.stage == TraceStage.HTTP)
        assert all(span.parent_span_id == root.span_id for span in record.spans if span is not root)
    asyncio.run(scenario())


def test_trace_retention_is_bounded_and_drops_do_not_fail_work():
    trace = RequestTrace(capacity=2)
    with trace.activate():
        for _ in range(100):
            with trace_stage(TraceStage.HTTP_SEND):
                pass
    record = trace.finish()
    assert len(record.spans) == 2 and record.dropped_spans == 98


@pytest.mark.parametrize("failure", [RuntimeError("secret-body"), asyncio.CancelledError("secret-body")])
def test_safe_outcome_without_exception_content(failure):
    trace = RequestTrace()
    with pytest.raises(type(failure)), trace.activate(), trace_stage(TraceStage.PROVIDER_ATTEMPT):
        raise failure
    record = trace.finish()
    assert record.spans[0].outcome == ("cancelled" if isinstance(failure, asyncio.CancelledError) else "error")
    assert "secret-body" not in repr(asdict(record))
    assert current_trace() is None


def test_call_id_is_not_fabricated_for_unadmitted_access():
    trace = RequestTrace()
    assert trace.finish().call_id is None
    with pytest.raises(ValueError):
        trace.admitted(uuid4())
    with pytest.raises(ValueError):
        trace.finish()


def test_reused_identity_and_unregistered_attributes_are_rejected():
    trace = RequestTrace()
    trace.admitted(uuid4())
    with pytest.raises(ValueError):
        trace.admitted(uuid4())
    with pytest.raises(ValueError), trace.span("arbitrary-private-text"):
        pass


def test_capacity_reserves_root_before_children_finish():
    trace = RequestTrace(capacity=1)
    with trace.activate(), trace_stage(TraceStage.HTTP):
        with trace_stage(TraceStage.ADMISSION):
            pass
    record = trace.finish()
    assert len(record.spans) == 1 and record.spans[0].stage == TraceStage.HTTP
    assert record.dropped_spans == 1


@pytest.mark.parametrize("sink_fails", [False, True])
def test_pre_admission_http_identity_and_export_failure_isolation(sink_fails):
    import httpx
    from llm_gateway.adapters.model_http import create_development_model_app

    async def scenario():
        records = []
        class Sink:
            def offer(self, record):
                records.append(record)
                if sink_fails:
                    raise RuntimeError("export unavailable")
                return True
        app = create_development_model_app(trace_sink=Sink())
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gateway") as client:
            response = await client.post("/v1/chat/completions", json={})
        assert response.status_code == 400
        assert len(records) == 1 and records[0].call_id is None
        assert str(records[0].request_id) == response.headers["x-request-id"]
        assert "x-gateway-call-id" not in response.headers
        assert records[0].spans[0].stage == TraceStage.HTTP
        assert records[0].spans[0].outcome == "error"
        assert current_trace() is None
    asyncio.run(scenario())


def test_normalized_failure_marks_span_without_throwing():
    trace = RequestTrace()
    with trace.activate(), trace_stage(TraceStage.PROVIDER_ATTEMPT, attempt_number=2) as observed:
        observed.mark_failed()
    span = trace.finish().spans[0]
    assert span.outcome == "error" and span.attempt_number == 2


@pytest.mark.parametrize("number", [0, 4, True, "1"])
def test_attempt_identity_is_bounded(number):
    trace = RequestTrace()
    with pytest.raises(ValueError), trace.span(TraceStage.PROVIDER_ATTEMPT, attempt_number=number):
        pass


def test_exact_send_aggregate_and_first_delta_offsets():
    times = iter([100, 110, 120, 140, 150, 170, 180, 210, 220])
    trace = RequestTrace(clock=lambda: next(times))
    trace.delta_sent()  # No business delta: do not invent a measurement.
    trace.delta_ready()
    with trace.http_send():
        pass
    trace.delta_sent()
    trace.delta_ready()
    trace.delta_sent()  # First-only: these do not read the clock again.
    with pytest.raises(RuntimeError), trace.http_send():
        raise RuntimeError("private")
    with pytest.raises(asyncio.CancelledError), trace.http_send():
        raise asyncio.CancelledError()
    record = trace.finish()
    assert record.first_delta_ready_ns == 10
    assert record.first_delta_sent_ns == 50
    assert asdict(record.http_send) == dict(calls=3, duration_ns=40, errors=1, cancellations=1)
    assert record.spans == () and record.dropped_spans == 0
    assert "private" not in repr(record)


def test_many_sends_do_not_exhaust_stage_capacity():
    trace = RequestTrace(capacity=2)
    with trace.activate(), trace_stage(TraceStage.HTTP):
        for _ in range(1000):
            with trace.http_send():
                pass
        with trace_stage(TraceStage.SETTLEMENT):
            pass
    record = trace.finish()
    assert record.http_send.calls == 1000
    assert len(record.spans) == 2 and record.dropped_spans == 0
    assert record.first_delta_ready_ns is None and record.first_delta_sent_ns is None
