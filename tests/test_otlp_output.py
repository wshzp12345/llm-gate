import asyncio
from dataclasses import replace
import json

import httpx
import pytest

from llm_gateway.adapters.otlp_http import OtlpExportFailure, OtlpHttpOutput
from llm_gateway.adapters.otlp_json import payloads, summary
from llm_gateway.application.access_metrics import AccessMetrics
from llm_gateway.application.request_trace import RequestTrace, TraceStage
from llm_gateway.application.trace_export import BoundedTraceExporter, TraceExport
from llm_gateway.application.invocation_observation import ObservedCost
from tests.test_trace_export import Reader, access, observation


class Body(httpx.AsyncByteStream):
    def __init__(self, content):
        self.content = content
    async def __aiter__(self):
        yield self.content


def test_otlp_projection_uses_monotonic_durations_and_preserves_unknown_cost():
    ticks = iter([100, 110, 120, 145, 160])
    trace = RequestTrace(clock=lambda: next(ticks), wall_clock=lambda: 1700000000000000000)
    with trace.activate(), trace.span(TraceStage.HTTP):
        with trace.span(TraceStage.PROVIDER_ATTEMPT, attempt_number=1):
            pass
    record = trace.finish()
    inv = replace(observation(record), costs=(ObservedCost("USD", None, "partial", "estimated"),))
    exported = TraceExport(record, inv, "available")
    result = payloads(exported, instance_id="instance", observed_unix_ns=1700000000000000999)
    child, root = result["traces"]["resourceSpans"][0]["scopeSpans"][0]["spans"]
    assert child["startTimeUnixNano"] == "1700000000000000020"
    assert int(child["endTimeUnixNano"]) - int(child["startTimeUnixNano"]) == 25
    assert root["kind"] == 2 and child["kind"] == 3
    assert child["parentSpanId"] == root["spanId"]
    assert child["status"] == {"code": 1}
    log = result["logs"]["resourceLogs"][0]["scopeLogs"][0]["logRecords"][0]
    assert log["spanId"] == root["spanId"] and log["traceId"] == record.trace_id
    body = json.loads(log["body"]["stringValue"])
    assert body["invocation"]["costs"][0]["total_cost"] is None
    assert body["invocation"]["usage"]["total_tokens"] is None
    assert not {"tracestate", "messages", "authorization", "subject", "tenant_id"}.intersection(body)


def test_metrics_have_only_bounded_labels_and_unknown_duration_is_not_zero():
    metrics = AccessMetrics()
    record = access()
    metrics.observe(record)
    metrics.observe(replace(record, spans=()))
    snapshot = metrics.snapshot()
    assert sum(snapshot.requests) == 2 and snapshot.latency_count == 1
    assert sum(snapshot.latency_buckets) == 1
    result = payloads(TraceExport(record, None, "not_admitted", snapshot), instance_id="process", observed_unix_ns=1800000000000000000)
    encoded = json.dumps(result["metrics"])
    assert record.trace_id not in encoded and str(record.request_id) not in encoded
    assert "task_id" not in encoded and "actual_model" not in encoded
    metric = result["metrics"]["resourceMetrics"][0]["scopeMetrics"][0]["metrics"][0]
    assert metric["sum"]["aggregationTemporality"] == 2


@pytest.mark.parametrize("endpoint", ["http://collector", "https://user:secret@collector", "https://collector/path",
    "https://collector?token=secret", "https://collector#part", "file:///tmp", "https://collector\\evil", "https://col lector"])
def test_collector_origin_is_explicit_and_no_credentials_or_redirect_target(endpoint):
    with pytest.raises(ValueError):
        OtlpHttpOutput(endpoint)


@pytest.mark.parametrize("mode", ["ok", "warning", "partial", "status", "redirect", "wrong_type", "encoding",
    "oversize", "invalid_json", "duplicate", "invalid_count", "invalid_warning", "timeout"])
def test_three_signals_bounded_and_failed_collector_response_is_not_success(mode):
    async def scenario():
        calls = []
        async def handler(request):
            calls.append(request)
            assert request.headers["content-type"] == "application/json"
            assert not {"authorization", "traceparent", "tracestate"}.intersection(request.headers)
            answer, status, headers = b"{}", 200, {"content-type": "application/json"}
            if request.url.path == "/v1/traces":
                if mode == "timeout":
                    raise httpx.ReadTimeout("private-diagnostic")
                if mode == "warning":
                    answer = b'{"partialSuccess":{"rejectedSpans":"0","errorMessage":"private-diagnostic"}}'
                if mode == "partial":
                    answer = b'{"partialSuccess":{"rejectedSpans":"1","errorMessage":"private-diagnostic"}}'
                if mode == "status":
                    status = 503
                if mode == "redirect":
                    status, headers["location"] = 302, "https://outside.invalid"
                if mode == "wrong_type":
                    headers["content-type"] = "text/plain"
                if mode == "encoding":
                    headers["content-encoding"] = "gzip"
                if mode == "oversize":
                    answer = b" " * 65537
                if mode == "invalid_json":
                    answer = b"private-diagnostic"
                if mode == "duplicate":
                    answer = b'{"partialSuccess":{},"partialSuccess":{}}'
                if mode == "invalid_count":
                    answer = b'{"partialSuccess":{"rejectedSpans":true}}'
                if mode == "invalid_warning":
                    answer = b'{"partialSuccess":{"errorMessage":{}}}'
            return httpx.Response(status, headers=headers, stream=Body(answer))
        output = OtlpHttpOutput("https://collector.invalid", transport=httpx.MockTransport(handler))
        record = access()
        metrics = AccessMetrics()
        metrics.observe(record)
        async with output.hold():
            if mode in {"ok", "warning"}:
                await output.write(TraceExport(record, None, "not_admitted", metrics.snapshot()))
            else:
                with pytest.raises(OtlpExportFailure) as failure:
                    await output.write(TraceExport(record, None, "not_admitted", metrics.snapshot()))
                assert "private" not in str(failure.value)
        assert len(calls) == 3 and {call.url.path for call in calls} == {"/v1/traces", "/v1/logs", "/v1/metrics"}
        assert output.counters["logs"].accepted == output.counters["metrics"].accepted == 1
        assert output.counters["traces"].failed == (mode not in {"ok", "warning"})
        assert output.counters["traces"].rejected == (mode == "partial")
        assert output.counters["traces"].warnings == (mode == "warning")
    asyncio.run(scenario())


def test_pipeline_metrics_include_records_dropped_at_queue_admission():
    async def scenario():
        bodies = []
        async def handler(request):
            if request.url.path == "/v1/metrics":
                bodies.append(json.loads(request.content))
            return httpx.Response(200, headers={"content-type": "application/json"}, stream=Body(b"{}"))
        async with OtlpHttpOutput("https://collector.invalid", transport=httpx.MockTransport(handler)).hold() as output:
            exporter = BoundedTraceExporter(Reader(), output, capacity=1)
            async with exporter.hold():
                assert exporter.offer(access())
                assert not exporter.offer(access())
        metrics = bodies[0]["resourceMetrics"][0]["scopeMetrics"][0]["metrics"]
        requests = metrics[0]["sum"]["dataPoints"]
        assert sum(int(point["asInt"]) for point in requests) == 2
    asyncio.run(scenario())
