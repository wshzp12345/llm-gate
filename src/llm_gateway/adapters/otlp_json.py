"""Explicit OTLP/JSON projection. No raw context, identity claims or exceptions."""

import json

from llm_gateway.application.access_metrics import BOUNDS, OUTCOMES


def attributes(values):
    result = []
    for key, value in values.items():
        if value is None:
            continue
        encoded = {"boolValue": value} if type(value) is bool else (
            {"intValue": str(value)} if type(value) is int else {"stringValue": str(value)})
        result.append({"key": key, "value": encoded})
    return result


def _usage(usage):
    values = {key: getattr(usage, key) for key in (
        "input_tokens", "output_tokens", "cached_tokens", "reasoning_tokens", "provider_reported_total")}
    values.update(total_tokens=usage.total_tokens, provenance=usage.provenance)
    known = [values[key] for key in ("input_tokens", "output_tokens", "cached_tokens", "reasoning_tokens")]
    values["state"] = "complete" if all(value is not None for value in known) else (
        "partial" if any(value is not None for value in values.values() if type(value) is int) else "unavailable")
    return values


def summary(record):
    access, invocation = record.access, record.invocation
    result = {"event": "gateway.access", "request_id": str(access.request_id), "trace_id": access.trace_id,
        "call_id": str(access.call_id) if access.call_id else None, "enrichment": record.enrichment,
        "task_id": access.correlation.task_id, "turn_id": access.correlation.turn_id, "step_id": access.correlation.step_id,
        "dropped_spans": access.dropped_spans, "first_delta_ready_ns": access.first_delta_ready_ns,
        "first_delta_sent_ns": access.first_delta_sent_ns,
        "http_send": {key: getattr(access.http_send, key) for key in ("calls", "duration_ns", "errors", "cancellations")}}
    if invocation is not None:
        result["invocation"] = {"state": invocation.state, "terminal_outcome": invocation.terminal_outcome,
            "error_code": invocation.error_code, "actual_model": invocation.actual_model,
            "model_conflict": invocation.model_conflict, "usage": _usage(invocation.settled_usage),
            "attempts": [{"number": attempt.number, "binding_id": attempt.binding_id,
                "candidate_attempt": attempt.candidate_attempt, "outcome": attempt.outcome,
                "recovery_action": attempt.recovery_action, "actual_model": attempt.actual_model,
                "model_conflict": attempt.model_conflict, "usage": _usage(attempt.usage),
                "late_usage": _usage(attempt.late_usage) if attempt.late_usage is not None else None,
                "pricing_revision": attempt.pricing_revision, "pricing_resource_id": attempt.pricing_resource_id}
                for attempt in invocation.attempts],
            "costs": [{"currency": cost.currency, "total_cost": cost.total_cost,
                "completeness": cost.completeness, "certainty": cost.certainty} for cost in invocation.costs]}
    return result


def payloads(record, *, instance_id, observed_unix_ns):
    access = record.access
    resource = {"attributes": attributes({"service.name": "llm-gateway", "service.instance.id": instance_id})}
    scope = {"name": "llm_gateway", "version": "0.1.0"}
    identity = {"gateway.request_id": str(access.request_id), "gateway.call_id": str(access.call_id) if access.call_id else None,
        "gateway.task_id": access.correlation.task_id, "gateway.turn_id": access.correlation.turn_id,
        "gateway.step_id": access.correlation.step_id}
    spans = []
    root = next((span for span in access.spans if span.stage == "http"), None)
    for span in access.spans:
        start = access.started_unix_ns + span.start_offset_ns
        item = {"traceId": span.trace_id, "spanId": span.span_id, "name": "gateway." + span.stage,
            "kind": 2 if span.stage == "http" else 3 if span.stage in {"provider_attempt", "adapter_request"} else 1,
            "startTimeUnixNano": str(start), "endTimeUnixNano": str(start + span.duration_ns),
            "attributes": attributes({**identity, "gateway.stage": span.stage, "gateway.outcome": span.outcome,
                "gateway.attempt_number": span.attempt_number,
                "gateway.adapter.first_delta_ns": span.first_delta_ns,
                "gateway.adapter.after_first_delta_ns": (max(0, span.duration_ns - span.first_delta_ns)
                    if span.first_delta_ns is not None else None)}),
            "status": {"code": 1 if span.outcome == "ok" else 2}}
        if span.parent_span_id is not None:
            item["parentSpanId"] = span.parent_span_id
        spans.append(item)
    log = {"timeUnixNano": str(access.started_unix_ns + (root.start_offset_ns + root.duration_ns if root else 0)),
        "observedTimeUnixNano": str(observed_unix_ns), "traceId": access.trace_id,
        "severityNumber": 9 if root is not None and root.outcome == "ok" else 17,
        "body": {"stringValue": json.dumps(summary(record), ensure_ascii=False, separators=(",", ":"))},
        "attributes": attributes(identity)}
    if root is not None:
        log["spanId"] = root.span_id
    result = {"traces": {"resourceSpans": [{"resource": resource, "scopeSpans": [{"scope": scope, "spans": spans}]}]},
              "logs": {"resourceLogs": [{"resource": resource, "scopeLogs": [{"scope": scope, "logRecords": [log]}]}]}}
    if record.metrics is not None:
        measured = record.metrics
        point_time = {"startTimeUnixNano": str(measured.started_unix_ns), "timeUnixNano": str(observed_unix_ns)}
        metrics = [{"name": "gateway.requests", "unit": "1", "sum": {"aggregationTemporality": 2,
            "isMonotonic": True, "dataPoints": [{**point_time, "asInt": str(count),
                "attributes": attributes({"gateway.outcome": outcome})} for outcome, count in zip(OUTCOMES, measured.requests)]}},
            {"name": "gateway.request.duration", "unit": "s", "histogram": {"aggregationTemporality": 2,
                "dataPoints": [{**point_time, "count": str(measured.latency_count),
                    "sum": measured.latency_sum_ns / 1_000_000_000, "explicitBounds": list(BOUNDS),
                    "bucketCounts": [str(count) for count in measured.latency_buckets]}]}}]
        if record.counters is not None:
            metrics.append({"name": "gateway.telemetry.records", "unit": "1", "sum": {"aggregationTemporality": 2,
                "isMonotonic": True, "dataPoints": [{**point_time, "asInt": str(getattr(record.counters, key)),
                    "attributes": attributes({"gateway.export_state": key})} for key in (
                        "accepted", "dropped", "exported", "failed", "enrichment_failed", "abandoned")]}})
        grouped = {}
        for point in record.invocation_metrics:
            encoded = {"asInt": str(point.value)} if type(point.value) is int else {"asDouble": float(point.value)}
            grouped.setdefault(point.name, []).append({**point_time, **encoded,
                "attributes": attributes(dict(point.labels))})
        for name, points in grouped.items():
            metrics.append({"name": name, "unit": "{currency}" if name == "gateway.observed.known_cost" else "1", "sum": {"aggregationTemporality": 2,
                "isMonotonic": name != "gateway.observed.known_cost", "dataPoints": points}})
        for name, value in record.resource_metrics:
            # Gauges are snapshots; rejection/peak readings can reset if the
            # execution owner is replaced while this exporter remains alive.
            metrics.append({"name": "gateway.resource." + name, "unit": "{item}",
                "gauge": {"dataPoints": [{"timeUnixNano": str(observed_unix_ns), "asInt": str(value)}]}})
        result["metrics"] = {"resourceMetrics": [{"resource": resource, "scopeMetrics": [{"scope": scope, "metrics": metrics}]}]}
    return result
