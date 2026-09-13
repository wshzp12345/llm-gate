from dataclasses import replace
from decimal import Decimal
import asyncio
import json

from llm_gateway.application.invocation_metrics import InvocationMetrics
from llm_gateway.application.invocation_observation import ObservedAttempt, ObservedCost
from llm_gateway.application.trace_export import BoundedTraceExporter
from llm_gateway.adapters.otlp_json import payloads
from llm_gateway.domain.model import Usage
from tests.test_trace_export import access, observation, Reader, Output


def attempt(number, binding, usage=Usage(), action="stop"):
    return ObservedAttempt(number, binding, 1, "failed", action, None, False, usage, None, 1, "price")


def values(metrics, suffix):
    return {point.labels: point.value for point in metrics.snapshot() if point.name == "gateway.observed." + suffix}


def test_known_attempt_usage_is_not_whole_invocation_and_subsets_are_separate():
    record = observation(access(True))
    record = replace(record, attempts=(attempt(1, "a", action="retry"), attempt(2, "a", action="advance"),
        attempt(3, "b", Usage(10, 3, 2, 1))), costs=(ObservedCost("USD", None, "unavailable", "unavailable"),))
    metrics = InvocationMetrics()
    metrics.observe(record)
    assert values(metrics, "recovery") == {(("action", "retry"),): 1, (("action", "fallback"),): 1}
    assert values(metrics, "known_tokens") == {(("token_type", key),): value for key, value in
        {"input_tokens": 10, "output_tokens": 3, "cached_tokens": 2, "reasoning_tokens": 1}.items()}
    assert sum(values(metrics, "usage_measurements").values()) == 12
    assert values(metrics, "known_cost") == {}
    assert record.settled_usage.total_tokens is None


def test_retry_intention_without_next_attempt_is_not_a_retry_and_late_usage_not_counted():
    metrics = InvocationMetrics()
    record = replace(observation(access(True)), attempts=(replace(attempt(1, "a", action="retry"), late_usage=Usage(100, 100)),))
    metrics.observe(record)
    assert set(values(metrics, "recovery").values()) == {0}
    assert values(metrics, "known_tokens") == {}


def test_costs_are_exact_separated_and_currency_cardinality_is_bounded():
    metrics = InvocationMetrics()
    record = observation(access(True))
    costs = (ObservedCost("USD", "0.1", "complete", "estimated"),
             ObservedCost("CNY", "0.2", "partial", "estimated"))
    metrics.observe(replace(record, costs=costs))
    metrics.observe(replace(record, costs=costs))
    totals = values(metrics, "known_cost")
    assert set(totals.values()) == {Decimal("0.2"), Decimal("0.4")}
    metrics.observe(replace(record, costs=tuple(ObservedCost(f"X{i:02}", "1", "complete", "estimated") for i in range(20))))
    assert len(values(metrics, "known_cost")) == 16
    assert values(metrics, "cost_currency_overflow") == {(): 6}


def test_worker_emits_cumulative_metrics_without_identity_labels():
    async def scenario():
        record = access(True)
        observed = replace(observation(record), attempts=(attempt(1, "private-binding", Usage(2, 1)),),
            costs=(ObservedCost("USD", "0.25", "partial", "estimated"),))
        output = Output()
        worker = BoundedTraceExporter(Reader(observed), output)
        async with worker.hold():
            assert worker.offer(record)
        projected = payloads(output.records[0], instance_id="instance", observed_unix_ns=record.started_unix_ns + 100)
        encoded = json.dumps(projected["metrics"])
        for private in (str(record.call_id), record.trace_id, "private-binding", "price", "actual_model"):
            assert private not in encoded
        assert "gateway.observed.known_tokens" in encoded and '"asDouble": 0.25' in encoded
    asyncio.run(scenario())


def test_missing_enrichment_produces_no_fabricated_invocation_metrics():
    metrics = InvocationMetrics()
    metrics.observe(None)
    assert metrics.snapshot() == ()
