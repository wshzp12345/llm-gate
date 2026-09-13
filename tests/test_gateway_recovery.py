"""Two Providers through one public contract, real storage and controlled faults."""

import asyncio
from copy import deepcopy
import json
from pathlib import Path
from uuid import UUID, uuid4

import httpx
import psycopg
import pytest

from llm_gateway.infrastructure.gateway import Gateway
from llm_gateway.infrastructure.invocation_observation import PostgresInvocationObservation
from llm_gateway.infrastructure.gateway_bootstrap import load_bootstrap
from tests.test_gateway_bootstrap import bootstrap, write
from tests.test_configuration import database, run
from tests.config_fixtures import bundle_submission
from tests.test_completion import envelope
from tests.test_provider_streaming import Stream, chunk, wire


@pytest.mark.postgres
@pytest.mark.parametrize("streaming", [False, True])
@pytest.mark.parametrize("mode,expected,status,retries", [
    ("success", ["a"], 200, 0),
    ("retry", ["a", "a"], 200, 1),
    ("fallback", ["a", "b"], 200, 0),
    ("retry_fallback", ["a", "a", "b"], 200, 1),
    ("invalid", ["a"], 400, 0),
    ("uncertain", ["a"], 504, 0),
    ("protocol", ["a", "b"], 200, 0),
    ("mismatch", ["b"], 200, 0),
    ("budget", ["a", "a"], 503, 1),
    ("retry_after", ["a", "b"], 200, 0),
    ("shared_credentials", ["a"], 503, 0),
    ("fallback_mismatch", ["a", "a"], 503, 1),
    ("refusal", ["a"], 200, 0),
])
def test_two_provider_api_recovery_and_pinned_prompt(database, tmp_path, mode, expected, status, retries, streaming):
    body = bootstrap(tmp_path)
    Path(body["fingerprint_keys"][0]["file"]).write_bytes(b"f" * 32)
    Path(body["provider_secret_files"]["deepseek-api-key"]).write_text(json.dumps({
        "secret_version": str(uuid4()), "value": "synthetic-credential", "revoked": False,
        "valid_until": "9999-12-31T23:59:59Z"}))
    second_secret = tmp_path / "second-provider"
    second_secret.write_text(json.dumps({"secret_version": str(uuid4()), "value": "different-synthetic-credential",
        "revoked": False, "valid_until": "9999-12-31T23:59:59Z"}))
    body["provider_secret_files"]["second-provider-key"] = str(second_secret)
    config = load_bootstrap(write(tmp_path, body))

    async def scenario():
        calls = []
        traces = []

        class TraceOutput:
            async def write(self, record):
                assert record.enrichment == "available"
                assert record.invocation.call_id == record.access.call_id
                assert record.invocation.trace_id == record.access.trace_id
                assert record.invocation.correlation == record.access.correlation
                traces.append(record.access)
        async def handler(request):
            assert not {"traceparent", "tracestate", "x-gateway-task-id", "x-gateway-turn-id", "x-gateway-step-id",
                "x-gateway-caller-type", "x-gateway-operation-scope"}.intersection(request.headers)
            candidate = "a" if request.url.host == "provider.invalid" else "b"
            calls.append((candidate, json.loads(request.content)))
            if len(calls) == 1:
                # Publish new instructions while an Attempt owns the old query.
                updated = await control.post(prompt_path + "/versions", json={"messages": [
                    {"role": "user", "content": "New published instructions"}]})
                assert updated.status_code == 201
                published = await control.post(prompt_path + "/publish", json={
                    "version_id": updated.json()["version_id"], "expected_generation": 1})
                assert published.status_code == 200
            if candidate == "a":
                if mode == "retry" and len(calls) == 1 or mode in {"retry_fallback", "budget", "fallback_mismatch"}:
                    return httpx.Response(503, json={"error": "private-upstream-body"})
                if mode in {"fallback", "shared_credentials"}:
                    return httpx.Response(401, json={"error": "private-upstream-body"})
                if mode == "invalid":
                    return httpx.Response(400, json={"error": "private-upstream-body"})
                if mode == "uncertain":
                    raise httpx.ReadTimeout("private-endpoint-details")
                if mode == "protocol":
                    return httpx.Response(200, json={"invalid": "private-upstream-body"})
                if mode == "retry_after":
                    return httpx.Response(429, headers={"Retry-After": "6"})
            if streaming:
                return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=Stream([
                    wire(chunk({"refusal": "Declined"} if mode == "refusal" else {"content": "OK"}, model="actual-" + candidate)),
                    wire(chunk(finish="stop", model="actual-" + candidate,
                        usage={"prompt_tokens": 10, "completion_tokens": 2, "total_tokens": 12})), wire("[DONE]")]))
            response = envelope(model="actual-" + candidate,
                usage={"prompt_tokens": 10, "completion_tokens": 2, "total_tokens": 12},
                vendor_extension="private-vendor-extension")
            if mode == "refusal":
                response["choices"][0]["message"] = {"role": "assistant", "content": None, "refusal": "Declined"}
            return httpx.Response(200, json=response)

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as upstream:
            upstream.stop_acquiring = lambda: None
            gateway = Gateway(config, database, enable_streaming=streaming, trace_output=TraceOutput())
            gateway.transports._factory = lambda plan: upstream
            async with gateway.hold():
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway.management_app), base_url="http://control") as control, \
                    httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway.model_app), base_url="http://model") as model:
                    submission = bundle_submission()
                    bundle = submission["bundle"]
                    bundle["provider_model_bindings"]["binding-a"]["capabilities"]["streaming"] = streaming
                    first = bundle["providers"]["provider-a"]
                    first["credential"]["secret_ref"] = "deepseek-api-key"
                    first["rate_limit"] = {"max_concurrency": 10, "qps": 100, "burst": 100}
                    second = deepcopy(first)
                    second["endpoint"]["base_url"] = "https://second.invalid/api/v1"
                    second["egress"]["allowed_hosts"] = ["second.invalid"]
                    if mode != "shared_credentials":
                        second["credential"]["secret_ref"] = "second-provider-key"
                    bundle["providers"]["provider-b"] = second
                    bundle["provider_model_bindings"]["binding-b"] = {
                        **deepcopy(bundle["provider_model_bindings"]["binding-a"]),
                        "provider": "provider-b", "upstream_model": "Second-Model"}
                    bundle["model_aliases"]["general"]["candidates"].append({
                        "binding": "binding-b", "service_level": "full", "priority": 1, "weight": 1})
                    if mode == "mismatch":
                        bundle["provider_model_bindings"]["binding-a"]["limits"]["max_output_tokens"] = 16
                    if mode == "fallback_mismatch":
                        bundle["provider_model_bindings"]["binding-b"]["limits"]["max_output_tokens"] = 16
                    if mode == "budget":
                        bundle["routing_policies"]["route-a"]["retry"]["max_attempts"] = 2
                    created = await control.post("/gateway/v1/config/revisions", json=submission,
                        headers={"X-Gateway-Command-Id": str(uuid4())})
                    assert created.status_code == 201, created.text
                    revision = created.json()["revision"]
                    published = await control.post(f"/gateway/v1/config/revisions/{revision['revision']}/publish",
                        json={"expected_active_revision": "0", "candidate_snapshot_digest": revision["snapshot_digest"],
                              "description": "two-provider acceptance"}, headers={"X-Gateway-Command-Id": str(uuid4())})
                    assert published.status_code == 200
                    async with asyncio.timeout(5):
                        while not gateway.ready:
                            await asyncio.sleep(.02)
                    created = await control.post("/gateway/v1/prompts", json={"messages": [
                        {"role": "user", "content": "Pinned {{value}}"}]})
                    assert created.status_code == 201
                    prompt = created.json()
                    prompt_path = "/gateway/v1/prompts/" + prompt["asset_id"]
                    assert (await control.post(prompt_path + "/publish", json={
                        "version_id": prompt["version_id"], "expected_generation": 0})).status_code == 200
                    query = {"model": "general", "max_tokens": 32, "temperature": .5, "top_p": .9,
                             "stream": streaming,
                             "prompt": {"asset_id": prompt["asset_id"], "version_id": prompt["version_id"],
                                        "variables": {"value": "instructions"}}}
                    result = await model.post("/v1/chat/completions", json=query, headers={
                        "X-Gateway-Caller-Type": "agent", "X-Gateway-Operation-Scope": "step",
                        "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
                        "tracestate": "vendor=opaque-private-state",
                        "X-Gateway-Task-Id": "task-acceptance", "X-Gateway-Turn-Id": "turn-1", "X-Gateway-Step-Id": "step-1"})
                    assert result.status_code == status, result.text
                    assert [name for name, _ in calls] == expected
                    if not streaming or status != 200:
                        assert result.headers["x-gateway-attempts"] == str(len(expected))
                        assert result.headers["x-gateway-retries"] == str(retries)
                        assert result.headers["x-gateway-fallback-used"] == str(len(set(expected)) > 1).lower()
                    assert "private-" not in result.text
                    for name, payload in calls:
                        assert payload == {"model": "Upstream-Model" if name == "a" else "Second-Model",
                            "messages": [{"role": "user", "content": "Pinned instructions"}],
                            "max_tokens": 32, "temperature": .5, "top_p": .9, "stream": streaming,
                            **({"stream_options": {"include_usage": True}} if streaming else {})}
                    if status == 200:
                        if streaming:
                            frames = result.text.strip().split("\n\n")
                            assert frames[-1] == "data: [DONE]"
                            response_body = json.loads(frames[-2][6:])
                        else:
                            response_body = result.json()
                        assert response_body["gateway"]["requested_model"] == "general"
                        assert response_body["model"] == "actual-" + expected[-1]
                        assert response_body["gateway"]["prompt"]["version_id"] == prompt["version_id"]
                        assert response_body["gateway"]["recovery"] == {"attempts": len(expected),
                            "retries": retries, "fallback_used": len(set(expected)) > 1}
                        if mode == "refusal":
                            assert (json.loads(frames[0][6:])["choices"][0]["delta"]["refusal"] if streaming
                                    else response_body["choices"][0]["message"]["refusal"]) == "Declined"
                    async with asyncio.timeout(5):
                        while not traces:
                            await asyncio.sleep(.01)
                    assert len(traces) == 1
                    assert gateway.trace_exporter.counters.exported == 1
                    assert traces[0].trace_id == "4bf92f3577b34da6a3ce929d0e0e4736"
                    assert traces[0].correlation.task_id == "task-acceptance"
                    assert traces[0].correlation.turn_id == "turn-1" and traces[0].correlation.step_id == "step-1"
                    assert "opaque-private-state" not in repr(traces[0])
                    assert next(span for span in traces[0].spans if span.stage == "http").parent_span_id == "00f067aa0ba902b7"
                    assert str(traces[0].request_id) == result.headers["x-request-id"]
                    assert str(traces[0].call_id) == result.headers["x-gateway-call-id"]
                    assert {span.stage for span in traces[0].spans} == {
                        "http", "authorization", "prompt", "fingerprint", "admission", "routing", "provider_attempt", "adapter_request", "settlement"} | ({"backoff"} if retries else set())
                    assert next(span for span in traces[0].spans if span.stage == "http").outcome == ("ok" if status == 200 else "error")
                    assert all(span.duration_ns >= 0 for span in traces[0].spans)
                    attempt_spans = [span for span in traces[0].spans if span.stage == "provider_attempt"]
                    adapter_spans = [span for span in traces[0].spans if span.stage == "adapter_request"]
                    assert len(adapter_spans) == len(attempt_spans)
                    assert {span.parent_span_id for span in adapter_spans} == {span.span_id for span in attempt_spans}
                    assert [span.attempt_number for span in attempt_spans] == list(range(1, len(expected) + 1))
                    assert [span.outcome for span in attempt_spans] == (["error"] * (len(expected) - 1) + ["ok"] if status == 200
                                                                      else ["error"] * len(expected))
                    return result.headers["x-gateway-call-id"], prompt["version_id"], traces[0].trace_id
    call_id, version_id, trace_id = run(scenario())
    observation = run(PostgresInvocationObservation(database).read(UUID(call_id)))
    assert observation.trace_id == trace_id
    assert (observation.correlation.task_id, observation.correlation.turn_id, observation.correlation.step_id) == (
        "task-acceptance", "turn-1", "step-1")
    assert len(observation.attempts) == len(expected)
    assert [attempt.binding_id for attempt in observation.attempts] == ["binding-" + name for name in expected]
    assert all(attempt.pricing_revision is not None and attempt.pricing_resource_id is not None
               for attempt in observation.attempts)
    assert observation.terminal_outcome == observation.state
    assert not observation.model_conflict
    if status == 200:
        assert observation.actual_model == "actual-" + expected[-1]
    assert "private-" not in repr(observation)
    with psycopg.connect(database) as connection:
        settled = connection.execute("SELECT input_tokens,output_tokens FROM invocation_settlement WHERE call_id=%s", (call_id,)).fetchone()
        assert (observation.settled_usage.input_tokens, observation.settled_usage.output_tokens) == settled
        costs = connection.execute("SELECT currency,total_cost,completeness,certainty FROM invocation_cost_summary WHERE call_id=%s ORDER BY currency", (call_id,)).fetchall()
        assert [(cost.currency, cost.total_cost, cost.completeness, cost.certainty) for cost in observation.costs] == [
            (currency, str(total) if total is not None else None, completeness, certainty)
            for currency, total, completeness, certainty in costs]
        assert connection.execute("SELECT trace_id FROM model_invocation WHERE call_id=%s", (call_id,)).fetchone()[0] == trace_id
        rows = connection.execute("SELECT a.binding_id,a.candidate_attempt,o.recovery_action FROM provider_attempt a "
            "JOIN provider_attempt_outcome o USING(call_id,number) WHERE call_id=%s ORDER BY number", (call_id,)).fetchall()
        assert [row[0] for row in rows] == ["binding-" + item for item in expected]
        assert sum(row[1] > 1 for row in rows) == retries
        assert str(connection.execute("SELECT version_id FROM invocation_prompt WHERE call_id=%s", (call_id,)).fetchone()[0]) == version_id
