"""Read only the isolated Docker fixture's synthetic Collector debug output."""

import json
import re
import subprocess


def logs(service):
    result = subprocess.run(["docker", "compose", "-f", "deploy/acceptance/compose.yaml",
        "logs", "--no-color", "--no-log-prefix", service], capture_output=True, text=True,
        encoding="utf-8", check=True, timeout=15)
    return result.stdout + result.stderr


def verify(api_output, collector_output):
    calls = [json.loads(line) for line in api_output.splitlines() if line.startswith('{"status":')]
    assert len(calls) == 4 and all(item["status"] == "passed" for item in calls)
    records = [json.loads(line[len("Body: Str("):-1]) for line in collector_output.splitlines()
        if line.startswith("Body: Str(") and line.endswith(")")]
    admitted = {record["call_id"]: record for record in records if record.get("call_id")}
    spans = re.findall(r"Span #\d+\n(.*?)(?=\nSpan #|\n\t\{|\Z)", collector_output, re.S)
    assert len(admitted) == 4
    for call in calls:
        record = admitted[call["call_id"]]
        assert record["enrichment"] == "available"
        invocation = record["invocation"]
        mode = call.get("mode", "sync")
        expected = {"sync": "completed", "stream": "completed", "broken": "failed", "disconnect": "cancelled"}[mode]
        assert invocation["state"] == expected
        assert invocation["actual_model"] == call["actual_model"]
        assert len(invocation["attempts"]) == (3 if expected == "completed" else 1)
        related = [span for span in spans if "Trace ID       : " + record["trace_id"] in span]
        roots = [span for span in related if "Name           : gateway.http\n" in span]
        attempts = [span for span in related if "Name           : gateway.provider_attempt\n" in span]
        adapters = [span for span in related if "Name           : gateway.adapter_request\n" in span]
        assert len(roots) == 1 and len(attempts) == len(invocation["attempts"])
        assert len(adapters) == len(attempts)
        attempt_ids = {re.search(r"\n    ID             : ([0-9a-f]{16})", span).group(1) for span in attempts}
        adapter_parents = {re.search(r"Parent ID      : ([0-9a-f]{16})", span).group(1) for span in adapters}
        assert adapter_parents == attempt_ids
        assert all("Kind           : Client" in span for span in adapters)
        timed = [span for span in adapters if "gateway.adapter.first_delta_ns" in span]
        assert len(timed) == (0 if mode == "sync" else 1)
        for span in timed:
            assert "gateway.adapter.after_first_delta_ns" in span
        if expected != "completed":
            assert all("Status code    : Error" in span for span in adapters)
        assert "Status code    : " + ("Ok" if expected == "completed" else "Error") in roots[0]
        assert all("gateway.request_id: Str(" + record["request_id"] + ")" in span for span in related)
        assert all("gateway.call_id: Str(" + call["call_id"] + ")" in span for span in related)
        assert invocation["costs"]  # Unknown amounts must retain completeness metadata.
        for cost in invocation["costs"]:
            assert cost["completeness"] in {"complete", "partial", "unavailable"}
        if mode == "stream":
            assert record["first_delta_ready_ns"] is not None and record["first_delta_sent_ns"] is not None
        if expected == "completed":
            assert [attempt["recovery_action"] for attempt in invocation["attempts"]] == ["retry", "advance", "stop"]
            assert invocation["attempts"][-1]["usage"]["input_tokens"] == 10
            assert invocation["attempts"][-1]["usage"]["output_tokens"] == 2
            assert invocation["attempts"][-1]["usage"]["total_tokens"] == 12
        # Earlier failed Attempts (or interrupted streams) have unknown usage;
        # final-Attempt tokens must not masquerade as the whole invocation.
        assert invocation["usage"]["total_tokens"] is None
        assert all(cost["total_cost"] is None for cost in invocation["costs"])
    for metric in ("gateway.requests", "gateway.request.duration", "gateway.telemetry.records"):
        assert "Name: " + metric in collector_output
    for private in ("synthetic-a-credential", "synthetic-b-credential", "Reply with", "fixture:broken", "private-"):
        assert private not in collector_output
    return {"status": "passed", "correlated_calls": len(admitted), "modes": ["sync", "broken", "disconnect", "stream"]}


def scrape():
    # Collector shares this container's loopback, with no published scrape port.
    command = ["docker", "compose", "-f", "deploy/acceptance/compose.yaml", "exec", "-T", "gateway",
        "python", "-c", "import httpx; r=httpx.get('http://127.0.0.1:9464/metrics',trust_env=False,timeout=5); r.raise_for_status(); print(r.text)"]
    return subprocess.run(command, capture_output=True, text=True, encoding="utf-8", check=True, timeout=15).stdout


def verify_metrics(text):
    def samples(prefix, **labels):
        return [float(line.split()[-1]) for line in text.splitlines()
            if (line.startswith(prefix + "{") or line.startswith(prefix + " "))
            and all(f'{key}="{value}"' in line for key, value in labels.items())]
    assert sum(samples("gateway_observed_attempts_total")) == 8
    assert sum(samples("gateway_observed_recovery_total", action="retry")) == 2
    assert sum(samples("gateway_observed_recovery_total", action="fallback")) == 2
    assert sum(samples("gateway_observed_known_tokens_total", token_type="input_tokens")) == 20
    assert sum(samples("gateway_observed_known_tokens_total", token_type="output_tokens")) == 4
    assert sum(samples("gateway_observed_cost_records_total", availability="unknown")) == 4
    assert not samples("gateway_observed_known_cost")
    assert samples("gateway_resource_admission_limit") == [200]
    assert samples("gateway_resource_export_queue_limit") == [256]
    assert "gateway_request_duration_seconds_bucket" in text
    for forbidden in ("task_id", "call_id", "trace_id", "private-", "synthetic-a-credential", "actual-secondary"):
        assert forbidden not in text
    return {"prometheus": "passed", "attempts": 8, "retries": 2, "fallbacks": 2, "known_input": 20, "known_output": 4}


if __name__ == "__main__":
    print(json.dumps(verify(logs("verify"), logs("collector"))))
    print(json.dumps(verify_metrics(scrape())))
