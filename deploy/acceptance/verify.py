"""API-only Docker recovery acceptance; all endpoints are synthetic and internal."""

from copy import deepcopy
import json
from pathlib import Path
import time
from uuid import uuid4

import httpx


def checked(response, expected=200):
    if response.status_code != expected:
        raise RuntimeError(f"Acceptance HTTP {response.status_code}; expected {expected}")
    return response.json()


with httpx.Client(trust_env=False, timeout=30) as client:
    control, model = "http://gateway:8001", "http://gateway:8000"
    if checked(client.get(control + "/gateway/v1/config/active")) != {"active": None}:
        raise RuntimeError("Acceptance requires its own empty database")
    submission = json.loads(Path("/acceptance/bundle.json").read_bytes())
    bundle = submission["bundle"]
    prototype = next(iter(bundle["providers"].values()))
    binding = next(iter(bundle["provider_model_bindings"].values()))
    bundle["providers"], bundle["provider_model_bindings"] = {}, {}
    for name in ("a", "b"):
        provider = deepcopy(prototype)
        provider["endpoint"]["base_url"] = f"http://provider-{name}:8080/v1"
        provider["credential"]["secret_ref"] = f"mock-{name}-key"
        provider["egress"] = {"allowed_hosts": [f"provider-{name}"], "allowed_networks": ["10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"], "proxy": "disabled"}
        provider["rate_limit"] = {"max_concurrency": 10, "qps": 100, "burst": 100}
        bundle["providers"][f"provider-{name}"] = provider
        bundle["provider_model_bindings"][f"binding-{name}"] = {**deepcopy(binding), "provider": f"provider-{name}", "upstream_model": f"configured-{name}"}
        bundle["provider_model_bindings"][f"binding-{name}"]["capabilities"]["streaming"] = True
    alias = bundle["model_aliases"]["general"]
    alias["candidates"] = [{"binding": f"binding-{name}", "service_level": "full", "priority": index, "weight": 1} for index, name in enumerate(("a", "b"))]
    retry = bundle["routing_policies"][alias["routing_policy"]]["retry"]
    retry.update(max_attempts=3, max_attempts_per_candidate=2)
    revision = checked(client.post(control + "/gateway/v1/config/revisions", json=submission,
        headers={"X-Gateway-Command-Id": str(uuid4())}), 201)["revision"]
    checked(client.post(control + f"/gateway/v1/config/revisions/{revision['revision']}/publish", json={
        "expected_active_revision": "0", "candidate_snapshot_digest": revision["snapshot_digest"], "description": "isolated recovery acceptance"},
        headers={"X-Gateway-Command-Id": str(uuid4())}))
    deadline = time.monotonic() + 10
    while client.get(control + "/readyz").status_code != 200:
        if time.monotonic() >= deadline:
            raise RuntimeError("Acceptance Gateway not ready")
        time.sleep(.1)
    prompt = checked(client.post(control + "/gateway/v1/prompts", json={"messages": [
        {"role": "user", "content": "Reply with {{answer}}"}]}), 201)
    checked(client.post(control + f"/gateway/v1/prompts/{prompt['asset_id']}/publish", json={"version_id": prompt["version_id"], "expected_generation": 0}))
    response = client.post(model + "/v1/chat/completions", json={"model": "general", "max_tokens": 32,
        "prompt": {"asset_id": prompt["asset_id"], "version_id": prompt["version_id"], "variables": {"answer": "OK"}}})
    result = checked(response)
    recovery = {"attempts": 3, "retries": 1, "fallback_used": True}
    if result["gateway"]["recovery"] != recovery or result["model"] != "actual-secondary" or result["choices"][0]["message"]["content"] != "OK":
        raise RuntimeError("Incorrect recovered model result")
    if "private-" in response.text:
        raise RuntimeError("Provider internals leaked")
    a = checked(client.get("http://provider-a:8080/records"))["records"]
    b = checked(client.get("http://provider-b:8080/records"))["records"]
    if len(a) != 2 or len(b) != 1 or len({item["intent_digest"] for item in a + b}) != 1:
        raise RuntimeError("Attempt count or pinned intent changed")
    print(json.dumps({"status": "passed", "call_id": response.headers["x-gateway-call-id"],
        "actual_model": result["model"], "recovery": recovery, "identical_intent": True,
        "real_provider_calls": 0}), flush=True)

    for index, fault in enumerate(("broken", "disconnect"), start=3):
        with client.stream("POST", model + "/v1/chat/completions", json={"model": "general", "max_tokens": 32,
            "stream": True, "messages": [{"role": "user", "content": "fixture:" + fault}]}) as response:
            if response.status_code != 200:
                raise RuntimeError("Fault stream did not start")
            call_id = response.headers["x-gateway-call-id"]
            lines = response.iter_lines()
            first = json.loads(next(lines)[6:])
            if first["model"] != "actual-primary" or first["choices"][0]["delta"] != {"content": "partial"}:
                raise RuntimeError("Wrong fault stream source")
            if fault == "broken":
                rest = [line for line in lines if line]
                if len(rest) != 1 or "[DONE]" in rest[0] or json.loads(rest[0][6:])["error"]["code"] != "provider_protocol_error":
                    raise RuntimeError("Failed stream treated as success")
        deadline = time.monotonic() + 10
        while True:
            a = checked(client.get("http://provider-a:8080/records"))["records"]
            b = checked(client.get("http://provider-b:8080/records"))["records"]
            if len(a) != index or len(b) != 1:
                raise RuntimeError("Postcommit stream retried or switched model")
            if a[-1].get("closed") and (fault != "disconnect" or a[-1].get("peer_disconnected")):
                break
            if time.monotonic() >= deadline:
                raise RuntimeError("Upstream socket was not released")
            time.sleep(.05)
        print(json.dumps({"status": "passed", "mode": fault, "call_id": call_id,
            "actual_model": "actual-primary", "attempts": 1, "fallback_used": False,
            "upstream_closed": True, "real_provider_calls": 0}), flush=True)
    prompt_path = control + f"/gateway/v1/prompts/{prompt['asset_id']}"
    newer = checked(client.post(prompt_path + "/versions", json={"messages": [
        {"role": "user", "content": "Different published instructions"}]}), 201)
    checked(client.post(prompt_path + "/publish", json={"version_id": newer["version_id"], "expected_generation": 1}))
    with client.stream("POST", model + "/v1/chat/completions", json={"model": "general", "max_tokens": 32, "stream": True,
        "prompt": {"asset_id": prompt["asset_id"], "version_id": prompt["version_id"], "variables": {"answer": "OK"}}}) as response:
        if response.status_code != 200 or not response.headers.get("content-type", "").startswith("text/event-stream"):
            raise RuntimeError("Streaming endpoint unavailable")
        lines = [line for line in response.iter_lines() if line]
        stream_call = response.headers["x-gateway-call-id"]
    if len(lines) != 3 or lines[-1] != "data: [DONE]":
        raise RuntimeError("Unexpected stream termination")
    first, terminal = [json.loads(line[6:]) for line in lines[:2]]
    if first["choices"][0]["delta"] != {"content": "OK"} or terminal["gateway"]["recovery"] != recovery:
        raise RuntimeError("Incorrect stream or recovery")
    if first["id"] != terminal["id"] or terminal["model"] != "actual-secondary" or terminal["gateway"]["prompt"]["version_id"] != prompt["version_id"]:
        raise RuntimeError("Stream identity or pinned Prompt changed")
    a = checked(client.get("http://provider-a:8080/records"))["records"]
    b = checked(client.get("http://provider-b:8080/records"))["records"]
    if len(a) != 6 or len(b) != 2 or len({item["intent_digest"] for item in a[4:] + b[1:]}) != 1:
        raise RuntimeError("Streaming attempt count or pinned intent changed")
    print(json.dumps({"status": "passed", "mode": "stream", "call_id": stream_call,
        "actual_model": terminal["model"], "recovery": recovery, "pinned_version": prompt["version_id"],
        "real_provider_calls": 0}), flush=True)
