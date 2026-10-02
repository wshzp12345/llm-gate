"""Finish the isolated four-call acceptance with a real Gateway RPM 429 check.

Run only after verify and verify_telemetry on the same disposable Compose
project. Every Provider in that project is synthetic; no paid model is called.
"""

import json

import httpx
from ruamel.yaml import YAML


CONTROL = "http://gateway:8001"
MODEL = "http://gateway:8000/v1/chat/completions"
PROVIDERS = ("http://provider-a:8080/records", "http://provider-b:8080/records")


def check(client: httpx.Client) -> dict[str, object]:
    active = client.get(CONTROL + "/gateway/v1/config/active")
    active.raise_for_status()
    revision = active.json()["active"]["revision"]
    exported = client.get(CONTROL + f"/gateway/v1/config/revisions/{revision}/export",
                          headers={"Accept": "application/yaml"})
    exported.raise_for_status()
    alias = YAML(typ="safe", pure=True).load(exported.text)["model_aliases"]["general"]
    if alias.get("requests_per_minute", 5) != 5:
        raise RuntimeError("Acceptance requires default five-RPM general alias")

    def records():
        result = []
        for endpoint in PROVIDERS:
            response = client.get(endpoint)
            response.raise_for_status()
            result.append(response.json()["records"])
        return result

    before = records()
    if [len(items) for items in before] != [6, 2]:
        raise RuntimeError("Run verify and verify_telemetry before the RPM check")
    if not (before[0][0]["intent_digest"] == before[0][1]["intent_digest"] == before[1][0]["intent_digest"]):
        raise RuntimeError("Existing retry/fallback request changed intent")
    first_retry_ms = (before[0][1]["received_ns"] - before[0][0]["received_ns"]) // 1_000_000
    if first_retry_ms < 1000:
        raise RuntimeError("Retry ignored the synthetic Provider's one-second Retry-After")
    payload = {"model": "general", "max_completion_tokens": 32,
               "messages": [{"role": "user", "content": "acceptance RPM"}]}
    fifth = client.post(MODEL, json=payload)
    if fifth.status_code != 200 or fifth.json()["choices"][0]["message"]["content"] != "OK":
        raise RuntimeError("Fifth admitted logical request did not complete")
    after_fifth = records()
    if sum(map(len, after_fifth)) <= sum(map(len, before)):
        raise RuntimeError("Fifth request did not reach a synthetic Provider")
    sixth = client.post(MODEL, json=payload)
    if (sixth.status_code != 429 or sixth.json().get("error", {}).get("code") != "rate_limited"
            or "x-gateway-call-id" in sixth.headers or records() != after_fifth):
        raise RuntimeError("Sixth request was not rejected before admission and Provider I/O")
    return {"status": "passed", "mode": "model_rpm", "model": "general", "limit": 5,
            "fifth_call_id": fifth.headers["x-gateway-call-id"], "sixth_status": 429,
            "first_retry_wait_ms": first_retry_ms,
            "additional_provider_attempts": sum(map(len, after_fifth)) - sum(map(len, before)),
            "real_provider_calls": 0}


if __name__ == "__main__":
    with httpx.Client(trust_env=False, timeout=30) as http:
        print(json.dumps(check(http)), flush=True)
