"""Exercise distinct OpenAI-compatible and Messages protocols via synthetic Docker Providers."""

from copy import deepcopy
import json
import time
from uuid import uuid4

import httpx
from ruamel.yaml import YAML


CONTROL = "http://gateway:8001"
MODEL = "http://gateway:8000/v1/chat/completions"
RECORDS = "http://provider-c:8080/records"


def checked(response: httpx.Response, expected: int = 200) -> dict:
    if response.status_code != expected:
        raise RuntimeError(f"Protocol acceptance HTTP {response.status_code}; expected {expected}")
    return response.json()


def check(client: httpx.Client) -> dict[str, object]:
    active = checked(client.get(CONTROL + "/gateway/v1/config/active"))["active"]
    if active is None:
        raise RuntimeError("Run the base acceptance before protocol acceptance")
    current = active["revision"]
    exported = client.get(CONTROL + f"/gateway/v1/config/revisions/{current}/export",
                          headers={"Accept": "application/yaml"})
    exported.raise_for_status()
    bundle = YAML(typ="safe", pure=True).load(exported.text)
    if "provider-c" in bundle["providers"] or checked(client.get(RECORDS))["records"]:
        raise RuntimeError("Protocol acceptance requires fresh synthetic Provider C")

    provider = deepcopy(bundle["providers"]["provider-b"])
    provider["adapter"] = {"type": "anthropic_messages", "version": "v1"}
    provider["endpoint"]["base_url"] = "http://provider-c:8080/v1"
    provider["credential"]["secret_ref"] = "mock-c-key"
    provider["egress"] = {"allowed_hosts": ["provider-c"],
                          "allowed_networks": ["10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"],
                          "proxy": "disabled"}
    bundle["providers"]["provider-c"] = provider
    binding = deepcopy(bundle["provider_model_bindings"]["binding-b"])
    binding.update(provider="provider-c", upstream_model="configured-c")
    binding["capabilities"].update(streaming=False, structured_output="json_schema")
    bundle["provider_model_bindings"]["binding-c"] = binding
    alias = deepcopy(bundle["model_aliases"]["general"])
    alias["candidates"] = [{"binding": "binding-c", "service_level": "full",
                            "priority": 0, "weight": 1}]
    bundle["model_aliases"]["messages"] = alias

    validation = checked(client.post(CONTROL + "/gateway/v1/config/validate",
                                     json={"kind": "bundle", "bundle": bundle}))
    if not validation.get("valid"):
        raise RuntimeError("Synthetic Messages configuration did not validate")
    revision = checked(client.post(CONTROL + "/gateway/v1/config/revisions",
                                   json={"kind": "bundle", "bundle": bundle},
                                   headers={"X-Gateway-Command-Id": str(uuid4())}), 201)["revision"]
    checked(client.post(CONTROL + f"/gateway/v1/config/revisions/{revision['revision']}/publish",
                        json={"expected_active_revision": current,
                              "candidate_snapshot_digest": revision["snapshot_digest"],
                              "description": "synthetic Messages protocol acceptance"},
                        headers={"X-Gateway-Command-Id": str(uuid4())}))
    deadline = time.monotonic() + 10
    while client.get(CONTROL + "/readyz").status_code != 200:
        if time.monotonic() >= deadline:
            raise RuntimeError("Gateway did not become ready after protocol publish")
        time.sleep(.1)

    schema = {"type": "object", "properties": {"answer": {"type": "integer"}},
              "required": ["answer"]}
    request = {"model": "messages", "max_completion_tokens": 32,
               "messages": [{"role": "user", "content": "Return the answer as JSON"}],
               "response_format": {"type": "json_schema", "json_schema": {
                   "name": "answer", "schema": schema}}}
    rejected = client.post(MODEL, json=request)
    if (rejected.status_code != 422
            or rejected.json().get("error", {}).get("code") != "unsupported_capability"
            or checked(client.get(RECORDS))["records"]):
        raise RuntimeError("Open-object Schema reached synthetic Messages Provider")

    closed = deepcopy(request)
    closed["response_format"]["json_schema"]["schema"]["additionalProperties"] = False
    response = client.post(MODEL, json=closed)
    result = checked(response)
    records = checked(client.get(RECORDS))["records"]
    if (len(records) != 1 or records[0] != {"model": "configured-c", "path": "/v1/messages",
                                         "schema_closed": True, "messages": 1}
            or result["model"] != "actual-messages"
            or json.loads(result["choices"][0]["message"]["content"]) != {"answer": 1}
            or result["usage"]["prompt_tokens"] != 5
            or result["usage"]["completion_tokens"] != 3):
        raise RuntimeError("Messages protocol mapping or structured result failed")
    return {"status": "passed", "mode": "two_protocols", "open_schema_status": 422,
            "messages_path": records[0]["path"], "actual_model": result["model"],
            "call_id": response.headers["x-gateway-call-id"], "real_provider_calls": 0}


if __name__ == "__main__":
    with httpx.Client(trust_env=False, timeout=30) as http:
        print(json.dumps(check(http)), flush=True)
