"""Explicit Prompt lifecycle acceptance; --invoke makes exactly one paid request."""

import argparse
import json

import httpx


def checked(response, status):
    if response.status_code != status:
        raise RuntimeError(f"Prompt acceptance returned HTTP {response.status_code}; expected {status}")
    return response.json()


def main():
    parser = argparse.ArgumentParser(description="Create/publish test Prompt assets; optionally invoke once")
    parser.add_argument("--management", default="http://127.0.0.1:8001")
    parser.add_argument("--model", default="http://127.0.0.1:8000")
    parser.add_argument("--invoke", action="store_true")
    args = parser.parse_args()
    with httpx.Client(trust_env=False, timeout=310) as client:
        if client.get(args.management + "/readyz").status_code != 200:
            raise RuntimeError("Gateway is not ready")
        created = checked(client.post(args.management + "/gateway/v1/prompts", json={"messages": [
            {"role": "user", "content": "Reply with the single word {{answer}}."}]}), 201)
        asset_id, version_id = created["asset_id"], created["version_id"]
        path = args.management + "/gateway/v1/prompts/" + asset_id
        rendering = path + "/versions/" + version_id + "/render"
        checked(client.post(rendering, json={"variables": {"answer": "OK"}}), 404)
        checked(client.post(path + "/publish", json={"version_id": version_id, "expected_generation": 0}), 200)
        second = checked(client.post(path + "/versions", json={"messages": [
            {"role": "user", "content": "Reply with the single word CHANGED."}]}), 201)
        checked(client.post(path + "/publish", json={"version_id": second["version_id"], "expected_generation": 1}), 200)
        checked(client.post(path + "/publish", json={"version_id": version_id, "expected_generation": 1}), 409)
        rendered = checked(client.post(rendering, json={"variables": {"answer": "OK"}}), 200)
        if rendered["messages"] != [{"role": "user", "content": "Reply with the single word OK."}]:
            raise RuntimeError("Historical Prompt version was not pinned")
        checked(client.post(rendering, json={"variables": {}}), 422)
        evidence = {"asset_id": asset_id, "pinned_version": version_id,
                    "current_version": second["version_id"], "publication_generation": 2,
                    "management_checks": "passed", "provider_calls": 0}
        if args.invoke:
            # No automatic retries or private user text. Existing credentials are
            # read only by the Gateway, never by this API-only acceptance client.
            response = client.post(args.model + "/v1/chat/completions", json={"model": "general",
                "prompt": {"asset_id": asset_id, "version_id": version_id, "variables": {"answer": "OK"}},
                "max_tokens": 2048})
            result = checked(response, 200)
            if result["gateway"]["prompt"]["version_id"] != version_id or not result["choices"][0]["message"]["content"]:
                raise RuntimeError("Prompt call did not return a pinned nonempty result")
            evidence.update(provider_calls=1, call_id=response.headers["x-gateway-call-id"],
                            actual_model=result["model"], usage=result.get("usage"),
                            finish_reason=result["choices"][0]["finish_reason"])
        print(json.dumps(evidence))


if __name__ == "__main__":
    main()
