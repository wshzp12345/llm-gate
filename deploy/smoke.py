"""Explicit API-only first publication and one model call; no Secret access."""

import argparse
import json
from pathlib import Path
import time
from uuid import uuid4

import httpx


def check(response, expected):
    if response.status_code != expected:
        raise RuntimeError(f"HTTP {response.status_code}; expected {expected}")
    return response


def main():
    parser = argparse.ArgumentParser(description="API publish and one bounded live smoke call (may incur cost)")
    parser.add_argument("--management", default="http://127.0.0.1:8001")
    parser.add_argument("--model", default="http://127.0.0.1:8000")
    parser.add_argument("--bundle", type=Path, default=Path("deploy/deepseek.bundle.json"))
    parser.add_argument("--invoke-only", action="store_true")
    args = parser.parse_args()
    with httpx.Client(trust_env=False, timeout=310) as client:
        check(client.get(args.management + "/healthz"), 200)
        check(client.get(args.model + "/healthz"), 404)
        check(client.post(args.management + "/v1/chat/completions", json={}), 404)
        if not args.invoke_only:
            # Fail on a nonempty database instead of replacing its configuration.
            if check(client.get(args.management + "/gateway/v1/config/active"), 200).json() != {"active": None}:
                raise RuntimeError("Refusing to replace an existing Active configuration; use --invoke-only")
            check(client.get(args.management + "/readyz"), 503)
            submission = json.loads(args.bundle.read_bytes())
            validated = check(client.post(args.management + "/gateway/v1/config/validate", json=submission), 200)
            if validated.json()["valid"] is not True:
                raise RuntimeError("Bundle validation failed")
            created = check(client.post(args.management + "/gateway/v1/config/revisions", json=submission,
                headers={"X-Gateway-Command-Id": str(uuid4())}), 201).json()["revision"]
            check(client.get(args.management + "/readyz"), 503)
            check(client.post(args.management + f"/gateway/v1/config/revisions/{created['revision']}/publish",
                json={"expected_active_revision": "0", "candidate_snapshot_digest": created["snapshot_digest"],
                    "description": "Explicit API smoke publication"},
                headers={"X-Gateway-Command-Id": str(uuid4())}), 200)
        deadline = time.monotonic() + 30
        while client.get(args.management + "/readyz").status_code != 200:
            if time.monotonic() >= deadline:
                raise RuntimeError("Gateway did not become ready")
            time.sleep(.1)
        # No retry, no private user data, no credential in this request.
        response = client.post(args.model + "/v1/chat/completions", json={"model": "general",
            "messages": [{"role": "user", "content": "Reply with the single word OK."}], "max_tokens": 256})
        check(response, 200)
        result = response.json()
        if not result["choices"][0]["message"]["content"]:
            raise RuntimeError("Model response contained no final text")
        print(json.dumps({"http_status": 200, "model": result["model"],
            "content": result["choices"][0]["message"]["content"], "usage": result.get("usage")}, ensure_ascii=False))


if __name__ == "__main__":
    main()
