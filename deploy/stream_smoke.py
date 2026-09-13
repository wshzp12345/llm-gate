"""Explicit existing-config amendment and one live stream; no Secret access."""

import argparse
import json
import time
from uuid import uuid4

import httpx
from ruamel.yaml import YAML


def checked(response, expected=200):
    if response.status_code != expected:
        raise RuntimeError(f"Gateway HTTP {response.status_code}; expected {expected}")
    return response


def main():
    parser = argparse.ArgumentParser(description="Inspect, explicitly publish streaming capability, and optionally make one paid call")
    parser.add_argument("--management", default="http://127.0.0.1:8001")
    parser.add_argument("--model", default="http://127.0.0.1:8000")
    parser.add_argument("--binding", default="deepseek-text")
    parser.add_argument("--publish-streaming", action="store_true")
    parser.add_argument("--invoke", action="store_true")
    args = parser.parse_args()
    with httpx.Client(trust_env=False, timeout=310) as client:
        active = checked(client.get(args.management + "/gateway/v1/config/active")).json()["active"]
        if active is None:
            raise RuntimeError("Existing Active configuration required")
        revision = active["revision"]
        exported = checked(client.get(args.management + f"/gateway/v1/config/revisions/{revision}/export",
                                      headers={"Accept": "application/yaml"}))
        bundle = YAML(typ="safe", pure=True).load(exported.text)
        binding = bundle["provider_model_bindings"][args.binding]
        print(json.dumps({"revision": revision, "binding": args.binding,
                          "streaming": binding["capabilities"]["streaming"]}), flush=True)
        if not binding["capabilities"]["streaming"]:
            if not args.publish_streaming:
                if args.invoke:
                    raise RuntimeError("Binding lacks streaming; explicit --publish-streaming required")
                return
            # Export carries its exact base_revision; preserve every other field.
            binding["capabilities"]["streaming"] = True
            submission = {"kind": "bundle", "bundle": bundle}
            validated = checked(client.post(args.management + "/gateway/v1/config/validate", json=submission)).json()
            if not validated["valid"]:
                raise RuntimeError("Streaming configuration did not validate")
            candidate = checked(client.post(args.management + "/gateway/v1/config/revisions", json=submission,
                headers={"X-Gateway-Command-Id": str(uuid4())}), 201).json()["revision"]
            checked(client.post(args.management + f"/gateway/v1/config/revisions/{candidate['revision']}/publish",
                json={"expected_active_revision": revision, "candidate_snapshot_digest": candidate["snapshot_digest"],
                      "description": "Enable streaming for explicit live smoke"},
                headers={"X-Gateway-Command-Id": str(uuid4())}))
            print(json.dumps({"previous_revision": revision, "published_revision": candidate["revision"]}), flush=True)
        if not args.invoke:
            return
        deadline = time.monotonic() + 30
        while client.get(args.management + "/readyz").status_code != 200:
            if time.monotonic() >= deadline:
                raise RuntimeError("Gateway did not become ready")
            time.sleep(.1)
        # One logical request, no client-side retry. Existing Gateway policy is
        # unchanged; the final recovery summary discloses actual Attempt count.
        started = time.monotonic()
        first_delta = None
        content_size = 0
        terminal = None
        done = False
        with client.stream("POST", args.model + "/v1/chat/completions",
            headers={"Accept": "text/event-stream"}, json={"model": "general", "stream": True,
                "max_completion_tokens": 2048,
                "messages": [{"role": "user", "content": "Reply with the single word OK."}]}) as response:
            checked(response)
            call_id = response.headers["x-gateway-call-id"]
            if not response.headers.get("content-type", "").startswith("text/event-stream"):
                raise RuntimeError("Expected SSE response")
            for line in response.iter_lines():
                if not line:
                    continue
                if done or not line.startswith("data: "):
                    raise RuntimeError("Unexpected stream frame")
                if line == "data: [DONE]":
                    if terminal is None:
                        raise RuntimeError("DONE before settled terminal")
                    done = True
                    continue
                event = json.loads(line[6:])
                if "error" in event:
                    raise RuntimeError("Gateway stream failed: " + event["error"]["code"])
                if event["id"] != "chatcmpl-" + call_id or terminal is not None:
                    raise RuntimeError("Stream identity or terminal order changed")
                choice = event["choices"][0]
                text = choice["delta"].get("content", "")
                if text:
                    first_delta = first_delta if first_delta is not None else time.monotonic() - started
                    content_size += len(text.encode("utf-8"))
                if choice["finish_reason"] is not None:
                    terminal = event
        if not done or not content_size or terminal["choices"][0]["finish_reason"] != "stop":
            raise RuntimeError("Missing nonempty successful stream")
        print(json.dumps({"status": "passed", "call_id": call_id, "actual_model": terminal["model"],
            "content_bytes": content_size, "client_first_delta_seconds": first_delta,
            "client_total_seconds": time.monotonic() - started, "usage": terminal.get("usage"),
            "recovery": terminal["gateway"].get("recovery")}), flush=True)


if __name__ == "__main__":
    main()
