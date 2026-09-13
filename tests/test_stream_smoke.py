import copy
import json
import sys

import httpx
import pytest

from deploy import stream_smoke


@pytest.mark.parametrize("mode", ["success", "error", "missing_done", "early_done", "empty", "after_done"])
def test_live_smoke_uses_explicit_single_field_publication_and_one_request(monkeypatch, capsys, mode):
    bundle = {"base_revision": "7", "provider_model_bindings": {"deepseek-text": {
        "capabilities": {"streaming": False, "tool_calling": False}}}, "preserve": {"value": 42}}
    original = copy.deepcopy(bundle)
    requests = []
    event = {"id": "chatcmpl-call", "model": "actual", "choices": [{"delta": {"content": "OK"}, "finish_reason": None}]}
    terminal = {**event, "choices": [{"delta": {}, "finish_reason": "stop"}],
                "gateway": {"recovery": {"attempts": 1, "retries": 0, "fallback_used": False}}}
    frames = [event, terminal, "[DONE]"]
    if mode == "error":
        frames = [{"error": {"code": "provider_protocol_error"}}]
    elif mode == "missing_done":
        frames.pop()
    elif mode == "early_done":
        frames = ["[DONE]"]
    elif mode == "empty":
        frames = [terminal, "[DONE]"]
    elif mode == "after_done":
        frames.append(event)

    def handler(request):
        requests.append(request)
        path = request.url.path
        if path.endswith("/active"):
            return httpx.Response(200, json={"active": {"revision": "7"}})
        if path.endswith("/export"):
            return httpx.Response(200, text=json.dumps(original))
        if path.endswith("/validate") or path.endswith("/revisions"):
            sent = json.loads(request.content)
            expected = copy.deepcopy(original)
            expected["provider_model_bindings"]["deepseek-text"]["capabilities"]["streaming"] = True
            assert sent == {"kind": "bundle", "bundle": expected}
            return httpx.Response(200, json={"valid": True}) if path.endswith("/validate") else httpx.Response(
                201, json={"revision": {"revision": "8", "snapshot_digest": "digest"}})
        if path.endswith("/publish"):
            assert json.loads(request.content)["expected_active_revision"] == "7"
            return httpx.Response(200, json={})
        if path == "/readyz":
            return httpx.Response(200)
        assert path == "/v1/chat/completions"
        assert json.loads(request.content)["stream"] is True
        return httpx.Response(200, headers={"Content-Type": "text/event-stream", "X-Gateway-Call-Id": "call"},
            content=b"".join(b"data: " + (value.encode() if isinstance(value, str) else json.dumps(value).encode()) + b"\n\n" for value in frames))

    client_type = httpx.Client
    monkeypatch.setattr(stream_smoke.httpx, "Client", lambda **kwargs: client_type(transport=httpx.MockTransport(handler), **kwargs))
    monkeypatch.setattr(sys, "argv", ["stream_smoke", "--publish-streaming", "--invoke"])
    if mode == "success":
        stream_smoke.main()
        assert '"status": "passed"' in capsys.readouterr().out
    else:
        with pytest.raises(RuntimeError):
            stream_smoke.main()
        assert '"status": "passed"' not in capsys.readouterr().out
    assert sum(request.url.path == "/v1/chat/completions" for request in requests) == 1
    assert original == bundle
