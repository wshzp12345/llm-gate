"""Isolated deterministic HTTP fault fixture; no credentials or text in logs."""

import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import select
import socket
import sys
import time


mode = sys.argv[1]
records = []


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def respond(self, status, body):
        raw = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        self.respond(200, {"records": records} if self.path == "/records" else {"healthy": True})

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        if self.path != "/v1/chat/completions" or not 0 < length <= 1048576 or len(records) >= 16:
            self.respond(400, {"error": "invalid_fixture_request"})
            return
        payload = json.loads(self.rfile.read(length))
        projection = {name: payload.get(name) for name in ("messages", "temperature", "top_p", "max_tokens", "stream")}
        record = {"model": payload["model"], "intent_digest": hashlib.sha256(
            json.dumps(projection, sort_keys=True, ensure_ascii=False).encode()).hexdigest()}
        records.append(record)
        fault = next((name for name in ("broken", "disconnect") if payload.get("stream") and
            payload.get("messages") == [{"role": "user", "content": "fixture:" + name}]), None)
        if fault:
            record["closed"] = False
            try:
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.end_headers()
                event = {"id": "fault-fixture", "object": "chat.completion.chunk", "model": "actual-primary",
                    "choices": [{"index": 0, "delta": {"content": "partial"}, "finish_reason": None}]}
                self.wfile.write(b"data: " + json.dumps(event).encode() + b"\n\n")
                self.wfile.flush()
                if fault == "disconnect":
                    deadline = time.monotonic() + 15
                    while time.monotonic() < deadline:
                        readable, _, _ = select.select([self.connection], [], [], .1)
                        if readable and not self.connection.recv(1, socket.MSG_PEEK):
                            record["peer_disconnected"] = True
                            break
                # Both fault modes deliberately omit finish and DONE.
            finally:
                record["closed"] = True
            return
        if mode == "unavailable":
            self.respond(503, {"error": "private-upstream-body"})
        elif payload.get("stream"):
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.end_headers()
            for delta, finish in (({"content": "OK"}, None), ({}, "stop")):
                event = {"id": "synthetic-provider-stream", "object": "chat.completion.chunk",
                    "model": "actual-secondary", "choices": [{"index": 0, "delta": delta, "finish_reason": finish}]}
                if finish:
                    event["usage"] = {"prompt_tokens": 10, "completion_tokens": 2, "total_tokens": 12}
                self.wfile.write(b"data: " + json.dumps(event).encode() + b"\n\n")
                self.wfile.flush()
            self.wfile.write(b"data: [DONE]\n\n")
            self.wfile.flush()
        else:
            self.respond(200, {"object": "chat.completion", "model": "actual-secondary",
                "vendor_extension": "private-vendor-data", "choices": [{"index": 0,
                "message": {"role": "assistant", "content": "OK"}, "finish_reason": "stop"}],
                "usage": {"prompt_tokens": 10, "completion_tokens": 2, "total_tokens": 12}})


ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
