"""Bounded ASGI stream delivery; Invocation execution owns persistence/recovery."""

import asyncio
from contextvars import copy_context
import json
import math

from starlette.responses import Response

from llm_gateway.domain.streaming import StreamDelta
from llm_gateway.application.request_trace import current_trace


class ModelStreamResponse(Response):
    def __init__(self, invoke, *, error_response, max_event_bytes=262144, send_timeout=5):
        super().__init__(media_type="text/event-stream", headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no"})
        del self.headers["content-length"]
        if type(max_event_bytes) is not int or max_event_bytes < 1:
            raise ValueError("Positive SSE event bound required")
        if type(send_timeout) not in (int, float) or not math.isfinite(send_timeout) or send_timeout <= 0:
            raise ValueError("Finite positive write deadline required")
        self._invoke, self._error = invoke, error_response
        self._maximum, self._timeout = max_event_bytes, send_timeout
        self._context = copy_context()
        self._queue = asyncio.Queue(maxsize=1)
        self._metadata = None
        self._used = False

    def admitted(self, call_id, accepted_at):
        # Local metadata assignment only: no output before durable first delta.
        if self._metadata is not None:
            raise RuntimeError("Stream admission cannot be reused")
        self._metadata = {"id": "chatcmpl-" + str(call_id), "object": "chat.completion.chunk",
                          "created": int(accepted_at.timestamp())}

    def _frame(self, value):
        data = b"data: " + json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8") + b"\n\n"
        if len(data) > self._maximum:
            raise ValueError("SSE event exceeds output bound")
        return data

    async def delta(self, event):
        if not isinstance(event, StreamDelta) or self._metadata is None:
            raise ValueError("Admitted Gateway stream event required")
        frame = self._frame({**self._metadata, "model": event.resolved_model,
            "choices": [{"index": 0, "delta": {"content" if event.kind == "text" else "refusal": event.text},
                         "finish_reason": None}]})
        acknowledged = asyncio.get_running_loop().create_future()
        trace = current_trace()
        if trace is not None:
            trace.delta_ready()
        await self._queue.put((frame, acknowledged))
        await acknowledged

    async def __call__(self, scope, receive, send):
        if self._used:
            raise RuntimeError("Stream response cannot be reused")
        self._used = True
        finished = False

        async def disconnected():
            while (await receive())["type"] != "http.disconnect":
                pass

        async def write(message):
            nonlocal finished
            async with asyncio.timeout(self._timeout):
                await send(message)
            if message["type"] == "http.response.body" and not message.get("more_body", False):
                finished = True

        async def drive():
            worker = asyncio.create_task(self._invoke(self), context=self._context)
            pending = None
            started = False
            try:
                while True:
                    pending = asyncio.create_task(self._queue.get())
                    ready, _ = await asyncio.wait({pending, worker}, return_when=asyncio.FIRST_COMPLETED)
                    if worker in ready:
                        pending.cancel()
                        await asyncio.gather(pending, return_exceptions=True)
                        if worker.cancelled():
                            response = self._error("uncertain")
                        elif worker.exception() is not None:
                            response = self._error("internal")
                        else:
                            response = worker.result()
                        break
                    frame, acknowledgement = pending.result()
                    if not started:
                        started = True
                        headers = list(self.raw_headers)
                        headers.append((b"x-gateway-call-id", self._metadata["id"][9:].encode("ascii")))
                        await write({"type": "http.response.start", "status": 200, "headers": headers})
                    await write({"type": "http.response.body", "body": frame, "more_body": True})
                    trace = current_trace()
                    if trace is not None:
                        trace.delta_sent()
                    if not acknowledgement.done():
                        acknowledgement.set_result(None)
                    # On write failure, cancel only the owning invocation task
                    # below. Cancelling the acknowledgement here would cancel
                    # its shielded execution before durable cancel reservation.
                if not started and response.status_code != 200:
                    async with asyncio.timeout(self._timeout):
                        await response(scope, receive, write)
                    return
                body = json.loads(response.body)
                if not started:
                    headers = list(self.raw_headers)
                    if "x-gateway-call-id" in response.headers:
                        headers.append((b"x-gateway-call-id", response.headers["x-gateway-call-id"].encode("ascii")))
                    await write({"type": "http.response.start", "status": 200, "headers": headers})
                if response.status_code != 200:
                    await write({"type": "http.response.body", "body": self._frame(body), "more_body": False})
                    return  # A failed stream NEVER gets DONE.
                terminal = {key: value for key, value in body.items() if key != "choices"}
                terminal["object"] = "chat.completion.chunk"
                terminal["choices"] = [{"index": 0, "delta": {}, "finish_reason": body["choices"][0]["finish_reason"]}]
                await write({"type": "http.response.body", "body": self._frame(terminal), "more_body": True})
                await write({"type": "http.response.body", "body": b"data: [DONE]\n\n", "more_body": False})
            finally:
                if pending is not None and not pending.done():
                    pending.cancel()
                if not worker.done():
                    worker.cancel()
                await asyncio.gather(worker, *([pending] if pending is not None else []), return_exceptions=True)

        sending = asyncio.create_task(drive())
        disconnect = asyncio.create_task(disconnected())
        try:
            ready, _ = await asyncio.wait({sending, disconnect}, return_when=asyncio.FIRST_COMPLETED)
            if disconnect in ready:
                trace = current_trace()
                if not finished and trace is not None:
                    trace.mark_http_cancelled()
                sending.cancel()
            await asyncio.shield(sending)
        except asyncio.CancelledError:
            if not disconnect.done():
                raise
        finally:
            if not sending.done() and not sending.cancelling():
                sending.cancel()
            disconnect.cancel()
            # Own cleanup through repeated cancellation; the invocation's own
            # lifecycle decides the durable outcome before releasing permits.
            cleanup = asyncio.gather(sending, disconnect, return_exceptions=True)
            while not cleanup.done():
                try:
                    await asyncio.shield(cleanup)
                except asyncio.CancelledError:
                    continue
