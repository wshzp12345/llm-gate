"""Explicit OTLP/HTTP JSON output with bounded responses and no retries."""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
import json
import math
import time
from urllib.parse import urlsplit
from uuid import uuid4

import httpx

from llm_gateway.adapters.configuration_json import ParseLimits, parse_strict_json
from llm_gateway.adapters.otlp_json import payloads


class OtlpExportFailure(Exception):
    """Deliberately content-free transport/protocol failure."""


@dataclass(frozen=True)
class SignalCounters:
    accepted: int = 0
    failed: int = 0
    rejected: int = 0
    warnings: int = 0


class OtlpHttpOutput:
    def __init__(self, endpoint, *, allow_plaintext=False, timeout=1, transport=None):
        if type(endpoint) is not str or type(allow_plaintext) is not bool or "\\" in endpoint:
            raise ValueError("Explicit OTLP origin required")
        parsed = urlsplit(endpoint)
        if (parsed.scheme not in {"https", "http"} or not parsed.hostname or parsed.username is not None
                or parsed.password is not None or parsed.query or parsed.fragment
                or parsed.path not in {"", "/"} or (parsed.scheme == "http" and not allow_plaintext)
                or any(ord(char) <= 32 for char in endpoint)):
            raise ValueError("Explicit OTLP origin without credentials required")
        if type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("Finite OTLP timeout required")
        self._origin, self._timeout, self._transport = endpoint.rstrip("/"), timeout, transport
        self._instance = str(uuid4())
        self._client = None
        self._used = False
        self._stats = {signal: SignalCounters() for signal in ("traces", "logs", "metrics")}

    @property
    def counters(self):
        return dict(self._stats)

    def _count(self, signal, field, amount=1):
        current = self._stats[signal]
        self._stats[signal] = replace(current, **{field: getattr(current, field) + amount})

    @asynccontextmanager
    async def hold(self):
        if self._used:
            raise RuntimeError("OTLP output lifecycle cannot be reused")
        self._used = True
        async with httpx.AsyncClient(transport=self._transport, timeout=self._timeout, trust_env=False,
                follow_redirects=False, limits=httpx.Limits(max_connections=3, max_keepalive_connections=3),
                headers={"Accept": "application/json", "Accept-Encoding": "identity"}) as client:
            self._client = client
            try:
                yield self
            finally:
                self._client = None

    async def _send(self, signal, body):
        try:
            async with self._client.stream("POST", self._origin + "/v1/" + signal,
                    content=body, headers={"Content-Type": "application/json"}) as response:
                if (response.status_code != 200 or response.headers.get("content-type", "").split(";")[0].strip().lower() != "application/json"
                        or response.headers.get("content-encoding", "identity").lower() != "identity"):
                    raise OtlpExportFailure()
                raw = bytearray()
                async for chunk in response.aiter_raw(chunk_size=4096):
                    if len(raw) + len(chunk) > 65536:
                        raise OtlpExportFailure()
                    raw.extend(chunk)
            answer = parse_strict_json(bytes(raw), ParseLimits(65536, 16, 2048, 128, 8192))
            if not isinstance(answer, dict):
                raise OtlpExportFailure()
            if "partialSuccess" in answer:
                partial = answer["partialSuccess"]
                if not isinstance(partial, dict):
                    raise OtlpExportFailure()
                field = {"traces": "rejectedSpans", "logs": "rejectedLogRecords", "metrics": "rejectedDataPoints"}[signal]
                rejected = partial.get(field, 0)
                if type(rejected) is str and rejected.isascii() and rejected.isdecimal() and len(rejected) <= 19:
                    rejected = int(rejected)
                if type(rejected) is not int or not 0 <= rejected <= 2**63 - 1:
                    raise OtlpExportFailure()
                if "errorMessage" in partial and type(partial["errorMessage"]) is not str:
                    raise OtlpExportFailure()
                if rejected:
                    self._count(signal, "rejected", rejected)
                    raise OtlpExportFailure()
                if partial.get("errorMessage"):
                    self._count(signal, "warnings")  # Never retain raw collector diagnostics.
            self._count(signal, "accepted")
        except asyncio.CancelledError:
            raise
        except Exception:
            self._count(signal, "failed")
            raise OtlpExportFailure() from None

    async def write(self, record):
        if self._client is None:
            raise OtlpExportFailure()
        bodies = {signal: json.dumps(value, ensure_ascii=False, allow_nan=False,
            separators=(",", ":")).encode("utf-8") for signal, value in payloads(record,
                instance_id=self._instance, observed_unix_ns=time.time_ns()).items()}
        if any(len(body) > 1048576 for body in bodies.values()):
            raise OtlpExportFailure()
        # Three fixed signals, no retry or unbounded destination fan-out. One
        # signal failing must not suppress delivery of the other signals.
        results = await asyncio.gather(*(self._send(signal, body) for signal, body in bodies.items()), return_exceptions=True)
        if any(isinstance(result, BaseException) for result in results):
            raise OtlpExportFailure()
