"""Bounded, content-free access tracing; no framework, exporter or I/O."""

import asyncio
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from enum import StrEnum
import time
from typing import Protocol
from uuid import UUID, uuid4
from llm_gateway.application.trace_context import BusinessCorrelation, TraceContext


class TraceStage(StrEnum):
    HTTP = "http"
    AUTHORIZATION = "authorization"
    PROMPT = "prompt"
    FINGERPRINT = "fingerprint"
    ADMISSION = "admission"
    ROUTING = "routing"
    PROVIDER_ATTEMPT = "provider_attempt"
    ADAPTER_REQUEST = "adapter_request"
    SETTLEMENT = "settlement"
    CANCELLATION = "cancellation"
    BACKOFF = "backoff"
    HTTP_SEND = "http_send"


@dataclass(frozen=True)
class TraceSpan:
    trace_id: str
    span_id: str
    parent_span_id: str | None
    stage: TraceStage
    start_offset_ns: int
    duration_ns: int
    outcome: str
    attempt_number: int | None = None
    first_delta_ns: int | None = None


class SpanOutcome:
    def __init__(self, clock=None, started=None, parent=None):
        self.failed = False
        self.first_delta_ns = None
        self._clock, self._started, self._parent = clock, started, parent

    def mark_failed(self):
        self.failed = True

    def mark_first_delta(self):
        if self.first_delta_ns is None and self._clock is not None:
            self.first_delta_ns = max(0, self._clock() - self._started)

    @contextmanager
    def yield_to_caller(self):
        # A suspended Adapter generator does not own downstream application spans.
        token = _parent.set(self._parent)
        try:
            yield
        finally:
            _parent.reset(token)


@dataclass(frozen=True)
class SendTiming:
    calls: int = 0
    duration_ns: int = 0
    errors: int = 0
    cancellations: int = 0


@dataclass(frozen=True)
class AccessTrace:
    request_id: UUID
    trace_id: str
    call_id: UUID | None
    spans: tuple[TraceSpan, ...]
    dropped_spans: int
    http_send: SendTiming
    first_delta_ready_ns: int | None
    first_delta_sent_ns: int | None
    correlation: BusinessCorrelation
    parent_sampled: bool | None
    started_unix_ns: int


class AccessTraceSink(Protocol):
    def offer(self, trace: AccessTrace) -> bool:
        """Accept into bounded export resources without I/O; False means drop."""
        ...


_current: ContextVar["RequestTrace | None"] = ContextVar("gateway_request_trace", default=None)
_parent: ContextVar[tuple[str, str] | None] = ContextVar("gateway_parent_span", default=None)


def current_trace():
    return _current.get()


def current_trace_id():
    trace = current_trace()
    return trace.trace_id if trace is not None else uuid4().hex


class RequestTrace:
    def __init__(self, *, clock=time.monotonic_ns, wall_clock=time.time_ns, capacity=64, incoming: TraceContext | None = None):
        if type(capacity) is not int or not 1 <= capacity <= 256:
            raise ValueError("Bounded trace capacity required")
        if incoming is not None and not isinstance(incoming, TraceContext):
            raise ValueError("Typed incoming Trace context required")
        self.request_id, self.trace_id = uuid4(), uuid4().hex
        self.incoming = incoming
        if incoming is not None:
            self.trace_id = incoming.trace_id
        self.correlation = BusinessCorrelation()
        self._clock, self._capacity = clock, capacity
        self._started = clock()
        self._started_unix_ns = wall_clock()
        self._spans = []
        self._allocated = 0
        self._dropped = 0
        self._call_id = None
        self._closed = False
        self._http_failed = False
        self._http_cancelled = False
        self._send = SendTiming()
        self._first_delta_ready = None
        self._first_delta_sent = None

    def delta_ready(self):
        if not self._closed and self._first_delta_ready is None:
            self._first_delta_ready = max(0, self._clock() - self._started)

    def delta_sent(self):
        if not self._closed and self._first_delta_ready is not None and self._first_delta_sent is None:
            self._first_delta_sent = max(0, self._clock() - self._started)

    @contextmanager
    def http_send(self):
        """Aggregate ASGI sends in constant space without consuming span slots."""
        if self._closed:
            raise ValueError("Closed request trace")
        started = self._clock()
        errors = cancellations = 0
        try:
            yield
        except asyncio.CancelledError:
            cancellations = 1
            raise
        except BaseException:
            errors = 1
            raise
        finally:
            previous = self._send
            self._send = SendTiming(previous.calls + 1,
                previous.duration_ns + max(0, self._clock() - started),
                previous.errors + errors, previous.cancellations + cancellations)

    def mark_http_failed(self):
        self._http_failed = True

    def mark_http_cancelled(self):
        self._http_cancelled = True

    def admitted(self, call_id):
        if not isinstance(call_id, UUID) or self._closed or self._call_id is not None:
            raise ValueError("Exactly one admitted invocation identity required")
        self._call_id = call_id

    @contextmanager
    def activate(self):
        if self._closed:
            raise ValueError("Closed request trace")
        token = _current.set(self)
        parent_token = _parent.set(None)
        try:
            yield self
        finally:
            _parent.reset(parent_token)
            _current.reset(token)

    @contextmanager
    def span(self, stage: TraceStage, *, attempt_number=None):
        if not isinstance(stage, TraceStage) or self._closed:
            raise ValueError("Open trace and registered stage required")
        if attempt_number is not None and (stage != TraceStage.PROVIDER_ATTEMPT or type(attempt_number) is not int or not 1 <= attempt_number <= 3):
            raise ValueError("Attempt span requires bounded attempt identity")
        parent = _parent.get()
        parent_id = (parent[1] if parent is not None and parent[0] == self.trace_id else
                     self.incoming.parent_span_id if self.incoming is not None else None)
        span_id = uuid4().hex[:16]
        retained = self._allocated < self._capacity
        if retained:
            self._allocated += 1
        else:
            self._dropped += 1
        token = _parent.set((self.trace_id, span_id))
        started = self._clock()
        outcome = "ok"
        observed = SpanOutcome(self._clock, started, parent)
        try:
            yield observed
            if stage == TraceStage.HTTP and self._http_cancelled:
                outcome = "cancelled"
            elif observed.failed or stage == TraceStage.HTTP and self._http_failed:
                outcome = "error"
        except asyncio.CancelledError:
            outcome = "cancelled"
            raise
        except BaseException:
            outcome = "error"
            raise
        finally:
            ended = self._clock()
            _parent.reset(token)
            # No exception strings, messages, headers or arbitrary attributes.
            if retained:
                self._spans.append(TraceSpan(self.trace_id, span_id, parent_id, stage,
                    max(0, started - self._started), max(0, ended - started), outcome, attempt_number,
                    observed.first_delta_ns))

    def finish(self):
        if self._closed:
            raise ValueError("Request trace already finished")
        self._closed = True
        return AccessTrace(self.request_id, self.trace_id, self._call_id, tuple(self._spans), self._dropped,
                           self._send, self._first_delta_ready, self._first_delta_sent,
                           self.correlation, self.incoming.sampled if self.incoming is not None else None,
                           self._started_unix_ns)


@contextmanager
def trace_stage(stage: TraceStage, *, attempt_number=None):
    trace = current_trace()
    if trace is None:
        yield SpanOutcome()
    else:
        with trace.span(stage, attempt_number=attempt_number) as observed:
            yield observed
