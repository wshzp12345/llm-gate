"""Bounded best-effort telemetry, isolated from request execution ownership."""

import asyncio
from contextlib import asynccontextmanager
from contextvars import Context
from dataclasses import dataclass, replace
import math
from typing import Protocol

from llm_gateway.application.invocation_observation import InvocationObservation, InvocationObservationReader
from llm_gateway.application.request_trace import AccessTrace
from llm_gateway.application.access_metrics import AccessMetrics, AccessMetricsSnapshot
from llm_gateway.application.invocation_metrics import InvocationMetrics, MetricPoint


@dataclass(frozen=True)
class TraceExport:
    access: AccessTrace
    invocation: InvocationObservation | None
    enrichment: str
    metrics: AccessMetricsSnapshot | None = None
    counters: "ExportCounters | None" = None
    invocation_metrics: tuple[MetricPoint, ...] = ()
    resource_metrics: tuple[tuple[str, int], ...] = ()


class TraceExportPort(Protocol):
    async def write(self, record: TraceExport) -> None:
        """Bounded, cancellation-cooperative I/O; no hidden queue or retries."""
        ...


@dataclass(frozen=True)
class ExportCounters:
    accepted: int = 0
    dropped: int = 0
    exported: int = 0
    failed: int = 0
    enrichment_failed: int = 0
    abandoned: int = 0


class BoundedTraceExporter:
    def __init__(self, reader: InvocationObservationReader, output: TraceExportPort, *,
                 capacity=256, operation_timeout=2, drain_timeout=5, resource_reader=None):
        if type(capacity) is not int or not 1 <= capacity <= 4096:
            raise ValueError("Bounded export capacity required")
        for value in (operation_timeout, drain_timeout):
            if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
                raise ValueError("Finite export deadline required")
        self._reader, self._output = reader, output
        self._queue = asyncio.Queue(maxsize=capacity)
        self._timeout, self._drain_timeout = operation_timeout, drain_timeout
        self._counters = ExportCounters()
        self.metrics = AccessMetrics()
        self.invocation_metrics = InvocationMetrics()
        self._resource_reader = resource_reader
        self._started = self._accepting = False
        self._worker = self._loop = None

    @property
    def counters(self):
        return self._counters

    def _count(self, field):
        self._counters = replace(self._counters, **{field: getattr(self._counters, field) + 1})

    def offer(self, record):
        # Called synchronously on the owning request loop. No database/export
        # I/O, task creation, blocking wait or reference to request context.
        try:
            same_loop = asyncio.get_running_loop() is self._loop
        except RuntimeError:
            same_loop = False
        if same_loop and type(record) is AccessTrace and len(record.spans) <= 256:
            self.metrics.observe(record)
        if (not same_loop or not self._accepting or self._worker.done()
                or type(record) is not AccessTrace or len(record.spans) > 256):
            self._count("dropped")
            return False
        try:
            self._queue.put_nowait(record)
        except asyncio.QueueFull:
            self._count("dropped")
            return False
        self._count("accepted")
        return True

    async def _enrich(self, access):
        if access.call_id is None:
            return TraceExport(access, None, "not_admitted")
        try:
            async with asyncio.timeout(self._timeout):
                result = await self._reader.read(access.call_id)
            if result is None:
                self._count("enrichment_failed")
                return TraceExport(access, None, "missing")
            if (type(result) is not InvocationObservation or result.call_id != access.call_id
                    or result.trace_id != access.trace_id or result.correlation != access.correlation):
                self._count("enrichment_failed")
                return TraceExport(access, None, "identity_mismatch")
            return TraceExport(access, result, "available")
        except Exception:
            self._count("enrichment_failed")
            return TraceExport(access, None, "unavailable")

    async def _run(self):
        while True:
            access = await self._queue.get()
            record = None
            try:
                record = await self._enrich(access)
                self.invocation_metrics.observe(record.invocation)
                resources = {"export.queue_used": self._queue.qsize(), "export.queue_limit": self._queue.maxsize}
                if self._resource_reader is not None:
                    try:
                        resources.update(self._resource_reader())
                    except Exception:
                        pass  # Missing gauge is not zero; no change to invocation/export outcome.
                record = replace(record, metrics=self.metrics.snapshot(), counters=self.counters,
                    invocation_metrics=self.invocation_metrics.snapshot(), resource_metrics=tuple(resources.items()))
                async with asyncio.timeout(self._timeout):
                    await self._output.write(record)
                self._count("exported")
            except asyncio.CancelledError:
                self._count("abandoned")
                raise
            except Exception:
                self._count("failed")
            finally:
                self._queue.task_done()
                del access, record

    async def _drain(self):
        try:
            async with asyncio.timeout(self._drain_timeout):
                await self._queue.join()
        except TimeoutError:
            pass
        finally:
            self._worker.cancel()
            await asyncio.gather(self._worker, return_exceptions=True)
            while not self._queue.empty():
                self._queue.get_nowait()
                self._queue.task_done()
                self._count("abandoned")

    @asynccontextmanager
    async def hold(self):
        if self._started:
            raise RuntimeError("Exporter lifecycle cannot be reused")
        self._started = self._accepting = True
        self._loop = asyncio.get_running_loop()
        # Never inherit tenant authorization, inbound tracestate or a current
        # request/parent span into the long-lived export worker.
        self._worker = asyncio.create_task(self._run(), context=Context())
        try:
            yield self
        finally:
            self._accepting = False
            cleanup = asyncio.create_task(self._drain(), context=Context())
            interrupted = False
            while not cleanup.done():
                try:
                    await asyncio.shield(cleanup)
                except asyncio.CancelledError:
                    interrupted = True
            cleanup.result()
            if interrupted:
                raise asyncio.CancelledError()
