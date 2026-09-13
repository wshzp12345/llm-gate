"""Send synthetic telemetry only; no database, credentials or Provider calls."""

import asyncio
import json

from llm_gateway.adapters.otlp_http import OtlpHttpOutput
from llm_gateway.application.request_trace import RequestTrace, TraceStage
from llm_gateway.application.trace_export import BoundedTraceExporter


class NoInvocationReader:
    async def read(self, call_id):
        raise AssertionError("Synthetic unadmitted access must not query storage")


async def main():
    trace = RequestTrace()
    with trace.activate(), trace.span(TraceStage.HTTP):
        with trace.span(TraceStage.AUTHORIZATION):
            pass
    async with OtlpHttpOutput("http://127.0.0.1:14318", allow_plaintext=True).hold() as output:
        exporter = BoundedTraceExporter(NoInvocationReader(), output)
        async with exporter.hold():
            assert exporter.offer(trace.finish())
        assert exporter.counters.exported == 1, exporter.counters
        assert all(item.accepted == 1 and item.failed == 0 for item in output.counters.values())
    print(json.dumps({"trace_id": trace.trace_id, "request_id": str(trace.request_id),
        "signals_accepted": ["traces", "logs", "metrics"], "provider_calls": 0}))


if __name__ == "__main__":
    asyncio.run(main())
