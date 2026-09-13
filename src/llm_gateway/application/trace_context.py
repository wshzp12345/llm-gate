"""Application-owned inbound correlation, independent of transport headers."""

from dataclasses import dataclass, field
from llm_gateway.domain.correlation import BusinessCorrelation


@dataclass(frozen=True)
class TraceContext:
    trace_id: str
    parent_span_id: str
    sampled: bool
    # Opaque propagation data, never included in AccessTrace or logging DTOs.
    tracestate: str = field(default="", repr=False)

