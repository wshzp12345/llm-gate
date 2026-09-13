"""W3C Trace Context Level 1 extraction; no outbound propagation authority."""

import re

from llm_gateway.application.trace_context import BusinessCorrelation, TraceContext


_PARENT = re.compile(rb"([0-9a-f]{2})-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})")
_KEY = re.compile(r"(?:[a-z][a-z0-9_*/-]{0,255}|[a-z0-9][a-z0-9_*/-]{0,240}@[a-z][a-z0-9_*/-]{0,13})")
_VALUE = re.compile(r"[\x20-\x2b\x2d-\x3c\x3e-\x7e]{0,255}[\x21-\x2b\x2d-\x3c\x3e-\x7e]")
_BUSINESS = re.compile(rb"[A-Za-z0-9._:-]{1,128}")


def _state(values):
    combined = b",".join(values)
    if len(combined) > 512:
        return ""  # Local retention ceiling; never retain a truncated entry.
    try:
        members = combined.decode("ascii").split(",")
    except UnicodeDecodeError:
        return ""
    if len(members) > 32:
        return ""
    result, keys = [], set()
    for member in members:
        member = member.strip(" \t")
        if not member:
            continue
        key, separator, value = member.partition("=")
        if not separator or not _KEY.fullmatch(key) or not _VALUE.fullmatch(value) or key in keys:
            return ""
        keys.add(key)
        result.append(member)
    return ",".join(result)


def incoming_trace(headers):
    parents = [value for name, value in headers if name.lower() == b"traceparent"]
    if len(parents) != 1:
        return None
    value = parents[0].strip(b" \t")
    match = _PARENT.match(value)
    if match is None:
        return None
    version, trace_id, parent_id, flags = match.groups()
    if version == b"ff" or trace_id == b"0" * 32 or parent_id == b"0" * 16:
        return None
    if len(value) != 55 and (version == b"00" or value[55:56] != b"-"):
        return None
    return TraceContext(trace_id.decode("ascii"), parent_id.decode("ascii"), bool(int(flags, 16) & 1),
                        _state([value for name, value in headers if name.lower() == b"tracestate"]))


def business_correlation(headers):
    result = []
    for field in (b"task", b"turn", b"step"):
        values = [value for name, value in headers if name.lower() == b"x-gateway-" + field + b"-id"]
        if len(values) > 1 or values and not _BUSINESS.fullmatch(values[0]):
            raise ValueError("Invalid business correlation")
        result.append(values[0].decode("ascii") if values else None)
    correlation = BusinessCorrelation(*result)
    declarations = []
    for field, default, allowed in ((b"caller-type", b"non-agent", {b"agent", b"non-agent"}),
            (b"operation-scope", b"turn", {b"turn", b"step"})):
        values = [value for name, value in headers if name.lower() == b"x-gateway-" + field]
        if len(values) > 1 or values and values[0] not in allowed:
            raise ValueError("Invalid correlation declaration")
        declarations.append((values[0] if values else default).decode("ascii"))
    correlation.require_for(*declarations)
    return correlation
