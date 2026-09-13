import asyncio
from dataclasses import asdict

import pytest
from starlette.responses import Response

from llm_gateway.adapters.model_http import _ResponseIdentity
from llm_gateway.adapters.trace_context import incoming_trace, business_correlation
from llm_gateway.application.request_trace import RequestTrace, TraceStage, current_trace


TRACE = b"4bf92f3577b34da6a3ce929d0e0e4736"
PARENT = b"00f067aa0ba902b7"
VALID = b"00-" + TRACE + b"-" + PARENT + b"-01"


@pytest.mark.parametrize("value", [b"", VALID.upper(), VALID[:-1], VALID + b"-extra",
    b"ff" + VALID[2:], VALID.replace(TRACE, b"0" * 32), VALID.replace(PARENT, b"0" * 16),
    VALID[:-2] + b"zz", b"01" + VALID[2:] + b"x"])
def test_invalid_traceparent_is_ignored(value):
    assert incoming_trace([(b"traceparent", value), (b"tracestate", b"vendor=opaque")]) is None


@pytest.mark.parametrize("value", [VALID, b"01" + VALID[2:], b"01" + VALID[2:] + b"-unknown", b" \t" + VALID + b" "])
def test_supported_and_future_traceparent(value):
    context = incoming_trace([(b"TraceParent", value)])
    assert context.trace_id == TRACE.decode() and context.parent_span_id == PARENT.decode()
    assert context.sampled


def test_duplicates_and_orphan_state_restart_context():
    assert incoming_trace([(b"traceparent", VALID), (b"TraceParent", VALID)]) is None
    assert incoming_trace([(b"tracestate", b"vendor=opaque")]) is None


@pytest.mark.parametrize("state", [b"a=one,a=two", b"A=value", b"1=value", b"a=", b"a=x=y",
    b"a=x\ny", b"a=x\ty", b"a=\xff", b"a=" + b"x" * 257, b"," * 32,
    b"a=" + b"x" * 255 + b",b=" + b"y" * 255])
def test_bad_tracestate_does_not_break_valid_parent(state):
    context = incoming_trace([(b"traceparent", VALID), (b"tracestate", state)])
    assert context.trace_id == TRACE.decode() and context.tracestate == ""


def test_state_combines_in_order_and_is_not_exported():
    context = incoming_trace([(b"traceparent", VALID[:-2] + b"fe"),
        (b"tracestate", b" \t, tenant@system=opaque-private ,"), (b"Tracestate", b"a=  value")])
    assert context.tracestate == "tenant@system=opaque-private,a=  value"
    assert not context.sampled
    trace = RequestTrace(incoming=context)
    with trace.activate(), trace.span(TraceStage.HTTP):
        with trace.span(TraceStage.PROMPT):
            pass
    record = trace.finish()
    child, root = record.spans
    assert root.parent_span_id == PARENT.decode() and child.parent_span_id == root.span_id
    assert "opaque-private" not in repr(asdict(record)) and "opaque-private" not in repr(context)


@pytest.mark.parametrize("value", [b"", b"a b", b" a", b"a\n", b"\xff", b"x" * 129])
def test_invalid_business_id(value):
    with pytest.raises(ValueError):
        business_correlation([(b"x-gateway-task-id", value)])


def test_business_ids_are_optional_and_duplicates_rejected():
    assert business_correlation([]).task_id is None
    result = business_correlation([(b"X-Gateway-Task-Id", b"Task._:-09"), (b"x-gateway-step-id", b"s")])
    assert result.task_id == "Task._:-09" and result.turn_id is None and result.step_id == "s"
    with pytest.raises(ValueError):
        business_correlation([(b"x-gateway-task-id", b"a"), (b"X-Gateway-Task-Id", b"a")])


@pytest.mark.parametrize("mode,status", [("valid", 204), ("bad_trace", 204), ("bad_business", 400), ("oversize", 413)])
def test_middleware_rejects_before_dispatch_but_always_has_request_identity(mode, status):
    async def scenario():
        records, calls, messages = [], [], []
        class Sink:
            def offer(self, record):
                records.append(record)
                return True
        async def app(scope, receive, send):
            calls.append(current_trace().trace_id)
            await Response(status_code=204)(scope, receive, send)
        async def send(message):
            messages.append(message)
        async def receive():
            return {"type": "http.request", "body": b""}
        headers = [(b"traceparent", b"invalid" if mode == "bad_trace" else VALID),
            (b"x-gateway-task-id", b"invalid value" if mode == "bad_business" else b"task-1")]
        if mode == "oversize":
            headers.append((b"x-large", b"x" * 32768))
        await _ResponseIdentity(app, Sink())({"type": "http", "headers": headers}, receive, send)
        record, = records
        assert messages[0]["status"] == status
        assert len(calls) == (status == 204)
        assert dict(messages[0]["headers"])[b"x-request-id"] == str(record.request_id).encode()
        assert b"x-gateway-call-id" not in dict(messages[0]["headers"])
        assert record.call_id is None
        assert (record.trace_id == TRACE.decode()) == (mode not in {"bad_trace", "oversize"})
        assert "invalid value" not in repr(record)
    asyncio.run(scenario())


def test_nested_requests_with_same_distributed_trace_do_not_share_local_parent():
    context = incoming_trace([(b"traceparent", VALID)])
    outer, inner = RequestTrace(incoming=context), RequestTrace(incoming=context)
    with outer.activate(), outer.span(TraceStage.HTTP):
        with inner.activate(), inner.span(TraceStage.HTTP):
            pass
    assert inner.finish().spans[0].parent_span_id == PARENT.decode()


@pytest.mark.parametrize("caller,scope,ids,valid", [
    ("agent", "turn", (), False), ("agent", "turn", ("task",), False),
    ("agent", "turn", ("turn",), False), ("agent", "turn", ("task", "turn"), True),
    ("agent", "step", ("task", "turn"), False),
    ("agent", "step", ("task", "turn", "step"), True),
    ("non-agent", "turn", (), True), ("non-agent", "turn", ("step",), True),
    ("non-agent", "step", ("task", "turn", "step"), False),
    ("Agent", "turn", (), False), ("agent", "unknown", (), False)])
def test_caller_aware_correlation_before_app_dispatch(caller, scope, ids, valid):
    async def scenario():
        headers = [(b"X-Gateway-Caller-Type", caller.encode()), (b"X-Gateway-Operation-Scope", scope.encode())]
        headers += [(f"x-gateway-{field}-id".encode(), field.encode()) for field in ids]
        calls, sent = [], []
        async def app(scope, receive, send):
            calls.append(current_trace().correlation)
            await Response(status_code=204)(scope, receive, send)
        async def receive():
            raise AssertionError("Invalid correlation must fail before reading request body")
        async def send(message):
            sent.append(message)
        await _ResponseIdentity(app)({"type": "http", "headers": headers}, receive, send)
        assert sent[0]["status"] == (204 if valid else 400)
        assert len(calls) == int(valid)
        assert b"x-request-id" in dict(sent[0]["headers"])
        assert b"x-gateway-call-id" not in dict(sent[0]["headers"])
    asyncio.run(scenario())


@pytest.mark.parametrize("name,value", [(b"caller-type", b""), (b"caller-type", b"agent "),
    (b"caller-type", b"\xff"), (b"operation-scope", b"Step")])
def test_invalid_correlation_declarations(name, value):
    with pytest.raises(ValueError):
        business_correlation([(b"x-gateway-" + name, value)])


@pytest.mark.parametrize("name,value", [(b"caller-type", b"non-agent"), (b"operation-scope", b"turn")])
def test_duplicate_correlation_declarations(name, value):
    header = b"x-gateway-" + name
    with pytest.raises(ValueError):
        business_correlation([(header, value), (header.upper(), value)])
