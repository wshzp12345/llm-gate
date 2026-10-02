import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace

import pytest

from llm_gateway.application.attempt_execution import ExecutionCandidate
from llm_gateway.application.streaming_attempt_execution import StreamingAttemptExecutor
from llm_gateway.adapters.structured_output import validate_structured_stream
from llm_gateway.application.text_failure_recovery import classify_text_failure
from llm_gateway.domain.model import FailureCode, OutputFormat, ProviderFailure, ProviderResult, TextOutput, Usage
from llm_gateway.domain.recovery import RetryPolicy
from llm_gateway.domain.streaming import StreamDelta, StreamCompleted, StreamFailed, DeltaKind
from tests.test_completion import REQUEST


def delta(text="visible", sequence=1, model="actual"):
    return StreamDelta(sequence, model, DeltaKind.TEXT, text)


def success(sequence=2, text="visible"):
    return StreamCompleted(sequence, ProviderResult("general", "actual", TextOutput(text), Usage(2, 1), "stop"))


def failure(sequence=1, code=FailureCode.PROVIDER_UNAVAILABLE, usage=Usage()):
    return StreamFailed(sequence, ProviderFailure(code, True), usage)


class Harness:
    def __init__(self, scripts, *, fail_delivery=False, structured=False):
        self.scripts, self.calls, self.events, self.sent = scripts, [], [], []
        self.active, self.closed, self.fail_delivery = 0, 0, fail_delivery
        self.structured = structured

    @asynccontextmanager
    async def acquire(self, binding):
        self.active += 1
        owner = self
        class Provider:
            async def stream(self, request, *, context=None):
                owner.calls.append((binding, request))
                try:
                    script = owner.scripts.pop(0)
                    async def source():
                        for event in script:
                            yield event
                    events = validate_structured_stream(source(), OutputFormat("json_object")) if owner.structured else source()
                    async for event in events:
                        yield event
                finally:
                    owner.closed += 1
        try:
            yield Provider()
        finally:
            self.active -= 1

    async def started(self, number, binding, candidate_attempt):
        self.events.append(("started", number, binding, candidate_attempt))

    async def finished(self, number, terminal, recovery):
        self.events.append(("finished", number, terminal, recovery.action))

    async def delta(self, event):
        self.sent.append(event)
        if self.fail_delivery:
            raise OSError("write outcome unknown")

    async def first_delta(self, number, event):
        assert not self.sent

    async def sleep(self, seconds):
        assert self.active == 0


def execute(harness, **options):
    options.setdefault("commit", harness)
    async def scenario():
        executor = StreamingAttemptExecutor(runtime=harness, journal=harness, output=harness,
            classify=classify_text_failure, draw_jitter=lambda _: 0, sleep=harness.sleep, **options)
        result = await executor.execute((ExecutionCandidate("a", REQUEST), ExecutionCandidate("b", replace(REQUEST, resolved_model="second"))),
                                        policy=RetryPolicy(), deadline=asyncio.get_running_loop().time() + 2)
        assert harness.active == 0
        return result, executor
    return asyncio.run(scenario())


def test_precommit_retry_and_fallback_keep_request_intent():
    harness = Harness([[failure()], [failure()], [delta(), success()]])
    result, executor = execute(harness)
    assert isinstance(result, StreamCompleted) and executor.committed
    assert [binding for binding, _ in harness.calls] == ["a", "a", "b"]
    assert all(request.messages == REQUEST.messages for _, request in harness.calls)
    assert [item[3] for item in harness.events if item[0] == "finished"] == ["retry", "advance", "stop"]
    assert len(harness.sent) == 1 and harness.closed == 3


@pytest.mark.parametrize("kind", [DeltaKind.TEXT, DeltaKind.REFUSAL])
def test_postcommit_failure_is_terminal_even_when_provider_error_is_retryable(kind):
    observed = Usage(2, 1)
    harness = Harness([[replace(delta(), kind=kind), failure(2, usage=observed)]])
    result, executor = execute(harness)
    assert isinstance(result, StreamFailed) and result.usage == observed
    assert executor.committed and len(harness.calls) == 1
    assert harness.events[-1][3] == "stop"
    assert harness.events[-1][2].usage == observed
    assert result.resolved_model == "actual"


def test_structured_json_failure_after_commit_never_switches_candidate():
    malformed = '{"answer":'
    harness = Harness([[delta(malformed), success(text=malformed)]], structured=True)
    result, executor = execute(harness)
    assert isinstance(result, StreamFailed)
    assert result.failure.code == FailureCode.STRUCTURED_OUTPUT_INVALID
    assert result.failure.structured_reason == "json_malformed"
    assert executor.committed and [item.text for item in harness.sent] == [malformed]
    assert len(harness.calls) == 1 and harness.events[-1][3] == "stop"


def test_structured_invalid_root_before_commit_does_not_spend_fallback():
    harness = Harness([[delta("not json"), success(text="not json")]], structured=True)
    result, executor = execute(harness)
    assert isinstance(result, StreamFailed)
    assert result.failure.structured_reason == "json_wrong_root"
    assert not executor.committed and not harness.sent
    assert len(harness.calls) == 1 and harness.events[-1][3] == "stop"


def test_structured_stream_stops_on_second_value_without_forwarding_suffix():
    harness = Harness([[delta('{"answer":1}'), delta(' {"again":2}', sequence=2),
                        success(sequence=3, text='{"answer":1} {"again":2}')]], structured=True)
    result, executor = execute(harness)
    assert isinstance(result, StreamFailed)
    assert result.failure.structured_reason == "json_extraneous_content"
    assert executor.committed and [item.text for item in harness.sent] == ['{"answer":1}']
    assert len(harness.calls) == 1 and harness.closed == 1


@pytest.mark.parametrize("script", [[delta(), success(), delta(sequence=3)], [delta()],
    [delta(), delta(sequence=3)], [delta(), delta(sequence=2, model="switched")],
    [delta(), success(text="invented completion")]])
def test_malformed_or_missing_terminal_cannot_turn_into_success_or_fallback(script):
    harness = Harness([script])
    result, executor = execute(harness)
    assert isinstance(result, StreamFailed) and result.failure.code == FailureCode.PROVIDER_PROTOCOL_ERROR
    assert len(harness.calls) == 1 and executor.committed and harness.closed == 1


def test_output_failure_does_not_authorize_another_provider():
    harness = Harness([[delta(), success()]], fail_delivery=True)
    with pytest.raises(OSError):
        execute(harness)
    assert len(harness.calls) == 1 and harness.active == 0 and harness.closed == 1
    assert not any(item[0] == "finished" for item in harness.events)


def test_uncertain_never_recovers_before_commit():
    harness = Harness([[failure(code=FailureCode.UNCERTAIN)]])
    result, executor = execute(harness)
    assert isinstance(result, StreamFailed) and not executor.committed
    assert len(harness.calls) == 1 and harness.events[-1][3] == "stop"


def test_output_byte_bound_rejects_before_delivery():
    harness = Harness([[delta("你好")], [delta("你好")]])
    result, executor = execute(harness, max_output_bytes=5)
    assert isinstance(result, StreamFailed) and not executor.committed and not harness.sent
    assert len(harness.calls) == 2  # Protocol failure may advance before commitment.


def test_real_adapter_events_drive_precommit_recovery_and_postcommit_stop():
    import httpx
    from llm_gateway.adapters.openai_compatible import OpenAICompatibleCompletion
    from tests.test_provider_streaming import Stream, wire, chunk

    async def scenario(after_delta):
        requests, streams = [], []
        async def handler(request):
            requests.append(request)
            if request.url.host == "first.invalid" and not after_delta:
                return httpx.Response(503)
            data = Stream([wire(chunk({"content": "visible"}))] if after_delta else
                          [wire(chunk({"content": "visible"}), chunk(finish="stop"), "[DONE]")],
                          error=httpx.ReadTimeout("private-network-error") if after_delta else None)
            streams.append(data)
            return httpx.Response(200, stream=data, headers={"Content-Type": "text/event-stream"})
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            class Runtime:
                @asynccontextmanager
                async def acquire(self, binding):
                    yield OpenAICompatibleCompletion(client, base_url=f"https://{'first' if binding == 'a' else 'second'}.invalid/v1", credential="synthetic")
            journal = Harness([])
            executor = StreamingAttemptExecutor(runtime=Runtime(), journal=journal, output=journal,
                classify=classify_text_failure, draw_jitter=lambda _: 0, sleep=journal.sleep, commit=journal)
            result = await executor.execute((ExecutionCandidate("a", REQUEST), ExecutionCandidate("b", REQUEST)),
                policy=RetryPolicy(), deadline=asyncio.get_running_loop().time() + 2)
            assert isinstance(result, StreamFailed if after_delta else StreamCompleted)
            assert len(requests) == (1 if after_delta else 3)
            assert len(journal.sent) == 1 and executor.committed
            assert journal.events[-1][3] == "stop" and all(stream.closed for stream in streams)
    asyncio.run(scenario(False))
    asyncio.run(scenario(True))
