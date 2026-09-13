import asyncio
from contextlib import asynccontextmanager

import pytest

from llm_gateway.application.attempt_execution import (
    ExecutionCandidate, FailureRecovery, NoEligibleCandidate, SynchronousAttemptExecutor,
)
from llm_gateway.domain.model import (
    CompletionRequest, FailureCode, Message, ProviderFailure, ProviderResult, TextOutput, Usage,
)
from llm_gateway.domain.recovery import RetryPolicy


REQUEST = CompletionRequest("general", "model", (Message("user", "hello"),), 32)
SUCCESS = ProviderResult("general", "model", TextOutput("hello"), Usage(), "stop")
FAILURE = ProviderFailure(FailureCode.PROVIDER_UNAVAILABLE, True)
CANDIDATES = tuple(ExecutionCandidate(name, REQUEST) for name in ("a", "b", "c"))


class Harness:
    def __init__(self, results, *, fail_at=None, skipped=(), failover=True):
        self.results = iter(results)
        self.events = []
        self.fail_at = fail_at
        self.skipped = set(skipped)
        self.failover = failover
        self.leased = False

    @asynccontextmanager
    async def acquire(self, binding_id):
        self.events.append(("gate", binding_id))
        assert not self.leased
        self.leased = True
        try:
            yield None if binding_id in self.skipped else self
        finally:
            self.leased = False
            self.events.append(("release", binding_id))

    async def started(self, number, binding_id, candidate_attempt):
        assert self.leased
        self.events.append(("start", number, binding_id, candidate_attempt))
        if self.fail_at == "start":
            raise RuntimeError("checkpoint unavailable")

    async def complete(self, request):
        assert self.leased
        assert request is REQUEST
        self.events.append(("provider",))
        result = next(self.results)
        if isinstance(result, BaseException):
            raise result
        return result

    async def finished(self, number, result, recovery):
        assert self.leased
        self.events.append(("finish", number, recovery.action))
        if self.fail_at == "finish":
            raise RuntimeError("checkpoint unavailable")

    async def sleep(self, seconds):
        assert not self.leased
        self.events.append(("sleep", seconds))
        if self.fail_at == "sleep":
            raise asyncio.CancelledError()

    def executor(self):
        return SynchronousAttemptExecutor(
            runtime=self, journal=self,
            classify=lambda failure: FailureRecovery(failure.retryable, self.failover),
            draw_jitter=lambda maximum: maximum, sleep=self.sleep,
        )

    async def run(self, candidates=CANDIDATES, policy=RetryPolicy()):
        return await self.executor().execute(candidates, policy=policy,
                                             deadline=asyncio.get_running_loop().time() + 30)


def test_start_before_io_and_durable_outcome_before_recovery():
    harness = Harness([FAILURE, FAILURE, SUCCESS])
    assert asyncio.run(harness.run()) == SUCCESS
    assert harness.events == [
        ("gate", "a"), ("start", 1, "a", 1), ("provider",), ("finish", 1, "retry"),
        ("release", "a"), ("sleep", 0.2),
        ("gate", "a"), ("start", 2, "a", 2), ("provider",), ("finish", 2, "advance"),
        ("release", "a"),
        ("gate", "b"), ("start", 3, "b", 1), ("provider",), ("finish", 3, "stop"),
        ("release", "b"),
    ]


@pytest.mark.parametrize("fail_at,calls", [("start", 0), ("finish", 1)])
def test_checkpoint_failure_stops_io_or_recovery_and_releases(fail_at, calls):
    harness = Harness([FAILURE, SUCCESS], fail_at=fail_at)
    with pytest.raises(RuntimeError, match="checkpoint unavailable"):
        asyncio.run(harness.run())
    assert harness.events.count(("provider",)) == calls
    assert not harness.leased
    assert not any(event[0] == "sleep" for event in harness.events)


def test_success_is_not_returned_when_outcome_checkpoint_fails():
    harness = Harness([SUCCESS], fail_at="finish")
    with pytest.raises(RuntimeError):
        asyncio.run(harness.run())
    assert not harness.leased


@pytest.mark.parametrize("fail_at,results", [(None, [asyncio.CancelledError()]), ("sleep", [FAILURE])])
def test_cancel_never_retries_or_falls_back(fail_at, results):
    harness = Harness(results, fail_at=fail_at)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(harness.run())
    assert harness.events.count(("provider",)) == 1
    assert not harness.leased


def test_total_attempt_cap_stops_before_third_candidate():
    harness = Harness([FAILURE] * 4)
    assert asyncio.run(harness.run()) == FAILURE
    assert harness.events.count(("provider",)) == 3
    assert ("gate", "c") not in harness.events


def test_skipped_candidates_do_not_consume_attempts():
    harness = Harness([SUCCESS], skipped=("a", "b"))
    assert asyncio.run(harness.run()) == SUCCESS
    assert ("start", 1, "c", 1) in harness.events


def test_all_skipped_has_no_invented_provider_failure():
    harness = Harness([], skipped=("a", "b", "c"))
    with pytest.raises(NoEligibleCandidate):
        asyncio.run(harness.run())
    assert not any(event[0] == "start" for event in harness.events)


def test_same_candidate_only_recovery_never_advances():
    harness = Harness([FAILURE, FAILURE, SUCCESS], failover=False)
    assert asyncio.run(harness.run()) == FAILURE
    assert harness.events.count(("provider",)) == 2
    assert ("gate", "b") not in harness.events


def test_gates_rechecked_after_backoff_without_reordering():
    harness = Harness([FAILURE, SUCCESS])
    original_sleep = harness.sleep
    async def sleep(seconds):
        await original_sleep(seconds)
        harness.skipped.add("a")
    harness.sleep = sleep
    assert asyncio.run(harness.run()) == SUCCESS
    assert ("start", 2, "b", 1) in harness.events
    assert ("start", 2, "a", 2) not in harness.events


@pytest.mark.parametrize("deadline", [float("nan"), float("inf"), True])
def test_invalid_deadline_rejected_without_gates(deadline):
    harness = Harness([])
    with pytest.raises(ValueError):
        asyncio.run(harness.executor().execute(CANDIDATES, policy=RetryPolicy(), deadline=deadline))
    assert harness.events == []


def test_expired_deadline_does_not_start_io():
    harness = Harness([])
    with pytest.raises(TimeoutError):
        asyncio.run(harness.executor().execute(CANDIDATES, policy=RetryPolicy(), deadline=0))
    assert harness.events == []


def test_duplicate_candidates_rejected_before_io():
    harness = Harness([])
    with pytest.raises(ValueError):
        asyncio.run(harness.run((CANDIDATES[0], CANDIDATES[0])))
    assert harness.events == []


@pytest.mark.parametrize("stage,provider_calls", [("started", 0), ("sleep", 1), ("finished", 1)])
def test_deadline_is_shared_across_checkpoints_and_backoff(monkeypatch, stage, provider_calls):
    harness = Harness([FAILURE, SUCCESS])
    async def run():
        loop = asyncio.get_running_loop()
        now = [loop.time()]
        original = getattr(harness, stage)
        async def consume_deadline(*args):
            await original(*args)
            now[0] += 31
        setattr(harness, stage, consume_deadline)
        with monkeypatch.context() as patch:
            patch.setattr(loop, "time", lambda: now[0])
            await harness.run()
    with pytest.raises(TimeoutError):
        asyncio.run(run())
    assert harness.events.count(("provider",)) == provider_calls
    assert not harness.leased


def test_adapter_mock_transport_executes_retry_then_fallback():
    import httpx
    from llm_gateway.adapters.openai_compatible import OpenAICompatibleCompletion

    harness = Harness([])
    requests = []
    def handler(request):
        requests.append(request)
        assert harness.leased
        assert harness.events[-1][0] == "start"
        if len(requests) < 3:
            return httpx.Response(503)
        return httpx.Response(200, json={
            "object": "chat.completion", "model": "model-b",
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "ok"},
                         "finish_reason": "stop"}],
        })
    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False) as client:
            original_acquire = harness.acquire
            @asynccontextmanager
            async def acquire(binding_id):
                async with original_acquire(binding_id):
                    yield OpenAICompatibleCompletion(
                        client, base_url=f"https://{binding_id}.invalid/v1", credential="test-only")
            harness.acquire = acquire
            return await harness.run()
    result = asyncio.run(run())
    assert [request.url.host for request in requests] == ["a.invalid", "a.invalid", "b.invalid"]
    assert result.requested_model == "general"
    assert result.resolved_model == "model-b"
    assert result.usage == Usage()
    assert harness.events[-2:] == [("finish", 3, "stop"), ("release", "b")]
