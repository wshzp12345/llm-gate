import asyncio
import json
from uuid import uuid4

import httpx
import pytest

from llm_gateway.adapters.openai_compatible import OpenAICompatibleCompletion
from llm_gateway.application.cancellation import LocalCancellationReason as Reason, ProviderCancelResult as Result
from llm_gateway.application.cancellation_execution import CancellationAttemptHandle
from llm_gateway.application.provider_context import GatewayCancellationToken, ProviderInvocationContext
from tests.test_completion import REQUEST, envelope


def context(*, remaining=10):
    return ProviderInvocationContext(CancellationAttemptHandle(uuid4(), 1, uuid4()),
        GatewayCancellationToken(), asyncio.get_running_loop().time() + remaining)


class Body(httpx.AsyncByteStream):
    def __init__(self):
        self.entered, self.release, self.closed = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def __aiter__(self):
        self.entered.set()
        await self.release.wait()
        yield json.dumps(envelope()).encode()

    async def aclose(self):
        self.closed.set()


def adapter(client):
    return OpenAICompatibleCompletion(client, base_url="https://provider.invalid/v1", credential="test-only")


def test_token_first_reason_wins_and_scope_releases_abort():
    token, calls = GatewayCancellationToken(), []
    with token.abort_on_cancel(lambda: calls.append("expired")):
        pass
    with token.abort_on_cancel(lambda: calls.append("active")):
        token.request(Reason.CLIENT_DISCONNECTED)
        token.request(Reason.CONTEXT_CANCELLED)
    assert calls == ["active"]
    assert token.reason == Reason.CLIENT_DISCONNECTED
    with pytest.raises(asyncio.CancelledError):
        with token.abort_on_cancel(lambda: calls.append("late")):
            pass
    assert calls == ["active"]


def test_one_failing_abort_does_not_skip_other_local_abort():
    token, calls = GatewayCancellationToken(), []
    def fail():
        raise RuntimeError("local bug")
    with token.abort_on_cancel(fail), token.abort_on_cancel(lambda: calls.append("aborted")):
        with pytest.raises(ExceptionGroup):
            token.request(Reason.CONTEXT_CANCELLED)
    assert calls == ["aborted"]


@pytest.mark.parametrize("case", ["cancelled", "expired", "invalid"])
def test_bad_or_stopped_context_does_no_provider_io(case):
    async def scenario():
        calls = []
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: calls.append(request)), trust_env=False) as client:
            ctx = context(remaining=-1 if case == "expired" else 10)
            if case == "cancelled":
                ctx.cancellation.request(Reason.CONTEXT_CANCELLED)
            with pytest.raises(asyncio.CancelledError if case == "cancelled" else TimeoutError if case == "expired" else ValueError):
                await adapter(client).complete(REQUEST, context="bad" if case == "invalid" else ctx)
        assert calls == []
    asyncio.run(scenario())


@pytest.mark.parametrize("via", ["token", "cancel", "expired_cancel", "deadline"])
def test_cancellation_closes_body_and_never_reports_remote_ack(via):
    async def scenario():
        body = Body()
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(200,
                headers={"Content-Type": "application/json"}, stream=body)), trust_env=False) as client:
            provider, ctx = adapter(client), context(remaining=.05 if via == "deadline" else 10)
            task = asyncio.create_task(provider.complete(REQUEST, context=ctx))
            try:
                await asyncio.wait_for(body.entered.wait(), 1)
                if via == "token":
                    ctx.cancellation.request(Reason.CLIENT_DISCONNECTED)
                elif via in {"cancel", "expired_cancel"}:
                    result = await provider.cancel(ctx.handle, Reason.CONTEXT_CANCELLED,
                        asyncio.get_running_loop().time() + (-1 if via == "expired_cancel" else 1))
                    assert result == (Result.UNKNOWN if via == "expired_cancel" else Result.NOT_SUPPORTED)
                with pytest.raises(TimeoutError if via == "deadline" else asyncio.CancelledError):
                    await task
                assert body.closed.is_set()
                assert provider._active == {}
                assert not provider.supports_remote_cancellation
                assert await provider.cancel(ctx.handle, Reason.CONTEXT_CANCELLED,
                    asyncio.get_running_loop().time() + 1) == Result.UNKNOWN
            finally:
                if not task.done():
                    task.cancel()
                await asyncio.gather(task, return_exceptions=True)
    asyncio.run(scenario())


def test_success_releases_callback_before_owner_continues():
    async def scenario():
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(200, json=envelope())), trust_env=False) as client:
            provider, ctx = adapter(client), context()
            assert (await provider.complete(REQUEST, context=ctx)).output.text == "你好"
            ctx.cancellation.request(Reason.CONTEXT_CANCELLED)
            await asyncio.sleep(0)  # The completed operation must not cancel its owner now.
            assert provider._active == {}
    asyncio.run(scenario())


def test_repeated_cancel_does_not_interrupt_first_transport_cleanup():
    async def scenario():
        close_started, allow_close = asyncio.Event(), asyncio.Event()
        class SlowClose(Body):
            async def aclose(self):
                close_started.set()
                await allow_close.wait()
                self.closed.set()
        body = SlowClose()
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(200,
                headers={"Content-Type": "application/json"}, stream=body)), trust_env=False) as client:
            provider, ctx = adapter(client), context()
            task = asyncio.create_task(provider.complete(REQUEST, context=ctx))
            try:
                await asyncio.wait_for(body.entered.wait(), 1)
                for _ in range(2):
                    assert await provider.cancel(ctx.handle, Reason.CONTEXT_CANCELLED,
                        asyncio.get_running_loop().time() + .01) == Result.UNKNOWN
                assert close_started.is_set() and not task.done()
                allow_close.set()
                with pytest.raises(asyncio.CancelledError):
                    await task
                assert body.closed.is_set()
            finally:
                allow_close.set()
                if not task.done():
                    task.cancel()
                await asyncio.gather(task, return_exceptions=True)
    asyncio.run(scenario())


def test_cancelling_one_attempt_does_not_close_shared_client_or_other_attempt():
    async def scenario():
        bodies, calls = [Body(), Body()], []
        def handler(request):
            body = bodies[len(calls)]
            calls.append(request)
            return httpx.Response(200, headers={"Content-Type": "application/json"}, stream=body)
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False) as client:
            provider, contexts = adapter(client), [context(), context()]
            tasks = [asyncio.create_task(provider.complete(REQUEST, context=ctx)) for ctx in contexts]
            try:
                await asyncio.wait_for(asyncio.gather(*(body.entered.wait() for body in bodies)), 1)
                with pytest.raises(ValueError):
                    await provider.complete(REQUEST, context=contexts[0])
                assert len(calls) == 2
                assert await provider.cancel(contexts[0].handle, Reason.CONTEXT_CANCELLED,
                    asyncio.get_running_loop().time() + 1) == Result.NOT_SUPPORTED
                assert tasks[0].cancelled() and bodies[0].closed.is_set()
                assert not tasks[1].done() and not client.is_closed
                bodies[1].release.set()
                assert (await tasks[1]).output.text == "你好"
            finally:
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
    asyncio.run(scenario())


def test_credentialed_runtime_forwards_context_and_releases_lease_on_cancel():
    from tests.test_credentialed_runtime import Harness

    async def scenario():
        harness, body = Harness(), Body()
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(200,
                headers={"Content-Type": "application/json"}, stream=body)), trust_env=False) as client:
            runtime, ctx, ports = harness.runtime(client), context(), []
            async def invoke():
                async with runtime.acquire("a") as provider:
                    ports.append(provider)
                    return await provider.complete(REQUEST, context=ctx)
            task = asyncio.create_task(invoke())
            try:
                await asyncio.wait_for(body.entered.wait(), 1)
                assert await ports[0].cancel(ctx.handle, Reason.CONTEXT_CANCELLED,
                    asyncio.get_running_loop().time() + 1) == Result.NOT_SUPPORTED
                assert task.cancelled() and body.closed.is_set()
                assert ports[0]._adapter is None
                assert all(not lease._material for lease in harness.leases)
                assert await ports[0].cancel(ctx.handle, Reason.CONTEXT_CANCELLED,
                    asyncio.get_running_loop().time() + 1) == Result.UNKNOWN
            finally:
                if not task.done():
                    task.cancel()
                await asyncio.gather(task, return_exceptions=True)
    asyncio.run(scenario())
