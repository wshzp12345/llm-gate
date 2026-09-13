import asyncio
from contextlib import aclosing

import httpx
import pytest

from llm_gateway.domain.streaming import StreamCompleted, StreamFailed
from tests.test_completion import REQUEST
from tests.test_credentialed_runtime import Harness
from tests.test_provider_streaming import Stream, chunk, wire


@pytest.mark.parametrize("mode", ["success", "close", "cancel", "401", "403"])
def test_stream_lease_closes_and_attempt_cannot_be_replayed(mode):
    async def scenario():
        harness = Harness()
        body = Stream([wire(chunk({"content": "first"})), wire(chunk(finish="stop"), "[DONE]")],
                      error=asyncio.CancelledError() if mode == "cancel" else None)
        calls = []

        def handler(request):
            calls.append(request)
            assert request.headers["authorization"] == "Bearer " + harness.leases[-1].bearer_value()
            return httpx.Response(int(mode) if mode.isdigit() else 200, stream=body,
                                  headers={"content-type": "text/event-stream"})

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False) as client:
            runtime = harness.runtime(client)
            async with runtime.acquire("a") as provider:
                async with aclosing(provider.stream(REQUEST)) as events:
                    if mode == "close":
                        assert (await anext(events)).text == "first"
                        assert body.read_count == 1
                    elif mode == "cancel":
                        with pytest.raises(asyncio.CancelledError):
                            _ = [event async for event in events]
                    else:
                        observed = [event async for event in events]
                        assert isinstance(observed[-1], StreamFailed if mode.isdigit() else StreamCompleted)
                assert body.closed and provider._adapter is None
                with pytest.raises(RuntimeError, match="cannot be reused"):
                    await provider.complete(REQUEST)
                async with aclosing(provider.stream(REQUEST)) as replay:
                    with pytest.raises(RuntimeError, match="cannot be reused"):
                        await anext(replay)
            assert not harness.leases[-1]._material
            if mode.isdigit():
                async with runtime.acquire("a") as rejected:
                    assert rejected is None
            assert not client.is_closed and len(calls) == 1
        assert all(not lease._material for lease in harness.leases)
    asyncio.run(scenario())
