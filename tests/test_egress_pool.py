import asyncio

import httpcore
import pytest

from llm_gateway.adapters.egress_pool import EgressConnectionPool, ProviderTransportRetired


HEADERS = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n"


class Network(httpcore.AsyncNetworkBackend):
    def __init__(self):
        self.streams = []

    async def connect_tcp(self, *args, **kwargs):
        class Stream(httpcore.AsyncMockStream):
            closed = False
            async def aclose(self):
                self.closed = True
                await super().aclose()
        stream = Stream([HEADERS, b"ok", HEADERS, b"ok"])
        self.streams.append(stream)
        return stream


def test_idle_connection_reused_before_deadline_replaced_at_deadline():
    async def run():
        now, network = [0], Network()
        async with EgressConnectionPool(clock=lambda: now[0], max_connection_lifetime_seconds=30,
                network_backend=network, max_connections=1, keepalive_expiry=None) as pool:
            assert (await pool.request("GET", "http://provider.invalid/")).content == b"ok"
            now[0] = 29
            assert (await pool.request("GET", "http://provider.invalid/")).content == b"ok"
            assert len(network.streams) == 1
            now[0] = 30
            assert (await pool.request("GET", "http://provider.invalid/")).content == b"ok"
            assert len(network.streams) == 2 and network.streams[0].closed
        assert network.streams[1].closed
    asyncio.run(run())


def test_active_response_survives_age_limit_but_cannot_be_reacquired():
    async def run():
        now, network = [0], Network()
        async with EgressConnectionPool(clock=lambda: now[0], max_connection_lifetime_seconds=30,
                network_backend=network, max_connections=1, keepalive_expiry=None) as pool:
            async with pool.stream("GET", "http://provider.invalid/") as response:
                now[0] = 40
                with pytest.raises(httpcore.PoolTimeout):
                    await pool.request("GET", "http://provider.invalid/",
                        extensions={"timeout": {"pool": 0}})
                assert not network.streams[0].closed
                assert await response.aread() == b"ok"
            assert network.streams[0].closed
            await pool.request("GET", "http://provider.invalid/")
            assert len(network.streams) == 2
    asyncio.run(run())


@pytest.mark.parametrize("lifetime", [True, 0, 29, 3601, 30.0])
def test_invalid_lifetime_fails_locally(lifetime):
    with pytest.raises(ValueError, match="lifetime"):
        EgressConnectionPool(max_connection_lifetime_seconds=lifetime)


def test_retirement_closes_idle_immediately_and_active_only_after_response_release():
    async def run():
        network = Network()
        async with EgressConnectionPool(network_backend=network, max_connections=2) as pool:
            async with pool.stream("GET", "http://provider.invalid/") as response:
                await pool.request("GET", "http://provider.invalid/")
                pool.stop_acquiring()
                await pool.close_idle()
                assert len(network.streams) == 2
                assert not network.streams[0].closed and network.streams[1].closed
                assert not pool.drained
                with pytest.raises(ProviderTransportRetired):
                    await pool.request("GET", "http://provider.invalid/")
                assert await response.aread() == b"ok"
            assert network.streams[0].closed and pool.drained
    asyncio.run(run())


def test_retirement_during_existing_pool_wait_does_not_break_active_response_cleanup():
    async def run():
        network, started = Network(), asyncio.Event()
        async with EgressConnectionPool(network_backend=network, max_connections=1) as pool:
            async with pool.stream("GET", "http://provider.invalid/") as response:
                async def waiting():
                    started.set()
                    return await pool.request("GET", "http://provider.invalid/",
                        extensions={"timeout": {"pool": 1}})
                task = asyncio.create_task(waiting())
                await started.wait()
                pool.stop_acquiring()
                await pool.close_idle()
                assert await response.aread() == b"ok"
            with pytest.raises(ProviderTransportRetired):
                await task
            assert len(network.streams) == 1 and network.streams[0].closed and pool.drained
    asyncio.run(run())
