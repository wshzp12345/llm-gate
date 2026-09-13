import asyncio
import ssl

import httpcore
import httpx
import pytest

from llm_gateway.adapters.egress_httpx import _EgressTransport, create_egress_client
from llm_gateway.adapters.openai_compatible import _network_retryable
from llm_gateway.domain.provider_egress import ProviderEgressPolicy


class Pool:
    def __init__(self):
        self.requests = []
        self.closed = False

    async def handle_async_request(self, request):
        self.requests.append(request)
        return httpcore.Response(302, headers=[(b"location", b"https://other.invalid/")], content=b"")

    async def aclose(self):
        self.closed = True


@pytest.mark.parametrize("url,headers,extensions", [
    ("https://other.invalid/", {}, {}),
    ("http://provider.invalid/", {}, {}),
    ("https://provider.invalid:444/", {}, {}),
    ("https://user:pass@provider.invalid/", {}, {}),
    ("https://provider.invalid/", {"host": "other.invalid"}, {}),
    ("https://provider.invalid/", {}, {"sni_hostname": "other.invalid"}),
])
def test_request_overrides_rejected_before_pool(url, headers, extensions):
    async def run():
        pool = Pool()
        async with httpx.AsyncClient(transport=_EgressTransport(pool, ("https", "provider.invalid", 443)),
                                    trust_env=False) as client:
            with pytest.raises(httpx.ConnectError) as error:
                await client.get(url, headers=headers, extensions=extensions)
            assert not _network_retryable(error.value)
            assert not pool.requests
        assert pool.closed
    asyncio.run(run())


def test_redirect_returned_without_following_and_closed_transport_rejects():
    async def run():
        pool = Pool()
        transport = _EgressTransport(pool, ("https", "provider.invalid", 443))
        async with httpx.AsyncClient(transport=transport, trust_env=False, follow_redirects=False) as client:
            response = await client.get("https://provider.invalid/", timeout=None, extensions={"trace": object()})
            assert response.status_code == 302 and len(pool.requests) == 1
            assert "trace" not in pool.requests[0].extensions
            assert pool.requests[0].extensions["timeout"]["pool"] == 0
        with pytest.raises(RuntimeError, match="closed"):
            await transport.handle_async_request(httpx.Request("GET", "https://provider.invalid/"))
    asyncio.run(run())


@pytest.mark.parametrize("kind", [httpcore.ConnectTimeout, httpcore.ReadTimeout,
    httpcore.WriteError, httpcore.RemoteProtocolError, httpcore.PoolTimeout])
def test_exception_type_and_cause_preserved(kind):
    async def run():
        class FailedPool(Pool):
            async def handle_async_request(self, request):
                raise kind("synthetic")
        async with httpx.AsyncClient(transport=_EgressTransport(FailedPool(), ("https", "provider.invalid", 443)),
                                    trust_env=False) as client:
            with pytest.raises(getattr(httpx, kind.__name__)) as error:
                await client.get("https://provider.invalid/")
            assert isinstance(error.value.__cause__, kind)
    asyncio.run(run())


def test_insecure_tls_rejected_without_resolution():
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.check_hostname = False
    with pytest.raises(ValueError, match="Verified TLS"):
        create_egress_client(policy=ProviderEgressPolicy(("provider.invalid",), ("public",)),
            host="provider.invalid", port=443, resolver=None, ssl_context=context, max_connections=1)
