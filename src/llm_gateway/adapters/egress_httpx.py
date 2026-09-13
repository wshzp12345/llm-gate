"""HTTPX bridge to the direct DNS-pinned HTTP/1.1 pool.

Composition owns validated TLS roots, pool identity, expiry and retirement.
This factory performs no DNS or connection I/O.
"""

from contextlib import contextmanager
import ssl

import httpcore
import httpx

from llm_gateway.adapters.egress_network import EgressNetworkBackend
from llm_gateway.adapters.egress_pool import EgressConnectionPool
from llm_gateway.domain.provider_egress import EgressPolicyViolation


@contextmanager
def _mapped_errors():
    try:
        yield
    except (httpcore.TimeoutException, httpcore.NetworkError,
            httpcore.ProtocolError, httpcore.ProxyError, httpcore.UnsupportedProtocol) as error:
        # Resolve from the concrete MRO, preserving typed causes for retry policy.
        for kind in type(error).__mro__:
            mapped = getattr(httpx, kind.__name__, None)
            if isinstance(mapped, type) and issubclass(mapped, httpx.TransportError):
                raise mapped("Provider transport failed") from error
        raise


class _ResponseStream(httpx.AsyncByteStream):
    def __init__(self, stream):
        self._stream = stream

    async def __aiter__(self):
        with _mapped_errors():
            async for chunk in self._stream:
                yield chunk

    async def aclose(self):
        with _mapped_errors():
            if hasattr(self._stream, "aclose"):
                await self._stream.aclose()


class _EgressTransport(httpx.AsyncBaseTransport):
    def __init__(self, pool, origin):
        self._pool, self._origin = pool, origin
        self._closed = False

    async def handle_async_request(self, request):
        if self._closed:
            raise RuntimeError("Provider transport is closed")
        url = request.url
        origin = (url.scheme, url.host, url.port or (443 if url.scheme == "https" else 80))
        authority = url.netloc.decode("ascii")
        if (origin != self._origin or url.userinfo
                or request.headers.get_list("host") != [authority]
                or "sni_hostname" in request.extensions):
            raise httpx.ConnectError("Provider egress policy rejected request", request=request) from EgressPolicyViolation()
        # Do not forward caller trace callbacks or TLS overrides to httpcore.
        timeouts = dict(request.extensions.get("timeout", {}))
        timeouts["pool"] = 0
        extensions = {"timeout": timeouts}
        core_request = httpcore.Request(method=request.method,
            url=httpcore.URL(scheme=url.raw_scheme, host=url.raw_host, port=url.port, target=url.raw_path),
            headers=request.headers.raw, content=request.stream, extensions=extensions)
        with _mapped_errors():
            response = await self._pool.handle_async_request(core_request)
        return httpx.Response(response.status, headers=response.headers,
            stream=_ResponseStream(response.stream), extensions=response.extensions)

    async def aclose(self):
        self._closed = True
        with _mapped_errors():
            await self._pool.aclose()

    def stop_acquiring(self):
        self._pool.stop_acquiring()

    async def close_idle(self):
        with _mapped_errors():
            await self._pool.close_idle()

    @property
    def drained(self):
        return self._pool.drained


class ProviderHttpClient(httpx.AsyncClient):
    """Origin-bound HTTPX client with explicit, non-cancelling retirement."""

    def __init__(self, *, transport, **kwargs):
        super().__init__(transport=transport, **kwargs)
        self._provider_transport = transport

    def stop_acquiring(self):
        self._provider_transport.stop_acquiring()

    async def close_idle(self):
        await self._provider_transport.close_idle()

    @property
    def drained(self):
        return self._provider_transport.drained


def create_egress_client(*, policy, host, port, resolver, ssl_context=None, trust_bundle=None,
                         max_connections, idle_timeout_seconds=30,
                         connect_timeout_seconds=5, max_connection_lifetime_seconds=300,
                         allow_plaintext=False):
    """Build a lazy origin-bound client; caller must close it after draining work."""
    if trust_bundle is not None:
        if ssl_context is not None or allow_plaintext:
            raise ValueError("Embedded trust requires exclusive HTTPS context")
        ssl_context = trust_bundle.context
    if type(max_connections) is not int or max_connections < 1:
        raise ValueError("Provider concurrency ceiling required")
    if type(idle_timeout_seconds) is not int or idle_timeout_seconds < 1:
        raise ValueError("Positive published idle timeout required")
    if (not isinstance(ssl_context, ssl.SSLContext) or not ssl_context.check_hostname
            or ssl_context.verify_mode != ssl.CERT_REQUIRED
            or ssl_context.minimum_version < ssl.TLSVersion.TLSv1_2):
        raise ValueError("Verified TLS 1.2 or newer context required")
    network = EgressNetworkBackend(policy=policy, host=host, port=port, resolver=resolver,
        allow_plaintext=allow_plaintext, connect_timeout_seconds=connect_timeout_seconds,
        connection_guard=trust_bundle.require_current if trust_bundle is not None else None)
    pool = EgressConnectionPool(ssl_context=ssl_context, network_backend=network,
        max_connection_lifetime_seconds=max_connection_lifetime_seconds,
        http1=True, http2=False, retries=0, max_connections=max_connections,
        max_keepalive_connections=max_connections, keepalive_expiry=idle_timeout_seconds)
    scheme = "http" if allow_plaintext else "https"
    return ProviderHttpClient(transport=_EgressTransport(pool, (scheme, host, port)),
        trust_env=False, follow_redirects=False,
        timeout=httpx.Timeout(None, connect=connect_timeout_seconds, pool=0))
