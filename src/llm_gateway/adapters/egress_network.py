"""DNS-pinned direct httpcore backend; pool lifecycle remains composition-owned.

Every new TCP connection resolves anew and validates the complete answer set.
Only the first validated numeric address is dialled: no hidden address retry.
TLS retains the configured hostname; DNS/TCP/TLS share one connect deadline.
"""

import asyncio
import math
import ssl

import httpcore

from llm_gateway.adapters.provider_dns import DnsResolutionFailure
from llm_gateway.adapters.provider_trust import TrustBundleUnavailable
from llm_gateway.domain.provider_egress import EgressPolicyViolation, ProviderEgressPolicy


def _reject():
    # The typed argument survives httpcore's pool-level `raise ... from None`.
    violation = EgressPolicyViolation()
    raise httpcore.ConnectError(violation) from violation


async def _close_failed(stream, deadline):
    try:
        remaining = min(2, max(0, deadline - asyncio.get_running_loop().time()))
        async with asyncio.timeout(remaining):
            await stream.aclose()
    except BaseException:
        # The original rejection/cancellation is authoritative, not cleanup.
        pass


class EgressNetworkBackend(httpcore.AsyncNetworkBackend):
    def __init__(self, *, policy: ProviderEgressPolicy, host: str, port: int, resolver,
                 allow_plaintext: bool = False, connect_timeout_seconds: int = 5, connector=None,
                 connection_guard=None):
        if not isinstance(policy, ProviderEgressPolicy) or host not in policy.allowed_hosts:
            raise ValueError("Configured egress hostname required")
        if type(port) is not int or not 1 <= port <= 65535 or type(allow_plaintext) is not bool:
            raise ValueError("Configured egress port and transport required")
        if type(connect_timeout_seconds) is not int or not 1 <= connect_timeout_seconds <= 30:
            raise ValueError("Published connection timeout required")
        self._policy, self._host, self._port = policy, host, port
        self._resolver, self._allow_plaintext = resolver, allow_plaintext
        self._connect_timeout = connect_timeout_seconds
        self._connection_guard = connection_guard
        self._connector = connector if connector is not None else httpcore.AnyIOBackend()

    async def connect_tcp(self, host, port, timeout=None, local_address=None, socket_options=None):
        if host != self._host or port != self._port or local_address is not None:
            _reject()
        if timeout is not None and (type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout <= 0):
            raise httpcore.ConnectTimeout("Provider connection deadline exhausted")
        loop = asyncio.get_running_loop()
        deadline = loop.time() + min(self._connect_timeout, timeout if timeout is not None else self._connect_timeout)
        stream = None
        try:
            async with asyncio.timeout_at(deadline):
                if self._connection_guard is not None:
                    self._connection_guard()
                addresses = await self._resolver.resolve(host, port)
                checked = self._policy.validate_addresses(host, addresses)
                if loop.time() >= deadline:
                    raise TimeoutError()
                remaining = max(0, deadline - loop.time())
                stream = await self._connector.connect_tcp(checked[0], port, timeout=remaining,
                    local_address=None, socket_options=socket_options)
                if loop.time() >= deadline:
                    raise TimeoutError()
                peer = stream.get_extra_info("server_addr")
                if (not isinstance(peer, tuple) or len(peer) < 2 or peer[1] != port
                        or self._policy.validate_addresses(host, (peer[0],)) != (checked[0],)):
                    raise EgressPolicyViolation()
                return _PinnedStream(stream, host, deadline, self._allow_plaintext, self._connection_guard)
        except BaseException as error:
            if stream is not None:
                await _close_failed(stream, deadline)
            if isinstance(error, EgressPolicyViolation):
                _reject()
            if isinstance(error, TrustBundleUnavailable):
                raise httpcore.ConnectError(error) from error
            if isinstance(error, DnsResolutionFailure):
                raise httpcore.ConnectError("Provider DNS resolution unavailable") from error
            if isinstance(error, TimeoutError):
                raise httpcore.ConnectTimeout("Provider connection deadline exhausted") from None
            raise

    async def connect_unix_socket(self, path, timeout=None, socket_options=None):
        _reject()

    async def sleep(self, seconds):
        raise RuntimeError("Transport-owned retries are disabled")


class _PinnedStream(httpcore.AsyncNetworkStream):
    def __init__(self, stream, host, connect_deadline, allow_plaintext, connection_guard=None):
        self._stream, self._host = stream, host
        self._connect_deadline, self._allow_plaintext = connect_deadline, allow_plaintext
        self._tls = False
        self._connection_guard = connection_guard

    async def start_tls(self, ssl_context, server_hostname=None, timeout=None):
        try:
            if self._connection_guard is not None:
                self._connection_guard()
            if (self._tls or server_hostname != self._host or not isinstance(ssl_context, ssl.SSLContext)
                    or not ssl_context.check_hostname or ssl_context.verify_mode != ssl.CERT_REQUIRED):
                raise EgressPolicyViolation()
            deadline = self._connect_deadline
            if timeout is not None:
                if type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout <= 0:
                    raise TimeoutError()
                bound = asyncio.get_running_loop().time() + timeout
                deadline = min(deadline, bound)
            if asyncio.get_running_loop().time() >= deadline:
                raise TimeoutError()
            async with asyncio.timeout_at(deadline):
                ssl_context.set_alpn_protocols(["http/1.1"])
                remaining = max(0, deadline - asyncio.get_running_loop().time())
                self._stream = await self._stream.start_tls(ssl_context, server_hostname=self._host, timeout=remaining)
                if asyncio.get_running_loop().time() >= deadline:
                    raise TimeoutError()
                ssl_object = self._stream.get_extra_info("ssl_object")
                if ssl_object is None or ssl_object.selected_alpn_protocol() not in {None, "http/1.1"}:
                    raise EgressPolicyViolation()
                self._tls = True
                return self
        except BaseException as error:
            await _close_failed(self._stream, self._connect_deadline)
            if isinstance(error, EgressPolicyViolation):
                _reject()
            if isinstance(error, TrustBundleUnavailable):
                raise httpcore.ConnectError(error) from error
            if isinstance(error, TimeoutError):
                raise httpcore.ConnectTimeout("Provider connection deadline exhausted") from None
            raise

    def _require_transport(self):
        if not self._tls and not self._allow_plaintext:
            _reject()

    async def read(self, max_bytes, timeout=None):
        self._require_transport()
        return await self._stream.read(max_bytes, timeout)

    async def write(self, buffer, timeout=None):
        self._require_transport()
        return await self._stream.write(buffer, timeout)

    async def aclose(self):
        await self._stream.aclose()

    def get_extra_info(self, info):
        return self._stream.get_extra_info(info)
