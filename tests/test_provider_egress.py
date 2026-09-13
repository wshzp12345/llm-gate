import asyncio
import json
import socket
import ssl
from contextlib import closing
from dataclasses import replace
from threading import Event

import httpcore
import pytest

from llm_gateway.adapters.egress_network import EgressNetworkBackend
from llm_gateway.adapters.egress_projection import project_provider_egress
from llm_gateway.adapters.openai_compatible import _network_retryable
from llm_gateway.adapters.provider_dns import DnsResolutionFailure, DnsResolverInternalError
from llm_gateway.domain.provider_egress import EgressPolicyViolation, ProviderEgressPolicy
from llm_gateway.infrastructure.provider_dns import BoundedDnsResolver
from tests.test_text_completion_projection import snapshot


POLICY = ProviderEgressPolicy(("provider.invalid",), ("public",))


@pytest.mark.parametrize("address", ["127.0.0.1", "10.0.0.1", "169.254.169.254", "100.64.0.1", "0.0.0.0",
    "224.0.0.1", "240.0.0.1", "::1", "::", "fc00::1", "fe80::1", "ff02::1", "2001:db8::1",
    "::ffff:127.0.0.1", "2002:7f00:1::1", "fe80::1%1"])
def test_public_never_authorizes_private_special_or_scoped_destinations(address):
    with pytest.raises(EgressPolicyViolation) as error:
        POLICY.validate_addresses("provider.invalid", ("8.8.8.8", address))
    assert address not in str(error.value)


def test_explicit_cidr_authorizes_private_development_and_checks_every_answer():
    private = ProviderEgressPolicy(("provider.invalid",), ("127.0.0.0/8", "::1/128"))
    assert private.validate_addresses("provider.invalid", ("127.0.0.1", "::1")) == ("127.0.0.1", "::1")
    assert POLICY.validate_addresses("provider.invalid", ("8.8.8.8", "2606:4700:4700::1111", "8.8.8.8")) == ("8.8.8.8", "2606:4700:4700::1111")
    assert POLICY.validate_addresses("provider.invalid", ("::ffff:8.8.8.8",))
    with pytest.raises(EgressPolicyViolation):
        POLICY.validate_addresses("other.invalid", ("8.8.8.8",))
    with pytest.raises(EgressPolicyViolation):
        POLICY.validate_addresses("provider.invalid", ())


@pytest.mark.parametrize("hosts,networks", [(("*.invalid",), ("public",)), (("PROVIDER.invalid",), ("public",)),
    (("127.0.0.1",), ("public",)), (("provider.invalid",), ("10.0.0.1/8",)), (("provider.invalid",), ()),
    (("provider.invalid", "provider.invalid"), ("public",))])
def test_noncanonical_or_empty_policy_is_not_repaired(hosts, networks):
    with pytest.raises(ValueError):
        ProviderEgressPolicy(hosts, networks)


class Resolver:
    def __init__(self, answers):
        self.answers, self.calls = answers, []
    async def resolve(self, host, port):
        self.calls.append((host, port))
        return self.answers


class Stream:
    def __init__(self, peer=("8.8.8.8", 443)):
        self.peer, self.closed, self.writes, self.tls_calls = peer, False, [], []
    def get_extra_info(self, info):
        return self.peer if info == "server_addr" else None
    async def aclose(self):
        self.closed = True
    async def write(self, data, timeout=None):
        self.writes.append(data)
    async def start_tls(self, *args, **kwargs):
        self.tls_calls.append(kwargs)
        return self


class Connector:
    def __init__(self, stream=None):
        self.calls, self.stream = [], stream or Stream()
    async def connect_tcp(self, host, port, **kwargs):
        self.calls.append((host, port, kwargs))
        return self.stream


def backend(resolver, connector, **changes):
    return EgressNetworkBackend(policy=POLICY, host="provider.invalid", port=443,
        resolver=resolver, connector=connector, **changes)


def test_mixed_answer_denies_before_connect_and_each_new_connection_resolves_again():
    async def run():
        resolver, connector = Resolver(("8.8.8.8", "127.0.0.1")), Connector()
        network = backend(resolver, connector)
        with pytest.raises(httpcore.ConnectError) as error:
            await network.connect_tcp("provider.invalid", 443)
        assert not _network_retryable(error.value) and not connector.calls
        resolver.answers = ("8.8.8.8",)
        stream = await network.connect_tcp("provider.invalid", 443)
        await stream.aclose()
        assert connector.calls[0][0:2] == ("8.8.8.8", 443)
        resolver.answers = ("127.0.0.1",)
        with pytest.raises(httpcore.ConnectError):
            await network.connect_tcp("provider.invalid", 443)
        assert len(resolver.calls) == 3 and len(connector.calls) == 1
    asyncio.run(run())


@pytest.mark.parametrize("peer", [("127.0.0.1", 443), ("8.8.8.8", 80), None])
def test_actual_peer_mismatch_closes_before_application_bytes(peer):
    async def run():
        raw = Stream(peer)
        with pytest.raises(httpcore.ConnectError):
            await backend(Resolver(("8.8.8.8",)), Connector(raw)).connect_tcp("provider.invalid", 443)
        assert raw.closed and not raw.writes
    asyncio.run(run())


def test_host_port_unix_and_plaintext_bypasses_fail_closed():
    async def run():
        resolver, connector = Resolver(("8.8.8.8",)), Connector()
        network = backend(resolver, connector)
        for host, port in (("other.invalid", 443), ("provider.invalid", 80)):
            with pytest.raises(httpcore.ConnectError):
                await network.connect_tcp(host, port)
        with pytest.raises(httpcore.ConnectError):
            await network.connect_unix_socket("not-an-approved-route")
        assert not resolver.calls and not connector.calls
        stream = await network.connect_tcp("provider.invalid", 443)
        with pytest.raises(httpcore.ConnectError):
            await stream.write(b"must-not-be-sent")
        assert not connector.stream.writes
        await stream.aclose()
    asyncio.run(run())


@pytest.mark.parametrize("kind", ["hostname", "verification", "deadline"])
def test_tls_keeps_original_hostname_verification_and_connection_deadline(kind):
    async def run():
        raw = Stream()
        stream = await backend(Resolver(("8.8.8.8",)), Connector(raw)).connect_tcp("provider.invalid", 443, timeout=1)
        context = ssl.create_default_context()
        if kind == "verification":
            context.check_hostname = False
        if kind == "deadline":
            stream._connect_deadline = asyncio.get_running_loop().time() - 1
        with pytest.raises(httpcore.ConnectError if kind != "deadline" else httpcore.ConnectTimeout):
            await stream.start_tls(context, server_hostname="other.invalid" if kind == "hostname" else "provider.invalid", timeout=30)
        assert raw.closed and not raw.writes
        if kind != "deadline":
            assert not raw.tls_calls
    asyncio.run(run())


def answers(host, port, **kwargs):
    return [(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("8.8.8.8", port))]


def test_dns_worker_capacity_remains_occupied_after_waiter_cancellation():
    async def run():
        entered, release = Event(), Event()
        def lookup(host, port, **kwargs):
            entered.set()
            release.wait(5)
            return answers(host, port, **kwargs)
        resolver = BoundedDnsResolver(max_concurrent_lookups=1, lookup=lookup)
        try:
            task = asyncio.create_task(resolver.resolve("provider.invalid", 443))
            assert await asyncio.to_thread(entered.wait, 2)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            with pytest.raises(DnsResolutionFailure) as error:
                await resolver.resolve("provider.invalid", 443)
            assert error.value.retryable is False and resolver._pool.in_use == 1
        finally:
            release.set()
            resolver.close()
        with pytest.raises(DnsResolutionFailure):
            await resolver.resolve("provider.invalid", 443)
    asyncio.run(run())


@pytest.mark.parametrize("kind,retryable", [("transient", True), ("permanent", False), ("oversized", False)])
def test_dns_failures_are_typed_safe_and_not_truncated_to_allowed_subset(kind, retryable):
    async def run():
        def lookup(host, port, **kwargs):
            if kind == "oversized":
                return answers(host, port, **kwargs) * 2
            raise socket.gaierror(socket.EAI_AGAIN if kind == "transient" else socket.EAI_NONAME, "sensitive destination")
        with closing(BoundedDnsResolver(max_concurrent_lookups=1, max_addresses=1, lookup=lookup)) as resolver:
            with pytest.raises(DnsResolutionFailure) as error:
                await resolver.resolve("provider.invalid", 443)
        assert error.value.retryable is retryable and _network_retryable(error.value) is retryable
        assert "sensitive" not in str(error.value)
    asyncio.run(run())


def test_unexpected_resolver_bug_is_internal_not_provider_health_signal():
    async def run():
        def lookup(*args, **kwargs):
            raise RuntimeError("sensitive implementation details")
        with closing(BoundedDnsResolver(max_concurrent_lookups=1, lookup=lookup)) as resolver:
            with pytest.raises(DnsResolverInternalError) as error:
                await resolver.resolve("provider.invalid", 443)
        assert "sensitive" not in str(error.value)
    asyncio.run(run())


def test_snapshot_projection_requires_explicit_proxy_free_allowlists():
    content = {"providers": {"provider": {"egress": {"proxy": "disabled", "allowed_hosts": ["provider.invalid"], "allowed_networks": ["public"]}}}}
    loaded = replace(snapshot(), snapshot_json=json.dumps(content).encode())
    assert project_provider_egress(loaded, "provider") == POLICY
    content["providers"]["provider"]["egress"]["proxy"] = "http"
    with pytest.raises(ValueError):
        project_provider_egress(replace(loaded, snapshot_json=json.dumps(content).encode()), "provider")


def test_cleanup_error_preserves_original_nonretryable_rejection():
    async def run():
        class BrokenClose(Stream):
            async def aclose(self):
                self.closed = True
                raise RuntimeError("private cleanup failure")
        raw = BrokenClose(peer=("127.0.0.1", 443))
        with pytest.raises(httpcore.ConnectError) as error:
            await backend(Resolver(("8.8.8.8",)), Connector(raw)).connect_tcp("provider.invalid", 443)
        assert not _network_retryable(error.value) and raw.closed
    asyncio.run(run())


def test_connect_failure_does_not_hide_an_address_retry():
    async def run():
        class FailingConnector(Connector):
            async def connect_tcp(self, host, port, **kwargs):
                self.calls.append((host, port))
                raise httpcore.ConnectError("synthetic failure")
        connector = FailingConnector()
        with pytest.raises(httpcore.ConnectError):
            await backend(Resolver(("8.8.8.8", "1.1.1.1")), connector).connect_tcp("provider.invalid", 443)
        assert connector.calls == [("8.8.8.8", 443)]
    asyncio.run(run())
