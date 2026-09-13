import asyncio
from datetime import timedelta
import ssl

import httpcore
import httpx
import pytest

from llm_gateway.adapters.egress_httpx import create_egress_client
from llm_gateway.adapters.provider_trust import prepare_trust_bundle, TrustBundleUnavailable
from llm_gateway.adapters.trust_bundle import pem_identity
from llm_gateway.adapters.openai_compatible import _network_retryable
from tests.test_provider_egress import Resolver, Connector, Stream, backend, POLICY
from tests.test_trust_bundle import certificate, NOW


def test_embedded_roots_replace_defaults_and_all_dates_constrain_window(monkeypatch):
    async def run():
        first = certificate(end=NOW + timedelta(days=2))
        second = certificate(start=NOW - timedelta(hours=1), end=NOW + timedelta(hours=1))
        pem = first + second
        monkeypatch.setenv("SSL_CERT_FILE", "missing-ca.pem")
        prepared = await prepare_trust_bundle(identity=pem_identity(pem), pem=pem, clock=lambda: NOW)
        assert prepared.context.cert_store_stats()["x509"] == 2
        assert len(prepared.context.get_ca_certs()) == 2
        assert prepared.context.minimum_version == ssl.TLSVersion.TLSv1_2
        assert prepared.context.check_hostname and prepared.context.verify_mode == ssl.CERT_REQUIRED
        assert prepared.not_before == NOW - timedelta(hours=1)
        assert prepared.not_after == NOW + timedelta(hours=1)
        prepared.require_current()
    asyncio.run(run())


@pytest.mark.parametrize("offset,reason", [(2, "trust_bundle_expired"), (-2, "security_invalidated")])
def test_first_lazy_connection_after_expiry_or_clock_rollback_denied_before_dns(offset, reason):
    async def run():
        pem = certificate()
        prepared = await prepare_trust_bundle(identity=pem_identity(pem), pem=pem,
            clock=lambda: NOW + timedelta(days=offset))
        resolver = Resolver(("8.8.8.8",))
        async with create_egress_client(policy=POLICY, host="provider.invalid", port=443,
                resolver=resolver, trust_bundle=prepared, max_connections=1) as client:
            with pytest.raises(httpx.ConnectError) as error:
                await client.get("https://provider.invalid/")
            assert not _network_retryable(error.value) and not resolver.calls
        with pytest.raises(TrustBundleUnavailable) as error:
            prepared.require_current()
        assert error.value.reason == reason
    asyncio.run(run())


def test_expiry_between_tcp_and_tls_closes_before_handshake():
    async def run():
        now = [NOW]
        pem = certificate()
        prepared = await prepare_trust_bundle(identity=pem_identity(pem), pem=pem, clock=lambda: now[0])
        raw = Stream()
        stream = await backend(Resolver(("8.8.8.8",)), Connector(raw),
            connection_guard=prepared.require_current).connect_tcp("provider.invalid", 443)
        now[0] += timedelta(days=2)
        with pytest.raises(httpcore.ConnectError) as error:
            await stream.start_tls(prepared.context, server_hostname="provider.invalid")
        assert not _network_retryable(error.value) and raw.closed and not raw.tls_calls
    asyncio.run(run())


def test_established_tls_writes_continue_after_bundle_expiry():
    async def run():
        now = [NOW]
        pem = certificate()
        prepared = await prepare_trust_bundle(identity=pem_identity(pem), pem=pem, clock=lambda: now[0])
        class TlsStream(Stream):
            def selected_alpn_protocol(self):
                return "http/1.1"
            def get_extra_info(self, info):
                return self if info == "ssl_object" else super().get_extra_info(info)
        raw = TlsStream()
        stream = await backend(Resolver(("8.8.8.8",)), Connector(raw),
            connection_guard=prepared.require_current).connect_tcp("provider.invalid", 443)
        await stream.start_tls(prepared.context, server_hostname="provider.invalid")
        now[0] += timedelta(days=2)
        await stream.write(b"active response continues")
        assert raw.writes == [b"active response continues"] and not raw.closed
        await stream.aclose()
    asyncio.run(run())


def test_invalid_material_is_internal_and_naive_clock_is_not_provider_failure():
    async def run():
        pem = certificate()
        with pytest.raises(ValueError, match="initialize"):
            await prepare_trust_bundle(identity="sha256:invalid", pem=pem)
        prepared = await prepare_trust_bundle(identity=pem_identity(pem), pem=pem,
            clock=lambda: NOW.replace(tzinfo=None))
        with pytest.raises(ValueError, match="aware"):
            prepared.require_current()
    asyncio.run(run())
