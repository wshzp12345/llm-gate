import asyncio
import ssl
from datetime import datetime, timedelta, timezone

import httpcore
import httpx
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

from llm_gateway.adapters.egress_network import EgressNetworkBackend
from llm_gateway.adapters.egress_httpx import create_egress_client
from llm_gateway.adapters.provider_trust import prepare_trust_bundle
from llm_gateway.adapters.trust_bundle import pem_identity
from llm_gateway.adapters.openai_compatible import _network_retryable
from llm_gateway.domain.provider_egress import ProviderEgressPolicy


def tls_contexts(directory, hostname, *, return_root=False):
    now = datetime.now(timezone.utc)
    root_key, server_key = ec.generate_private_key(ec.SECP256R1()), ec.generate_private_key(ec.SECP256R1())
    root_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "Ephemeral Egress Test CA")])
    root = (x509.CertificateBuilder().subject_name(root_name).issuer_name(root_name)
        .public_key(root_key.public_key()).serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(days=1)).not_valid_after(now + timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .add_extension(x509.KeyUsage(True, False, False, False, False, True, True, False, False), critical=True)
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(root_key.public_key()), critical=False)
        .sign(root_key, hashes.SHA256()))
    certificate = (x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, hostname)]))
        .issuer_name(root_name).public_key(server_key.public_key()).serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(days=1)).not_valid_after(now + timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(x509.KeyUsage(True, False, False, False, False, False, False, False, False), critical=True)
        .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), critical=False)
        .add_extension(x509.SubjectAlternativeName([x509.DNSName(hostname)]), critical=False)
        .add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(root_key.public_key()), critical=False)
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(server_key.public_key()), critical=False)
        .sign(root_key, hashes.SHA256()))
    certificate_path, key_path = directory / "server.pem", directory / "server-key.pem"
    certificate_path.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(server_key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))
    server = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server.load_cert_chain(certificate_path, key_path)
    client = ssl.create_default_context(cadata=root.public_bytes(serialization.Encoding.PEM).decode())
    if return_root:
        return server, client, root.public_bytes(serialization.Encoding.PEM).decode()
    return server, client


@pytest.mark.parametrize("certificate_hostname", ["provider.invalid", "other.invalid"])
@pytest.mark.parametrize("bridge", [False, True, "bundle"])
def test_real_numeric_tcp_keeps_original_http_host_sni_and_certificate_hostname(tmp_path, certificate_hostname, monkeypatch, bridge):
    async def run():
        server_context, client_context, root_pem = tls_contexts(tmp_path, certificate_hostname, return_root=True)
        trust = await prepare_trust_bundle(identity=pem_identity(root_pem), pem=root_pem)
        sni, requests, connections = [], [], set()
        server_context.sni_callback = lambda sock, host, context: sni.append(host)
        async def handle(reader, writer):
            task = asyncio.current_task()
            connections.add(task)
            try:
                requests.append(await reader.readuntil(b"\r\n\r\n"))
                writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
                await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()
                connections.discard(task)
        server = await asyncio.start_server(handle, "127.0.0.1", 0, ssl=server_context)
        port = server.sockets[0].getsockname()[1]
        class Resolver:
            calls = 0
            async def resolve(self, host, destination_port):
                assert (host, destination_port) == ("provider.invalid", port)
                self.calls += 1
                return ("127.0.0.1",)
        resolver = Resolver()
        network = EgressNetworkBackend(policy=ProviderEgressPolicy(("provider.invalid",), ("127.0.0.1/32",)),
            host="provider.invalid", port=port, resolver=resolver)
        # A direct numeric AnyIO dial must not perform a second DNS lookup.
        async def unexpected_lookup(*args, **kwargs):
            pytest.fail("The validated hostname must not be resolved again by the connector")
        monkeypatch.setattr(asyncio.get_running_loop(), "getaddrinfo", unexpected_lookup)
        monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:1")
        monkeypatch.setenv("ALL_PROXY", "http://127.0.0.1:1")
        monkeypatch.setenv("SSL_CERT_FILE", str(tmp_path / "nonexistent.pem"))
        client = (create_egress_client(policy=ProviderEgressPolicy(("provider.invalid",), ("127.0.0.1/32",)),
                    host="provider.invalid", port=port, resolver=resolver,
                    ssl_context=client_context if bridge != "bundle" else None,
                    trust_bundle=trust if bridge == "bundle" else None,
                    max_connections=1) if bridge else
                  httpcore.AsyncConnectionPool(network_backend=network, ssl_context=client_context,
                    http1=True, http2=False, retries=0, max_connections=1))
        assert resolver.calls == 0
        try:
            async with client as pool:
                if certificate_hostname == "provider.invalid":
                    for _ in range(2):
                        response = await pool.request("GET", f"https://provider.invalid:{port}/health")
                        assert (response.status_code if bridge else response.status) == 200 and response.content == b"ok"
                    assert resolver.calls == 2 and sni == ["provider.invalid", "provider.invalid"]
                    assert all(f"Host: provider.invalid:{port}".encode() in data for data in requests)
                else:
                    with pytest.raises(httpx.ConnectError if bridge else httpcore.ConnectError) as error:
                        await pool.request("GET", f"https://provider.invalid:{port}/health")
                    assert not _network_retryable(error.value) and not requests
                    assert sni == ["provider.invalid"]
                lookups = resolver.calls
                with pytest.raises(httpx.ConnectError if bridge else httpcore.ConnectError) as error:
                    await pool.request("GET", f"https://other.invalid:{port}/health")
                assert not _network_retryable(error.value) and resolver.calls == lookups
        finally:
            server.close()
            await server.wait_closed()
            if connections:
                await asyncio.wait_for(asyncio.gather(*connections), 3)
    asyncio.run(run(), loop_factory=asyncio.SelectorEventLoop)
