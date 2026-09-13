import ssl
from types import SimpleNamespace

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import serialization

from llm_gateway.infrastructure.system_tls import system_tls_context
from tests.test_trust_bundle import certificate


def test_system_context_uses_compiled_paths_not_environment_overrides(tmp_path, monkeypatch):
    trusted = tmp_path / "system.pem"
    trusted.write_text(certificate(), encoding="ascii")
    ignored = tmp_path / "environment.pem"
    ignored.write_text("not a CA", encoding="ascii")
    monkeypatch.setenv("SSL_CERT_FILE", str(ignored))
    monkeypatch.setenv("SSL_CERT_DIR", str(tmp_path / "missing"))
    monkeypatch.setattr(ssl, "enum_certificates", lambda store: [], raising=False)
    monkeypatch.setattr(ssl, "get_default_verify_paths", lambda: SimpleNamespace(
        cafile=str(ignored), capath=str(tmp_path / "missing"), openssl_cafile=str(trusted), openssl_capath=None))
    context = system_tls_context()
    assert context.verify_mode == ssl.CERT_REQUIRED and context.check_hostname
    assert context.minimum_version == ssl.TLSVersion.TLSv1_2
    assert len(context.get_ca_certs()) == 1


def test_windows_store_filters_non_server_trust_and_unsupported_encoding(monkeypatch):
    trusted, ignored = [x509.load_pem_x509_certificate(certificate().encode()).public_bytes(serialization.Encoding.DER)
                        for _ in range(2)]
    monkeypatch.setattr(ssl, "enum_certificates", lambda store: [
        (trusted, "x509_asn", {ssl.Purpose.SERVER_AUTH.oid}),
        (ignored, "x509_asn", {ssl.Purpose.CLIENT_AUTH.oid}),
        (ignored, "pkcs_7_asn", True)], raising=False)
    monkeypatch.setattr(ssl, "get_default_verify_paths", lambda: SimpleNamespace(openssl_cafile=None, openssl_capath=None))
    assert len(system_tls_context().get_ca_certs()) == 1


def test_missing_system_roots_is_local_initialization_fault(monkeypatch):
    monkeypatch.setattr(ssl, "enum_certificates", lambda store: [], raising=False)
    monkeypatch.setattr(ssl, "get_default_verify_paths", lambda: SimpleNamespace(openssl_cafile=None, openssl_capath=None))
    with pytest.raises(RuntimeError, match="No system"):
        system_tls_context()
