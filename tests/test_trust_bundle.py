import asyncio
from datetime import datetime, timedelta, timezone

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

from llm_gateway.adapters.trust_bundle import LocalTrustBundleValidator, canonical_pem, pem_identity
from llm_gateway.application.configuration_validation import LocalConfigurationValidator
from tests.config_fixtures import bundle_submission
from tests.test_configuration_preparation import draft
from tests.test_configuration_validation import POLICY


NOW = datetime(2026, 9, 7, tzinfo=timezone.utc)


def certificate(*, ca=True, start=None, end=None):
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "Gateway Test CA")])
    cert = (x509.CertificateBuilder().subject_name(name).issuer_name(name)
            .public_key(key.public_key()).serial_number(x509.random_serial_number())
            .not_valid_before(start or NOW - timedelta(days=1))
            .not_valid_after(end or NOW + timedelta(days=1))
            .add_extension(x509.BasicConstraints(ca=ca, path_length=None), critical=True)
            .sign(key, hashes.SHA256()))
    return cert.public_bytes(serialization.Encoding.PEM).decode()


def valid(pem, **limits):
    validator = LocalTrustBundleValidator(clock=lambda: NOW, **limits)
    return asyncio.run(validator.valid(pem_identity(pem), pem))


def test_current_ca_is_accepted_without_network():
    assert valid(certificate())


def test_normalization_preserves_certificate_order_and_bytes():
    first, second = certificate(), certificate()
    original = first + second
    variant = "\ufeff" + original.replace("\n", "\r\n") + "\r\n"
    assert canonical_pem(variant) == original
    assert valid(canonical_pem(variant))


def test_noncanonical_input_is_not_implicitly_accepted_by_validator():
    assert not valid(certificate().replace("\n", "\r\n"))


def test_wrong_content_identity_is_rejected():
    validator = LocalTrustBundleValidator(clock=lambda: NOW)
    assert not asyncio.run(validator.valid("sha256:" + "0" * 64, certificate()))


@pytest.mark.parametrize("content", ["", "\n", "not a certificate\n", "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n"])
def test_malformed_content_is_rejected(content):
    assert not valid(content)


def test_private_key_is_rejected_even_alongside_valid_ca():
    key = ec.generate_private_key(ec.SECP256R1())
    private = key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                serialization.NoEncryption()).decode()
    assert not valid(certificate() + private)


@pytest.mark.parametrize("before,after", [(-3, -1), (1, 3)])
def test_every_certificate_must_be_current(before, after):
    invalid = certificate(start=NOW + timedelta(days=before), end=NOW + timedelta(days=after))
    assert not valid(certificate() + invalid)


def test_leaf_only_bundle_is_rejected():
    assert not valid(certificate(ca=False))


def test_leaf_mixed_with_ca_is_rejected():
    assert not valid(certificate() + certificate(ca=False))


def test_resource_limits_are_enforced():
    pem = certificate()
    assert not valid(pem, max_bytes=10)
    assert not valid(pem + certificate(), max_certificates=1)


@pytest.mark.parametrize("kwargs", [{"max_bytes": 1048577}, {"max_certificates": 101}, {"max_bytes": True}, {"max_certificates": 0}])
def test_config_cannot_raise_hard_ceiling(kwargs):
    with pytest.raises(ValueError):
        LocalTrustBundleValidator(clock=lambda: NOW, **kwargs)


def test_naive_clock_is_a_local_fault_not_invalid_user_certificate():
    validator = LocalTrustBundleValidator(clock=lambda: NOW.replace(tzinfo=None))
    with pytest.raises(ValueError, match="aware clock"):
        asyncio.run(validator.valid("unused", certificate()))


def test_bundle_mapper_and_local_validator_integration():
    pem = certificate()
    body = bundle_submission()
    trust = {"identity": pem_identity(pem), "pem": "\ufeff" + pem.replace("\n", "\r\n"), "label": "Test CA"}
    body["bundle"]["providers"]["provider-a"]["transport"]["tls"] = {"trust": "bundle", "trust_bundle": trust}
    candidate = draft(body)
    validator = LocalConfigurationValidator(POLICY, LocalTrustBundleValidator(clock=lambda: NOW))
    assert asyncio.run(validator.validate(candidate)) == ()
    trust["pem"] = pem
    assert draft(body).snapshot_digest == candidate.snapshot_digest
    trust["label"] = "Different label"
    assert asyncio.run(validator.validate(draft(body))) == ()
    assert trust["identity"] == pem_identity(pem)
