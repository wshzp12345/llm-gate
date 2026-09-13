import copy
import hashlib
import hmac
import pickle
from dataclasses import FrozenInstanceError, replace

import pytest

from llm_gateway.adapters.fingerprint_material import (
    FingerprintMaterial, FingerprintMaterialMetadata, FingerprintMaterialUnavailable,
)
from llm_gateway.domain.fingerprint_keys import FingerprintKeyMember, FingerprintKeyRing


ACTIVE = FingerprintKeyMember("fingerprint", "v2", "active", "exact-private-ref-v2")
OLD = FingerprintKeyMember("fingerprint", "v1", "verification_only", "exact-private-ref-v1")
METADATA = FingerprintMaterialMetadata(ACTIVE.key_id, ACTIVE.key_version, ACTIVE.secret_ref)


def test_ring_preserves_versions_and_exact_lookup_without_active_fallback():
    ring = FingerprintKeyRing((OLD, ACTIVE))
    assert ring.active is ACTIVE
    assert ring.member("fingerprint", "v1") is OLD
    with pytest.raises(ValueError, match="identity unavailable"):
        ring.member("fingerprint", "absent")
    with pytest.raises(FrozenInstanceError):
        ring.members = (ACTIVE,)
    with pytest.raises(FrozenInstanceError):
        ACTIVE.key_version = "changed"
    assert ACTIVE.secret_ref not in repr(ring)


@pytest.mark.parametrize("members", [(), (OLD,), (ACTIVE, ACTIVE), [ACTIVE],
    (ACTIVE, replace(OLD, key_version="v2")),
    (ACTIVE, replace(OLD, secret_ref=ACTIVE.secret_ref)),
    (ACTIVE, replace(OLD, role="active")), (ACTIVE, object()),
    (ACTIVE, *(replace(OLD, key_version=str(i), secret_ref=str(i)) for i in range(9))),
])
def test_rejects_invalid_ring(members):
    with pytest.raises(ValueError):
        FingerprintKeyRing(members)


def test_all_nine_members_are_allowed_and_profile_is_closed():
    members = (ACTIVE, *(replace(OLD, key_version=str(i), secret_ref=str(i)) for i in range(8)))
    assert len(FingerprintKeyRing(members).members) == 9
    with pytest.raises(ValueError):
        FingerprintKeyRing((ACTIVE,), profile="gateway.idempotency-index-key-ring/v1")


@pytest.mark.parametrize("change", [{"key_id": ""}, {"key_version": 1}, {"role": "other"},
                                    {"secret_ref": ""}, {"role": []}])
def test_rejects_invalid_member(change):
    with pytest.raises(ValueError):
        replace(ACTIVE, **change)


def test_member_wire_fields_are_closed():
    wire = dict(key_id="a", key_version="b", role="active", secret_ref="ref")
    assert FingerprintKeyMember.from_mapping(wire).identity == ("a", "b")
    for invalid in ({**wire, "key_bytes": "secret"}, {k: v for k, v in wire.items() if k != "role"}, []):
        with pytest.raises(ValueError):
            FingerprintKeyMember.from_mapping(invalid)


def test_raw_binary_material_is_not_decoded_and_closes_on_error():
    raw = bytes(range(32))
    material = FingerprintMaterial(ACTIVE, METADATA, raw)
    with pytest.raises(RuntimeError):
        with material:
            assert material.digest(b"input") == hmac.new(raw, b"input", hashlib.sha256).digest()
            raise RuntimeError("operation failed")
    assert material._material == bytearray()
    material.close()
    with pytest.raises(FingerprintMaterialUnavailable):
        material.digest(b"input")
    with pytest.raises(FingerprintMaterialUnavailable):
        material.__enter__()


@pytest.mark.parametrize("raw", [b"", b"a" * 31, b"a" * 33, b"ab" * 32,
                                 "a" * 32, bytearray(32), None])
def test_material_is_exactly_32_raw_bytes(raw):
    with pytest.raises(FingerprintMaterialUnavailable):
        FingerprintMaterial(ACTIVE, METADATA, raw)


@pytest.mark.parametrize("change", [{"key_id": "wrong"}, {"key_version": "wrong"},
    {"secret_ref": "wrong"}, {"profile": "SHA-256"}, {"revoked": True}, {"revoked": 0}])
def test_source_identity_profile_and_revocation_must_match(change):
    with pytest.raises(FingerprintMaterialUnavailable) as error:
        FingerprintMaterial(ACTIVE, replace(METADATA, **change), b"s" * 32)
    assert str(error.value) == "Fingerprint key material unavailable"


def test_material_cannot_be_copied_serialized_or_reentered():
    with FingerprintMaterial(ACTIVE, METADATA, b"s" * 32) as material:
        assert repr(material) == "<FingerprintMaterial redacted>"
        assert ACTIVE.secret_ref not in repr(METADATA)
        for serialize in (pickle.dumps, copy.copy, copy.deepcopy):
            with pytest.raises(TypeError, match="cannot be serialized"):
                serialize(material)
        with pytest.raises(FingerprintMaterialUnavailable):
            material.__enter__()
