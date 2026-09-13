"""Operation-owned raw HMAC material, distinct from Provider bearer credentials.

This primitive has no source, cache, capacity pool, readiness or admission logic.
Closing overwrites our buffer, not every copy held by Python or a SecretSource.
"""

import hashlib
import hmac
from dataclasses import dataclass, field

from llm_gateway.domain.fingerprint_keys import FingerprintKeyMember


class FingerprintMaterialUnavailable(Exception):
    def __init__(self):
        super().__init__("Fingerprint key material unavailable")


@dataclass(frozen=True)
class FingerprintMaterialMetadata:
    key_id: str
    key_version: str
    secret_ref: str = field(repr=False)
    profile: str = "HMAC-SHA-256"
    revoked: bool = False


class FingerprintMaterial:
    __slots__ = ("_material", "_closed", "_entered")

    def __init__(self, member: FingerprintKeyMember,
                 metadata: FingerprintMaterialMetadata, material: bytes):
        if (type(member) is not FingerprintKeyMember
                or type(metadata) is not FingerprintMaterialMetadata
                or metadata.key_id != member.key_id
                or metadata.key_version != member.key_version
                or metadata.secret_ref != member.secret_ref
                or metadata.profile != "HMAC-SHA-256"
                or metadata.revoked is not False
                or type(material) is not bytes or len(material) != 32):
            raise FingerprintMaterialUnavailable()
        self._material = bytearray(material)
        self._closed = False
        self._entered = False

    def digest(self, message: bytes) -> bytes:
        """HMAC only; the caller owns canonicalization and domain separation."""
        if self._closed:
            raise FingerprintMaterialUnavailable()
        if type(message) is not bytes:
            raise TypeError("HMAC input must be bytes")
        return hmac.new(self._material, message, hashlib.sha256).digest()

    def close(self) -> None:
        self._material[:] = b"\x00" * len(self._material)
        self._material.clear()
        self._closed = True

    def __enter__(self):
        if self._closed or self._entered:
            raise FingerprintMaterialUnavailable()
        self._entered = True
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.close()
        return False

    def __repr__(self):
        return "<FingerprintMaterial redacted>"

    def __reduce_ex__(self, protocol):
        raise TypeError("Fingerprint material cannot be serialized")
