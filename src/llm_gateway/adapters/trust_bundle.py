"""Local certificate validation only; this does not perform TLS handshakes."""

import hashlib
import re
from collections.abc import Callable
from datetime import datetime, timezone

from cryptography import x509
from cryptography.exceptions import UnsupportedAlgorithm


def canonical_pem(pem: str) -> str:
    """Normalize transport encoding without reordering or re-encoding certificates."""
    return pem.removeprefix("\ufeff").replace("\r\n", "\n").replace("\r", "\n").rstrip("\n") + "\n"


def pem_identity(pem: str) -> str:
    return "sha256:" + hashlib.sha256(pem.encode("utf-8")).hexdigest()


_CERTIFICATE = re.compile(
    r"-----BEGIN CERTIFICATE-----\n[A-Za-z0-9+/=\n]+-----END CERTIFICATE-----"
)


class TrustBundleIntegrityValidator:
    """Published material checks; date eligibility belongs to new connections."""

    def __init__(
        self, *, max_bytes: int = 1048576,
        max_certificates: int = 100,
    ) -> None:
        if type(max_bytes) is not int or not 1 <= max_bytes <= 1048576:
            raise ValueError("Invalid Trust Bundle byte ceiling")
        if type(max_certificates) is not int or not 1 <= max_certificates <= 100:
            raise ValueError("Invalid Trust Bundle certificate ceiling")
        self._max_bytes = max_bytes
        self._max_certificates = max_certificates

    async def valid(self, identity: str, canonical_pem_text: str) -> bool:
        return self.certificates(identity, canonical_pem_text) is not None

    def certificates(self, identity: str, canonical_pem_text: str):
        """Validate immutable material independently of runtime date eligibility."""
        try:
            body = canonical_pem_text.encode("utf-8")
            if (len(body) > self._max_bytes or canonical_pem(canonical_pem_text) != canonical_pem_text
                    or pem_identity(canonical_pem_text) != identity):
                return None
            count, end = 0, 0
            certificates = []
            for block in _CERTIFICATE.finditer(canonical_pem_text):
                # Reject private keys, comments, arbitrary text and other PEM types.
                if canonical_pem_text[end:block.start()].strip():
                    return None
                count += 1
                if count > self._max_certificates:
                    return None
                cert = x509.load_pem_x509_certificate(block.group().encode("ascii"))
                certificates.append(cert)
                try:
                    if not cert.extensions.get_extension_for_class(x509.BasicConstraints).value.ca:
                        return None
                except x509.ExtensionNotFound:
                    return None
                end = block.end()
            if count > 0 and not canonical_pem_text[end:].strip():
                return tuple(certificates)
            return None
        except (ValueError, UnsupportedAlgorithm, x509.DuplicateExtension):
            return None


class LocalTrustBundleValidator(TrustBundleIntegrityValidator):
    """Candidate validation additionally requires every CA to be current."""

    def __init__(self, *, clock: Callable[[], datetime], max_bytes=1048576, max_certificates=100):
        super().__init__(max_bytes=max_bytes, max_certificates=max_certificates)
        self._clock = clock

    async def valid(self, identity: str, canonical_pem_text: str) -> bool:
        now = self._clock()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("Trust Bundle validation requires an aware clock")
        now = now.astimezone(timezone.utc)
        certificates = self.certificates(identity, canonical_pem_text)
        return certificates is not None and all(
            cert.not_valid_before_utc <= now <= cert.not_valid_after_utc for cert in certificates)
