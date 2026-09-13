"""Locked embedded roots and network-free, whole-bundle validity checks."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
import ssl

from llm_gateway.adapters.trust_bundle import TrustBundleIntegrityValidator


class TrustBundleUnavailable(PermissionError):
    def __init__(self, reason):
        self.reason = reason
        super().__init__("Provider Trust Bundle is not currently valid")


@dataclass(frozen=True)
class PreparedTrustBundle:
    identity: str
    not_before: datetime
    not_after: datetime
    context: ssl.SSLContext = field(repr=False, compare=False)
    _clock: object = field(repr=False, compare=False)

    def require_current(self):
        now = self._clock()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("Trust Bundle validation requires an aware clock")
        if now > self.not_after:
            raise TrustBundleUnavailable("trust_bundle_expired")
        if now < self.not_before:
            raise TrustBundleUnavailable("security_invalidated")


async def prepare_trust_bundle(*, identity, pem, clock=None,
                               max_bytes=1048576, max_certificates=100):
    return load_trust_bundle(identity=identity, pem=pem, clock=clock,
                            max_bytes=max_bytes, max_certificates=max_certificates)


def load_trust_bundle(*, identity, pem, clock=None,
                      max_bytes=1048576, max_certificates=100):
    """Invalid published material is a local initialization fault, not failover."""
    if clock is None:
        clock = lambda: datetime.now(timezone.utc)
    validator = TrustBundleIntegrityValidator(max_bytes=max_bytes, max_certificates=max_certificates)
    certificates = validator.certificates(identity, pem)
    if certificates is None:
        raise ValueError("Cannot initialize invalid Provider Trust Bundle")
    # A fresh context starts empty: embedded roots replace, not extend, defaults.
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.load_verify_locations(cadata=pem)
    return PreparedTrustBundle(identity,
        max(cert.not_valid_before_utc for cert in certificates),
        min(cert.not_valid_after_utc for cert in certificates), context, clock)
