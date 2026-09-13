"""Credential-only boundary. Never pass these objects to application queries."""

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol


class CredentialUnavailable(Exception):
    def __init__(self):
        super().__init__("Provider credentials unavailable")


@dataclass(frozen=True)
class CredentialMetadata:
    secret_ref: str
    secret_version: str
    source: str
    valid_until: datetime
    revoked: bool = False


class ProviderCredentialLease:
    """One immutable value/version until release; no guarantee of Python zeroization.

    Private material is overwritten on close, but HTTP/Python may hold copies.
    Only the transport boundary may call bearer_value; never serialize a lease.
    """

    __slots__ = ("_metadata", "_material", "_closed", "_entered")

    def __init__(self, metadata: CredentialMetadata, material: bytes):
        self._metadata = metadata
        self._material = bytearray(material)
        self._closed = False
        self._entered = False

    @property
    def metadata(self) -> CredentialMetadata:
        return self._metadata

    def bearer_value(self) -> str:
        if self._closed:
            raise CredentialUnavailable()
        return self._material.decode("ascii")

    def close(self) -> None:
        self._material[:] = b"\x00" * len(self._material)
        self._material.clear()
        self._closed = True

    def __enter__(self):
        if self._closed or self._entered:
            raise CredentialUnavailable()
        self._entered = True
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.close()
        return False

    def __reduce_ex__(self, protocol):
        raise TypeError("Credential leases cannot be serialized")


class ProviderSecretSource(Protocol):
    def resolve(self, secret_ref: str) -> ProviderCredentialLease:
        """Resolve fresh material before Attempt start; caller must close lease."""
        ...
