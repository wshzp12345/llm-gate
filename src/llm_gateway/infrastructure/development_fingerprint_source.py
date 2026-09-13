"""Dev-only mounted raw key files and an immutable explicit-version catalog.

Each catalog entry declares one version, reference and absolute file path. The
file contains exactly 32 raw bytes, not JSON, hex or base64. Catalog metadata is
trusted deployment input, not inferred from file content or a request. Operators
must mount an immutable version's file; planned rotation uses a new entry and
Gateway restart. Missing files fail closed. There is no environment fallback,
key generation, in-memory material cache, live catalog reload or file writing.
"""

from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType

from llm_gateway.adapters.fingerprint_material import (
    FingerprintMaterial, FingerprintMaterialMetadata, FingerprintMaterialUnavailable,
)
from llm_gateway.domain.fingerprint_keys import FingerprintKeyMember


@dataclass(frozen=True)
class MountedFingerprintSecret:
    metadata: FingerprintMaterialMetadata
    path: Path = field(repr=False)

    def __post_init__(self):
        if (type(self.metadata) is not FingerprintMaterialMetadata
                or not isinstance(self.path, Path) or not self.path.is_absolute()):
            raise ValueError("Invalid mounted Fingerprint key declaration")
        # Reuse declaration lexical checks without exposing any submitted value.
        FingerprintKeyMember(self.metadata.key_id, self.metadata.key_version,
                             "verification_only", self.metadata.secret_ref)
        if self.metadata.profile != "HMAC-SHA-256" or type(self.metadata.revoked) is not bool:
            raise ValueError("Invalid mounted Fingerprint key metadata")


class DevelopmentFingerprintSource:
    def __init__(self, *, startup_profile: str, entries: tuple[MountedFingerprintSecret, ...]):
        if (startup_profile != "dev" or type(entries) is not tuple or not entries
                or any(type(entry) is not MountedFingerprintSecret for entry in entries)):
            raise ValueError("Invalid development Fingerprint source configuration")
        identities = {(entry.metadata.key_id, entry.metadata.key_version) for entry in entries}
        references = {entry.metadata.secret_ref for entry in entries}
        paths = {entry.path for entry in entries}
        if len(identities) != len(entries) or len(references) != len(entries) or len(paths) != len(entries):
            raise ValueError("Duplicate mounted Fingerprint key declaration")
        self._entries = MappingProxyType({entry.metadata.secret_ref: entry for entry in entries})

    def resolve(self, member: FingerprintKeyMember) -> FingerprintMaterial:
        try:
            entry = self._entries[member.secret_ref]
            metadata = entry.metadata
            if ((metadata.key_id, metadata.key_version) != member.identity or metadata.revoked):
                raise FingerprintMaterialUnavailable()
            with entry.path.open("rb") as stream:
                raw = stream.read(33)
            return FingerprintMaterial(member, metadata, raw)
        except (OSError, KeyError, ValueError, TypeError, FingerprintMaterialUnavailable):
            pass
        raise FingerprintMaterialUnavailable()
