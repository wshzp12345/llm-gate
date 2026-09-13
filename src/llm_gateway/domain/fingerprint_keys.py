"""Immutable Fingerprint key declarations; no material or source access."""

from dataclasses import dataclass, field


FINGERPRINT_RING_PROFILE = "gateway.fingerprint-key-ring/v1"


@dataclass(frozen=True)
class FingerprintKeyMember:
    key_id: str
    key_version: str
    role: str
    secret_ref: str = field(repr=False)

    def __post_init__(self):
        if any(type(value) is not str or not value for value in
               (self.key_id, self.key_version, self.secret_ref)):
            raise ValueError("Invalid Fingerprint key member")
        if self.role not in ("active", "verification_only"):
            raise ValueError("Invalid Fingerprint key role")

    @classmethod
    def from_mapping(cls, value):
        if type(value) is not dict or set(value) != {"key_id", "key_version", "role", "secret_ref"}:
            raise ValueError("Invalid Fingerprint key member")
        return cls(**value)

    @property
    def identity(self) -> tuple[str, str]:
        return self.key_id, self.key_version


@dataclass(frozen=True)
class FingerprintKeyRing:
    members: tuple[FingerprintKeyMember, ...]
    profile: str = FINGERPRINT_RING_PROFILE

    def __post_init__(self):
        if self.profile != FINGERPRINT_RING_PROFILE:
            raise ValueError("Invalid Fingerprint key ring profile")
        if type(self.members) is not tuple or not 1 <= len(self.members) <= 9:
            raise ValueError("Invalid Fingerprint key ring members")
        if any(type(member) is not FingerprintKeyMember for member in self.members):
            raise ValueError("Invalid Fingerprint key ring members")
        if sum(member.role == "active" for member in self.members) != 1:
            raise ValueError("Fingerprint key ring requires one active member")
        if len({member.identity for member in self.members}) != len(self.members):
            raise ValueError("Duplicate Fingerprint key identity")
        if len({member.secret_ref for member in self.members}) != len(self.members):
            raise ValueError("Duplicate Fingerprint key reference")

    @property
    def active(self) -> FingerprintKeyMember:
        return next(member for member in self.members if member.role == "active")

    def member(self, key_id: str, key_version: str) -> FingerprintKeyMember:
        for member in self.members:
            if member.identity == (key_id, key_version):
                return member
        raise ValueError("Fingerprint key identity unavailable")
