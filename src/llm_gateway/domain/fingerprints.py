"""Non-content keyed identity suitable for authorized durable evidence."""

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class FingerprintIdentity:
    canonicalization: str
    key_id: str
    key_version: str
    digest: str
    algorithm: str = "HMAC-SHA-256"

    def __post_init__(self):
        if self.algorithm != "HMAC-SHA-256":
            raise ValueError("Unsupported Fingerprint algorithm")
        if any(type(value) is not str or not value for value in
               (self.canonicalization, self.key_id, self.key_version)):
            raise ValueError("Invalid Fingerprint identity")
        if type(self.digest) is not str or not re.fullmatch(r"[0-9a-f]{64}", self.digest):
            raise ValueError("Invalid Fingerprint digest")
