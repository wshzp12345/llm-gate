"""Explicit dev-only versioned credential records, not a Bootstrap loader.

    A record is a closed UTF-8 JSON object: secret_version (canonical UUID4),
    value (ASCII bearer token), valid_until (UTC ISO timestamp), revoked (bool).
    Publish mounted records by atomic replacement, never in-place editing.
    Version identities are supplied by the operator, never derived from a key.
    This local synchronous reader has no cache, retry, dotenv lookup or watcher.
"""

import json
import os
from collections.abc import Callable, Mapping
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

from llm_gateway.adapters.provider_credentials import (
    CredentialMetadata, CredentialUnavailable, ProviderCredentialLease,
)


def _unique(pairs):
    result = {}
    for name, value in pairs:
        if name in result:
            raise ValueError()
        result[name] = value
    return result


class DevelopmentSecretSource:
    def __init__(self, *, startup_profile: str, files: Mapping[str, Path],
                 environment_names: Mapping[str, str] | None = None,
                 enable_environment: bool = False,
                 environ: Mapping[str, str] | None = None,
                 now: Callable[[], datetime] = lambda: datetime.now(timezone.utc)):
        names = dict(environment_names or {})
        if (startup_profile != "dev" or (names and enable_environment is not True)
                or set(files) & set(names)):
            raise ValueError("Invalid development secret source configuration")
        if any(not isinstance(path, Path) or not path.is_absolute() for path in files.values()):
            raise ValueError("Absolute mounted secret paths required")
        if any(not isinstance(name, str) or not name or "=" in name or "\x00" in name for name in names.values()):
            raise ValueError("Invalid environment source name")
        self._files = dict(files)
        self._names = names
        self._environ = os.environ if environ is None else environ
        self._now = now

    def resolve(self, secret_ref: str) -> ProviderCredentialLease:
        # Keep sensitive parsing/I/O exceptions out of the public traceback.
        try:
            return self._resolve(secret_ref)
        except (OSError, ValueError, TypeError, KeyError, RecursionError):
            pass
        raise CredentialUnavailable()

    def _resolve(self, secret_ref: str) -> ProviderCredentialLease:
        if secret_ref in self._files:
            with self._files[secret_ref].open("rb") as stream:
                raw = stream.read(16385)
            source = "mounted_file"
        elif secret_ref in self._names:
            value = self._environ[self._names[secret_ref]]
            if not isinstance(value, str) or len(value) > 16384:
                raise ValueError()
            raw = value.encode("utf-8")
            source = "environment"
        else:
            raise KeyError()
        if len(raw) > 16384:
            raise ValueError()
        record = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique)
        if not isinstance(record, dict) or set(record) != {"secret_version", "value", "valid_until", "revoked"}:
            raise ValueError()
        version, value, expiry = (record[name] for name in ("secret_version", "value", "valid_until"))
        if not all(isinstance(item, str) for item in (version, value, expiry)):
            raise ValueError()
        parsed_version = UUID(version)
        if parsed_version.version != 4 or str(parsed_version) != version:
            raise ValueError()
        if not value or len(value) > 8192 or any(not 33 <= ord(char) <= 126 for char in value):
            raise ValueError()
        valid_until = datetime.fromisoformat(expiry)
        now = self._now()
        if (record["revoked"] is not False or valid_until.tzinfo is None
                or valid_until.utcoffset() != timezone.utc.utcoffset(None)
                or now.tzinfo is None or now.utcoffset() != timezone.utc.utcoffset(None)
                or valid_until <= now):
            raise ValueError()
        return ProviderCredentialLease(
            CredentialMetadata(secret_ref, version, source, valid_until), value.encode("ascii"),
        )
