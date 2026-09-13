"""Immutable configuration-command values; HTTP and PostgreSQL remain outside."""

import re
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from uuid import UUID


def revision_number(value: str) -> int:
    if not isinstance(value, str) or not re.fullmatch(r"[1-9][0-9]{0,18}", value):
        raise ValueError("Invalid Revision")
    result = int(value)
    if result > 9223372036854775807:
        raise ValueError("Revision out of range")
    return result


class RevisionState(StrEnum):
    CANDIDATE = "candidate"
    ACTIVE = "active"
    SUPERSEDED = "superseded"
    STALE = "stale"


@dataclass(frozen=True)
class Revision:
    revision: str
    state: RevisionState
    base_revision: str | None
    snapshot_digest: str
    created_at: datetime
    rollback_of: str | None = None


@dataclass(frozen=True)
class RevisionExport:
    revision: str
    snapshot_digest: str
    snapshot_json: bytes
    creation_description: str | None


@dataclass(frozen=True)
class CommandIdentity:
    tenant_id: str
    subject: str
    operation: str
    command_id: str
    digest: str

    def __post_init__(self) -> None:
        parsed = UUID(self.command_id)
        if parsed.version != 4 or str(parsed) != self.command_id:
            raise ValueError("Command ID must be canonical UUIDv4")
        if self.operation not in {"create", "publish", "rebase", "rollback"}:
            raise ValueError("Invalid command operation")
        if not self.tenant_id or not self.subject:
            raise ValueError("Authorization identity required")
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", self.digest):
            raise ValueError("Invalid command digest")


@dataclass(frozen=True)
class PreparedCandidate:
    """Output of full application validation, never an ingress DTO.

    This does not replace domain/reference validation. No public endpoint may
    construct it directly from caller input.
    """

    base_revision: str | None
    snapshot_json: bytes
    snapshot_digest: str
    change_set_json: bytes
    description: str | None = None
    rollback_of: str | None = None


@dataclass(frozen=True)
class ConfigurationDiagnostic:
    reason: str
    path: str | None

    def __post_init__(self) -> None:
        if self.reason not in {
            "invalid_structure", "reference_not_found", "reference_cycle", "target_exists",
            "target_missing", "resource_changed", "active_revision_mismatch", "invalid_revision_state",
            "command_reused", "adapter_incompatible", "capability_invalid", "routing_invalid",
            "pricing_invalid", "safety_invalid", "resource_policy_invalid", "secret_reference_invalid",
            "trust_bundle_invalid",
        }:
            raise ValueError("Unknown configuration diagnostic")
        if self.path is not None and (not isinstance(self.path, str) or (self.path and not self.path.startswith("/"))):
            raise ValueError("Invalid diagnostic path")


class ConfigurationValidationFailed(Exception):
    def __init__(self, diagnostics: tuple[ConfigurationDiagnostic, ...], truncated: bool = False) -> None:
        if not diagnostics or len(diagnostics) > 100:
            raise ValueError("Bounded non-empty validation diagnostics required")
        self.diagnostics = diagnostics
        self.truncated = truncated
        super().__init__("Configuration validation failed")


class ConfigurationChangeConflict(ConfigurationValidationFailed):
    """Deterministic target precondition failure, retained as a 409 outcome."""


@dataclass(frozen=True)
class CommandOutcome:
    revision: Revision | None = None
    error: str | None = None
    diagnostics: tuple[ConfigurationDiagnostic, ...] = ()
    diagnostics_truncated: bool = False

    def __post_init__(self) -> None:
        if (self.revision is None) == (self.error is None):
            raise ValueError("Outcome must have exactly one result or error")
        if len(self.diagnostics) > 100 or (self.revision is not None and self.diagnostics):
            raise ValueError("Invalid outcome diagnostics")


@dataclass(frozen=True)
class IndexedOutcome:
    digest: str
    outcome: CommandOutcome


class ConfigurationPersistenceUnavailable(Exception):
    def __init__(self) -> None:
        super().__init__("Configuration persistence unavailable")


class ConfigurationDeadlineExceeded(Exception):
    def __init__(self) -> None:
        super().__init__("Configuration command deadline exceeded")
