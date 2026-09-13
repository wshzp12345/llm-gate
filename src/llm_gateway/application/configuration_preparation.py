from dataclasses import dataclass
from typing import Protocol

from llm_gateway.application.configuration import ConfigurationTransaction
from llm_gateway.domain.configuration import (
    ConfigurationDiagnostic, ConfigurationValidationFailed, PreparedCandidate,
)
from llm_gateway.domain.configuration_changes import SnapshotResources, Operation, derive_changes


@dataclass(frozen=True)
class BundleDraft:
    base_revision: str | None
    snapshot_json: bytes
    snapshot_digest: str
    resources: SnapshotResources
    validation_resources: SnapshotResources
    description: str | None


class ConfigurationValidationPort(Protocol):
    async def validate(self, draft: BundleDraft) -> tuple[ConfigurationDiagnostic, ...]:
        """Full semantic, deployment, Secret Reference and capability validation."""
        ...


class ConfigurationCodec(Protocol):
    def resources(self, snapshot_json: bytes) -> SnapshotResources: ...
    def change_set(self, base_revision: str | None, operations: tuple[Operation, ...]) -> bytes: ...


class BundlePreparer:
    def __init__(self, draft: BundleDraft, validator: ConfigurationValidationPort, codec: ConfigurationCodec):
        self._draft = draft
        self._validator = validator
        self._codec = codec

    async def prepare(self, transaction: ConfigurationTransaction) -> PreparedCandidate:
        # The caller has already locked and checked the Active base. A full
        # validator is mandatory; there is no default accept-all implementation.
        diagnostics = await self._validator.validate(self._draft)
        if diagnostics:
            raise ConfigurationValidationFailed(diagnostics[:100], len(diagnostics) > 100)
        base = SnapshotResources({})
        if self._draft.base_revision is not None:
            base = self._codec.resources(await transaction.get_snapshot(self._draft.base_revision))
        operations = derive_changes(base, self._draft.resources)
        return PreparedCandidate(
            self._draft.base_revision, self._draft.snapshot_json, self._draft.snapshot_digest,
            self._codec.change_set(self._draft.base_revision, operations), self._draft.description,
        )
