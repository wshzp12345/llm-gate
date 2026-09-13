"""Load one coherent, validated Active Snapshot without publishing or caching it."""

from dataclasses import dataclass
from typing import Protocol

from llm_gateway.application.configuration import ConfigurationUnitOfWork
from llm_gateway.application.configuration_preparation import BundleDraft, ConfigurationValidationPort
from llm_gateway.domain.configuration import RevisionExport, ConfigurationValidationFailed
from llm_gateway.domain.configuration_changes import SnapshotResources


class SnapshotDecoder(Protocol):
    def decode(self, stored: RevisionExport) -> BundleDraft: ...


@dataclass(frozen=True)
class LoadedConfiguration:
    revision: str
    snapshot_digest: str
    snapshot_json: bytes
    resources: SnapshotResources


class ActiveConfigurationLoader:
    def __init__(self, unit_of_work: ConfigurationUnitOfWork, decoder: SnapshotDecoder,
                 validator: ConfigurationValidationPort):
        self._uow, self._decoder, self._validator = unit_of_work, decoder, validator

    async def load(self, *, timeout_seconds: float) -> LoadedConfiguration | None:
        # The database pointer and immutable content are read in one statement.
        # Validation remains inside the operation deadline; no publication lock
        # is acquired, no last-known-good snapshot masks a failed fresh load.
        async with self._uow.transaction(timeout_seconds) as tx:
            stored = await tx.get_active_export()
            if stored is None:
                return None
            draft = self._decoder.decode(stored)
            diagnostics = await self._validator.validate(draft)
            if diagnostics:
                raise ConfigurationValidationFailed(diagnostics[:100], len(diagnostics) > 100)
            return LoadedConfiguration(stored.revision, draft.snapshot_digest,
                                       draft.snapshot_json, draft.resources)
