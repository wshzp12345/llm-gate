"""Atomic command effects for already-authorized, validated configuration.

Ingress parsing, complete validation/diff, and HTTP presentation are separate
pending components. The unit of work commits on normal exit, including cached
deterministic conflicts, and rolls back on exceptions or cancellation.
"""

from contextlib import AbstractAsyncContextManager
from dataclasses import replace
from typing import Protocol

from llm_gateway.domain.configuration import (
    CommandIdentity, CommandOutcome, IndexedOutcome, PreparedCandidate, Revision,
    RevisionState, ConfigurationValidationFailed, ConfigurationChangeConflict, RevisionExport,
)


class ConfigurationTransaction(Protocol):
    async def indexed_outcome(self, identity: CommandIdentity) -> IndexedOutcome | None: ...
    async def lock_active(self) -> str | None: ...
    async def get_revision(self, revision: str) -> Revision | None: ...
    async def get_snapshot(self, revision: str) -> bytes: ...
    async def get_change_set(self, revision: str) -> bytes: ...
    async def get_export(self, revision: str) -> RevisionExport | None: ...
    async def get_active_export(self) -> RevisionExport | None: ...
    async def insert_candidate(self, candidate: PreparedCandidate) -> Revision: ...
    async def activate(self, revision: str) -> Revision: ...
    async def record_outcome(self, identity: CommandIdentity, outcome: CommandOutcome, description: str | None) -> None: ...


class ConfigurationUnitOfWork(Protocol):
    def transaction(self, timeout_seconds: float) -> AbstractAsyncContextManager[ConfigurationTransaction]: ...


class CandidatePreparer(Protocol):
    async def prepare(self, transaction: ConfigurationTransaction) -> PreparedCandidate:
        """Validate/resolve against the locked base, derive Snapshot and Change Set."""
        ...


class RollbackPreparationPort(Protocol):
    async def prepare(self, transaction: ConfigurationTransaction, source: RevisionExport,
                      base_revision: str, description: str) -> PreparedCandidate: ...


class RebasePreparationPort(Protocol):
    async def prepare(self, transaction: ConfigurationTransaction, source: Revision,
                      base_revision: str, description: str) -> PreparedCandidate: ...


class ConfigurationCommands:
    def __init__(self, unit_of_work: ConfigurationUnitOfWork) -> None:
        self._uow = unit_of_work

    @staticmethod
    async def _replay(tx: ConfigurationTransaction, identity: CommandIdentity) -> CommandOutcome | None:
        indexed = await tx.indexed_outcome(identity)
        if indexed is None:
            return None
        if indexed.digest != identity.digest:
            return CommandOutcome(error="conflict")
        return indexed.outcome

    async def create(
        self, identity: CommandIdentity, base_revision: str | None,
        preparer: CandidatePreparer, *, timeout_seconds: float,
    ) -> CommandOutcome:
        if identity.operation != "create":
            raise ValueError("Wrong command operation")
        async with self._uow.transaction(timeout_seconds) as tx:
            replay = await self._replay(tx, identity)
            if replay is not None:
                return replay
            active = await tx.lock_active()
            description = None
            if base_revision != active:
                outcome = CommandOutcome(error="conflict")
            else:
                try:
                    candidate = await preparer.prepare(tx)
                except ConfigurationChangeConflict as failure:
                    outcome = CommandOutcome(error="conflict", diagnostics=failure.diagnostics,
                                             diagnostics_truncated=failure.truncated)
                except ConfigurationValidationFailed as failure:
                    outcome = CommandOutcome(error="configuration_invalid", diagnostics=failure.diagnostics,
                                             diagnostics_truncated=failure.truncated)
                else:
                    if candidate.base_revision != base_revision:
                        raise ValueError("Candidate preparation changed the locked base")
                    description = candidate.description
                    outcome = CommandOutcome(revision=await tx.insert_candidate(candidate))
            await tx.record_outcome(identity, outcome, description)
            return outcome

    async def publish(
        self, identity: CommandIdentity, revision: str, expected_active: str | None,
        candidate_digest: str, description: str, *, timeout_seconds: float,
    ) -> CommandOutcome:
        if identity.operation != "publish":
            raise ValueError("Wrong command operation")
        async with self._uow.transaction(timeout_seconds) as tx:
            replay = await self._replay(tx, identity)
            if replay is not None:
                return replay
            active = await tx.lock_active()
            candidate = await tx.get_revision(revision)
            if candidate is None:
                return CommandOutcome(error="not_found")
            if (active != expected_active or candidate.base_revision != active
                    or candidate.state != RevisionState.CANDIDATE
                    or candidate.snapshot_digest != candidate_digest):
                outcome = CommandOutcome(error="conflict")
            else:
                outcome = CommandOutcome(revision=await tx.activate(revision))
            await tx.record_outcome(identity, outcome, description)
            return outcome

    async def rebase(
        self, identity: CommandIdentity, revision: str, expected_active: str,
        description: str, preparer: RebasePreparationPort, *, timeout_seconds: float,
    ) -> CommandOutcome:
        if identity.operation != "rebase":
            raise ValueError("Wrong command operation")
        async with self._uow.transaction(timeout_seconds) as tx:
            replay = await self._replay(tx, identity)
            if replay is not None:
                return replay
            active = await tx.lock_active()
            source = await tx.get_revision(revision)
            if source is None:
                return CommandOutcome(error="not_found")
            if (active is None or expected_active != active or source.state != RevisionState.STALE
                    or source.base_revision == active):
                outcome = CommandOutcome(error="conflict")
            else:
                try:
                    candidate = await preparer.prepare(tx, source, active, description)
                except ConfigurationChangeConflict as failure:
                    outcome = CommandOutcome(error="conflict", diagnostics=failure.diagnostics,
                                             diagnostics_truncated=failure.truncated)
                except ConfigurationValidationFailed as failure:
                    outcome = CommandOutcome(error="configuration_invalid", diagnostics=failure.diagnostics,
                                             diagnostics_truncated=failure.truncated)
                else:
                    if candidate.base_revision != active:
                        raise ValueError("Rebase preparation changed the locked base")
                    outcome = CommandOutcome(revision=await tx.insert_candidate(candidate))
            await tx.record_outcome(identity, outcome, description)
            return outcome

    async def rollback(
        self, identity: CommandIdentity, revision: str, expected_active: str,
        description: str, preparer: RollbackPreparationPort, *, timeout_seconds: float,
    ) -> CommandOutcome:
        if identity.operation != "rollback":
            raise ValueError("Wrong command operation")
        async with self._uow.transaction(timeout_seconds) as tx:
            replay = await self._replay(tx, identity)
            if replay is not None:
                return replay
            active = await tx.lock_active()
            source = await tx.get_revision(revision)
            if source is None:
                return CommandOutcome(error="not_found")
            if active is None or expected_active != active or source.state != RevisionState.SUPERSEDED:
                outcome = CommandOutcome(error="conflict")
            else:
                exported = await tx.get_export(revision)
                if exported is None:
                    from llm_gateway.domain.configuration import ConfigurationPersistenceUnavailable
                    raise ConfigurationPersistenceUnavailable()
                try:
                    candidate = await preparer.prepare(tx, exported, active, description)
                except ConfigurationValidationFailed as failure:
                    outcome = CommandOutcome(error="configuration_invalid", diagnostics=failure.diagnostics,
                                             diagnostics_truncated=failure.truncated)
                else:
                    if candidate.base_revision != active or candidate.snapshot_digest != source.snapshot_digest:
                        raise ValueError("Rollback preparation changed source content or target base")
                    candidate = replace(candidate, rollback_of=source.revision, description=description)
                    outcome = CommandOutcome(revision=await tx.insert_candidate(candidate))
            await tx.record_outcome(identity, outcome, description)
            return outcome
