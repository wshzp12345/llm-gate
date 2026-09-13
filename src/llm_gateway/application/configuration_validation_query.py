from dataclasses import dataclass

from llm_gateway.domain.configuration import ConfigurationDiagnostic, ConfigurationValidationFailed, PreparedCandidate


@dataclass(frozen=True)
class ValidationOutcome:
    base_revision: str | None
    prepared: PreparedCandidate | None = None
    diagnostics: tuple[ConfigurationDiagnostic, ...] = ()
    diagnostics_truncated: bool = False


async def validate_candidate(unit_of_work, base_revision, preparer, timeout_seconds):
    """Dry run: no command index, Revision, publication or audit mutation."""
    async with unit_of_work.transaction(timeout_seconds) as transaction:
        active = await transaction.lock_active()
        if active != base_revision:
            return ValidationOutcome(base_revision, diagnostics=(ConfigurationDiagnostic("active_revision_mismatch", None),))
        try:
            prepared = await preparer.prepare(transaction)
        except ConfigurationValidationFailed as failure:
            return ValidationOutcome(base_revision, diagnostics=failure.diagnostics, diagnostics_truncated=failure.truncated)
        return ValidationOutcome(base_revision, prepared)
