"""Rehydrate persisted content without trusting its shape or stored digest."""

from pydantic import ValidationError

from llm_gateway.adapters.canonical_json import canonical_digest
from llm_gateway.adapters.configuration_dto import Bundle
from llm_gateway.adapters.configuration_json import ParseLimits, parse_strict_json, ConfigurationStructureError
from llm_gateway.adapters.configuration_mapping import bundle_draft
from llm_gateway.domain.configuration import ConfigurationPersistenceUnavailable, RevisionExport


class CanonicalSnapshotDecoder:
    def __init__(self, limits: ParseLimits):
        self._limits = limits

    def decode(self, stored: RevisionExport):
        try:
            content = parse_strict_json(stored.snapshot_json, self._limits)
            if not isinstance(content, dict) or "base_revision" in content or "metadata" in content:
                raise ValueError("Invalid snapshot envelope")
            if canonical_digest(content) != stored.snapshot_digest:
                raise ValueError("Snapshot digest mismatch")
            draft = bundle_draft(Bundle.model_validate({**content, "base_revision": stored.revision}))
            # Persisted content must already have materialized defaults and
            # canonical semantic ordering; loading must not silently upgrade it.
            if draft.snapshot_digest != stored.snapshot_digest:
                raise ValueError("Snapshot requires normalization")
            return draft
        except (ConfigurationStructureError, ValidationError, ValueError, TypeError, OverflowError):
            raise ConfigurationPersistenceUnavailable() from None
