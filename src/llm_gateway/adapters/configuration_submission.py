"""Typed Submission-to-application bridge; no HTTP or database side effects."""

import json
from pydantic import ValidationError

from llm_gateway.adapters.canonical_json import canonical_bytes
from llm_gateway.adapters.configuration_dto import Bundle, BundleSubmission, ChangeSetSubmission
from llm_gateway.adapters.configuration_mapping import CanonicalConfigurationCodec, bundle_draft
from llm_gateway.application.configuration_preparation import BundlePreparer
from llm_gateway.domain.configuration import ConfigurationChangeConflict, ConfigurationDiagnostic, ConfigurationValidationFailed
from llm_gateway.domain.configuration_changes import (
    REGISTRIES, SINGLETONS, ChangeConflict, Operation, SnapshotResources, Target, apply_changes, rebase_changes,
)


def _omit_absent_labels(provider):
    trust = provider["transport"]["tls"]["trust_bundle"]
    if trust is not None and trust.get("label") is None:
        trust.pop("label", None)


def submission_semantic(submission):
    """Materialized, closed semantic command value shared by JSON and YAML."""
    data = submission.model_dump()
    payload = data[submission.kind]
    if payload.get("metadata") is None:
        payload.pop("metadata", None)
    if submission.kind == "bundle":
        for provider in payload["providers"].values():
            _omit_absent_labels(provider)
    else:
        for operation in payload["operations"]:
            if operation["resource_id"] is None:
                del operation["resource_id"]
            if operation["op"] == "remove":
                del operation["value"]
            elif operation["section"] == "providers":
                _omit_absent_labels(operation["value"])
    return data


def submission_base(submission):
    value = getattr(submission, submission.kind).base_revision
    return None if value == "0" else value


class ChangeSetPreparer:
    def __init__(self, submission: ChangeSetSubmission, validator):
        self._submission = submission
        self._validator = validator

    async def prepare(self, transaction):
        payload = submission_semantic(self._submission)["change_set"]
        base_revision = submission_base(self._submission)
        codec = CanonicalConfigurationCodec()
        original = SnapshotResources({}) if base_revision is None else codec.resources(await transaction.get_snapshot(base_revision))
        operations, indices = [], {}
        for index, item in enumerate(payload["operations"]):
            target = Target(item["section"], item.get("resource_id"))
            operations.append(Operation(item["op"], target, canonical_bytes(item["value"]) if "value" in item else None))
            indices[target] = index
        try:
            provisional, _ = apply_changes(original, tuple(operations))
        except ChangeConflict as failure:
            raise ConfigurationChangeConflict((ConfigurationDiagnostic(failure.reason, f"/change_set/operations/{indices[failure.target]}"),)) from None
        bundle = {"schema_version": "gateway.config/v1", "base_revision": base_revision or "0"}
        bundle.update({section: {} for section in REGISTRIES})
        for target, value in provisional.values.items():
            if target.section in SINGLETONS:
                bundle[target.section] = json.loads(value)
            else:
                bundle[target.section][target.resource_id] = json.loads(value)
        if "metadata" in payload:
            bundle["metadata"] = payload["metadata"]
        # Provisional resources were typed at ingress; cross-resource checks run
        # only after every operation is applied, never on an intermediate graph.
        try:
            draft = bundle_draft(Bundle.model_validate(bundle))
        except ValidationError:
            raise ConfigurationValidationFailed((ConfigurationDiagnostic("invalid_structure", None),)) from None
        validator = self._validator

        class MappedValidator:
            async def validate(self, candidate):
                mapped = []
                for diagnostic in await validator.validate(candidate):
                    path = None
                    pieces = diagnostic.path.split("/") if diagnostic.path else []
                    if len(pieces) >= 3 and pieces[1] == "bundle":
                        section = pieces[2]
                        resource = pieces[3] if section in REGISTRIES and len(pieces) > 3 else None
                        if section in (*REGISTRIES, *SINGLETONS):
                            target = Target(section, resource)
                            if target in indices:
                                suffix = pieces[4:] if resource is not None else pieces[3:]
                                index = indices[target]
                                path = f"/change_set/operations/{index}/value" + ("/" + "/".join(suffix) if suffix else "")
                    mapped.append(ConfigurationDiagnostic(diagnostic.reason, path))
                mapped.sort(key=lambda entry: (entry.path is None, (entry.path or "").encode(), entry.reason.encode()))
                return tuple(mapped)

        return await BundlePreparer(draft, MappedValidator(), codec).prepare(transaction)


def submission_preparer(submission, validator):
    if isinstance(submission, BundleSubmission):
        return BundlePreparer(bundle_draft(submission.bundle), validator, CanonicalConfigurationCodec())
    return ChangeSetPreparer(submission, validator)


class RollbackPreparer:
    def __init__(self, validator):
        self._validator = validator

    async def prepare(self, transaction, source, base_revision, description):
        bundle = json.loads(source.snapshot_json)
        bundle.update(base_revision=base_revision, metadata={"description": description})
        candidate = bundle_draft(Bundle.model_validate(bundle))
        return await BundlePreparer(candidate, self._validator, CanonicalConfigurationCodec()).prepare(transaction)


class RebasePreparer:
    def __init__(self, validator):
        self._validator = validator

    async def prepare(self, transaction, source, base_revision, description):
        codec = CanonicalConfigurationCodec()
        original = SnapshotResources({}) if source.base_revision is None else codec.resources(await transaction.get_snapshot(source.base_revision))
        intended = codec.resources(await transaction.get_snapshot(source.revision))
        current = codec.resources(await transaction.get_snapshot(base_revision))
        stored = json.loads(await transaction.get_change_set(source.revision))
        operations = tuple(Operation(item["op"], Target(item["section"], item.get("resource_id")),
                                     canonical_bytes(item["value"]) if "value" in item else None)
                           for item in stored["operations"])
        try:
            provisional, _ = rebase_changes(original, intended, current, operations)
        except ChangeConflict as failure:
            raise ConfigurationChangeConflict((ConfigurationDiagnostic(failure.reason, None),)) from None
        bundle = {"schema_version": "gateway.config/v1", "base_revision": base_revision,
                  "metadata": {"description": description}, **{section: {} for section in REGISTRIES}}
        for target, value in provisional.values.items():
            if target.section in SINGLETONS:
                bundle[target.section] = json.loads(value)
            else:
                bundle[target.section][target.resource_id] = json.loads(value)
        try:
            candidate = bundle_draft(Bundle.model_validate(bundle))
        except ValidationError:
            raise ConfigurationValidationFailed((ConfigurationDiagnostic("invalid_structure", None),)) from None
        validator = self._validator

        class RebaseValidator:
            async def validate(self, draft):
                # Rebase request has no Bundle/operations body to point into.
                return tuple(ConfigurationDiagnostic(item.reason, None) for item in await validator.validate(draft))

        return await BundlePreparer(candidate, RebaseValidator(), codec).prepare(transaction)
