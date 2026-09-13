import json

from llm_gateway.adapters.canonical_json import canonical_bytes, canonical_digest
from llm_gateway.adapters.configuration_dto import Bundle
from llm_gateway.adapters.trust_bundle import canonical_pem
from llm_gateway.application.configuration_preparation import BundleDraft
from llm_gateway.domain.configuration_changes import REGISTRIES, SINGLETONS, SnapshotResources, Target


class CanonicalConfigurationCodec:
    def resources(self, snapshot_json):
        snapshot = json.loads(snapshot_json)
        result = {}
        for section in REGISTRIES:
            for resource_id, value in snapshot[section].items():
                result[Target(section, resource_id)] = canonical_bytes(value)
        for section in SINGLETONS:
            result[Target(section)] = canonical_bytes(snapshot[section])
        return SnapshotResources(result)

    def change_set(self, base_revision, operations):
        result = []
        for operation in operations:
            item = {"op": operation.op, "section": operation.target.section}
            if operation.target.resource_id is not None:
                item["resource_id"] = operation.target.resource_id
            if operation.value is not None:
                item["value"] = json.loads(operation.value)
            result.append(item)
        return canonical_bytes({"schema_version": "gateway.config-change-set/v1", "base_revision": base_revision, "operations": result})


def bundle_draft(bundle: Bundle) -> BundleDraft:
    snapshot = bundle.model_dump(exclude={"base_revision", "metadata"})
    # Diagnostics refer to submitted array indices, not canonical array order.
    validation_resources = CanonicalConfigurationCodec().resources(canonical_bytes(snapshot))
    # Order-independent sets have one canonical order before Snapshot identity.
    for provider in snapshot["providers"].values():
        for field in ("allowed_hosts", "allowed_networks"):
            provider["egress"][field].sort(key=lambda value: value.encode("utf-8"))
        trust = provider["transport"]["tls"]["trust_bundle"]
        if trust is not None:
            trust["pem"] = canonical_pem(trust["pem"])
            if trust["label"] is None:
                del trust["label"]
    for alias in snapshot["model_aliases"].values():
        alias["candidates"].sort(key=lambda candidate: candidate["binding"].encode("utf-8"))
    replay = snapshot["replay_policies"]
    if "eligible_results" in replay:
        replay["eligible_results"].sort()
    body = canonical_bytes(snapshot)
    return BundleDraft(
        None if bundle.base_revision == "0" else bundle.base_revision,
        body, canonical_digest(snapshot), CanonicalConfigurationCodec().resources(body),
        validation_resources,
        bundle.metadata.description if bundle.metadata is not None else None,
    )
