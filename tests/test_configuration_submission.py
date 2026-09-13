import asyncio
import json

import pytest

from llm_gateway.adapters.configuration_dto import SUBMISSION_DTO
from llm_gateway.adapters.configuration_submission import submission_preparer
from llm_gateway.domain.configuration import ConfigurationChangeConflict, ConfigurationValidationFailed
from tests.test_configuration_http import local_validator
from tests.test_configuration_preparation import draft


class Transaction:
    async def get_snapshot(self, revision):
        assert revision == "1"
        return draft().snapshot_json


def prepare(operations):
    submission = SUBMISSION_DTO.validate_python({"kind": "change_set", "change_set": {
        "schema_version": "gateway.config-change-set/v1", "base_revision": "1", "operations": operations,
    }})
    return asyncio.run(submission_preparer(submission, local_validator()).prepare(Transaction()))


def test_empty_changeset_keeps_snapshot_and_has_no_operations():
    result = prepare([])
    assert result.snapshot_digest == draft().snapshot_digest
    assert json.loads(result.change_set_json)["operations"] == []


def test_remove_referenced_resource_fails_whole_submission():
    with pytest.raises(ConfigurationValidationFailed) as failure:
        prepare([{"op": "remove", "section": "providers", "resource_id": "provider-a"}])
    assert failure.value.diagnostics[0].reason == "reference_not_found"
    assert failure.value.diagnostics[0].path is None  # Reference belongs to untouched Binding.


def test_target_precondition_conflict_is_distinct_from_invalid_graph():
    with pytest.raises(ConfigurationChangeConflict) as failure:
        prepare([{"op": "remove", "section": "providers", "resource_id": "missing"}])
    assert failure.value.diagnostics[0].reason == "target_missing"
    assert failure.value.diagnostics[0].path == "/change_set/operations/0"


def test_changeset_diagnostic_points_to_original_operation_value():
    snapshot = json.loads(draft().snapshot_json)
    binding = snapshot["provider_model_bindings"]["binding-a"]
    binding["provider"] = "missing"
    with pytest.raises(ConfigurationValidationFailed) as failure:
        prepare([{"op": "replace", "section": "provider_model_bindings", "resource_id": "binding-a", "value": binding}])
    assert failure.value.diagnostics[0].path == "/change_set/operations/0/value/provider"


def test_coordinated_resource_rename_validates_final_graph_only():
    snapshot = json.loads(draft().snapshot_json)
    binding = snapshot["provider_model_bindings"]["binding-a"]
    binding["provider"] = "provider-b"
    result = prepare([
        {"op": "remove", "section": "providers", "resource_id": "provider-a"},
        {"op": "add", "section": "providers", "resource_id": "provider-b", "value": snapshot["providers"]["provider-a"]},
        {"op": "replace", "section": "provider_model_bindings", "resource_id": "binding-a", "value": binding},
    ])
    value = json.loads(result.snapshot_json)
    assert "provider-a" not in value["providers"]
    assert value["provider_model_bindings"]["binding-a"]["provider"] == "provider-b"


def test_initial_empty_changeset_is_semantically_invalid():
    submission = SUBMISSION_DTO.validate_python({"kind": "change_set", "change_set": {
        "schema_version": "gateway.config-change-set/v1", "base_revision": "0", "operations": [],
    }})
    with pytest.raises(ConfigurationValidationFailed):
        asyncio.run(submission_preparer(submission, local_validator()).prepare(Transaction()))
