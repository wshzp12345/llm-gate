"""Extract published Provider limits from a validated locked Snapshot."""

import json

from llm_gateway.application.active_configuration import LoadedConfiguration
from llm_gateway.application.attempt_capacity import AttemptCapacityLimits


def project_attempt_capacity(snapshot: LoadedConfiguration, binding_ids: tuple[str, ...]) -> dict[str, AttemptCapacityLimits]:
    if len(set(binding_ids)) != len(binding_ids):
        raise ValueError("Duplicate capacity Binding")
    content = json.loads(snapshot.snapshot_json)
    result = {}
    for binding_id in binding_ids:
        provider = content["provider_model_bindings"][binding_id]["provider"]
        ceiling = content["providers"][provider]["rate_limit"]["max_concurrency"]
        result[binding_id] = AttemptCapacityLimits(binding_id, provider, ceiling)
    return result
