"""Provider QPS is explicit locked configuration, never model-derived."""

import json

from llm_gateway.adapters.capacity_projection import project_attempt_capacity
from llm_gateway.application.provider_rate import ProviderRateLimits


def project_provider_rate(snapshot, binding_ids):
    capacities = project_attempt_capacity(snapshot, binding_ids)
    content = json.loads(snapshot.snapshot_json)
    result = {}
    for binding, capacity in capacities.items():
        policy = content["providers"][capacity.provider]["rate_limit"]
        result[binding] = ProviderRateLimits(capacity, policy["qps"], policy["burst"])
    return result
