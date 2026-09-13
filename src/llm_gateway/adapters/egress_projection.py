"""Egress policy comes only from the validated immutable Provider Snapshot."""

import json

from llm_gateway.domain.provider_egress import ProviderEgressPolicy


def project_provider_egress(snapshot, provider_id):
    policy = json.loads(snapshot.snapshot_json)["providers"][provider_id]["egress"]
    if policy["proxy"] != "disabled":
        raise ValueError("Provider proxies are not supported")
    return ProviderEgressPolicy(tuple(policy["allowed_hosts"]), tuple(policy["allowed_networks"]))
