"""Bind locked configuration to lazy, revision-fenced Provider client access."""

from functools import partial
import json

from llm_gateway.adapters.credentialed_runtime import CredentialBinding
from llm_gateway.adapters.provider_transport_registry import project_provider_transports


def project_transport_credentials(snapshot, binding_ids, registry):
    if len(set(binding_ids)) != len(binding_ids):
        raise ValueError("Duplicate credential Binding")
    content = json.loads(snapshot.snapshot_json)
    plans = project_provider_transports(snapshot)
    result = {}
    for binding_id in binding_ids:
        provider_id = content["provider_model_bindings"][binding_id]["provider"]
        provider = content["providers"][provider_id]
        result[binding_id] = CredentialBinding(provider["credential"]["secret_ref"],
            provider["endpoint"]["base_url"], partial(registry.acquire, plans[provider_id]),
            provider["adapter"]["type"], provider["adapter"]["version"])
    return result
