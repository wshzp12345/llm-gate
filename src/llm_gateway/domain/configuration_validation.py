"""Network-free relationship and capability checks on canonical domain resources."""

import json
from collections.abc import Mapping
from dataclasses import dataclass

from llm_gateway.domain.configuration import ConfigurationDiagnostic
from llm_gateway.domain.configuration_changes import REGISTRIES, SnapshotResources


@dataclass(frozen=True)
class AdapterCapabilities:
    streaming: bool
    tool_calling: bool
    structured_output: str


def validate_relationships(
    snapshot: SnapshotResources,
    contracts: Mapping[tuple[str, str], AdapterCapabilities],
) -> tuple[ConfigurationDiagnostic, ...]:
    """Structural validity is a prerequisite; this is not the full validator."""
    registries = {section: {} for section in REGISTRIES}
    for target, value in snapshot.values.items():
        if target.section in registries:
            registries[target.section][target.resource_id] = json.loads(value)
    errors = []

    def report(reason, section, resource_id, field):
        errors.append(ConfigurationDiagnostic(reason, f"/bundle/{section}/{resource_id}/{field}"))

    def reference(section, resource_id, field, destination, value):
        if value not in registries[destination]:
            report("reference_not_found", section, resource_id, field)
            return None
        return registries[destination][value]

    providers = registries["providers"]
    for resource_id, provider in providers.items():
        adapter = provider["adapter"]
        if (adapter["type"], adapter["version"]) not in contracts:
            report("adapter_incompatible", "providers", resource_id, "adapter")

    for resource_id, binding in registries["provider_model_bindings"].items():
        section = "provider_model_bindings"
        provider = reference(section, resource_id, "provider", "providers", binding["provider"])
        reference(section, resource_id, "pricing_table", "pricing_tables", binding["pricing_table"])
        if binding["limits"]["context_tokens"] < binding["limits"]["max_output_tokens"]:
            report("capability_invalid", section, resource_id, "limits")
        if provider is not None:
            adapter = provider["adapter"]
            ceiling = contracts.get((adapter["type"], adapter["version"]))
            if ceiling is not None:
                claim = binding["capabilities"]
                levels = ("none", "json_object", "json_schema")
                if ((claim["streaming"] and not ceiling.streaming)
                        or (claim["tool_calling"] and not ceiling.tool_calling)
                        or levels.index(claim["structured_output"]) > levels.index(ceiling.structured_output)):
                    report("capability_invalid", section, resource_id, "capabilities")

    for resource_id, policy in registries["routing_policies"].items():
        retry = policy["retry"]
        if retry["max_attempts_per_candidate"] > retry["max_attempts"] or retry["max_delay_ms"] < retry["base_delay_ms"]:
            report("routing_invalid", "routing_policies", resource_id, "retry")

    for resource_id, alias in registries["model_aliases"].items():
        section = "model_aliases"
        policy = reference(section, resource_id, "routing_policy", "routing_policies", alias["routing_policy"])
        reference(section, resource_id, "safety_policy", "safety_policies", alias["safety_policy"])
        full = [candidate for candidate in alias["candidates"] if candidate["service_level"] == "full" and candidate["weight"] > 0]
        if not full:
            report("routing_invalid", section, resource_id, "candidates")
        complete_references = True
        for index, candidate in enumerate(alias["candidates"]):
            if reference(section, resource_id, f"candidates/{index}/binding", "provider_model_bindings", candidate["binding"]) is None:
                complete_references = False
        if complete_references and full and not any(
            registries["provider_model_bindings"][candidate["binding"]]["limits"]["max_output_tokens"] >= alias["generation_defaults"]["max_output_tokens"]
            for candidate in full
        ):
            report("capability_invalid", section, resource_id, "generation_defaults/max_output_tokens")
        if policy and policy["degradation"]["reduced_service"]["enabled"] and not any(
            candidate["service_level"] == "reduced" and candidate["weight"] > 0 for candidate in alias["candidates"]
        ):
            report("routing_invalid", section, resource_id, "candidates")
    return tuple(sorted(errors, key=lambda item: ((item.path or "").encode(), item.reason.encode())))
