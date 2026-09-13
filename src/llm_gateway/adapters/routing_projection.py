"""Project an already validated immutable Snapshot into static routing inputs."""

import json
from dataclasses import dataclass

from llm_gateway.application.active_configuration import LoadedConfiguration
from llm_gateway.domain.configuration_validation import AdapterCapabilities
from llm_gateway.domain.routing_eligibility import StaticCandidate
from llm_gateway.domain.routing_order import WeightedCandidate


class UnknownLogicalModel(Exception):
    def __init__(self):
        super().__init__("Logical model is not configured")


@dataclass(frozen=True)
class AliasRoutingProjection:
    requested_model: str
    configuration_revision: str
    routing_policy: str
    reduced_enabled: bool
    candidates: tuple[StaticCandidate, ...]


def project_alias(snapshot: LoadedConfiguration, requested_model: str) -> AliasRoutingProjection:
    content = json.loads(snapshot.snapshot_json)
    alias = content["model_aliases"].get(requested_model)
    if alias is None:
        raise UnknownLogicalModel()
    policy = content["routing_policies"][alias["routing_policy"]]
    candidates = []
    for item in alias["candidates"]:
        binding = content["provider_model_bindings"][item["binding"]]
        provider = content["providers"][binding["provider"]]
        candidates.append(StaticCandidate(
            WeightedCandidate(item["binding"], item["service_level"], item["priority"], item["weight"]),
            binding["provider"], binding["upstream_model"], provider["status"] == "enabled",
            binding["status"] == "enabled", AdapterCapabilities(**binding["capabilities"]),
            binding["limits"]["max_output_tokens"],
        ))
    return AliasRoutingProjection(requested_model, snapshot.revision, alias["routing_policy"],
                                  policy["degradation"]["reduced_service"]["enabled"], tuple(candidates))
