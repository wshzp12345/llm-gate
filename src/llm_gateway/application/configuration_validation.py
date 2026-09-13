"""Local-only configuration validation, with explicit deployment dependencies."""

import ipaddress
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol
from types import MappingProxyType
from urllib.parse import urlsplit

from llm_gateway.application.configuration_preparation import BundleDraft
from llm_gateway.domain.configuration import ConfigurationDiagnostic
from llm_gateway.domain.configuration_validation import AdapterCapabilities, validate_relationships


class TrustBundleValidationPort(Protocol):
    async def valid(self, identity: str, canonical_pem: str) -> bool:
        """Validate local certificate content/identity/expiry without network I/O."""
        ...


@dataclass(frozen=True)
class ConfigurationDeploymentPolicy:
    production: bool
    resource_ceilings: Mapping[str, int]
    routing_evidence_max_ttl_seconds: int
    replay_max_ttl_seconds: int
    replay_max_artifact_bytes: int
    known_secret_references: frozenset[str]
    currency_codes: frozenset[str]
    adapter_contracts: Mapping[tuple[str, str], AdapterCapabilities]

    def __post_init__(self):
        required = {"max_request_bytes", "max_messages", "max_content_item_bytes", "max_output_schema_bytes", "max_sse_event_bytes", "max_invocation_seconds"}
        if set(self.resource_ceilings) != required or any(type(value) is not int or value < 1 for value in self.resource_ceilings.values()):
            raise ValueError("Complete positive resource ceilings required")
        if not 86400 <= self.routing_evidence_max_ttl_seconds <= 31536000:
            raise ValueError("Invalid routing Evidence ceiling")
        if self.replay_max_ttl_seconds < 1 or self.replay_max_artifact_bytes < 1:
            raise ValueError("Positive replay ceilings required")
        object.__setattr__(self, "resource_ceilings", MappingProxyType(dict(self.resource_ceilings)))
        object.__setattr__(self, "adapter_contracts", MappingProxyType(dict(self.adapter_contracts)))


def _hostname(value: str) -> bool:
    if len(value) > 253 or value != value.lower():
        return False
    try:
        ipaddress.ip_address(value)
        return False
    except ValueError:
        return all(re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", label) for label in value.split("."))


class LocalConfigurationValidator:
    def __init__(self, policy: ConfigurationDeploymentPolicy, trust_bundles: TrustBundleValidationPort):
        self._policy = policy
        self._trust_bundles = trust_bundles

    async def validate(self, draft: BundleDraft) -> tuple[ConfigurationDiagnostic, ...]:
        errors = list(validate_relationships(draft.validation_resources, self._policy.adapter_contracts))
        snapshot = json.loads(draft.snapshot_json)

        def error(reason, path):
            errors.append(ConfigurationDiagnostic(reason, "/bundle/" + path))

        def secret(value, path):
            # Only identities in the deployment's SecretSource catalog are legal.
            # This never reads material or treats an arbitrary string as a key.
            if value not in self._policy.known_secret_references:
                error("secret_reference_invalid", path)

        for resource_id, provider in snapshot["providers"].items():
            path = f"providers/{resource_id}/"
            secret(provider["credential"]["secret_ref"], path + "credential/secret_ref")
            egress = provider["egress"]
            hosts = egress["allowed_hosts"]
            if not all(_hostname(host) for host in hosts):
                error("invalid_structure", path + "egress/allowed_hosts")
            for network in egress["allowed_networks"]:
                if network != "public":
                    try:
                        if str(ipaddress.ip_network(network, strict=True)) != network:
                            raise ValueError("Noncanonical CIDR")
                    except ValueError:
                        error("invalid_structure", path + "egress/allowed_networks")
                        break
            endpoint = provider["endpoint"]["base_url"]
            try:
                url = urlsplit(endpoint)
                if (url.scheme not in ({"https"} if self._policy.production else {"https", "http"})
                        or not url.hostname or url.hostname not in hosts or url.username is not None
                        or url.password is not None or "?" in endpoint or "#" in endpoint
                        or endpoint.endswith("/") or any(ord(char) <= 32 or ord(char) == 127 for char in endpoint)
                        or "\\" in endpoint or (url.port is not None and not 1 <= url.port <= 65535)):
                    raise ValueError("Invalid endpoint")
            except ValueError:
                error("invalid_structure", path + "endpoint/base_url")
            probe = provider["health"]["active_probe"]
            if probe is not None:
                probe_path = probe["path"]
                if (not probe_path.startswith("/") or probe_path.startswith("//") or "\\" in probe_path
                        or "#" in probe_path or any(ord(char) <= 32 for char in probe_path)
                        or probe["timeout_seconds"] >= probe["interval_seconds"]):
                    error("invalid_structure", path + "health/active_probe")
            bundle = provider["transport"]["tls"]["trust_bundle"]
            if bundle is not None and not await self._trust_bundles.valid(bundle["identity"], bundle["pem"]):
                error("trust_bundle_invalid", path + "transport/tls/trust_bundle")

        for resource_id, pricing in snapshot["pricing_tables"].items():
            path = f"pricing_tables/{resource_id}/"
            if pricing["currency"] not in self._policy.currency_codes:
                error("pricing_invalid", path + "currency")
            timestamp = pricing["effective_from"]
            try:
                if not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|\+00:00)", timestamp):
                    raise ValueError("UTC RFC3339 required")
                datetime.fromisoformat(timestamp)
            except ValueError:
                error("pricing_invalid", path + "effective_from")

        resources = snapshot["resource_policies"]
        for name, value in resources["limits"].items():
            if value != "inherit" and value > self._policy.resource_ceilings[name]:
                error("resource_policy_invalid", "resource_policies/limits/" + name)
        if resources["routing_evidence_ttl_seconds"] > self._policy.routing_evidence_max_ttl_seconds:
            error("resource_policy_invalid", "resource_policies/routing_evidence_ttl_seconds")
        for resource_id, routing in snapshot["routing_policies"].items():
            cache = routing["degradation"]["cache"]
            if cache["enabled"]:
                secret(cache["encryption_key_secret_ref"], f"routing_policies/{resource_id}/degradation/cache/encryption_key_secret_ref")
        replay = snapshot["replay_policies"]
        if replay["mode"] == "encrypted_exact_replay":
            secret(replay["encryption_key_secret_ref"], "replay_policies/encryption_key_secret_ref")
            if replay["ttl_seconds"] > self._policy.replay_max_ttl_seconds:
                error("resource_policy_invalid", "replay_policies/ttl_seconds")
            if replay["max_artifact_bytes"] > self._policy.replay_max_artifact_bytes:
                error("resource_policy_invalid", "replay_policies/max_artifact_bytes")
        # All entries here are semantic-phase diagnostics, sorted before truncation.
        return tuple(sorted(set(errors), key=lambda item: ((item.path or "").encode("utf-8"), item.reason.encode("utf-8"))))
