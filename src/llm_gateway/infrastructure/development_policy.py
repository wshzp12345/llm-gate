"""Explicit local harness policy; not the production Bootstrap defaults.

Secret Reference identities are catalog entries, not secret values. Publication
does not resolve Provider material or advertise capabilities absent from code.
"""

from datetime import datetime, timezone

from llm_gateway.adapters.trust_bundle import LocalTrustBundleValidator, TrustBundleIntegrityValidator
from llm_gateway.application.configuration_validation import ConfigurationDeploymentPolicy, LocalConfigurationValidator
from llm_gateway.domain.configuration_validation import AdapterCapabilities


DEEPSEEK_SECRET_REFERENCE = "deepseek-api-key"


def _development_policy(*, known_secret_references=None, enable_streaming=False):
    return ConfigurationDeploymentPolicy(
        production=False,
        resource_ceilings={
            "max_request_bytes": 1048576, "max_messages": 100,
            "max_content_item_bytes": 262144, "max_output_schema_bytes": 65536,
            "max_sse_event_bytes": 262144, "max_invocation_seconds": 300,
        },
        routing_evidence_max_ttl_seconds=7776000,
        replay_max_ttl_seconds=86400,
        replay_max_artifact_bytes=1048576,
        known_secret_references=frozenset({DEEPSEEK_SECRET_REFERENCE}) if known_secret_references is None else frozenset(known_secret_references),
        currency_codes=frozenset({"USD"}),
        adapter_contracts={("compatible", "v1"): AdapterCapabilities(enable_streaming, False, "none")},
    )


def development_validator(*, known_secret_references=None, enable_streaming=False) -> LocalConfigurationValidator:
    return LocalConfigurationValidator(_development_policy(known_secret_references=known_secret_references, enable_streaming=enable_streaming), LocalTrustBundleValidator(clock=lambda: datetime.now(timezone.utc)))


def development_active_validator(*, known_secret_references=None, enable_streaming=False) -> LocalConfigurationValidator:
    return LocalConfigurationValidator(_development_policy(known_secret_references=known_secret_references, enable_streaming=enable_streaming), TrustBundleIntegrityValidator())
