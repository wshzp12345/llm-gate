"""Closed configuration Bundle DTOs. These types never cross into Domain."""

import unicodedata
from typing import Annotated, Literal

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, TypeAdapter, field_validator, model_validator, model_serializer

from llm_gateway.domain.configuration import revision_number


ResourceId = Annotated[str, Field(min_length=1, max_length=128, pattern=r"^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$")]
Positive = Annotated[int, Field(gt=0)]
Digest = Annotated[str, Field(pattern=r"^sha256:[0-9a-f]{64}$")]


def _boolean(value):
    if type(value) is not bool:
        raise ValueError("Boolean required")
    return value


TrueFlag = Annotated[Literal[True], BeforeValidator(_boolean)]
FalseFlag = Annotated[Literal[False], BeforeValidator(_boolean)]


class Closed(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", allow_inf_nan=False)


class Metadata(Closed):
    description: str

    @field_validator("description")
    @classmethod
    def valid_description(cls, value):
        if (not 1 <= len(value.encode("utf-8")) <= 1024 or value != value.strip()
                or any(unicodedata.category(char) in {"Cc", "Zl", "Zp"} for char in value)):
            raise ValueError("Invalid description")
        return value


class Adapter(Closed):
    type: ResourceId
    version: Annotated[str, Field(min_length=1, max_length=64, pattern=r"^[\x20-\x7e]+$")]


class Endpoint(Closed):
    base_url: str


class Credential(Closed):
    secret_ref: str


class Egress(Closed):
    allowed_hosts: Annotated[list[str], Field(min_length=1)]
    allowed_networks: Annotated[list[str], Field(min_length=1)]
    proxy: Literal["disabled"]

    @field_validator("allowed_hosts", "allowed_networks")
    @classmethod
    def unique_entries(cls, value):
        if len(value) != len(set(value)):
            raise ValueError("Duplicate egress entry")
        return value


class TrustBundle(Closed):
    identity: Digest
    pem: str
    label: str | None = None

    @field_validator("label", mode="before")
    @classmethod
    def no_explicit_null_label(cls, value):
        if value is None:
            raise ValueError("Omit absent label")
        return value


class SystemTLS(Closed):
    trust: Literal["system"]
    trust_bundle: None


class BundleTLS(Closed):
    trust: Literal["bundle"]
    trust_bundle: TrustBundle


class Transport(Closed):
    http_version: Literal["1.1"]
    connect_timeout_seconds: Annotated[int, Field(ge=1, le=30)] = 5
    idle_timeout_seconds: Annotated[int, Field(ge=1, le=300)] = 30
    max_connection_lifetime_seconds: Annotated[int, Field(ge=30, le=3600)] = 300
    tls: Annotated[SystemTLS | BundleTLS, Field(discriminator="trust")]


class Probe(Closed):
    path: str
    interval_seconds: Annotated[int, Field(ge=10, le=3600)]
    timeout_seconds: Annotated[int, Field(ge=1, le=10)]


class Health(Closed):
    active_probe: Probe | None


class RateLimit(Closed):
    max_concurrency: Positive
    qps: Positive
    burst: Positive


class Provider(Closed):
    status: Literal["enabled", "disabled"]
    adapter: Adapter
    endpoint: Endpoint
    credential: Credential
    egress: Egress
    transport: Transport
    health: Health
    rate_limit: RateLimit


class Capabilities(Closed):
    streaming: bool
    tool_calling: bool
    structured_output: Literal["none", "json_object", "json_schema"]


class ModelLimits(Closed):
    context_tokens: Positive
    max_output_tokens: Positive


class Binding(Closed):
    status: Literal["enabled", "disabled"]
    provider: ResourceId
    upstream_model: Annotated[str, Field(min_length=1)]
    capabilities: Capabilities
    limits: ModelLimits
    pricing_table: ResourceId
    tokenizer: str | None = None


class Candidate(Closed):
    binding: ResourceId
    service_level: Literal["full", "reduced"]
    priority: Annotated[int, Field(ge=0, le=65535)]
    weight: Annotated[int, Field(ge=0, le=10000)]


class GenerationDefaults(Closed):
    temperature: Annotated[float, Field(ge=0, le=2)] = 1.0
    top_p: Annotated[float, Field(gt=0, le=1)] = 1.0
    max_output_tokens: Positive = 4096


class AliasStructuredOutput(Closed):
    local_extraction: Literal["inherit", "disabled"] = "inherit"


class ModelAlias(Closed):
    candidates: Annotated[list[Candidate], Field(min_length=1, max_length=64)]
    routing_policy: ResourceId
    safety_policy: ResourceId
    generation_defaults: GenerationDefaults
    structured_output: AliasStructuredOutput
    requests_per_minute: Positive = 5

    @model_serializer(mode="wrap")
    def serialize(self, handler):
        result = handler(self)
        # Preserve immutable pre-RPM v1 snapshot digests. Omission canonically
        # means 5; explicit default and omitted default have the same identity.
        if self.requests_per_minute == 5:
            result.pop("requests_per_minute", None)
        return result

    @field_validator("candidates")
    @classmethod
    def unique_bindings(cls, value):
        if len(value) != len({item.binding for item in value}):
            raise ValueError("Duplicate candidate")
        return value


class Selection(Closed):
    strategy: Literal["priority_weighted_without_replacement"]
    seed_profile: Literal["gateway.routing-seed/v1"]


class Retry(Closed):
    max_attempts: Annotated[int, Field(ge=1, le=3)]
    max_attempts_per_candidate: Annotated[int, Field(ge=1, le=2)]
    base_delay_ms: Annotated[int, Field(ge=1, le=2000)]
    multiplier: Annotated[int, Field(ge=1, le=4)]
    max_delay_ms: Annotated[int, Field(ge=1, le=2000)]
    jitter: Literal["full"]
    max_retry_after_seconds: Annotated[int, Field(ge=0, le=5)]


class Fallback(Closed):
    mode: Literal["pre_commit_only"]


class ReducedService(Closed):
    enabled: bool


class CacheDisabled(Closed):
    enabled: FalseFlag


class CacheEnabled(Closed):
    enabled: TrueFlag
    fresh_ttl_seconds: Annotated[int, Field(ge=1, le=86400)]
    stale_ttl_seconds: Annotated[int, Field(ge=0, le=604800)]
    encryption_key_secret_ref: str
    retention: Literal["successful_results_only"]
    key_profile: Literal["execution_fingerprint_v1"]


class Terminal(Closed):
    mode: Literal["preserve_live_failure"]


class Degradation(Closed):
    reduced_service: ReducedService
    cache: CacheDisabled | CacheEnabled
    terminal: Terminal


class RoutingPolicy(Closed):
    selection: Selection
    retry: Retry
    fallback: Fallback
    degradation: Degradation


Rate = Annotated[str, Field(pattern=r"^(?:0|[1-9][0-9]*)(?:\.[0-9]{1,12})?$")]


class Rates(Closed):
    input: Rate
    output: Rate
    cached_input: Rate | None
    reasoning_output: Rate | None


class PricingTable(Closed):
    currency: Annotated[str, Field(pattern=r"^[A-Z]{3}$")]
    unit: Literal["per_million_tokens"]
    effective_from: str
    rates: Rates
    rounding: Literal["half_even_12dp"]


class SafetyPolicy(Closed):
    mode: Literal["provider_refusal_terminal"]


class StructuredOutputPolicy(Closed):
    local_extraction_enabled: bool = True
    max_validation_errors: Annotated[int, Field(ge=1, le=32)] = 32


class IdempotencyPolicy(Closed):
    binding_ttl_seconds: Annotated[int, Field(gt=0, le=604800)] = 86400
    key_reuse_quarantine_seconds: Annotated[int, Field(gt=0, le=2592000)] = 604800


Limit = Literal["inherit"] | Positive


class ResourceLimits(Closed):
    max_request_bytes: Limit = "inherit"
    max_messages: Limit = "inherit"
    max_content_item_bytes: Limit = "inherit"
    max_output_schema_bytes: Limit = "inherit"
    max_sse_event_bytes: Limit = "inherit"
    max_invocation_seconds: Limit = "inherit"


class ResourcePolicy(Closed):
    structured_output: StructuredOutputPolicy
    idempotency: IdempotencyPolicy
    limits: ResourceLimits
    routing_evidence_ttl_seconds: Annotated[int, Field(ge=86400)] = 2592000


class DedupOnly(Closed):
    mode: Literal["execution_dedup_only"] = "execution_dedup_only"


class ExactReplay(Closed):
    mode: Literal["encrypted_exact_replay"]
    ttl_seconds: Positive
    max_artifact_bytes: Positive
    encryption_key_secret_ref: str
    eligible_results: Annotated[list[Literal["sync", "stream"]], Field(min_length=1, max_length=2)]
    access_scope: Literal["same_subject"]

    @field_validator("eligible_results")
    @classmethod
    def unique_results(cls, value):
        if len(value) != len(set(value)):
            raise ValueError("Duplicate replay eligibility")
        return value


class Bundle(Closed):
    schema_version: Literal["gateway.config/v1"]
    base_revision: str
    metadata: Metadata | None = None
    providers: dict[ResourceId, Provider]
    provider_model_bindings: dict[ResourceId, Binding]
    model_aliases: dict[ResourceId, ModelAlias]
    routing_policies: dict[ResourceId, RoutingPolicy]
    pricing_tables: dict[ResourceId, PricingTable]
    safety_policies: dict[ResourceId, SafetyPolicy]
    resource_policies: ResourcePolicy
    replay_policies: Annotated[DedupOnly | ExactReplay, Field(discriminator="mode")] = Field(default_factory=DedupOnly)

    @field_validator("base_revision")
    @classmethod
    def valid_base(cls, value):
        if value != "0":
            revision_number(value)
        return value

    @field_validator("metadata", mode="before")
    @classmethod
    def no_explicit_null_metadata(cls, value):
        if value is None:
            raise ValueError("Omit absent metadata")
        return value


class BundleSubmission(Closed):
    kind: Literal["bundle"]
    bundle: Bundle


RESOURCE_DTOS = {
    "providers": Provider, "provider_model_bindings": Binding, "model_aliases": ModelAlias,
    "routing_policies": RoutingPolicy, "pricing_tables": PricingTable, "safety_policies": SafetyPolicy,
    "resource_policies": ResourcePolicy,
}
REPLAY_DTO = TypeAdapter(Annotated[DedupOnly | ExactReplay, Field(discriminator="mode")])


class ConfigurationOperation(Closed):
    op: Literal["add", "replace", "remove"]
    section: Literal["providers", "provider_model_bindings", "model_aliases", "routing_policies", "pricing_tables", "safety_policies", "resource_policies", "replay_policies"]
    resource_id: ResourceId | None = None
    value: dict | None = None

    @model_validator(mode="before")
    @classmethod
    def exact_variant(cls, data):
        if not isinstance(data, dict):
            raise ValueError("Operation object required")
        section, op = data.get("section"), data.get("op")
        if not isinstance(section, str) or section not in (*RESOURCE_DTOS, "replay_policies"):
            raise ValueError("Unknown section")
        singleton = section in {"resource_policies", "replay_policies"}
        expected = {"section", "op"}
        if singleton:
            if op != "replace":
                raise ValueError("Singleton only permits replace")
        else:
            expected.add("resource_id")
        if op in {"add", "replace"}:
            expected.add("value")
        elif op != "remove":
            raise ValueError("Unknown operation")
        if set(data) != expected or (not singleton and data["resource_id"] is None):
            raise ValueError("Invalid operation members")
        result = dict(data)
        if "value" in expected:
            dto = REPLAY_DTO.validate_python(data["value"]) if section == "replay_policies" else RESOURCE_DTOS[section].model_validate(data["value"])
            result["value"] = dto.model_dump()
        return result


class ChangeSet(Closed):
    schema_version: Literal["gateway.config-change-set/v1"]
    base_revision: str
    metadata: Metadata | None = None
    operations: list[ConfigurationOperation]

    @field_validator("base_revision")
    @classmethod
    def valid_base(cls, value):
        if value != "0":
            revision_number(value)
        return value

    @field_validator("metadata", mode="before")
    @classmethod
    def no_explicit_null(cls, value):
        if value is None:
            raise ValueError("Omit absent metadata")
        return value

    @field_validator("operations")
    @classmethod
    def unique_targets(cls, value):
        targets = [(item.section, item.resource_id) for item in value]
        if len(targets) != len(set(targets)):
            raise ValueError("Duplicate operation target")
        return value


class ChangeSetSubmission(Closed):
    kind: Literal["change_set"]
    change_set: ChangeSet


SUBMISSION_DTO = TypeAdapter(Annotated[BundleSubmission | ChangeSetSubmission, Field(discriminator="kind")])


class PublishRequest(Metadata):
    expected_active_revision: str
    candidate_snapshot_digest: Digest

    @field_validator("expected_active_revision")
    @classmethod
    def valid_expected(cls, value):
        if value != "0":
            revision_number(value)
        return value


class RebaseOrRollbackRequest(Metadata):
    expected_active_revision: str

    @field_validator("expected_active_revision")
    @classmethod
    def positive_expected(cls, value):
        revision_number(value)
        return value
