"""Request identity for the currently accepted synchronous text API subset.

This explicitly versioned subset is not the final Tools/Schema/Streaming profile.
Only call after API validation and authorization, before mutable resolution.
Canonical sensitive bytes exist only within this calculation, not in its result.
"""

import json

from llm_gateway.adapters.canonical_json import canonical_bytes, canonical_digest
from llm_gateway.application.active_configuration import LoadedConfiguration
from llm_gateway.application.fingerprint_keys import FingerprintLease
from llm_gateway.application.model_api import TextInvocationQuery, ModelInvocationRejected
from llm_gateway.domain.fingerprints import FingerprintIdentity
from llm_gateway.domain.invocation import AuthorizationContext


REQUEST_PROFILE = "gateway.request-fingerprint/text-v1"
EXECUTION_PROFILE = "gateway.execution-fingerprint/text-v1"


def request_fingerprint(query: TextInvocationQuery, authorization: AuthorizationContext,
                        lease: FingerprintLease) -> FingerprintIdentity:
    if type(query) is not TextInvocationQuery or type(authorization) is not AuthorizationContext:
        raise ValueError("Validated text query and authorization required")
    if query.prompt is not None:
        raise ValueError("Prompt must be pinned and rendered before fingerprinting")
    projection = {
        "route": "POST /v1/chat/completions",
        "authorization_scope": {
            "tenant_id": authorization.tenant_id,
            "subject": authorization.subject,
            "issuer": authorization.issuer,
            "audience": authorization.audience,
            "scopes": sorted(authorization.scopes),
        },
        "model": query.requested_model,
        "messages": [{"role": message.role, "content": message.text} for message in query.messages],
        "generation": {"max_output_tokens": query.max_output_tokens,
                       "temperature": query.temperature, "top_p": query.top_p},
        "stream": query.stream,
        "include_usage": query.stream,
        "opaque_tools": None,
        "output_schema": None,
        "semantic_extensions": {},
        "caller_invocation_deadline": None,
    }
    profile = REQUEST_PROFILE
    if query.prompt_reference is not None:
        projection["prompt"] = {"asset_id": str(query.prompt_reference.asset_id), "version_id": str(query.prompt_reference.version_id)}
        profile = "gateway.request-fingerprint/prompt-text-v1"
    if query.stream:
        profile = "gateway.request-fingerprint/" + ("prompt-stream-text-v1" if query.prompt_reference else "stream-text-v1")
    message = profile.encode("ascii") + canonical_bytes(projection)
    digest = lease.digest(message, purpose="request").hex()
    return FingerprintIdentity(profile, lease.fence.key_id, lease.fence.key_version, digest)


def execution_fingerprint(query: TextInvocationQuery, snapshot: LoadedConfiguration,
                          lease: FingerprintLease, *, effective_limits: dict[str, int]) -> FingerprintIdentity:
    """Identity of the whole locked text routing contract, not a selected Attempt.

    effective_limits must already resolve deployment inheritance. Revision,
    pricing/evidence retention, unrelated resources and TLS labels are not model
    behavior. This text-only profile has no Schema or extraction pipeline.
    """
    if type(query) is not TextInvocationQuery:
        raise ValueError("Validated text query required")
    if query.prompt is not None:
        raise ValueError("Prompt must be pinned and rendered before fingerprinting")
    limit_names = {"max_request_bytes", "max_messages", "max_content_item_bytes",
                   "max_output_schema_bytes", "max_sse_event_bytes", "max_invocation_seconds"}
    if (type(effective_limits) is not dict or set(effective_limits) != limit_names
            or any(type(value) is not int or value <= 0 for value in effective_limits.values())):
        raise ValueError("Fully resolved resource limits required")
    content = json.loads(snapshot.snapshot_json)
    alias = content["model_aliases"][query.requested_model]
    defaults = alias["generation_defaults"]
    parameters = {name: defaults[name] if getattr(query, name) is None else getattr(query, name)
                  for name in ("max_output_tokens", "temperature", "top_p")}
    if parameters["max_output_tokens"] > defaults["max_output_tokens"]:
        raise ModelInvocationRejected("invalid_request")
    candidates = []
    for candidate in sorted(alias["candidates"], key=lambda item: item["binding"].encode("utf-8")):
        binding = content["provider_model_bindings"][candidate["binding"]]
        provider = content["providers"][binding["provider"]]
        transport = dict(provider["transport"])
        tls = provider["transport"]["tls"]
        transport["tls"] = {"trust": tls["trust"], "trust_bundle":
                            None if tls["trust"] == "system" else {"identity": tls["trust_bundle"]["identity"]}}
        candidates.append({
            "selection": candidate,
            "binding": {name: value for name, value in binding.items() if name != "pricing_table"},
            "provider": {**provider, "transport": transport},
        })
    projection = {
        "requested_model": query.requested_model,
        "message_hash": canonical_digest([{"role": item.role, "content": item.text} for item in query.messages]),
        "effective_generation": parameters,
        "alias_output_ceiling": defaults["max_output_tokens"],
        "candidates": candidates,
        "routing": content["routing_policies"][alias["routing_policy"]],
        "safety": content["safety_policies"][alias["safety_policy"]],
        "effective_limits": effective_limits,
        "stream": query.stream,
        "output_schema": None,
        "opaque_tools": None,
        "structured_extraction": {"enabled": False, "pipeline": None},
    }
    profile = EXECUTION_PROFILE
    if query.prompt_reference is not None:
        projection["prompt"] = {"asset_id": str(query.prompt_reference.asset_id), "version_id": str(query.prompt_reference.version_id)}
        profile = "gateway.execution-fingerprint/prompt-text-v1"
    if query.stream:
        profile = "gateway.execution-fingerprint/" + ("prompt-stream-text-v1" if query.prompt_reference else "stream-text-v1")
    message = profile.encode("ascii") + canonical_bytes(projection)
    digest = lease.digest(message, purpose="execution").hex()
    return FingerprintIdentity(profile, lease.fence.key_id, lease.fence.key_version, digest)
