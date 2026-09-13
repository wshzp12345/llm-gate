"""Resolve the text subset's fingerprint inputs from validated configuration."""

import json
from types import MappingProxyType

from llm_gateway.adapters.text_fingerprints import request_fingerprint, execution_fingerprint
from llm_gateway.application.fingerprinted_text_invocation import TextExecutionIdentity
from llm_gateway.application.model_api import ModelInvocationRejected


class TextFingerprintProjection:
    def __init__(self, *, resource_ceilings):
        names = {"max_request_bytes", "max_messages", "max_content_item_bytes",
                 "max_output_schema_bytes", "max_sse_event_bytes", "max_invocation_seconds"}
        if set(resource_ceilings) != names or any(type(value) is not int or value <= 0 for value in resource_ceilings.values()):
            raise ValueError("Complete positive resource ceilings required")
        self._ceilings = MappingProxyType(dict(resource_ceilings))

    def request(self, query, authorization, lease):
        return request_fingerprint(query, authorization, lease)

    def execution(self, query, snapshot, lease):
        content = json.loads(snapshot.snapshot_json)
        if query.requested_model not in content["model_aliases"]:
            raise ModelInvocationRejected("invalid_request")
        limits = {}
        for name, ceiling in self._ceilings.items():
            value = content["resource_policies"]["limits"][name]
            resolved = ceiling if value == "inherit" else value
            if type(resolved) is not int or not 0 < resolved <= ceiling:
                raise ValueError("Configuration exceeds deployment resource ceiling")
            limits[name] = resolved
        if type(query.body_bytes) is not int or query.body_bytes < 0:
            raise ValueError("Validated request body byte count required")
        if query.body_bytes > limits["max_request_bytes"] or any(
                len(message.text.encode("utf-8")) > limits["max_content_item_bytes"] for message in query.messages):
            raise ModelInvocationRejected("request_too_large")
        if query.prompt_reference is not None and sum(len(message.text.encode("utf-8")) for message in query.messages) > limits["max_request_bytes"]:
            raise ModelInvocationRejected("request_too_large")
        if len(query.messages) > limits["max_messages"]:
            raise ModelInvocationRejected("invalid_request")
        identity = execution_fingerprint(query, snapshot, lease, effective_limits=limits)
        policy = content["model_aliases"][query.requested_model]["routing_policy"]
        rpm = content["model_aliases"][query.requested_model].get("requests_per_minute", 5)
        return TextExecutionIdentity(identity, policy, limits["max_invocation_seconds"], rpm)
