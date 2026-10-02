"""Resolve effective text parameters after Router selects an eligible Binding.

This mapper does not authorize a Candidate, replace runtime gates, or acquire
secrets. Its immutable metadata is intended for execution Evidence/fingerprints.
"""

import json
from dataclasses import dataclass, replace

from llm_gateway.adapters.canonical_json import canonical_digest
from llm_gateway.application.active_configuration import LoadedConfiguration
from llm_gateway.application.model_api import ModelInvocationRejected, TextInvocationQuery
from llm_gateway.domain.model import CompletionRequest


@dataclass(frozen=True)
class PreparedTextCompletion:
    request: CompletionRequest
    parameter_sources: tuple[tuple[str, str], ...]
    effective_parameters_digest: str


def prepare_text_completion(snapshot: LoadedConfiguration, query: TextInvocationQuery, binding_id: str) -> PreparedTextCompletion:
    content = json.loads(snapshot.snapshot_json)
    alias = content["model_aliases"][query.requested_model]
    if binding_id not in {item["binding"] for item in alias["candidates"]}:
        raise ValueError("Selected Binding is outside the requested Alias")
    binding = content["provider_model_bindings"][binding_id]
    defaults = alias["generation_defaults"]
    values, sources = {}, []
    for name in ("max_output_tokens", "temperature", "top_p"):
        supplied = getattr(query, name)
        values[name] = defaults[name] if supplied is None else supplied
        sources.append((name, "alias_default" if supplied is None else "caller"))
    if values["max_output_tokens"] > defaults["max_output_tokens"]:
        raise ModelInvocationRejected("invalid_request")
    if values["max_output_tokens"] > binding["limits"]["max_output_tokens"]:
        raise ModelInvocationRejected("unsupported_capability")
    output_format = query.output_format
    if output_format is not None:
        extraction = (content["resource_policies"]["structured_output"]["local_extraction_enabled"]
                      and alias["structured_output"]["local_extraction"] != "disabled")
        output_format = replace(output_format, local_extraction=extraction)
    request = CompletionRequest(query.requested_model, binding["upstream_model"], query.messages,
                                output_format=output_format, **values)
    return PreparedTextCompletion(request, tuple(sources), canonical_digest(values))
