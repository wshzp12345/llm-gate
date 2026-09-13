"""Extract a Binding's immutable pricing basis from a validated Snapshot."""

import json
from datetime import datetime

from llm_gateway.application.active_configuration import LoadedConfiguration
from llm_gateway.domain.cost import PricingBasis


def project_pricing(snapshot: LoadedConfiguration, binding_id: str) -> PricingBasis:
    content = json.loads(snapshot.snapshot_json)
    resource_id = content["provider_model_bindings"][binding_id]["pricing_table"]
    table = content["pricing_tables"][resource_id]
    if table["unit"] != "per_million_tokens" or table["rounding"] != "half_even_12dp":
        raise ValueError("Unsupported pricing profile")
    rates = table["rates"]
    return PricingBasis(snapshot.revision, resource_id, table["currency"],
                        datetime.fromisoformat(table["effective_from"].replace("Z", "+00:00")),
                        rates["input"], rates["output"], rates["cached_input"], rates["reasoning_output"])
