import json
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal, Inexact, localcontext

import pytest

from llm_gateway.domain.cost import PricingBasis, calculate_cost
from llm_gateway.domain.model import Usage


PRICING = PricingBasis("1", "pricing", "USD", datetime(2026, 1, 1, tzinfo=timezone.utc), "2", "4", "0.5", "6")


def test_subsets_are_charged_once_and_parent_total_is_not_used():
    cost = calculate_cost(PRICING, Usage(100, 50, 20, 10, 999999))
    assert (cost.input_cost, cost.output_cost, cost.cached_cost, cost.reasoning_cost) == (
        Decimal("0.00016"), Decimal("0.00016"), Decimal("0.00001"), Decimal("0.00006"))
    assert cost.total_cost == Decimal("0.00039")
    assert cost.completeness == "complete"
    assert cost.pricing is PRICING


def test_unknown_usage_never_becomes_zero():
    cost = calculate_cost(PRICING, Usage())
    assert cost.total_cost is None and cost.input_cost is None
    assert cost.completeness == "unavailable"


def test_unknown_subset_distribution_never_gets_an_invented_discount():
    cost = calculate_cost(PRICING, Usage(100, 50, None, 0))
    assert cost.input_cost is None and cost.cached_cost is None
    assert cost.output_cost == Decimal("0.0002")
    assert cost.total_cost is None and cost.completeness == "partial"


def test_null_special_rate_retains_known_categories_without_guessed_total():
    cost = calculate_cost(replace(PRICING, cached_input_rate=None), Usage(100, 50, 20, 0))
    assert cost.input_cost == Decimal("0.00016")
    assert cost.cached_cost is None and cost.total_cost is None
    assert cost.completeness == "partial"


def test_zero_volume_needs_no_guessed_rate():
    cost = calculate_cost(replace(PRICING, cached_input_rate=None, reasoning_output_rate=None), Usage(0, 0))
    assert cost.total_cost == Decimal("0") and cost.completeness == "complete"


def test_approved_local_estimate_is_partial_and_uses_standard_parent_rates():
    cost = calculate_cost(PRICING, Usage(100, 50), locally_estimated=True)
    assert cost.total_cost == Decimal("0.0004")
    assert cost.cached_cost is None and cost.reasoning_cost is None
    assert cost.usage_source == "locally_estimated" and cost.completeness == "partial"
    assert calculate_cost(PRICING, Usage(), locally_estimated=True).completeness == "unavailable"
    with pytest.raises(ValueError):
        calculate_cost(PRICING, Usage(100, 50, 10), locally_estimated=True)


def test_round_half_even_and_total_rounds_exact_sum_not_rounded_components():
    pricing = replace(PRICING, input_rate="0.0000005", output_rate="0.0000005")
    cost = calculate_cost(pricing, Usage(1, 1, 0, 0))
    assert cost.input_cost == Decimal("0") and cost.output_cost == Decimal("0")
    assert cost.total_cost == Decimal("0.000000000001")
    assert calculate_cost(replace(pricing, input_rate="0.0000015"), Usage(1, 0, 0, 0)).input_cost == Decimal("0.000000000002")


def test_large_rates_do_not_inherit_ambient_decimal_precision():
    pricing = replace(PRICING, input_rate="1" + "0" * 80)
    with localcontext() as context:
        context.prec = 2
        context.traps[Inexact] = True
        context.Emax = 2
        cost = calculate_cost(pricing, Usage(1000000, 0, 0, 0))
    assert cost.input_cost == Decimal(pricing.input_rate)


@pytest.mark.parametrize("rate", ["-1", "1e2", ".5", "01", "NaN", "0.0000000000001", 1.0, None])
def test_noncanonical_rates_rejected(rate):
    with pytest.raises(ValueError):
        replace(PRICING, input_rate=rate)


def test_pricing_projection_preserves_configuration_revision_and_null_rates():
    from llm_gateway.adapters.pricing_projection import project_pricing
    from llm_gateway.application.active_configuration import LoadedConfiguration
    from llm_gateway.domain.configuration_changes import SnapshotResources

    content = {"provider_model_bindings": {"binding": {"pricing_table": "pricing"}},
               "pricing_tables": {"pricing": {"currency": "USD", "unit": "per_million_tokens",
                   "effective_from": "2026-01-01T00:00:00Z", "rounding": "half_even_12dp",
                   "rates": {"input": "2", "output": "4", "cached_input": None, "reasoning_output": None}}}}
    snapshot = LoadedConfiguration("99", "fixture", json.dumps(content).encode(), SnapshotResources({}))
    pricing = project_pricing(snapshot, "binding")
    assert pricing.configuration_revision == "99" and pricing.resource_id == "pricing"
    assert pricing.cached_input_rate is None and pricing.reasoning_output_rate is None
