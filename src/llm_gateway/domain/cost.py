"""Exact, conservative Attempt cost calculation; no billing reconciliation."""

import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Context, Decimal, MAX_EMAX, MIN_EMIN, ROUND_HALF_EVEN, localcontext

from llm_gateway.domain.configuration import revision_number
from llm_gateway.domain.model import Usage


@dataclass(frozen=True)
class PricingBasis:
    configuration_revision: str
    resource_id: str
    currency: str
    effective_from: datetime
    input_rate: str
    output_rate: str
    cached_input_rate: str | None
    reasoning_output_rate: str | None

    def __post_init__(self):
        revision_number(self.configuration_revision)
        if (not isinstance(self.resource_id, str) or len(self.resource_id) > 128
                or not re.fullmatch(r"[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*", self.resource_id)):
            raise ValueError("Invalid pricing identity")
        if not isinstance(self.currency, str) or not re.fullmatch(r"[A-Z]{3}", self.currency):
            raise ValueError("Invalid currency")
        if (not isinstance(self.effective_from, datetime) or self.effective_from.tzinfo is None
                or self.effective_from.utcoffset() != timedelta(0)):
            raise ValueError("UTC pricing effective time required")
        for index, rate in enumerate(self.rates):
            if rate is None and index >= 2:
                continue
            if not isinstance(rate, str) or not re.fullmatch(r"(?:0|[1-9][0-9]*)(?:\.[0-9]{1,12})?", rate):
                raise ValueError("Ordinary nonnegative decimal rate required")

    @property
    def rates(self):
        return (self.input_rate, self.output_rate, self.cached_input_rate, self.reasoning_output_rate)


@dataclass(frozen=True)
class AttemptCost:
    pricing: PricingBasis
    usage: Usage
    usage_source: str
    input_cost: Decimal | None
    output_cost: Decimal | None
    cached_cost: Decimal | None
    reasoning_cost: Decimal | None
    total_cost: Decimal | None
    completeness: str
    rounding: str = "half_even_12dp"
    unit: str = "per_million_tokens"

    @property
    def certainty(self):
        return "unavailable" if self.completeness == "unavailable" else "estimated"


def calculate_cost(pricing: PricingBasis, usage: Usage, *, locally_estimated: bool = False) -> AttemptCost:
    """Do not infer absent subsets or rates. Local estimates use only parents.

    Parent counts include their subsets. Each known category is charged once,
    and the sum of exact categories is rounded once for total cost. A partial
    known subtotal is never represented as the total. Local estimates require
    approval by the caller's version-locked tokenizer policy (FR-721).
    """
    if not isinstance(pricing, PricingBasis) or not isinstance(usage, Usage) or type(locally_estimated) is not bool:
        raise ValueError("Typed pricing, Usage and estimate provenance required")
    if locally_estimated and any(value is not None for value in (usage.cached_tokens, usage.reasoning_tokens, usage.provider_reported_total)):
        raise ValueError("Local estimates cannot invent Provider-internal Usage")
    rates = pricing.rates
    # Isolate precision and rounding from the ambient Decimal context. The
    # published rates have no integer-digit ceiling, so size precision exactly.
    digits = max(len(rate) for rate in rates if rate is not None)
    counts = (usage.input_tokens, usage.output_tokens, usage.cached_tokens, usage.reasoning_tokens)
    count_digits = max((len(str(value)) for value in counts if value is not None), default=1)
    with localcontext(Context(prec=digits + count_digits + 32, rounding=ROUND_HALF_EVEN,
                              Emax=MAX_EMAX, Emin=MIN_EMIN)):
        zero = Decimal(0)
        def amount(count, rate):
            if count == 0:
                return zero
            return None if count is None or rate is None else Decimal(count) * Decimal(rate) / Decimal(1000000)

        def side(parent, subset, standard, special):
            if parent == 0:
                return zero, zero
            if parent is None or subset is None:
                return None, None
            return amount(parent - subset, standard), amount(subset, special)

        if locally_estimated:
            raw = (amount(usage.input_tokens, rates[0]), amount(usage.output_tokens, rates[1]), None, None)
            total = raw[0] + raw[1] if raw[0] is not None and raw[1] is not None else None
            completeness = "partial" if any(value is not None for value in raw) else "unavailable"
            source = "locally_estimated"
        else:
            input_cost, cached = side(usage.input_tokens, usage.cached_tokens, rates[0], rates[2])
            output_cost, reasoning = side(usage.output_tokens, usage.reasoning_tokens, rates[1], rates[3])
            raw = (input_cost, output_cost, cached, reasoning)
            total = sum(raw, zero) if all(value is not None for value in raw) else None
            completeness = "complete" if total is not None else "partial" if any(value is not None for value in raw) else "unavailable"
            source = usage.provenance
        def rounded(value):
            return None if value is None else value.quantize(Decimal("0.000000000001"), rounding=ROUND_HALF_EVEN)
        return AttemptCost(pricing, usage, source, *(rounded(value) for value in raw), rounded(total), completeness)
