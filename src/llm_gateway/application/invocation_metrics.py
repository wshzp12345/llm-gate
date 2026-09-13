"""Bounded aggregates of enriched observations, not a replacement cost ledger."""

from dataclasses import dataclass
from decimal import Decimal, localcontext


@dataclass(frozen=True)
class MetricPoint:
    name: str
    labels: tuple[tuple[str, str], ...]
    value: int | Decimal


class InvocationMetrics:
    def __init__(self):
        self._values = {}
        self._currencies = set()

    def _add(self, name, amount=1, **labels):
        key = ("gateway.observed." + name, tuple(sorted(labels.items())))
        with localcontext() as context:
            context.prec = 64
            self._values[key] = self._values.get(key, 0) + amount

    def observe(self, observation):
        if observation is None:
            return
        state = observation.state if observation.state in {"completed", "failed", "cancelled"} else "pending"
        self._add("invocations", state=state)
        attempts = observation.attempts
        self._add("attempts", len(attempts))
        # Count actual transitions, not an intention to retry that was cancelled.
        retries = sum(current.binding_id == previous.binding_id for previous, current in zip(attempts, attempts[1:]))
        fallbacks = sum(current.binding_id != previous.binding_id for previous, current in zip(attempts, attempts[1:]))
        self._add("recovery", retries, action="retry")
        self._add("recovery", fallbacks, action="fallback")
        for attempt in attempts:
            for field in ("input_tokens", "output_tokens", "cached_tokens", "reasoning_tokens"):
                value = getattr(attempt.usage, field)
                self._add("usage_measurements", token_type=field, availability="unknown" if value is None else "known")
                if value is not None:
                    self._add("known_tokens", value, token_type=field)
        # Do not count late_usage a second time or replace settled Usage.
        if not observation.costs:
            self._add("cost_records_missing")
        for cost in observation.costs:
            if cost.currency not in self._currencies:
                if len(self._currencies) == 16:
                    self._add("cost_currency_overflow")
                    continue  # Never blend currencies into an 'other' amount.
                self._currencies.add(cost.currency)
            completeness = cost.completeness if cost.completeness in {"complete", "partial", "unavailable"} else "unavailable"
            certainty = cost.certainty if cost.certainty in {"authoritative", "estimated", "unavailable"} else "unavailable"
            labels = dict(currency=cost.currency, completeness=completeness, certainty=certainty)
            self._add("cost_records", availability="unknown" if cost.total_cost is None else "known", **labels)
            if cost.total_cost is not None:
                self._add("known_cost", Decimal(cost.total_cost), **labels)

    def snapshot(self):
        return tuple(MetricPoint(name, labels, value) for (name, labels), value in sorted(self._values.items()))
