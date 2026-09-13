"""Provider-neutral ordered stream events. No wire frames or vendor objects."""

from dataclasses import dataclass, field, replace
from enum import StrEnum

from llm_gateway.domain.model import ProviderFailure, ProviderResult, Usage


class DeltaKind(StrEnum):
    TEXT = "text"
    REFUSAL = "refusal"


def _sequence(number):
    if type(number) is not int or number < 1:
        raise ValueError("Positive stream sequence required")


@dataclass(frozen=True)
class StreamDelta:
    sequence: int
    resolved_model: str
    kind: DeltaKind
    text: str = field(repr=False)

    def __post_init__(self):
        _sequence(self.sequence)
        if (type(self.resolved_model) is not str or not self.resolved_model
                or not isinstance(self.kind, DeltaKind) or type(self.text) is not str or not self.text):
            raise ValueError("Invalid stream delta")
        self.text.encode("utf-8")


@dataclass(frozen=True)
class StreamCompleted:
    sequence: int
    result: ProviderResult = field(repr=False)

    def __post_init__(self):
        _sequence(self.sequence)
        if not isinstance(self.result, ProviderResult):
            raise ValueError("Typed stream result required")


@dataclass(frozen=True)
class StreamFailed:
    sequence: int
    failure: ProviderFailure
    usage: Usage = field(default_factory=Usage)
    resolved_model: str | None = None

    def __post_init__(self):
        _sequence(self.sequence)
        if not isinstance(self.failure, ProviderFailure) or not isinstance(self.usage, Usage):
            raise ValueError("Typed stream failure and observed Usage required")
        if self.resolved_model is not None and (type(self.resolved_model) is not str or not self.resolved_model):
            raise ValueError("Invalid stream model")

    def as_result(self) -> ProviderFailure:
        """Preserve observed facts when entering the shared Attempt journal."""
        return replace(self.failure, observed_usage=self.usage, observed_model=self.resolved_model)


ProviderStreamEvent = StreamDelta | StreamCompleted | StreamFailed
