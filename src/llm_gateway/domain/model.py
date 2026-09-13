from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class FailureCode(StrEnum):
    INVALID_REQUEST = "invalid_request"
    RATE_LIMITED = "rate_limited"
    PROVIDER_UNAVAILABLE = "provider_unavailable"
    PROVIDER_CREDENTIALS_UNAVAILABLE = "provider_credentials_unavailable"
    PROVIDER_PROTOCOL_ERROR = "provider_protocol_error"
    UPSTREAM_TIMEOUT = "upstream_timeout"
    UNCERTAIN = "uncertain"


@dataclass(frozen=True)
class ProviderFailure:
    code: FailureCode
    retryable: bool
    retry_after_ms: int | None = None
    observed_usage: Usage | None = None
    observed_model: str | None = None

    def __post_init__(self):
        if self.retry_after_ms is not None and (type(self.retry_after_ms) is not int or self.retry_after_ms < 0):
            raise ValueError("Retry-After must be a nonnegative millisecond duration")
        if self.observed_usage is not None and not isinstance(self.observed_usage, Usage):
            raise ValueError("Typed observed Usage required")
        if self.observed_model is not None:
            if type(self.observed_model) is not str or not self.observed_model or len(self.observed_model) > 256:
                raise ValueError("Invalid observed model")
            self.observed_model.encode("utf-8")


@dataclass(frozen=True)
class Message:
    role: str
    text: str

    def __post_init__(self) -> None:
        if self.role not in {"system", "developer", "user", "assistant"}:
            raise ValueError("Unsupported text-message role")
        if not isinstance(self.text, str):
            raise ValueError("Message text must be a string")


@dataclass(frozen=True)
class CompletionRequest:
    requested_model: str
    resolved_model: str
    messages: tuple[Message, ...]
    max_output_tokens: int
    temperature: float | None = None
    top_p: float | None = None

    def __post_init__(self) -> None:
        if not self.requested_model or not self.resolved_model or not self.messages:
            raise ValueError("Model identifiers and messages are required")
        if type(self.max_output_tokens) is not int or self.max_output_tokens < 1:
            raise ValueError("Output limit must be a positive integer")
        if self.temperature is not None and (type(self.temperature) not in (int, float) or not 0 <= self.temperature <= 2):
            raise ValueError("Temperature must be between zero and two")
        if self.top_p is not None and (type(self.top_p) not in (int, float) or not 0 < self.top_p <= 1):
            raise ValueError("Top-p must be greater than zero and at most one")


@dataclass(frozen=True)
class Usage:
    """Unknown counts stay unknown; subsets are never added to parent totals."""

    input_tokens: int | None = None
    output_tokens: int | None = None
    cached_tokens: int | None = None
    reasoning_tokens: int | None = None
    provider_reported_total: int | None = None

    def __post_init__(self) -> None:
        for count in (
            self.input_tokens, self.output_tokens, self.cached_tokens,
            self.reasoning_tokens, self.provider_reported_total,
        ):
            if count is not None and (type(count) is not int or count < 0):
                raise ValueError("Token counts must be non-negative integers")
        for subset, parent in (
            (self.cached_tokens, self.input_tokens),
            (self.reasoning_tokens, self.output_tokens),
        ):
            if subset is not None and (parent is None or subset > parent):
                raise ValueError("Token subset requires a sufficient known parent")

    @property
    def total_tokens(self) -> int | None:
        if self.input_tokens is None or self.output_tokens is None:
            return None
        return self.input_tokens + self.output_tokens

    @property
    def provenance(self) -> str:
        return "provider_reported" if any(count is not None for count in (
            self.input_tokens, self.output_tokens, self.provider_reported_total,
        )) else "unavailable"


@dataclass(frozen=True)
class TextOutput:
    text: str


@dataclass(frozen=True)
class RefusalOutput:
    text: str | None
    refusal: str | None

    def __post_init__(self):
        for value in (self.text, self.refusal):
            if value is not None:
                if not isinstance(value, str):
                    raise ValueError("Refusal content must be text or null")
                value.encode("utf-8")


@dataclass(frozen=True)
class ProviderResult:
    requested_model: str
    resolved_model: str
    output: TextOutput | RefusalOutput
    usage: Usage
    finish_reason: str

    def __post_init__(self):
        if isinstance(self.output, RefusalOutput):
            if self.finish_reason not in {"stop", "length", "content_filter"} or self.output.refusal is None and self.finish_reason != "content_filter":
                raise ValueError("Valid refusal representation required")

    @property
    def disposition(self):
        return "safety_refused" if isinstance(self.output, RefusalOutput) else "succeeded"
