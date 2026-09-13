"""Provider-independent input/output boundary for the initial text API slice."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Protocol
from llm_gateway.application.streaming_attempt_execution import StreamOutputPort
from uuid import UUID

from llm_gateway.domain.model import Message, ProviderFailure, ProviderResult
from llm_gateway.domain.routing_failure import UnattemptedRoutingFailure
from llm_gateway.domain.prompts import PromptReference, PromptSelection


class ModelInvocationRejected(Exception):
    """Safe pre-admission rejection, with no Invocation identity to disclose."""

    def __init__(self, code: str):
        if code not in {"invalid_request", "request_too_large", "unsupported_capability", "gateway_not_ready", "no_route", "rate_limited", "forbidden"}:
            raise ValueError("Unregistered model rejection")
        self.code = code
        super().__init__(code)


@dataclass(frozen=True)
class TextInvocationQuery:
    requested_model: str
    messages: tuple[Message, ...] = field(repr=False)
    max_output_tokens: int | None
    deprecated_max_tokens: bool = False
    store_false_requested: bool = False
    temperature: float | None = None
    top_p: float | None = None
    body_bytes: int | None = None
    prompt: PromptSelection | None = None
    prompt_reference: PromptReference | None = None
    stream: bool = False
    stream_output: StreamOutputPort | None = field(default=None, repr=False, compare=False)

    def __post_init__(self):
        if type(self.stream) is not bool or self.stream != (self.stream_output is not None):
            raise ValueError("Streaming query requires an explicit output port")
        if self.stream and any(not callable(getattr(self.stream_output, method, None)) for method in ("delta", "admitted")):
            raise ValueError("Streaming output must implement admission metadata and delta")


@dataclass(frozen=True)
class RecoverySummary:
    attempts: int
    retries: int
    fallback_used: bool

    def __post_init__(self):
        if (type(self.attempts) is not int or not 0 <= self.attempts <= 3
                or type(self.retries) is not int or not 0 <= self.retries <= max(0, self.attempts - 1)
                or type(self.fallback_used) is not bool
                or self.fallback_used and self.attempts - self.retries < 2):
            raise ValueError("Invalid committed recovery summary")


@dataclass(frozen=True)
class InvocationReply:
    call_id: UUID
    accepted_at: datetime
    result: ProviderResult | ProviderFailure | UnattemptedRoutingFailure
    prompt_reference: PromptReference | None = None
    recovery: RecoverySummary | None = None


class ModelInvocationPort(Protocol):
    async def invoke(self, query: TextInvocationQuery) -> InvocationReply:
        """Run admission/routing/execution/settlement; return committed aggregate Usage.

        Never return a raw uncommitted Adapter result. The implementation owns
        Invocation identity, concurrency and all runtime prerequisites.
        """
        ...
