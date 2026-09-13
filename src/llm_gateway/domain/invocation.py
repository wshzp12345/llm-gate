"""Immutable identity for the initial unkeyed Invocation persistence slice."""

import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timedelta
from uuid import UUID

from llm_gateway.domain.configuration import revision_number
from llm_gateway.domain.prompts import PromptReference
from llm_gateway.domain.correlation import BusinessCorrelation


class InvocationPersistenceUnavailable(Exception):
    pass


class InvocationAuthorizationExpired(Exception):
    pass


class InvocationTerminalConflict(Exception):
    """Cancellation or another terminal owner won; suppress this result."""


def _context_string(value: str, maximum: int) -> None:
    if (not isinstance(value, str) or not 1 <= len(value.encode("utf-8")) <= maximum
            or value != value.strip() or any(unicodedata.category(char) == "Cc" for char in value)):
        raise ValueError("Invalid Authorization Context string")


@dataclass(frozen=True)
class AuthorizationContext:
    tenant_id: str
    subject: str
    scopes: frozenset[str]
    issuer: str
    audience: str
    expires_at: datetime
    authentication_method: str

    def __post_init__(self):
        for value, maximum in ((self.tenant_id, 255), (self.subject, 255),
                               (self.issuer, 2048), (self.audience, 255)):
            _context_string(value, maximum)
        if self.authentication_method not in {"dev_bypass", "local_jwt", "online_authority"}:
            raise ValueError("Invalid authentication method")
        if (not isinstance(self.expires_at, datetime) or self.expires_at.tzinfo is None
                or self.expires_at.utcoffset() != timedelta(0)):
            raise ValueError("UTC authorization expiry required")
        if not isinstance(self.scopes, frozenset) or len(self.scopes) > 64:
            raise ValueError("Immutable bounded scope set required")
        for scope in self.scopes:
            if self.authentication_method == "dev_bypass" and scope == "dev:*":
                continue
            if (not isinstance(scope, str) or len(scope) > 128
                    or not re.fullmatch(r"[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*", scope)):
                raise ValueError("Invalid scope")
        if sum(len(scope) for scope in self.scopes) > 4096:
            raise ValueError("Scope aggregate too large")


@dataclass(frozen=True)
class InvocationAdmission:
    call_id: UUID
    trace_id: str
    configuration_revision: str
    requested_model: str
    authorization: AuthorizationContext
    prompt_reference: PromptReference | None = None
    correlation: BusinessCorrelation = BusinessCorrelation()

    def __post_init__(self):
        if type(self.correlation) is not BusinessCorrelation:
            raise ValueError("Typed business correlation required")
        if self.prompt_reference is not None and type(self.prompt_reference) is not PromptReference:
            raise ValueError("Invalid Prompt reference")
        if not isinstance(self.call_id, UUID) or self.call_id.version != 4:
            raise ValueError("Invocation requires UUIDv4")
        if (not isinstance(self.trace_id, str) or not re.fullmatch(r"[0-9a-f]{32}", self.trace_id)
                or self.trace_id == "0" * 32):
            raise ValueError("Invalid trace identity")
        revision_number(self.configuration_revision)
        if (not isinstance(self.requested_model, str) or len(self.requested_model) > 128
                or not re.fullmatch(r"[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*", self.requested_model)):
            raise ValueError("Invalid logical Alias")
        if not isinstance(self.authorization, AuthorizationContext):
            raise ValueError("Authorization Context required")
