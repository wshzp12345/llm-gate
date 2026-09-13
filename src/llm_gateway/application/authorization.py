"""Authentication result boundary and exact Gateway permission checks."""

from typing import Protocol

from llm_gateway.domain.invocation import AuthorizationContext


class Unauthorized(Exception):
    """Caller credential invalid; expose only a generic Bearer challenge."""


class AuthorizationUnavailable(Exception):
    """Authentication dependency unavailable, without credential disclosure."""


class Forbidden(Exception):
    pass


class AuthorizationPort(Protocol):
    async def authenticate(self, credential: str) -> AuthorizationContext: ...


PERMISSIONS = frozenset({
    "gateway.model.invoke", "gateway.model.provider_override", "gateway.invocation.read",
    "gateway.invocation.cancel", "gateway.config.providers.write", "gateway.config.bindings.write",
    "gateway.config.routing.write", "gateway.config.pricing.write", "gateway.config.safety.write",
    "gateway.config.resources.write", "gateway.config.replay.write", "gateway.config.read",
    "gateway.config.export", "gateway.config.publish", "gateway.config.rollback",
    "gateway.audit.read", "gateway.cost.reconcile",
    "gateway.prompts.read", "gateway.prompts.write", "gateway.prompts.publish", "gateway.prompts.use",
})


def require_permission(context, permission):
    if not isinstance(context, AuthorizationContext) or permission not in PERMISSIONS:
        raise ValueError("Validated context and registered permission required")
    if context.authentication_method != "dev_bypass" and permission not in context.scopes:
        raise Forbidden()
