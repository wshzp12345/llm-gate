"""Request-local verified identity, never a process-global last principal."""

from contextlib import asynccontextmanager
from contextvars import ContextVar

from llm_gateway.adapters.caller_bearer import caller_bearer
from llm_gateway.application.authorization import AuthorizationUnavailable, Unauthorized, require_permission
from llm_gateway.domain.invocation import AuthorizationContext


class ModelRequestAuthorization:
    def __init__(self, adapter, *, authentication_method):
        if authentication_method not in {"local_jwt", "online_authority"} or not callable(getattr(adapter, "authenticate", None)):
            raise ValueError("Registered production authentication method required")
        self._adapter, self._method = adapter, authentication_method
        self._current = ContextVar("gateway_model_authorization", default=None)

    @asynccontextmanager
    async def context(self, raw_headers):
        credential = caller_bearer(raw_headers)
        try:
            context = await self._adapter.authenticate(credential)
        except (Unauthorized, AuthorizationUnavailable):
            raise
        except Exception:
            raise AuthorizationUnavailable() from None
        del credential, raw_headers
        if not isinstance(context, AuthorizationContext) or context.authentication_method != self._method:
            raise AuthorizationUnavailable()
        require_permission(context, "gateway.model.invoke")
        token = self._current.set(context)
        try:
            yield
        finally:
            self._current.reset(token)

    async def authorize(self, query):
        context = self._current.get()
        if context is None:
            raise Unauthorized()
        require_permission(context, "gateway.model.invoke")
        return context
