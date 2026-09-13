"""Invocation-level capacity around authorized, new synchronous execution.

The authorizer is a required request-context-bound dependency: this service
never derives a tenant from a model payload or interprets credentials. Replay
must take a separate path and must not enter this new-Invocation wrapper.
"""

from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta, timezone
from typing import Protocol

from llm_gateway.application.admission_capacity import AdmissionCapacity
from llm_gateway.application.api_rate import ApiRate
from llm_gateway.application.request_trace import trace_stage, TraceStage
from llm_gateway.application.model_api import InvocationReply, ModelInvocationRejected, TextInvocationQuery
from llm_gateway.domain.invocation import AuthorizationContext, InvocationAuthorizationExpired


class AuthorizedNewInvocationPort(Protocol):
    async def invoke_authorized(self, query: TextInvocationQuery,
                                authorization: AuthorizationContext) -> InvocationReply:
        """Create admission/routing and execute through durable terminal settlement."""
        ...


class AdmissionControlledInvocation:
    def __init__(self, *, capacity: AdmissionCapacity, api_rate: ApiRate, backend: AuthorizedNewInvocationPort,
                 authorize: Callable[[TextInvocationQuery], Awaitable[AuthorizationContext]],
                 utcnow=lambda: datetime.now(timezone.utc)):
        self._capacity, self._backend = capacity, backend
        self._api_rate = api_rate
        self._authorize, self._utcnow = authorize, utcnow

    async def invoke(self, query: TextInvocationQuery) -> InvocationReply:
        with trace_stage(TraceStage.AUTHORIZATION):
            authorization = await self._authorize(query)
            if not isinstance(authorization, AuthorizationContext):
                raise TypeError("Authorizer must return a trusted Authorization Context")
            now = self._utcnow()
            if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() != timedelta(0):
                raise ValueError("UTC admission clock required")
            if authorization.expires_at <= now:
                raise InvocationAuthorizationExpired()
        # One lease spans shell creation, routing, every Attempt/backoff and
        # final commit. Synchronous exit cannot be interrupted by cancellation.
        with self._capacity.acquire(authorization):
            if not self._api_rate.try_consume(authorization):
                raise ModelInvocationRejected("rate_limited")
            return await self._backend.invoke_authorized(query, authorization)
