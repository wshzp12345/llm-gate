from dataclasses import replace
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from llm_gateway.domain.invocation import AuthorizationContext, InvocationAdmission


def authorization(**changes):
    return replace(AuthorizationContext("tenant", "subject", frozenset({"gateway.model.invoke"}),
                                       "issuer", "audience", datetime.now(timezone.utc) + timedelta(hours=1),
                                       "local_jwt"), **changes)


def admission(revision="1", **changes):
    return replace(InvocationAdmission(uuid4(), "1" * 32, revision, "general", authorization()), **changes)


@pytest.mark.parametrize("changes", [
    {"tenant_id": " leading"}, {"subject": "trailing "}, {"issuer": "a\x00b"},
    {"audience": "a\x85b"}, {"subject": "中" * 86}, {"tenant_id": ""},
    {"expires_at": datetime(2026, 1, 1)},
    {"expires_at": datetime(2026, 1, 1, tzinfo=timezone(timedelta(hours=1)))},
    {"authentication_method": "unknown"}, {"scopes": {"gateway.model.invoke"}},
    {"scopes": frozenset({"*"})}, {"scopes": frozenset({"dev:*"})},
    {"scopes": frozenset({"Gateway.Model.Invoke"})},
    {"scopes": frozenset("s" + str(i) for i in range(65))},
    {"scopes": frozenset({"a" * 129})},
])
def test_invalid_authorization_context(changes):
    with pytest.raises(ValueError):
        authorization(**changes)


def test_empty_scopes_unknown_tokens_and_dev_marker_are_preserved():
    assert authorization(scopes=frozenset()).scopes == frozenset()
    assert authorization(scopes=frozenset({"unregistered"})).scopes == frozenset({"unregistered"})
    assert authorization(authentication_method="dev_bypass", scopes=frozenset({"dev:*"})).scopes == frozenset({"dev:*"})


@pytest.mark.parametrize("changes", [
    {"call_id": "not-a-uuid"}, {"trace_id": "0" * 32}, {"trace_id": "A" * 32},
    {"configuration_revision": "01"}, {"requested_model": "provider/model"},
])
def test_invalid_admission_identity(changes):
    with pytest.raises(ValueError):
        admission(**changes)


def test_journal_rejects_unregistered_error_text_before_connecting():
    import asyncio
    from llm_gateway.domain.model import ProviderFailure
    from llm_gateway.domain.recovery import RecoveryPlan
    from llm_gateway.infrastructure.invocation import PostgresInvocationStore

    journal = PostgresInvocationStore("must not connect").journal(uuid4())
    with pytest.raises(ValueError, match="stable Gateway errors"):
        asyncio.run(journal.finished(1, ProviderFailure("untrusted-provider-text", False), RecoveryPlan("stop")))
