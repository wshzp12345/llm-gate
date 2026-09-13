import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from uuid import UUID, uuid4

import httpx
import pytest

from llm_gateway.adapters.configuration_http import create_development_management_app
from llm_gateway.domain.configuration import CommandOutcome, Revision, RevisionState, ConfigurationPersistenceUnavailable
from tests.test_configuration_dto import LIMITS


REVISION = Revision("9007199254740993", RevisionState.ACTIVE, None, "sha256:" + "a" * 64,
                    datetime(2026, 9, 8, tzinfo=timezone.utc))
BODY = {"expected_active_revision": "0", "candidate_snapshot_digest": REVISION.snapshot_digest, "description": "publish"}


class UnitOfWork:
    reads = 0
    selected = None

    @asynccontextmanager
    async def transaction(self, timeout_seconds):
        self.reads += 1
        yield self

    async def lock_active(self):
        return self.selected

    async def get_revision(self, revision):
        return REVISION


class Commands:
    def __init__(self):
        self.calls = []
        self.outcome = CommandOutcome(revision=REVISION)

    async def publish(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return self.outcome


def access(method="GET", path="/gateway/v1/config/active", *, commands=None, uow=None, limits=LIMITS, validator=None, **kwargs):
    commands = commands or Commands()
    uow = uow or UnitOfWork()
    async def scenario():
        app = create_development_management_app(commands, uow, limits, command_timeout_seconds=3, validator=validator)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://management.test") as client:
            return await client.request(method, path, **kwargs)
    return asyncio.run(scenario())


def test_empty_active_and_response_headers():
    response = access()
    assert response.status_code == 200
    assert response.json() == {"active": None}
    assert response.headers["cache-control"] == "no-store"
    assert UUID(response.headers["x-request-id"]).version == 4
    assert "etag" not in response.headers


def test_active_projection_has_only_closed_metadata():
    uow = UnitOfWork()
    uow.selected = REVISION.revision
    result = access(uow=uow).json()["active"]
    assert set(result) == {"revision", "state", "base_revision", "snapshot_digest", "created_at", "rollback_of"}
    assert result["revision"] == "9007199254740993"
    assert result["created_at"] == "2026-09-08T00:00:00.000000Z"


@pytest.mark.parametrize("headers,status", [
    ({"If-Match": "anything"}, 400), ({"X-Gateway-Command-Id": str(uuid4())}, 400),
    ({"Accept": "text/plain"}, 406), ({"Accept": "application/json;q=0, */*;q=1"}, 406),
])
def test_read_failures_prevent_persistence(headers, status):
    uow = UnitOfWork()
    response = access(headers=headers, uow=uow)
    assert response.status_code == status
    assert uow.reads == 0
    assert response.headers["cache-control"] == "no-store"
    assert "error" in response.json()


def test_publish_translates_command_and_echoes_identity():
    command_id, commands = str(uuid4()), Commands()
    response = access("POST", f"/gateway/v1/config/revisions/{REVISION.revision}/publish",
                      commands=commands, json=BODY, headers={"X-Gateway-Command-Id": command_id})
    assert response.status_code == 200
    assert response.headers["X-Gateway-Command-Id"] == command_id
    args, kwargs = commands.calls[0]
    assert args[0].tenant_id == args[0].subject == "dev"
    assert args[0].command_id == command_id
    assert args[2] is None
    assert response.json()["active"]["revision"] == REVISION.revision


@pytest.mark.parametrize("failure", [None, "conflict", "refresh", "timeout"])
def test_publication_refresh_runs_after_command_success_and_failures_are_safe(failure):
    async def run():
        events = []
        class Publishing(Commands):
            async def publish(self, *args, **kwargs):
                events.append("committed" if failure != "conflict" else "conflict")
                return CommandOutcome(error="conflict") if failure == "conflict" else self.outcome
        async def refresh():
            assert events == ["committed"]
            events.append("refresh")
            if failure == "refresh":
                raise ConfigurationPersistenceUnavailable()
            if failure == "timeout":
                await asyncio.Event().wait()
        app = create_development_management_app(Publishing(), UnitOfWork(), LIMITS,
            command_timeout_seconds=0.01 if failure == "timeout" else 3, after_publication=refresh)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://management.test") as client:
            response = await client.post(f"/gateway/v1/config/revisions/{REVISION.revision}/publish",
                json=BODY, headers={"X-Gateway-Command-Id": str(uuid4())})
        assert response.status_code == {None: 200, "conflict": 409, "refresh": 503, "timeout": 504}[failure]
        assert events == (["conflict"] if failure == "conflict" else ["committed", "refresh"])
    asyncio.run(run())


@pytest.mark.parametrize("headers,body,status", [
    ({}, BODY, 400),
    ({"X-Gateway-Command-Id": "bad"}, BODY, 400),
    ({"X-Gateway-Command-Id": str(uuid4()), "If-None-Match": "*"}, BODY, 400),
    ({"X-Gateway-Command-Id": str(uuid4()), "Content-Type": "application/yaml"}, BODY, 415),
    ({"X-Gateway-Command-Id": str(uuid4()), "Content-Type": "application/json;charset=latin1"}, BODY, 415),
    ({"X-Gateway-Command-Id": str(uuid4())}, BODY | {"command_id": "forbidden"}, 400),
    ({"X-Gateway-Command-Id": str(uuid4())}, BODY | {"expected_active_revision": 0}, 400),
])
def test_publish_rejects_before_command_execution(headers, body, status):
    commands = Commands()
    response = access("POST", "/gateway/v1/config/revisions/1/publish", commands=commands, json=body, headers=headers)
    assert response.status_code == status
    assert commands.calls == []


def test_duplicate_command_header_is_invalid():
    command_id = str(uuid4())
    response = access("POST", "/gateway/v1/config/revisions/1/publish", json=BODY,
                      headers=[("X-Gateway-Command-Id", command_id), ("X-Gateway-Command-Id", command_id)])
    assert response.status_code == 400


def test_provider_or_parser_details_are_not_disclosed():
    response = access("POST", "/gateway/v1/config/revisions/1/publish", content=b'{"sensitive":',
                      headers={"Content-Type": "application/json", "X-Gateway-Command-Id": str(uuid4())})
    assert response.status_code == 400
    assert "sensitive" not in response.text


def test_command_conflict_is_not_success():
    commands = Commands()
    commands.outcome = CommandOutcome(error="conflict")
    response = access("POST", "/gateway/v1/config/revisions/1/publish", commands=commands, json=BODY,
                      headers={"X-Gateway-Command-Id": str(uuid4())})
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "conflict"


def test_database_failure_is_safe_503():
    class Unavailable(UnitOfWork):
        async def lock_active(self):
            raise ConfigurationPersistenceUnavailable()
    response = access(uow=Unavailable())
    assert response.status_code == 503
    assert response.headers["cache-control"] == "no-store"


def test_internal_exception_is_safe_500():
    class Broken(UnitOfWork):
        async def lock_active(self):
            raise RuntimeError("sensitive database details")
    response = access(uow=Broken())
    assert response.status_code == 500
    assert "sensitive" not in response.text


def test_size_limit_precedes_negotiation():
    from dataclasses import replace
    response = access("POST", "/gateway/v1/config/revisions/1/publish", content=b"x" * 11,
                      limits=replace(LIMITS, max_bytes=10), headers={"Accept": "text/plain"})
    assert response.status_code == 413


def test_unimplemented_routes_are_not_registered():
    response = access(path="/v1/chat/completions")
    assert response.status_code == 404
    assert response.json()["error"]["code"] == "not_found"
    assert response.headers["cache-control"] == "no-store"


def local_validator():
    from llm_gateway.application.configuration_validation import LocalConfigurationValidator
    from tests.test_configuration_validation import POLICY, NoTrustBundleExpected
    return LocalConfigurationValidator(POLICY, NoTrustBundleExpected())


def test_validate_bundle_returns_closed_dry_run_response():
    from tests.config_fixtures import bundle_submission
    response = access("POST", "/gateway/v1/config/validate", json=bundle_submission(), validator=local_validator())
    assert response.status_code == 200
    body = response.json()
    assert set(body) == {"valid", "base_revision", "snapshot_digest", "change_set", "impact", "diagnostics", "diagnostics_truncated"}
    assert body["valid"] and body["base_revision"] is None
    assert len(body["impact"]) == 8


def test_validate_semantic_failure_is_200_invalid_not_request_failure():
    from tests.config_fixtures import bundle_submission
    body = bundle_submission()
    body["bundle"]["provider_model_bindings"]["binding-a"]["provider"] = "missing"
    response = access("POST", "/gateway/v1/config/validate", json=body, validator=local_validator())
    assert response.status_code == 200
    result = response.json()
    assert result["valid"] is False
    assert result["snapshot_digest"] is result["change_set"] is result["impact"] is None
    assert result["diagnostics"][0]["reason"] == "reference_not_found"


def test_validate_rejects_command_header_before_database():
    from tests.config_fixtures import bundle_submission
    uow = UnitOfWork()
    response = access("POST", "/gateway/v1/config/validate", json=bundle_submission(), validator=local_validator(),
                      uow=uow, headers={"X-Gateway-Command-Id": str(uuid4())})
    assert response.status_code == 400
    assert uow.reads == 0


def test_yaml_bundle_and_json_share_validation_digest():
    import json
    from tests.config_fixtures import bundle_submission
    body = bundle_submission()
    json_result = access("POST", "/gateway/v1/config/validate", json=body, validator=local_validator())
    yaml_result = access("POST", "/gateway/v1/config/validate", content=json.dumps(body["bundle"]),
                         headers={"Content-Type": "application/yaml"}, validator=local_validator())
    assert yaml_result.status_code == 200
    assert yaml_result.json() == json_result.json()


def test_export_returns_yaml_and_original_creation_metadata():
    from llm_gateway.domain.configuration import RevisionExport
    from llm_gateway.adapters.configuration_yaml import parse_bundle_yaml
    from tests.test_configuration_preparation import draft
    class ExportUnitOfWork(UnitOfWork):
        async def get_export(self, revision):
            return RevisionExport(revision, draft().snapshot_digest, draft().snapshot_json, "original creation")
    response = access(path="/gateway/v1/config/revisions/1/export", uow=ExportUnitOfWork(), headers={"Accept": "application/yaml"})
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/yaml")
    parsed = parse_bundle_yaml(response.content, LIMITS)
    assert parsed.base_revision == "1"
    assert parsed.metadata.description == "original creation"
    assert response.headers["x-gateway-snapshot-digest"] == draft().snapshot_digest
    assert response.headers["cache-control"] == "no-store"
    assert "etag" not in response.headers


@pytest.mark.parametrize("headers,status", [
    ({"Accept": "application/json"}, 406),
    ({"Accept": "application/yaml;q=0, */*;q=1"}, 406),
    ({"If-Modified-Since": "ignored"}, 400),
    ({"X-Gateway-Command-Id": str(uuid4())}, 400),
])
def test_export_rejects_before_lookup(headers, status):
    uow = UnitOfWork()
    response = access(path="/gateway/v1/config/revisions/1/export", uow=uow, headers=headers)
    assert response.status_code == status
    assert uow.reads == 0


def test_missing_export_is_safe_404():
    class Missing(UnitOfWork):
        async def get_export(self, revision):
            return None
    response = access(path="/gateway/v1/config/revisions/1/export", uow=Missing())
    assert response.status_code == 404
    assert response.json()["error"]["code"] == "not_found"
