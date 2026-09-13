"""Initial dev-only management HTTP surface, not a production composition root.

Active read, export, publication and internal health probes are registered;
validate/create/rebase/rollback also require a semantic validator. No automatic database setup
or configuration seeding is performed by this module.
"""

import re
import json
import asyncio
from collections.abc import Callable
from datetime import timezone
from uuid import UUID, uuid4

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response
from pydantic import ValidationError

from llm_gateway.adapters.canonical_json import canonical_digest
from llm_gateway.adapters.health_http import not_ready, register_health_routes
from llm_gateway.adapters.configuration_dto import PublishRequest, BundleSubmission, RebaseOrRollbackRequest
from llm_gateway.adapters.configuration_json import ConfigurationStructureError, ParseLimits, parse_strict_json, parse_configuration_json
from llm_gateway.adapters.configuration_yaml import parse_bundle_yaml, export_bundle_yaml
from llm_gateway.adapters.configuration_submission import submission_base, submission_preparer, submission_semantic, RollbackPreparer, RebasePreparer
from llm_gateway.application.configuration_validation_query import validate_candidate
from llm_gateway.application.configuration import ConfigurationCommands, ConfigurationUnitOfWork
from llm_gateway.domain.configuration import (
    CommandIdentity, ConfigurationDeadlineExceeded, ConfigurationPersistenceUnavailable,
    Revision, revision_number,
)


_ERRORS = {
    "invalid_request": (400, "Invalid request."), "not_found": (404, "Resource not found."),
    "not_acceptable": (406, "Requested representation is unavailable."),
    "unsupported_media_type": (415, "Unsupported request media type."),
    "request_too_large": (413, "Request exceeds the configured size limit."),
    "conflict": (409, "Configuration command conflicts with current state."),
    "configuration_invalid": (422, "Configuration is invalid."),
    "persistence_unavailable": (503, "Configuration persistence is unavailable."),
    "deadline_exceeded": (504, "Configuration command deadline exceeded."),
    "internal": (500, "Internal gateway error."),
}


class RequestFailure(Exception):
    def __init__(self, code):
        self.code = code


def _error(code, diagnostics=(), truncated=False):
    status, message = _ERRORS[code]
    body = {"code": code, "message": message}
    if diagnostics:
        body["gateway"] = {"diagnostics": [{"reason": item.reason, "path": item.path} for item in diagnostics],
                           "diagnostics_truncated": truncated}
    return JSONResponse({"error": body}, status_code=status)


def _one(request, name, *, required=False):
    values = request.headers.getlist(name)
    if len(values) > 1 or (required and not values):
        raise RequestFailure("invalid_request")
    return values[0] if values else None


def _accepts(header, representation="application/json"):
    if header is None:
        return True
    matches = []
    for entry in header.split(","):
        parts = [part.strip().lower() for part in entry.split(";")]
        media, quality = parts[0], 1.0
        for parameter in parts[1:]:
            if not re.fullmatch(r"q=(?:0(?:\.[0-9]{0,3})?|1(?:\.0{0,3})?)", parameter):
                return False
            quality = float(parameter[2:])
        wildcard = representation.split("/", 1)[0] + "/*"
        if media in {representation, wildcard, "*/*"}:
            matches.append(({representation: 2, wildcard: 1, "*/*": 0}[media], quality))
    if not matches:
        return False
    specificity = max(level for level, _ in matches)
    return max(quality for level, quality in matches if level == specificity) > 0


def _negotiate(request, *, has_body, allow_yaml=False):
    if not _accepts(_one(request, "accept")):
        raise RequestFailure("not_acceptable")
    if has_body:
        media = _one(request, "content-type")
        if media is None:
            raise RequestFailure("unsupported_media_type")
        parts = [part.strip().lower() for part in media.split(";")]
        allowed = {"application/json", "application/yaml"} if allow_yaml else {"application/json"}
        if parts[0] not in allowed or len(parts) > 2 or (len(parts) == 2 and parts[1] not in {"charset=utf-8", 'charset="utf-8"'}):
            raise RequestFailure("unsupported_media_type")
        return parts[0]


def _headers(request, *, mutation):
    if any(name.lower().startswith("if-") for name in request.headers):
        raise RequestFailure("invalid_request")
    if request.url.query:
        raise RequestFailure("invalid_request")
    command = _one(request, "x-gateway-command-id", required=mutation)
    if not mutation and command is not None:
        raise RequestFailure("invalid_request")
    if command is not None:
        try:
            parsed = UUID(command)
            if parsed.version != 4 or str(parsed) != command:
                raise ValueError("Invalid UUID")
        except ValueError:
            raise RequestFailure("invalid_request") from None
    return command


def _revision_view(revision: Revision):
    return {"revision": revision.revision,
            "state": revision.state.value, "base_revision": revision.base_revision,
            "snapshot_digest": revision.snapshot_digest,
            "created_at": revision.created_at.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z"),
            "rollback_of": revision.rollback_of}


def create_development_management_app(
    commands: ConfigurationCommands, unit_of_work: ConfigurationUnitOfWork,
    limits: ParseLimits, *, command_timeout_seconds: float,
    validator=None, readiness: Callable[[], bool] = not_ready, after_publication=None, lifespan=None,
) -> FastAPI:
    """Explicit dev bypass only. Never install this factory on a production ingress."""
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None, lifespan=lifespan)
    register_health_routes(app, readiness)

    @app.exception_handler(404)
    async def not_found(request, exception):
        return _error("not_found")

    @app.middleware("http")
    async def boundary(request: Request, call_next):
        # Probes do not consume request bodies, authenticate callers, or query
        # persistence. Data readiness must never gate configuration recovery.
        if request.method == "GET" and request.url.path in {"/healthz", "/readyz"}:
            return await call_next(request)
        request_id = str(uuid4())
        try:
            content_length = _one(request, "content-length")
            if content_length is not None:
                if not re.fullmatch(r"[0-9]+", content_length) or request.headers.get("transfer-encoding"):
                    raise RequestFailure("invalid_request")
                if int(content_length) > limits.max_bytes:
                    raise RequestFailure("request_too_large")
            body = bytearray()
            async for chunk in request.stream():
                if len(body) + len(chunk) > limits.max_bytes:
                    raise RequestFailure("request_too_large")
                body.extend(chunk)
            if content_length is not None and int(content_length) != len(body):
                raise RequestFailure("invalid_request")
            request.state.bounded_body = bytes(body)
            response = await call_next(request)
        except RequestFailure as failure:
            response = _error(failure.code)
        except (ConfigurationStructureError, ValidationError):
            response = _error("invalid_request")
        except ConfigurationDeadlineExceeded:
            response = _error("deadline_exceeded")
        except ConfigurationPersistenceUnavailable:
            response = _error("persistence_unavailable")
        except Exception:
            response = _error("internal")
        response.headers["Cache-Control"] = "no-store"
        response.headers["X-Request-Id"] = request_id
        return response

    @app.get("/gateway/v1/config/active")
    async def active(request: Request):
        _negotiate(request, has_body=False)
        _headers(request, mutation=False)
        async with unit_of_work.transaction(command_timeout_seconds) as tx:
            selected = await tx.lock_active()
            revision = await tx.get_revision(selected) if selected else None
            if selected is not None and revision is None:
                raise ConfigurationPersistenceUnavailable()
        return {"active": _revision_view(revision) if revision else None}

    @app.post("/gateway/v1/config/revisions/{revision}/publish")
    async def publish(revision: str, request: Request):
        _negotiate(request, has_body=True)
        command_id = _headers(request, mutation=True)
        try:
            revision_number(revision)
        except ValueError:
            raise RequestFailure("invalid_request") from None
        body = PublishRequest.model_validate(parse_strict_json(request.state.bounded_body, limits))
        command = CommandIdentity("dev", "dev", "publish", command_id,
                                  canonical_digest({"revision": revision, "body": body.model_dump()}))
        deadline = asyncio.get_running_loop().time() + command_timeout_seconds
        outcome = await commands.publish(
            command, revision, None if body.expected_active_revision == "0" else body.expected_active_revision,
            body.candidate_snapshot_digest, body.description, timeout_seconds=command_timeout_seconds,
        )
        if outcome.error:
            return _error(outcome.error, outcome.diagnostics, outcome.diagnostics_truncated)
        if after_publication is not None:
            # The command transaction has exited and committed. Replayed
            # commands refresh the current authority, not their historical result.
            try:
                async with asyncio.timeout_at(deadline):
                    await after_publication()
            except TimeoutError:
                raise ConfigurationDeadlineExceeded() from None
        return JSONResponse({"active": _revision_view(outcome.revision)}, headers={"X-Gateway-Command-Id": command_id})

    @app.get("/gateway/v1/config/revisions/{revision}/export")
    async def export(revision: str, request: Request):
        if not _accepts(_one(request, "accept"), "application/yaml"):
            raise RequestFailure("not_acceptable")
        _headers(request, mutation=False)
        try:
            revision_number(revision)
        except ValueError:
            raise RequestFailure("invalid_request") from None
        async with unit_of_work.transaction(command_timeout_seconds) as tx:
            selected = await tx.get_export(revision)
        if selected is None:
            raise RequestFailure("not_found")
        content = export_bundle_yaml(selected.snapshot_json, selected.revision, selected.creation_description)
        return Response(content, media_type="application/yaml",
                        headers={"X-Gateway-Snapshot-Digest": selected.snapshot_digest})

    if validator is not None:
        @app.post("/gateway/v1/config/revisions/{revision}/rebase")
        async def rebase(revision: str, request: Request):
            _negotiate(request, has_body=True)
            command_id = _headers(request, mutation=True)
            try:
                revision_number(revision)
            except ValueError:
                raise RequestFailure("invalid_request") from None
            body = RebaseOrRollbackRequest.model_validate(parse_strict_json(request.state.bounded_body, limits))
            command = CommandIdentity("dev", "dev", "rebase", command_id,
                                      canonical_digest({"revision": revision, "body": body.model_dump()}))
            outcome = await commands.rebase(command, revision, body.expected_active_revision, body.description,
                                            RebasePreparer(validator), timeout_seconds=command_timeout_seconds)
            if outcome.error:
                return _error(outcome.error, outcome.diagnostics, outcome.diagnostics_truncated)
            return JSONResponse({"revision": _revision_view(outcome.revision)}, status_code=201,
                                headers={"X-Gateway-Command-Id": command_id,
                                         "Location": f"/gateway/v1/config/revisions/{outcome.revision.revision}"})

        @app.post("/gateway/v1/config/revisions/{revision}/rollback")
        async def rollback(revision: str, request: Request):
            _negotiate(request, has_body=True)
            command_id = _headers(request, mutation=True)
            try:
                revision_number(revision)
            except ValueError:
                raise RequestFailure("invalid_request") from None
            body = RebaseOrRollbackRequest.model_validate(parse_strict_json(request.state.bounded_body, limits))
            command = CommandIdentity("dev", "dev", "rollback", command_id,
                                      canonical_digest({"revision": revision, "body": body.model_dump()}))
            outcome = await commands.rollback(command, revision, body.expected_active_revision, body.description,
                                              RollbackPreparer(validator), timeout_seconds=command_timeout_seconds)
            if outcome.error:
                return _error(outcome.error, outcome.diagnostics, outcome.diagnostics_truncated)
            return JSONResponse({"revision": _revision_view(outcome.revision)}, status_code=201,
                                headers={"X-Gateway-Command-Id": command_id,
                                         "Location": f"/gateway/v1/config/revisions/{outcome.revision.revision}"})

        def parse_submission(request, media):
            if media == "application/yaml":
                return BundleSubmission(kind="bundle", bundle=parse_bundle_yaml(request.state.bounded_body, limits))
            return parse_configuration_json(request.state.bounded_body, limits)

        @app.post("/gateway/v1/config/validate")
        async def validate(request: Request):
            media = _negotiate(request, has_body=True, allow_yaml=True)
            _headers(request, mutation=False)
            submission = parse_submission(request, media)
            outcome = await validate_candidate(unit_of_work, submission_base(submission),
                                               submission_preparer(submission, validator), command_timeout_seconds)
            prepared = outcome.prepared
            change_set = json.loads(prepared.change_set_json) if prepared else None
            impact = [{"section": item["section"], "resource_id": item.get("resource_id"), "change": item["op"]}
                      for item in change_set["operations"]] if change_set else None
            return {"valid": prepared is not None, "base_revision": outcome.base_revision,
                    "snapshot_digest": prepared.snapshot_digest if prepared else None,
                    "change_set": change_set, "impact": impact,
                    "diagnostics": [{"reason": item.reason, "path": item.path} for item in outcome.diagnostics],
                    "diagnostics_truncated": outcome.diagnostics_truncated}

        @app.post("/gateway/v1/config/revisions")
        async def create(request: Request):
            media = _negotiate(request, has_body=True, allow_yaml=True)
            command_id = _headers(request, mutation=True)
            submission = parse_submission(request, media)
            command = CommandIdentity("dev", "dev", "create", command_id, canonical_digest(submission_semantic(submission)))
            outcome = await commands.create(command, submission_base(submission), submission_preparer(submission, validator),
                                            timeout_seconds=command_timeout_seconds)
            if outcome.error:
                return _error(outcome.error, outcome.diagnostics, outcome.diagnostics_truncated)
            return JSONResponse({"revision": _revision_view(outcome.revision)}, status_code=201,
                                headers={"X-Gateway-Command-Id": command_id,
                                         "Location": f"/gateway/v1/config/revisions/{outcome.revision.revision}"})

    return app
