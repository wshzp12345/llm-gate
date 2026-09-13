"""Gateway-owned Prompt DTOs. Register only on an authenticated management app."""

import asyncio
from typing import Annotated, Literal
from uuid import UUID

from fastapi import Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from llm_gateway.adapters.configuration_http import _negotiate, RequestFailure
from llm_gateway.adapters.configuration_json import ConfigurationStructureError, ParseLimits, parse_strict_json
from llm_gateway.application.authorization import Forbidden, Unauthorized, AuthorizationUnavailable
from llm_gateway.application.prompts import PromptNotFound, PromptConflict, PromptPersistenceUnavailable
from llm_gateway.domain.model import Message
from llm_gateway.domain.prompts import PromptInvalid


class TemplateMessage(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    role: Literal["system", "developer", "user", "assistant"]
    content: str


class VersionBody(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    messages: Annotated[list[TemplateMessage], Field(min_length=1, max_length=100)]


class PublicationBody(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    version_id: str
    expected_generation: Annotated[int, Field(ge=0, lt=2**63 - 1)]


class RenderBody(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    variables: dict[str, str]


_LIMITS = ParseLimits(1024 * 1024, 8, 2048, 256, 256 * 1024)
_STATUS = {"invalid_request": 400, "unauthorized": 401, "forbidden": 403, "not_found": 404,
           "not_acceptable": 406, "conflict": 409, "request_too_large": 413,
           "unsupported_media_type": 415, "invalid_template": 422, "invalid_variables": 422,
           "persistence_unavailable": 503, "authorization_unavailable": 503, "deadline_exceeded": 504,
           "internal": 500}


def _uuid(value):
    try:
        parsed = UUID(value)
        if str(parsed) != value:
            raise ValueError()
        return parsed
    except (ValueError, TypeError, AttributeError):
        raise RequestFailure("invalid_request") from None


def _asset_view(asset):
    return {"asset_id": str(asset.asset_id), "generation": asset.generation,
            "published_version": str(asset.published_version) if asset.published_version else None}


def _version_view(version):
    return {"asset_id": str(version.asset_id), "version_id": str(version.version_id),
            "messages": [{"role": item.role, "content": item.text} for item in version.messages],
            "variables": list(version.variables)}


def register_prompt_routes(app, service, *, authorize):
    """authorize(request) must return a validated request-bound context."""

    async def execute(request, operation, asset_id=None, version_id=None):
        code = None
        try:
            async with asyncio.timeout(5):
                context = await authorize(request)
                if request.url.query or any(name.lower().startswith("if-") for name in request.headers):
                    raise RequestFailure("invalid_request")
                if "content-encoding" in request.headers or "idempotency-key" in request.headers or "x-gateway-command-id" in request.headers:
                    raise RequestFailure("invalid_request")
                _negotiate(request, has_body=request.method == "POST")
                body = bytearray()
                if hasattr(request.state, "bounded_body"):
                    body.extend(request.state.bounded_body)
                else:
                    async for chunk in request.stream():
                        if len(body) + len(chunk) > _LIMITS.max_bytes:
                            raise RequestFailure("request_too_large")
                        body.extend(chunk)
                if len(body) > _LIMITS.max_bytes:
                    raise RequestFailure("request_too_large")
                if request.method == "GET" and body:
                    raise RequestFailure("invalid_request")
                value = parse_strict_json(bytes(body), _LIMITS) if request.method == "POST" else None
                asset = _uuid(asset_id) if asset_id is not None else None
                version = _uuid(version_id) if version_id is not None else None
                status = 200
                if operation in {"create", "add"}:
                    payload = VersionBody.model_validate(value)
                    messages = tuple(Message(item.role, item.content) for item in payload.messages)
                    result = await service.create(context, messages) if operation == "create" else await service.add_version(context, asset, messages)
                    response, status = _version_view(result), 201
                elif operation == "asset":
                    response = _asset_view(await service.asset(context, asset))
                elif operation == "version":
                    response = _version_view(await service.version(context, asset, version))
                elif operation == "publish":
                    payload = PublicationBody.model_validate(value)
                    response = _asset_view(await service.publish(context, asset, _uuid(payload.version_id), payload.expected_generation))
                else:
                    payload = RenderBody.model_validate(value)
                    result = await service.render(context, asset, version, payload.variables)
                    response = {"asset_id": str(result.asset_id), "version_id": str(result.version_id),
                                "messages": [{"role": item.role, "content": item.text} for item in result.messages]}
                return JSONResponse(response, status_code=status, headers={"Cache-Control": "no-store"})
        except PromptInvalid as error:
            code = "request_too_large" if error.code == "prompt_too_large" else "invalid_request" if error.code == "invalid_identity" else error.code
        except RequestFailure as error:
            code = error.code
        except (ConfigurationStructureError, ValidationError):
            code = "invalid_request"
        except PromptNotFound:
            code = "not_found"
        except PromptConflict:
            code = "conflict"
        except PromptPersistenceUnavailable:
            code = "persistence_unavailable"
        except Forbidden:
            code = "forbidden"
        except Unauthorized:
            code = "unauthorized"
        except AuthorizationUnavailable:
            code = "authorization_unavailable"
        except TimeoutError:
            code = "deadline_exceeded"
        except Exception:
            code = "internal"
        return JSONResponse({"error": {"code": code, "type": "gateway_error", "message": "Prompt request could not be completed."}},
                            status_code=_STATUS[code], headers={"Cache-Control": "no-store", **({"WWW-Authenticate": "Bearer"} if code == "unauthorized" else {})})

    @app.post("/gateway/v1/prompts")
    async def create(request: Request):
        return await execute(request, "create")

    @app.get("/gateway/v1/prompts/{asset_id}")
    async def asset(asset_id: str, request: Request):
        return await execute(request, "asset", asset_id)

    @app.post("/gateway/v1/prompts/{asset_id}/versions")
    async def add(asset_id: str, request: Request):
        return await execute(request, "add", asset_id)

    @app.get("/gateway/v1/prompts/{asset_id}/versions/{version_id}")
    async def version(asset_id: str, version_id: str, request: Request):
        return await execute(request, "version", asset_id, version_id)

    @app.post("/gateway/v1/prompts/{asset_id}/publish")
    async def publish(asset_id: str, request: Request):
        return await execute(request, "publish", asset_id)

    @app.post("/gateway/v1/prompts/{asset_id}/versions/{version_id}/render")
    async def render(asset_id: str, version_id: str, request: Request):
        return await execute(request, "render", asset_id, version_id)
