"""Status-only probes for the internal management listener.

The readiness reader must read an already aggregated local observation, never
perform dependency I/O. Until startup wires all required checks, fail closed.
Provider health/capacity and optional cache availability are not readiness gates.
"""

from collections.abc import Callable

from fastapi import FastAPI
from fastapi.responses import Response


def not_ready() -> bool:
    return False


def register_health_routes(app: FastAPI, readiness: Callable[[], bool]) -> None:
    @app.get("/healthz")
    async def liveness():
        return Response(status_code=200, headers={"Cache-Control": "no-store"})

    @app.get("/readyz")
    async def ready():
        try:
            available = readiness() is True
        except Exception:
            available = False
        return Response(status_code=200 if available else 503,
                        headers={"Cache-Control": "no-store"})
