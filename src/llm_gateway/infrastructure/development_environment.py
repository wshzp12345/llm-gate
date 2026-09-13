"""Explicit development-only dotenv delivery, not the production Bootstrap.

No directory search, environment mutation, interpolation, shell evaluation,
watching, or diagnostic printing of file contents is performed.
"""

import io
import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path

from dotenv.parser import parse_stream
import psycopg
from psycopg.conninfo import conninfo_to_dict, make_conninfo


class DevelopmentEnvironmentError(Exception):
    def __init__(self):
        super().__init__("Development environment is missing or invalid")


@dataclass(frozen=True)
class DevelopmentEnvironment:
    database_url: str = field(repr=False)
    deepseek_api_key: str | None = field(default=None, repr=False)


def gateway_database_url(database_url: str) -> str:
    """Use only the user-approved Gateway schema; do not alter role defaults."""
    try:
        options = conninfo_to_dict(database_url).get("options", "")
        return make_conninfo(database_url, options=options + " -csearch_path=llm_gateway")
    except psycopg.Error:
        raise DevelopmentEnvironmentError() from None


def load_development_environment(path: Path, *, environ: Mapping[str, str] | None = None) -> DevelopmentEnvironment:
    environment = os.environ if environ is None else environ
    names = {"GATEWAY_DATABASE_URL", "AGENT_RUNTIME_POSTGRES_URL", "DEEPSEEK_API_KEY"}
    values = {}
    try:
        if path.exists():
            with path.open("rb") as stream:
                raw = stream.read(1024 * 1024 + 1)
            if len(raw) > 1024 * 1024:
                raise DevelopmentEnvironmentError()
            for binding in parse_stream(io.StringIO(raw.decode("utf-8-sig"))):
                if binding.error:
                    raise DevelopmentEnvironmentError()
                if binding.key in names:
                    if binding.key in values or binding.value is None:
                        raise DevelopmentEnvironmentError()
                    values[binding.key] = binding.value
        # Environment delivery wins across both accepted database names. The
        # Gateway-specific name wins when both names exist in the same source.
        database = next((source[name] for source in (environment, values)
                         for name in ("GATEWAY_DATABASE_URL", "AGENT_RUNTIME_POSTGRES_URL")
                         if name in source), None)
        key = environment.get("DEEPSEEK_API_KEY", values.get("DEEPSEEK_API_KEY"))
        if not database or not database.strip() or any(c in database for c in "\x00\r\n"):
            raise DevelopmentEnvironmentError()
        if key is not None and (not key.strip() or any(c in key for c in "\x00\r\n")):
            raise DevelopmentEnvironmentError()
        # SQLAlchemy's psycopg driver marker is not accepted by libpq. Change
        # only this exact prefix; retain escaped credentials and query options.
        if database.startswith("postgresql+psycopg://"):
            database = "postgresql://" + database[len("postgresql+psycopg://"):]
        return DevelopmentEnvironment(database, key)
    except (OSError, UnicodeError):
        raise DevelopmentEnvironmentError() from None
