"""Closed, explicit development deployment input; no dotenv discovery."""

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator
from urllib.parse import urlsplit

from llm_gateway.adapters.configuration_json import ParseLimits, parse_strict_json
from llm_gateway.domain.fingerprint_keys import FingerprintKeyMember, FingerprintKeyRing


class Closed(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class Authorization(Closed):
    mode: Literal["dev_bypass"] = "dev_bypass"


class FingerprintFile(Closed):
    key_id: str = Field(min_length=1, max_length=128)
    key_version: str = Field(min_length=1, max_length=128)
    role: Literal["active", "verification_only"]
    secret_ref: str = Field(min_length=1, max_length=128)
    file: str


class DevelopmentTelemetry(Closed):
    endpoint: str = Field(max_length=128)

    @field_validator("endpoint")
    @classmethod
    def local_origin(cls, value):
        # No DNS resolution: this dev integration reaches only a local sidecar.
        parsed = urlsplit(value)
        try:
            port = parsed.port
        except ValueError:
            raise ValueError("Local Collector origin required") from None
        if (parsed.scheme != "http" or parsed.hostname not in {"127.0.0.1", "::1"}
                or port is None or not 1 <= port <= 65535
                or value not in {f"http://127.0.0.1:{port}", f"http://[::1]:{port}"}):
            raise ValueError("Explicit local Collector origin required")
        return value


class Bootstrap(Closed):
    environment_id: str = Field(min_length=1, max_length=128)
    startup_profile: Literal["dev"] = "dev"
    authorization: Authorization = Field(default_factory=Authorization)
    bind_host: Literal["127.0.0.1", "0.0.0.0"] = "127.0.0.1"
    model_port: int = Field(default=8000, ge=1, le=65535)
    management_port: int = Field(default=8001, ge=1, le=65535)
    database_url_file: str
    expected_schema_version: Literal["0018_invocation_correlation.sql"]
    fingerprint_keys: list[FingerprintFile] = Field(min_length=1, max_length=9)
    provider_secret_files: dict[str, str]
    telemetry: DevelopmentTelemetry | None = None

    def ring(self):
        return FingerprintKeyRing(tuple(FingerprintKeyMember(
            item.key_id, item.key_version, item.role, item.secret_ref) for item in self.fingerprint_keys))


def load_bootstrap(path: Path) -> Bootstrap:
    with path.open("rb") as stream:
        raw = stream.read(65537)
    result = Bootstrap.model_validate(parse_strict_json(raw, ParseLimits(65536, 16, 2048, 128, 4096)))
    if result.model_port == result.management_port:
        raise ValueError("Listeners require distinct ports")
    paths = [result.database_url_file, *result.provider_secret_files.values(),
             *(item.file for item in result.fingerprint_keys)]
    if any(not Path(value).is_absolute() or value.startswith(("//", "\\\\")) for value in paths):
        raise ValueError("Absolute local Secret files required")
    if len(set(paths)) != len(paths):
        raise ValueError("Secret files must be distinct")
    result.ring()
    return result


def read_database_url(config):
    with Path(config.database_url_file).open("rb") as stream:
        raw = stream.read(16385)
    if len(raw) > 16384:
        raise ValueError("Invalid database Secret")
    value = raw.decode("utf-8").rstrip("\r\n")
    if not value.startswith(("postgresql://", "postgres://")) or any(ord(c) < 32 for c in value):
        raise ValueError("Invalid database Secret")
    return value
