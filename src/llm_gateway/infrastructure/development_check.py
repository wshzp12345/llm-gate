"""Read-only local environment preflight. This command never starts a Gateway."""

import argparse
import asyncio
import json
from pathlib import Path

from llm_gateway.domain.configuration import ConfigurationDeadlineExceeded, ConfigurationPersistenceUnavailable
from llm_gateway.infrastructure.development_environment import load_development_environment, DevelopmentEnvironmentError, gateway_database_url
from llm_gateway.infrastructure.migrate import verify_schema


async def check(path: Path) -> tuple[int, dict]:
    result = {"environment": "invalid", "schema": "not_checked", "gateway_ready": False}
    try:
        environment = load_development_environment(path)
        database_url = gateway_database_url(environment.database_url)
    except DevelopmentEnvironmentError:
        return 2, result
    result.update(environment="loaded", deepseek_key_configured=environment.deepseek_api_key is not None)
    try:
        await verify_schema(database_url, expected_version="0018_invocation_correlation.sql", timeout_seconds=5)
    except ConfigurationDeadlineExceeded:
        result["schema"] = "deadline_exceeded"
        return 1, result
    except ConfigurationPersistenceUnavailable:
        result["schema"] = "unavailable_or_incompatible"
        return 1, result
    result["schema"] = "verified"
    return 0, result


def main():
    parser = argparse.ArgumentParser(description="Development-only, read-only database preflight; no migration or Provider calls")
    parser.add_argument("--env-file", type=Path, default=Path(".env.local"))
    args = parser.parse_args()
    try:
        code, result = asyncio.run(check(args.env_file), loop_factory=asyncio.SelectorEventLoop)
    except KeyboardInterrupt:
        raise SystemExit(130) from None
    except Exception:
        # Never print raw library exceptions, DSNs, credentials or file paths.
        code, result = 1, {"environment": "check_failed", "schema": "not_verified", "gateway_ready": False}
    print(json.dumps(result, sort_keys=True))
    raise SystemExit(code)


if __name__ == "__main__":
    main()
