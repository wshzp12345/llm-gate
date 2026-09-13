"""Explicit host-side preparation of new, private development Secret files."""

import argparse
import json
import os
from pathlib import Path
import secrets
from uuid import uuid4

from llm_gateway.infrastructure.development_environment import load_development_environment


def initialize(directory, *, env_file=None):
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    names = ("postgres-password", "database-url", "fingerprint-key", "provider.json")
    if any((directory / name).exists() for name in names):
        raise ValueError("Refusing to overwrite existing development Secrets")
    key = None
    if env_file is not None:
        key = load_development_environment(env_file).deepseek_api_key
        if not key:
            raise ValueError("No DeepSeek credential in the selected development source")
    password = secrets.token_urlsafe(32)
    values = {"postgres-password": password.encode(),
        "database-url": f"postgresql://gateway:{password}@postgres:5432/gateway?options=-csearch_path%3Dllm_gateway".encode(),
        "fingerprint-key": secrets.token_bytes(32),
        "provider.json": json.dumps({"secret_version": str(uuid4()), "value": key or "development-placeholder",
            "valid_until": "9999-12-31T23:59:59Z", "revoked": key is None}).encode()}
    for name, value in values.items():
        descriptor = os.open(directory / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(value)


def main():
    parser = argparse.ArgumentParser(description="Create fresh dev Secrets; never overwrite or print them")
    parser.add_argument("--directory", type=Path, default=Path("deploy/local"))
    parser.add_argument("--provider-from-env-file", type=Path)
    args = parser.parse_args()
    try:
        initialize(args.directory, env_file=args.provider_from_env_file)
    except Exception:
        raise SystemExit("Development initialization failed; check source and use a fresh output directory") from None
    print("Development Secret files created; retain them with the database, never commit them")


if __name__ == "__main__":
    main()
