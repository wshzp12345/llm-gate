"""Only for the isolated Compose acceptance volume; never reads real secrets."""

import json
from pathlib import Path
import secrets
from uuid import uuid4


root = Path("/fixtures")
config = json.loads(Path("/acceptance/bootstrap.json").read_bytes())
config["environment_id"] = "recovery-acceptance"
config["telemetry"] = {"endpoint": "http://127.0.0.1:4318"}
config["database_url_file"] = str(root / "database-url")
config["fingerprint_keys"][0]["file"] = str(root / "fingerprint-key")
config["provider_secret_files"] = {f"mock-{name}-key": str(root / f"provider-{name}.json") for name in ("a", "b", "c")}
for name in ("a", "b", "c"):
    (root / f"provider-{name}.json").write_text(json.dumps({"secret_version": str(uuid4()),
        "value": f"synthetic-{name}-credential", "revoked": False, "valid_until": "9999-12-31T23:59:59Z"}))
(root / "fingerprint-key").write_bytes(secrets.token_bytes(32))
(root / "database-url").write_text("postgresql://gateway:acceptance-only@postgres/gateway?options=-csearch_path%3Dllm_gateway")
(root / "bootstrap.json").write_text(json.dumps(config))
