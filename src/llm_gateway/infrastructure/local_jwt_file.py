"""Read one deployment-owned public-key file once, with no reload or discovery.

Deployment must mount this file read-only. This loader only opens for reading;
it does not change mount/ACL permissions, read environment key material, or
fetch URL/SecretSource keys. Replacing the file affects only a later startup.
"""

import re
from pathlib import Path

from cryptography.exceptions import UnsupportedAlgorithm
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from llm_gateway.adapters.configuration_json import ConfigurationStructureError
from llm_gateway.adapters.local_jwt import (
    LocalJwtAuthorization, LocalJwtConfigurationInvalid, canonical_base64url, key_id, strict_object,
)


def load_local_jwt(configuration, *, utcnow=None):
    try:
        fields = {"mode", "key_file", "format", "issuer", "audience"}
        if not isinstance(configuration, dict) or configuration.get("mode") != "local_jwt":
            raise ValueError()
        form = configuration.get("format")
        if form not in {"pem", "jwks"} or set(configuration) != fields | ({"kid"} if form == "pem" else set()):
            raise ValueError()
        if (not isinstance(configuration["key_file"], str)
                or configuration["key_file"].startswith(("\\\\", "//"))):
            raise ValueError()
        path = Path(configuration["key_file"])
        if not path.is_absolute() or not path.is_file():
            raise ValueError()
        with path.open("rb") as stream:
            body = stream.read(256 * 1024 + 1)
        if not body or len(body) > 256 * 1024:
            raise ValueError()
        if form == "pem":
            if re.fullmatch(rb"\s*-----BEGIN (PUBLIC KEY|RSA PUBLIC KEY)-----[A-Za-z0-9+/=\r\n]+-----END \1-----\s*", body) is None:
                raise ValueError()
            keys = {key_id(configuration["kid"]): serialization.load_pem_public_key(body)}
        else:
            document = strict_object(body, 256 * 1024)
            if set(document) != {"keys"} or not isinstance(document["keys"], list) or not 1 <= len(document["keys"]) <= 32:
                raise ValueError()
            keys = {}
            for entry in document["keys"]:
                if (not isinstance(entry, dict) or entry.get("kty") != "RSA"
                        or set(entry) - {"kty", "kid", "n", "e", "alg", "use", "key_ops", "x5c", "x5t", "x5t#S256"}
                        or "alg" in entry and entry["alg"] != "RS256"
                        or "use" in entry and entry["use"] != "sig"
                        or "key_ops" in entry and entry["key_ops"] != ["verify"]):
                    raise ValueError()
                kid = key_id(entry["kid"])
                if kid in keys:
                    raise ValueError()
                for name in ("x5t", "x5t#S256"):
                    if name in entry and not isinstance(entry[name], str):
                        raise ValueError()
                if "x5c" in entry and (not isinstance(entry["x5c"], list) or any(not isinstance(v, str) for v in entry["x5c"])):
                    raise ValueError()
                n, e = (canonical_base64url(entry[name], 512) for name in ("n", "e"))
                if not n or not e or n[0] == 0 or e[0] == 0:
                    raise ValueError()
                keys[kid] = rsa.RSAPublicNumbers(int.from_bytes(e, "big"), int.from_bytes(n, "big")).public_key()
        return LocalJwtAuthorization(keys, issuer=configuration["issuer"], audience=configuration["audience"],
                                     **({"utcnow": utcnow} if utcnow is not None else {}))
    except (ValueError, TypeError, KeyError, OSError, UnsupportedAlgorithm, ConfigurationStructureError):
        raise LocalJwtConfigurationInvalid() from None
