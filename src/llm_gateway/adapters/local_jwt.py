"""Offline, fixed RS256 JWT verification with no key discovery or fallback."""

import base64
import math
import re
from datetime import datetime, timedelta, timezone
from types import MappingProxyType

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from llm_gateway.adapters.configuration_json import ParseLimits, ConfigurationStructureError, parse_strict_json
from llm_gateway.application.authorization import Unauthorized
from llm_gateway.domain.invocation import AuthorizationContext, _context_string


class LocalJwtConfigurationInvalid(Exception):
    """Static Bootstrap failure, never caller credential invalidity."""


def key_id(value):
    if not isinstance(value, str) or re.fullmatch(r"[\x21-\x7e]{1,128}", value) is None:
        raise ValueError("Invalid local key identity")
    return value


def canonical_base64url(value, maximum):
    if (not isinstance(value, str) or not value or len(value) > (maximum * 4 + 2) // 3
            or re.fullmatch(r"[A-Za-z0-9_-]+", value) is None):
        raise ValueError("Invalid encoded segment")
    decoded = base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
    if len(decoded) > maximum or base64.urlsafe_b64encode(decoded).rstrip(b"=").decode("ascii") != value:
        raise ValueError("Noncanonical encoded segment")
    return decoded


def strict_object(body, maximum):
    # The wire byte ceiling also bounds nodes/collections; do not impose a
    # smaller Claim-count limit before scope de-duplication.
    value = parse_strict_json(body, ParseLimits(maximum, maximum, maximum * 2, maximum, maximum))
    if not isinstance(value, dict):
        raise ValueError("JSON object required")
    pending = [value]
    while pending:
        item = pending.pop()
        if isinstance(item, dict):
            pending.extend(item.values())
        elif isinstance(item, list):
            pending.extend(item)
        elif isinstance(item, float) and not math.isfinite(item):
            raise ValueError("Nonfinite JSON number")
    return value


class LocalJwtAuthorization:
    def __init__(self, keys, *, issuer, audience, utcnow=lambda: datetime.now(timezone.utc)):
        try:
            _context_string(issuer, 2048)
            _context_string(audience, 255)
            if not 1 <= len(keys) <= 32 or not callable(utcnow):
                raise ValueError()
            for kid, key in keys.items():
                key_id(kid)
                if not isinstance(key, rsa.RSAPublicKey) or not 2048 <= key.key_size <= 4096:
                    raise ValueError()
        except (ValueError, TypeError, AttributeError):
            raise LocalJwtConfigurationInvalid() from None
        self._keys = MappingProxyType(dict(keys))
        self._issuer, self._audience, self._utcnow = issuer, audience, utcnow

    async def authenticate(self, credential):
        now = self._utcnow()
        if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() != timedelta(0):
            raise RuntimeError("UTC verification clock required")
        try:
            if not isinstance(credential, str) or not 1 <= len(credential) <= 16384:
                raise ValueError()
            parts = credential.split(".")
            if len(parts) != 3:
                raise ValueError()
            header = strict_object(canonical_base64url(parts[0], 4096), 4096)
            payload = canonical_base64url(parts[1], 12288)
            signature = canonical_base64url(parts[2], 512)
            if (header.get("alg") != "RS256" or key_id(header.get("kid")) not in self._keys
                    or {"crit", "zip", "jwk", "jku", "x5u"}.intersection(header)
                    or "b64" in header and header["b64"] is not True
                    or "typ" in header and header["typ"] not in ("JWT", "at+jwt")):
                raise ValueError()
            self._keys[header["kid"]].verify(signature, (parts[0] + "." + parts[1]).encode("ascii"),
                                            padding.PKCS1v15(), hashes.SHA256())
            claims = strict_object(payload, 12288)
            if claims.get("iss") != self._issuer:
                raise ValueError()
            audiences = claims["aud"]
            if isinstance(audiences, str):
                audiences = [audiences]
            if (not isinstance(audiences, list) or any(not isinstance(item, str) for item in audiences)
                    or len(set(audiences)) != len(audiences) or self._audience not in audiences):
                raise ValueError()
            if type(claims["exp"]) is not int:
                raise ValueError()
            for name in ("nbf", "iat"):
                if name in claims and (type(claims[name]) is not int or claims[name] > now.timestamp() + 30):
                    raise ValueError()
            expires = datetime.fromtimestamp(claims["exp"] + 30, timezone.utc)
            if now >= expires:
                raise ValueError()
            scopes = claims.get("scope", [])
            if isinstance(scopes, str):
                scopes = scopes.split(" ") if scopes else []
            if not isinstance(scopes, list) or any(not isinstance(scope, str) for scope in scopes):
                raise ValueError()
            return AuthorizationContext(claims["tenant_id"], claims["sub"], frozenset(scopes),
                                        self._issuer, self._audience, expires, "local_jwt")
        except (ValueError, TypeError, KeyError, OverflowError, OSError, InvalidSignature, ConfigurationStructureError):
            raise Unauthorized() from None
