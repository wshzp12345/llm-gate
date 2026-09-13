import asyncio
import base64
import json
from datetime import datetime, timezone

import pytest
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from llm_gateway.adapters.caller_bearer import caller_bearer
from llm_gateway.adapters.local_jwt import LocalJwtAuthorization, LocalJwtConfigurationInvalid
from llm_gateway.application.authorization import Unauthorized, Forbidden, require_permission
from llm_gateway.infrastructure.local_jwt_file import load_local_jwt


NOW = datetime(2030, 1, 1, tzinfo=timezone.utc)
CLAIMS = {"sub": "principal", "tenant_id": "tenant", "iss": "issuer", "aud": "gateway",
          "exp": int(NOW.timestamp()) + 60, "scope": "gateway.model.invoke"}


@pytest.fixture(scope="module")
def signing_key():
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def encoded(value):
    body = value if isinstance(value, bytes) else json.dumps(value, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(body).rstrip(b"=").decode()


def token(key, claims=None, header=None):
    first = encoded({"alg": "RS256", "kid": "key-a", "typ": "JWT"} if header is None else header)
    second = encoded(CLAIMS if claims is None else claims)
    signature = key.sign((first + "." + second).encode(), padding.PKCS1v15(), hashes.SHA256())
    return first + "." + second + "." + encoded(signature)


def verifier(key):
    return LocalJwtAuthorization({"key-a": key.public_key()}, issuer="issuer", audience="gateway", utcnow=lambda: NOW)


def jwk(key):
    public = key.public_key().public_numbers()
    return {"kty": "RSA", "kid": "key-a", "n": encoded(public.n.to_bytes((public.n.bit_length()+7)//8, "big")),
            "e": encoded(public.e.to_bytes((public.e.bit_length()+7)//8, "big"))}


def file_config(path, form="pem"):
    return {"mode": "local_jwt", "key_file": str(path), "format": form,
            "issuer": "issuer", "audience": "gateway", **({"kid": "key-a"} if form == "pem" else {})}


@pytest.mark.parametrize("headers", [[], [(b"authorization", b"")], [(b"authorization", b"Basic abc")],
    [(b"authorization", b" Bearer abc")], [(b"authorization", b"Bearer abc ")], [(b"authorization", b"Bearer\tabc")],
    [(b"authorization", b"Bearer  abc")], [(b"authorization", b"Bearer abc,def")],
    [(b"authorization", b"Bearer abc\n")], [(b"authorization", b"Bearer \xff")],
    [(b"authorization", b"Bearer a=b")], [(b"authorization", b"Bearer ")],
    [(b"Authorization", b"Bearer abc"), (b"authorization", b"Bearer abc")],
    [(b"authorization", b"Bearer " + b"a" * 16385)]])
def test_raw_bearer_rejects_ambiguity_without_echo(headers):
    with pytest.raises(Unauthorized) as failure:
        caller_bearer(headers)
    assert str(failure.value) == ""


def test_raw_bearer_preserves_token_and_accepts_exact_boundary():
    assert caller_bearer([(b"AUTHORIZATION", b"bEaReR AbC._~+/-==")]) == "AbC._~+/-=="
    assert len(caller_bearer([(b"authorization", b"Bearer " + b"a" * 16384)])) == 16384


@pytest.mark.parametrize("changes", [
    {"iss": "other"}, {"aud": ["gateway", "gateway"]}, {"aud": ["other"]}, {"aud": 4},
    {"sub": " principal"}, {"tenant_id": ""}, {"sub": "\u0000"}, {"sub": "\ud800"},
    {"exp": True}, {"exp": 1.2}, {"exp": int(NOW.timestamp()) - 30}, {"exp": 10**100},
    {"nbf": int(NOW.timestamp()) + 31}, {"iat": int(NOW.timestamp()) + 31}, {"nbf": True},
    {"scope": "gateway.model.invoke  other"}, {"scope": "dev:*"}, {"scope": [1]},
    {"scope": ["s" + str(i) for i in range(65)]}, {"scope": ["a"*127+str(i) for i in range(33)]},
])
def test_caller_claim_failures_are_only_unauthorized(signing_key, changes):
    with pytest.raises(Unauthorized) as failure:
        asyncio.run(verifier(signing_key).authenticate(token(signing_key, CLAIMS | changes)))
    assert str(failure.value) == ""


@pytest.mark.parametrize("missing", ["sub", "tenant_id", "iss", "aud", "exp"])
def test_mandatory_claims(signing_key, missing):
    claims = dict(CLAIMS)
    del claims[missing]
    with pytest.raises(Unauthorized):
        asyncio.run(verifier(signing_key).authenticate(token(signing_key, claims)))


def test_leeway_audience_scope_and_stable_subject(signing_key):
    async def scenario():
        adapter = verifier(signing_key)
        claims = CLAIMS | {"exp": int(NOW.timestamp()) - 29, "nbf": int(NOW.timestamp()) + 30,
                           "iat": int(NOW.timestamp()) + 30, "aud": ["other", "gateway"],
                           "scope": ["gateway.model.invoke", "gateway.model.invoke", "unknown.permission"]}
        first = await adapter.authenticate(token(signing_key, claims))
        refreshed = await adapter.authenticate(token(signing_key, claims | {"exp": int(NOW.timestamp()) + 600}))
        assert first.subject == refreshed.subject == "principal"
        assert first.audience == "gateway" and first.authentication_method == "local_jwt"
        assert first.expires_at.timestamp() == NOW.timestamp() + 1
        assert first.scopes == frozenset({"gateway.model.invoke", "unknown.permission"})
        require_permission(first, "gateway.model.invoke")
        with pytest.raises(Forbidden):
            require_permission(first, "gateway.config.publish")
        absent = {name: value for name, value in CLAIMS.items() if name != "scope"}
        for scope in ({}, {"scope": ""}, {"scope": []}, {"scope": ["a"] * 1100}):
            context = await adapter.authenticate(token(signing_key, absent | scope))
            with pytest.raises(Forbidden):
                require_permission(context, "gateway.model.invoke")
    asyncio.run(scenario())


@pytest.mark.parametrize("changes", [{"alg": "none"}, {"alg": "HS256"}, {"kid": "unknown"}, {"kid": "bad kid"},
    {"typ": "jwt"}, {"crit": []}, {"zip": "DEF"}, {"jwk": {}}, {"jku": "https://invalid"}, {"x5u": "https://invalid"}, {"b64": False}])
def test_protected_header_is_closed_against_key_confusion(signing_key, changes):
    with pytest.raises(Unauthorized):
        asyncio.run(verifier(signing_key).authenticate(token(signing_key, header={"alg": "RS256", "kid": "key-a"} | changes)))


@pytest.mark.parametrize("payload", [b'[]', b'{"sub":"one","sub":"two"}', b'{"x":NaN}', b'{"x":1e999}', b'\xef\xbb\xbf{}', b'{"x":"\xff"}'])
def test_strict_payload_json(signing_key, payload):
    with pytest.raises(Unauthorized):
        asyncio.run(verifier(signing_key).authenticate(token(signing_key, payload)))


def test_signature_segments_and_size_failures(signing_key):
    original = token(signing_key)
    first, second, signature = original.split(".")
    values = [original + ".extra", first + "=." + second + "." + signature, first + ".." + signature,
              first + "." + encoded(CLAIMS | {"sub": "forged"}) + "." + signature, "a" * 16385,
              token(signing_key, header={"alg": "RS256", "kid": "key-a", "extra": "a"*4096})]
    for value in values:
        with pytest.raises(Unauthorized):
            asyncio.run(verifier(signing_key).authenticate(value))


@pytest.mark.parametrize("form", [serialization.PublicFormat.SubjectPublicKeyInfo, serialization.PublicFormat.PKCS1])
def test_one_pem_read_no_hot_reload(signing_key, tmp_path, monkeypatch, form):
    path = tmp_path / "public.pem"
    path.write_bytes(signing_key.public_key().public_bytes(serialization.Encoding.PEM, form))
    original = type(path).open
    reads = []
    def opened(self, *args, **kwargs):
        if self == path and args == ("rb",):
            reads.append(True)
        return original(self, *args, **kwargs)
    monkeypatch.setattr(type(path), "open", opened)
    adapter = load_local_jwt(file_config(path), utcnow=lambda: NOW)
    path.write_bytes(b"broken replacement")
    assert asyncio.run(adapter.authenticate(token(signing_key))).subject == "principal"
    assert reads == [True]
    with pytest.raises(LocalJwtConfigurationInvalid):
        load_local_jwt(file_config(path))


@pytest.mark.parametrize("changes", [{"d": "private"}, {"x5u": "https://invalid"}, {"jku": "https://invalid"},
    {"kty": "EC"}, {"alg": "HS256"}, {"use": "enc"}, {"key_ops": ["sign", "verify"]},
    {"kid": "bad key"}, {"n": "AA"}, {"e": "Ag"}, {"n": "padding="}])
def test_jwks_defects_are_static_not_credential_failures(signing_key, tmp_path, changes):
    path = tmp_path / "keys.json"
    path.write_text(json.dumps({"keys": [jwk(signing_key) | changes]}), encoding="utf-8")
    with pytest.raises(LocalJwtConfigurationInvalid):
        load_local_jwt(file_config(path, "jwks"))


def test_jwks_public_parameters_only_and_unique_bounded_keyset(signing_key, tmp_path):
    path = tmp_path / "keys.json"
    entry = jwk(signing_key) | {"alg": "RS256", "use": "sig", "key_ops": ["verify"], "x5c": ["not a trust source"]}
    path.write_text(json.dumps({"keys": [entry]}), encoding="utf-8")
    adapter = load_local_jwt(file_config(path, "jwks"), utcnow=lambda: NOW)
    assert asyncio.run(adapter.authenticate(token(signing_key))).tenant_id == "tenant"
    path.write_text(json.dumps({"keys": [entry | {"kid": str(n)} for n in range(32)]}), encoding="utf-8")
    adapter = load_local_jwt(file_config(path, "jwks"), utcnow=lambda: NOW)
    assert asyncio.run(adapter.authenticate(token(signing_key, header={"kid": "31", "alg": "RS256"}))).subject == "principal"
    for entries in ([], [entry, entry], [entry | {"kid": str(n)} for n in range(33)]):
        path.write_text(json.dumps({"keys": entries}), encoding="utf-8")
        with pytest.raises(LocalJwtConfigurationInvalid):
            load_local_jwt(file_config(path, "jwks"))


def test_pem_and_bootstrap_static_defects(signing_key, tmp_path):
    path = tmp_path / "public.pem"
    valid = signing_key.public_key().public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo)
    private = signing_key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
    weak = rsa.generate_private_key(public_exponent=65537, key_size=1024).public_key().public_bytes(
        serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo)
    for body in (private, valid + valid, b"x" + valid, b"x" * (256*1024+1), weak):
        path.write_bytes(body)
        with pytest.raises(LocalJwtConfigurationInvalid):
            load_local_jwt(file_config(path))
    path.write_bytes(valid)
    for changes in ({"inline": "no"}, {"mode": "dev_bypass"}, {"key_file": "https://invalid/key"},
                    {"key_file": "relative.pem"}, {"key_file": "\\\\remote\\share\\key.pem"},
                    {"issuer": " issuer"}, {"kid": ""}, {"format": "other"}):
        with pytest.raises(LocalJwtConfigurationInvalid):
            load_local_jwt(file_config(path) | changes)


def test_non_rsa_public_key_is_static_failure(tmp_path):
    from cryptography.hazmat.primitives.asymmetric import ed25519
    path = tmp_path / "other.pem"
    path.write_bytes(ed25519.Ed25519PrivateKey.generate().public_key().public_bytes(
        serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo))
    with pytest.raises(LocalJwtConfigurationInvalid):
        load_local_jwt(file_config(path))
