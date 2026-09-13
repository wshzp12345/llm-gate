import asyncio
from dataclasses import replace
from pathlib import Path

import pytest

from llm_gateway.adapters.fingerprint_material import FingerprintMaterialMetadata, FingerprintMaterialUnavailable
from llm_gateway.infrastructure.development_fingerprint_source import DevelopmentFingerprintSource, MountedFingerprintSecret
from llm_gateway.infrastructure.fingerprint_resolution import AsyncFingerprintResolver
from llm_gateway.application.fingerprint_keys import FingerprintKeys
from tests.test_fingerprint_leases import ACTIVE, RING, Protection, deadline


def entry(path, **changes):
    metadata = FingerprintMaterialMetadata(ACTIVE.key_id, ACTIVE.key_version, ACTIVE.secret_ref)
    return MountedFingerprintSecret(replace(metadata, **changes), path)


def test_exact_binary_file_reads_fresh_material_without_environment_fallback(tmp_path, monkeypatch):
    path = tmp_path / "version-v2.raw"
    path.write_bytes(bytes(range(32)))
    source = DevelopmentFingerprintSource(startup_profile="dev", entries=(entry(path),))
    with source.resolve(ACTIVE) as first, source.resolve(ACTIVE) as second:
        assert first is not second
        assert first.digest(b"input") == second.digest(b"input")
    path.unlink()
    monkeypatch.setenv(ACTIVE.secret_ref, "a" * 32)
    with pytest.raises(FingerprintMaterialUnavailable) as error:
        source.resolve(ACTIVE)
    assert error.value.__context__ is None


@pytest.mark.parametrize("raw", [b"", b"a" * 31, b"a" * 33, b"ab" * 32, b"a" * 100000],
                         ids=["empty", "short", "long", "hex-text", "oversize"])
def test_no_decode_padding_truncation_or_bundle(tmp_path, raw):
    path = tmp_path / "version-v2.raw"
    path.write_bytes(raw)
    source = DevelopmentFingerprintSource(startup_profile="dev", entries=(entry(path),))
    with pytest.raises(FingerprintMaterialUnavailable):
        source.resolve(ACTIVE)


@pytest.mark.parametrize("change", [{"key_id": "other"}, {"key_version": "v1"},
                                    {"secret_ref": "missing"}])
def test_mismatch_does_not_open_file_or_select_other_version(tmp_path, change):
    path = tmp_path / "absent.raw"
    source = DevelopmentFingerprintSource(startup_profile="dev", entries=(entry(path),))
    with pytest.raises(FingerprintMaterialUnavailable):
        source.resolve(replace(ACTIVE, **change))


def test_revoked_metadata_and_invalid_catalog_are_rejected(tmp_path):
    path = tmp_path / "absent.raw"
    source = DevelopmentFingerprintSource(startup_profile="dev", entries=(entry(path, revoked=True),))
    with pytest.raises(FingerprintMaterialUnavailable):
        source.resolve(ACTIVE)
    for profile in ("production", "", None):
        with pytest.raises(ValueError):
            DevelopmentFingerprintSource(startup_profile=profile, entries=(entry(path),))
    with pytest.raises(ValueError):
        entry(Path("relative.raw"))
    with pytest.raises(ValueError):
        entry(path, profile="SHA-256")
    for duplicate in (entry(path), entry(path, key_version="v1", secret_ref="old"),
                      entry(tmp_path / "other.raw", key_version="v1")):
        with pytest.raises(ValueError):
            DevelopmentFingerprintSource(startup_profile="dev", entries=(entry(path), duplicate))
    assert str(path) not in repr(entry(path))
    assert ACTIVE.secret_ref not in repr(entry(path))


def test_file_to_worker_to_operation_lease_releases_all_owned_material(tmp_path):
    path = tmp_path / "version-v2.raw"
    path.write_bytes(bytes(range(32)))
    source = DevelopmentFingerprintSource(startup_profile="dev", entries=(entry(path),))
    async def run():
        resolver = AsyncFingerprintResolver(source)
        keys = FingerprintKeys(RING, resolver, Protection())
        try:
            assert await keys.validate_active()
            assert resolver.in_use == 0
            async with keys.active(deadline=deadline()) as lease:
                assert resolver.in_use == keys.in_use == 1
                assert len(lease.digest(b"test-domain/input", purpose="request")) == 32
            assert resolver.in_use == keys.in_use == 0
        finally:
            resolver.close()
    asyncio.run(run())
