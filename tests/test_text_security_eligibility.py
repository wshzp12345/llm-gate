import asyncio
import json
from dataclasses import replace
from datetime import timedelta
from uuid import uuid4

import pytest

from llm_gateway.adapters.provider_credentials import CredentialMetadata
from llm_gateway.adapters.provider_transport_registry import ProviderTransportRegistry, project_provider_transports
from llm_gateway.adapters.text_security_eligibility import TextSecurityEligibility
from llm_gateway.adapters.trust_bundle import pem_identity
from llm_gateway.domain.invocation import InvocationAdmission
from tests.test_text_fingerprints import AUTH, snapshot
from tests.test_trust_bundle import certificate, NOW


async def setup(*, embedded=False, plaintext=False):
    content = json.loads(snapshot().snapshot_json)
    provider = content["providers"]["provider-a"]
    if embedded:
        pem = certificate()
        provider["transport"]["tls"] = {"trust": "bundle", "trust_bundle": {"identity": pem_identity(pem), "pem": pem}}
    if plaintext:
        provider["endpoint"]["base_url"] = "http://provider.invalid/v1"
    selected = snapshot(content)
    registry = ProviderTransportRegistry(factory=lambda plan: pytest.fail("Security check must not create a pool"))
    await registry.install("1", project_provider_transports(selected))
    admission = InvocationAdmission(uuid4(), "a" * 32, "1", "general", AUTH)
    metadata = CredentialMetadata(provider["credential"]["secret_ref"], "v1", "test", NOW + timedelta(days=5))
    return selected, registry, admission, metadata


async def healthy(*args):
    return True


async def reasons(gate, selected, admission, metadata):
    async with gate(admission, selected, "binding-a", metadata) as values:
        return tuple(value.code for value in values)


@pytest.mark.parametrize("embedded", [False, True])
def test_valid_security_checks_do_not_initialize_transport(embedded):
    async def run():
        selected, registry, admission, metadata = await setup(embedded=embedded)
        gate = TextSecurityEligibility(transports=registry, health=healthy, clock=lambda: NOW)
        assert await reasons(gate, selected, admission, metadata) == ()
        assert registry._live == {} and registry._retired == set()

    asyncio.run(run())


@pytest.mark.parametrize("offset,code", [(2, "trust_bundle_expired"), (-2, "security_invalidated")])
def test_bundle_dates_reject_without_health_or_pool(offset, code):
    async def run():
        selected, registry, admission, metadata = await setup(embedded=True)
        async def forbidden(*args):
            pytest.fail("Rejected security must not call health")
        gate = TextSecurityEligibility(transports=registry, health=forbidden, clock=lambda: NOW + timedelta(days=offset))
        assert await reasons(gate, selected, admission, metadata) == (code,)

    asyncio.run(run())


@pytest.mark.parametrize("allow", [False, True])
def test_plaintext_requires_explicit_development_permission(allow):
    async def run():
        selected, registry, admission, metadata = await setup(plaintext=True)
        gate = TextSecurityEligibility(transports=registry, health=healthy, allow_plaintext=allow, clock=lambda: NOW)
        assert await reasons(gate, selected, admission, metadata) == (() if allow else ("egress_policy_violation",))

    asyncio.run(run())


@pytest.mark.parametrize("change", ["revoked", "expired", "wrong_ref"])
def test_unusable_credential_metadata_is_rejected(change):
    async def run():
        selected, registry, admission, metadata = await setup()
        metadata = replace(metadata, **{"revoked": {"revoked": True}, "expired": {"valid_until": NOW},
                                        "wrong_ref": {"secret_ref": "other"}}[change])
        gate = TextSecurityEligibility(transports=registry, health=healthy, clock=lambda: NOW)
        assert await reasons(gate, selected, admission, metadata) == ("provider_credentials_unavailable",)

    asyncio.run(run())


@pytest.mark.parametrize("health_result", [False, None])
def test_health_is_explicit_not_truthiness_or_default(health_result):
    async def run():
        selected, registry, admission, metadata = await setup()
        async def health(*args):
            return health_result
        gate = TextSecurityEligibility(transports=registry, health=health, clock=lambda: NOW)
        if health_result is None:
            with pytest.raises(TypeError):
                await reasons(gate, selected, admission, metadata)
        else:
            assert await reasons(gate, selected, admission, metadata) == ("health_unavailable",)

    asyncio.run(run())


@pytest.mark.parametrize("change", ["suspend", "expire"])
def test_security_rechecked_after_awaited_health(change):
    async def run():
        selected, registry, admission, metadata = await setup(embedded=True)
        now = [NOW]
        async def health(*args):
            await asyncio.sleep(0)
            if change == "suspend":
                registry.suspend()
            else:
                now[0] += timedelta(days=2)
            return True
        gate = TextSecurityEligibility(transports=registry, health=health, clock=lambda: now[0])
        assert await reasons(gate, selected, admission, metadata) == (
            "security_invalidated" if change == "suspend" else "trust_bundle_expired",)
        assert registry._live == {}

    asyncio.run(run())


def test_new_publication_restores_valid_transport_but_not_old_plan():
    async def run():
        selected, registry, admission, metadata = await setup()
        old = project_provider_transports(selected)["provider-a"]
        registry.suspend()
        assert not registry.eligible(old)
        await registry.install("1", {"provider-a": old})
        assert registry.eligible(old)
        content = json.loads(selected.snapshot_json)
        content["providers"]["provider-a"]["transport"]["idle_timeout_seconds"] += 1
        replacement = project_provider_transports(snapshot(content, revision="2"))
        await registry.install("2", replacement)
        assert not registry.eligible(old) and registry.eligible(replacement["provider-a"])
        assert registry._live == {}

    asyncio.run(run())
