import asyncio
from copy import deepcopy
import ssl

import pytest

from llm_gateway.adapters.egress_pool import ProviderTransportRetired
from llm_gateway.adapters.provider_transport_registry import (
    ProviderTransportFactory, ProviderTransportRegistry, project_provider_transports,
)
from llm_gateway.adapters.trust_bundle import pem_identity
from llm_gateway.application.active_configuration import LoadedConfiguration
from tests.config_fixtures import bundle_submission
from tests.test_configuration_preparation import draft
from tests.test_trust_bundle import certificate, NOW
from tests.test_credentialed_runtime import Harness
from llm_gateway.adapters.credentialed_runtime import CredentialedCandidateRuntime
from llm_gateway.adapters.transport_credential_projection import project_transport_credentials


def plans(body=None, revision="1"):
    prepared = draft(body or bundle_submission())
    snapshot = LoadedConfiguration(revision, prepared.snapshot_digest, prepared.snapshot_json, prepared.resources)
    return project_provider_transports(snapshot)


class Client:
    def __init__(self):
        self.accepting, self.closed, self.active = True, False, False
        self.idle_closes = 0

    def stop_acquiring(self):
        self.accepting = False

    async def close_idle(self):
        assert not self.accepting
        self.idle_closes += 1

    @property
    def drained(self):
        return not self.active

    async def aclose(self):
        assert not self.active
        self.closed = True


def test_lazy_install_reuse_and_label_credential_model_path_independence():
    async def run():
        created = []
        def factory(plan):
            client = Client()
            created.append(client)
            return client
        registry = ProviderTransportRegistry(factory=factory)
        first = plans()
        await registry.install("1", first)
        assert not created
        client = registry.acquire(first["provider-a"])
        body = bundle_submission()
        provider = body["bundle"]["providers"]["provider-a"]
        provider["credential"]["secret_ref"] = "another-reference"
        provider["endpoint"]["base_url"] += "/other-api"
        provider["rate_limit"]["qps"] = 10
        body["bundle"]["provider_model_bindings"]["binding-a"]["upstream_model"] = "Another-Model"
        second = plans(body)
        assert first == second
        await registry.install("2", second)
        assert registry.acquire(second["provider-a"]) is client and len(created) == 1
        await registry.aclose()
        assert client.closed
    asyncio.run(run())


@pytest.mark.parametrize("field,value", [("connect_timeout_seconds", 6), ("idle_timeout_seconds", 31),
    ("max_connection_lifetime_seconds", 301)])
def test_published_transport_limits_change_identity(field, value):
    body = bundle_submission()
    body["bundle"]["providers"]["provider-a"]["transport"][field] = value
    assert plans()["provider-a"].key != plans(body)["provider-a"].key


def test_provider_origin_capacity_and_bundle_identity_isolation():
    first = plans()["provider-a"].key
    for change in ("origin", "capacity", "provider"):
        body = bundle_submission()
        provider = body["bundle"]["providers"]["provider-a"]
        if change == "origin":
            provider["endpoint"]["base_url"] = "https://other.invalid/v1"
        elif change == "capacity":
            provider["rate_limit"]["max_concurrency"] = 3
        else:
            body["bundle"]["providers"]["provider-b"] = deepcopy(provider)
        other = plans(body)["provider-b" if change == "provider" else "provider-a"].key
        assert other != first
    body = bundle_submission()
    pem = certificate()
    bundle = {"identity": pem_identity(pem), "pem": pem, "label": "first"}
    body["bundle"]["providers"]["provider-a"]["transport"]["tls"] = {"trust": "bundle", "trust_bundle": bundle}
    original = plans(body)
    bundle["label"] = "second"
    assert plans(body) == original
    bundle["pem"] = certificate()
    bundle["identity"] = pem_identity(bundle["pem"])
    assert plans(body)["provider-a"].key != original["provider-a"].key


def test_replacement_stops_old_acquisition_but_preserves_active_work_until_reap():
    async def run():
        created = []
        def factory(plan):
            client = Client()
            created.append(client)
            return client
        registry = ProviderTransportRegistry(factory=factory)
        first = plans()
        await registry.install("1", first)
        old = registry.acquire(first["provider-a"])
        old.active = True
        body = bundle_submission()
        body["bundle"]["providers"]["provider-a"]["transport"]["idle_timeout_seconds"] = 60
        second = plans(body)
        await registry.install("2", second)
        assert not old.accepting and not old.closed and old.idle_closes == 1
        assert len(created) == 1
        with pytest.raises(ProviderTransportRetired):
            registry.acquire(first["provider-a"])
        assert registry.acquire(second["provider-a"]) is not old
        old.active = False
        await registry.reap()
        assert old.closed
        await registry.aclose()
    asyncio.run(run())


def test_egress_change_replaces_backend_even_when_origin_tls_pool_digest_is_same():
    async def run():
        registry = ProviderTransportRegistry(factory=lambda plan: Client())
        first = plans()
        await registry.install("1", first)
        old = registry.acquire(first["provider-a"])
        body = bundle_submission()
        body["bundle"]["providers"]["provider-a"]["egress"]["allowed_networks"] = ["8.8.8.0/24"]
        second = plans(body)
        assert first["provider-a"].key == second["provider-a"].key
        await registry.install("2", second)
        assert old.closed
        with pytest.raises(ProviderTransportRetired):
            registry.acquire(first["provider-a"])
        assert registry.acquire(second["provider-a"]) is not old
        await registry.aclose()
    asyncio.run(run())


def test_disabled_provider_retired_and_stale_revision_cannot_restore_it():
    async def run():
        registry = ProviderTransportRegistry(factory=lambda plan: Client())
        first = plans()
        await registry.install("1", first)
        old = registry.acquire(first["provider-a"])
        body = bundle_submission()
        body["bundle"]["providers"]["provider-a"]["status"] = "disabled"
        await registry.install("2", plans(body))
        assert old.closed
        assert not await registry.install("1", first)
        with pytest.raises(ProviderTransportRetired):
            registry.acquire(first["provider-a"])
        with pytest.raises(ValueError, match="Revision"):
            await registry.install("2", first)
        await registry.aclose()
    asyncio.run(run())


def test_local_initialization_defect_propagates_without_retry_or_client_cache():
    async def run():
        calls = []
        def factory(plan):
            calls.append(plan)
            raise RuntimeError("synthetic local defect")
        registry = ProviderTransportRegistry(factory=factory)
        first = plans()
        await registry.install("1", first)
        with pytest.raises(RuntimeError, match="local defect"):
            registry.acquire(first["provider-a"])
        assert len(calls) == 1
        await registry.aclose()
    asyncio.run(run())


def test_actual_factory_builds_clients_lazily_without_dns_and_retirement_rejects_stale_client():
    async def run():
        class Resolver:
            async def resolve(self, *args):
                pytest.fail("No DNS during pool preparation/publication")
        registry = ProviderTransportRegistry(factory=ProviderTransportFactory(resolver=Resolver(),
            system_context=ssl.create_default_context(), clock=lambda: NOW))
        body = bundle_submission()
        pem = certificate()
        body["bundle"]["providers"]["provider-a"]["transport"]["tls"] = {
            "trust": "bundle", "trust_bundle": {"identity": pem_identity(pem), "pem": pem}}
        first = plans(body)
        await registry.install("1", first)
        client = registry.acquire(first["provider-a"])
        await registry.install("2", {})
        assert client.is_closed
        with pytest.raises(RuntimeError):
            await client.get("https://provider.invalid/")
        await registry.aclose()
    asyncio.run(run())


def test_locked_binding_projection_is_lazy_and_registry_fences_runtime_acquisition():
    async def run():
        created, rejected = [], []
        def factory(plan):
            client = Client()
            created.append(client)
            return client
        registry = ProviderTransportRegistry(factory=factory)
        body = bundle_submission()
        body["bundle"]["providers"]["provider-a"]["credential"]["secret_ref"] = "provider-key"
        prepared = draft(body)
        snapshot = LoadedConfiguration("1", prepared.snapshot_digest, prepared.snapshot_json, prepared.resources)
        await registry.install("1", project_provider_transports(snapshot))
        bindings = project_transport_credentials(snapshot, ("binding-a",), registry)
        assert not created
        harness = Harness()
        async def reject(binding, code):
            rejected.append((binding, code))
        runtime = CredentialedCandidateRuntime(bindings=bindings, source=harness, gates=harness.gates, reject=reject)
        async with runtime.acquire("binding-a") as handle:
            assert handle._binding.client is created[0]
            assert handle._binding.base_url == "https://provider.invalid/v1"
        await registry.install("2", {})
        async with runtime.acquire("binding-a") as handle:
            assert handle is None
        assert rejected == [("binding-a", "security_invalidated")]
        assert len(created) == 1 and created[0].closed
        assert all(not lease._material for lease in harness.leases)
        await registry.aclose()
    asyncio.run(run())
