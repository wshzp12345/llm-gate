"""Single-event-loop, lazy Provider pools reconciled from published snapshots.

Installation fences changed clients synchronously before idle-close awaits.
It never initializes replacement clients or migrates active requests.
"""

import asyncio
from dataclasses import dataclass, field
import json

import httpx

from llm_gateway.adapters.canonical_json import canonical_digest
from llm_gateway.adapters.egress_pool import ProviderTransportRetired
from llm_gateway.adapters.egress_httpx import create_egress_client
from llm_gateway.adapters.provider_trust import load_trust_bundle
from llm_gateway.domain.configuration import revision_number
from llm_gateway.domain.provider_egress import ProviderEgressPolicy


@dataclass(frozen=True)
class ProviderPoolKey:
    provider_id: str
    trust_bundle_identity: str | None
    transport_config_digest: str


@dataclass(frozen=True)
class ProviderTransportPlan:
    key: ProviderPoolKey
    scheme: str
    host: str = field(repr=False)
    port: int
    policy: ProviderEgressPolicy
    connect_timeout_seconds: int
    idle_timeout_seconds: int
    max_connection_lifetime_seconds: int
    max_connections: int
    trust_pem: str | None = field(repr=False, compare=False)


def project_provider_transports(snapshot):
    """Validated canonical Snapshot only, never caller request material."""
    plans = {}
    for provider_id, provider in json.loads(snapshot.snapshot_json)["providers"].items():
        if provider["status"] != "enabled":
            continue
        endpoint = httpx.URL(provider["endpoint"]["base_url"])
        transport, egress = provider["transport"], provider["egress"]
        if (endpoint.scheme not in {"https", "http"} or not endpoint.host
                or endpoint.userinfo or endpoint.query or endpoint.fragment
                or transport["http_version"] != "1.1" or egress["proxy"] != "disabled"):
            raise ValueError("Validated Provider origin and direct HTTP/1.1 transport required")
        port = endpoint.port or (443 if endpoint.scheme == "https" else 80)
        bundle = transport["tls"]["trust_bundle"]
        trust_identity = bundle["identity"] if bundle is not None else None
        projection = {
            "origin": [endpoint.scheme, endpoint.host, port],
            "http_version": "1.1", "tls": {"trust": transport["tls"]["trust"],
                "trust_bundle_identity": trust_identity, "minimum_version": "1.2", "verify": True},
            "connect_timeout_seconds": transport["connect_timeout_seconds"],
            "idle_timeout_seconds": transport["idle_timeout_seconds"],
            "max_connection_lifetime_seconds": transport["max_connection_lifetime_seconds"],
            "max_connections": provider["rate_limit"]["max_concurrency"],
            "max_keepalive_connections": provider["rate_limit"]["max_concurrency"],
            "pool_timeout_seconds": 0, "retries": 0, "proxy": "disabled",
        }
        key = ProviderPoolKey(provider_id, trust_identity, canonical_digest(projection))
        plans[provider_id] = ProviderTransportPlan(key, endpoint.scheme, endpoint.host, port,
            ProviderEgressPolicy(tuple(egress["allowed_hosts"]), tuple(egress["allowed_networks"])),
            transport["connect_timeout_seconds"], transport["idle_timeout_seconds"],
            transport["max_connection_lifetime_seconds"], provider["rate_limit"]["max_concurrency"],
            bundle["pem"] if bundle is not None else None)
    return plans


class ProviderTransportFactory:
    def __init__(self, *, resolver, system_context, clock=None, allow_plaintext=False):
        self._resolver, self._system_context = resolver, system_context
        self._clock, self._allow_plaintext = clock, allow_plaintext

    def __call__(self, plan):
        if plan.scheme == "http" and not self._allow_plaintext:
            raise ValueError("Plaintext Provider requires an explicit development deployment")
        trust = None
        if plan.trust_pem is not None:
            trust = load_trust_bundle(identity=plan.key.trust_bundle_identity,
                                       pem=plan.trust_pem, clock=self._clock)
        context = None
        if trust is None:
            context = self._system_context() if callable(self._system_context) else self._system_context
        return create_egress_client(policy=plan.policy, host=plan.host, port=plan.port,
            resolver=self._resolver, ssl_context=context,
            trust_bundle=trust, allow_plaintext=plan.scheme == "http", max_connections=plan.max_connections,
            connect_timeout_seconds=plan.connect_timeout_seconds, idle_timeout_seconds=plan.idle_timeout_seconds,
            max_connection_lifetime_seconds=plan.max_connection_lifetime_seconds)


class ProviderTransportRegistry:
    def __init__(self, *, factory):
        # The synchronous factory builds local transport state only; it must not
        # resolve DNS, open sockets or resolve credentials.
        self._factory = factory
        self._revision = 0
        self._desired = {}
        self._live = {}
        self._retired = set()
        self._cleanup_lock = asyncio.Lock()
        self._closed = False
        self._suspended = False

    async def install(self, revision, plans):
        number = revision_number(revision)
        if self._closed:
            raise RuntimeError("Provider transport registry is closed")
        if number < self._revision:
            return False
        desired = dict(plans)
        if any(key != plan.key.provider_id for key, plan in desired.items()):
            raise ValueError("Provider transport plan identity mismatch")
        if number == self._revision and desired != self._desired:
            raise ValueError("One published Revision cannot have different transport plans")
        self._revision, self._desired = number, desired
        self._suspended = False
        # An allowlist change also fences the old backend even though it is not
        # part of the origin/TLS/pool digest. A new backend enforces the new list.
        for provider_id, (plan, client) in tuple(self._live.items()):
            if desired.get(provider_id) != plan:
                client.stop_acquiring()
                self._retired.add(client)
                del self._live[provider_id]
        await self.reap()
        return True

    def acquire(self, plan):
        """Only an eligible Attempt may call this lazy acquisition boundary."""
        if not self.eligible(plan):
            raise ProviderTransportRetired()
        entry = self._live.get(plan.key.provider_id)
        if entry is None:
            # Initialization defects propagate as local faults. No retry,
            # fallback, stale-client reuse or negative caching is added here.
            client = self._factory(plan)
            self._live[plan.key.provider_id] = (plan, client)
            return client
        return entry[1]

    def eligible(self, plan):
        """Read the publication fence without constructing or leasing a client."""
        return not self._closed and not self._suspended and self._desired.get(plan.key.provider_id) == plan

    async def reap(self):
        """Close retired idle pools; retain active ones without a drain timer."""
        async with self._cleanup_lock:
            for client in tuple(self._retired):
                await client.close_idle()
                if client.drained:
                    await client.aclose()
                    self._retired.discard(client)

    def stop_acquiring(self):
        self._closed = True
        self.suspend()

    def suspend(self):
        """Reversible failure fence; retain revision identity for fresh recovery."""
        self._suspended = True
        for _, client in self._live.values():
            client.stop_acquiring()
            self._retired.add(client)
        self._live.clear()

    async def aclose(self):
        """Final cleanup after the owner has joined/cancelled active Invocations."""
        self.stop_acquiring()
        async with self._cleanup_lock:
            for client in tuple(self._retired):
                await client.aclose()
                self._retired.discard(client)


class ProviderTransportSnapshots:
    """Application lifecycle port backed by the process-local transport registry."""

    def __init__(self, registry):
        self._registry = registry

    async def install(self, snapshot):
        if not await self._registry.install(snapshot.revision, project_provider_transports(snapshot)):
            raise ValueError("A stale Active Snapshot cannot replace runtime transport state")

    def suspend(self):
        self._registry.suspend()

    async def reap(self):
        await self._registry.reap()
