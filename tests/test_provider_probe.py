import asyncio
import json
import ssl
from contextlib import closing
from dataclasses import replace
from functools import partial

import httpx
import pytest

from llm_gateway.adapters.provider_credentials import CredentialUnavailable
from llm_gateway.adapters.provider_probe import HttpProviderProbe, project_probe_policy
from llm_gateway.application.provider_probe import ProbeOutcome, ProbePolicy, ProviderProbeRunner
from llm_gateway.infrastructure.credential_resolution import AsyncCredentialResolver
from tests.test_development_secrets import environment_source, record, NOW
from tests.test_text_completion_projection import snapshot


POLICY = ProbePolicy("provider", "1", 10, 1)
# Match the fixed SecretSource clock; wall-clock expiry must not prevent the
# transport/cancellation fixtures from entering their controlled handler.
HttpProviderProbe = partial(HttpProviderProbe, utcnow=lambda: NOW)


@pytest.mark.parametrize("status,code,sample", [
    (200, "available", True), (204, "available", True), (302, "provider_protocol_error", None),
    (401, "provider_credentials_unavailable", None), (403, "provider_credentials_unavailable", None),
    (429, "rate_limited", False), (503, "provider_unavailable", False),
    (501, "provider_protocol_error", None), (400, "provider_protocol_error", None),
])
def test_safe_get_has_no_generation_body_redirect_or_response_body_consumption(status, code, sample):
    async def run():
        seen = []
        class Body(httpx.AsyncByteStream):
            closed = False
            async def __aiter__(self):
                pytest.fail("Probe response content must not be read")
                yield b"unreachable"
            async def aclose(self):
                self.closed = True
        body = Body()
        def handler(request):
            seen.append(request)
            assert request.method == "GET" and request.url == "https://provider.invalid/health?mode=basic"
            assert request.content == b"" and request.headers["Authorization"] == "Bearer test-only"
            assert "Cookie" not in request.headers and "X-Caller" not in request.headers
            return httpx.Response(status, headers={"Location": "https://other.invalid/secret"}, stream=body)
        with closing(AsyncCredentialResolver(environment_source({"EXPLICIT_RECORD": record(value="test-only")}), max_concurrent_reads=1)) as source:
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler), follow_redirects=True,
                    auth=("ignored", "ignored"), params={"caller": "ignored"}, cookies={"caller": "ignored"},
                    headers={"X-Caller": "ignored"}) as client:
                port = HttpProviderProbe(client=client, source=source, base_url="https://provider.invalid/v1",
                    path="/health?mode=basic", secret_ref="provider-key")
                observation = await ProviderProbeRunner().run_due(POLICY, port)
        assert observation.outcome == ProbeOutcome(code) and observation.outcome.availability_sample is sample
        assert observation.signal == "active_probe" and body.closed and len(seen) == 1
        assert "test-only" not in repr(observation) and "provider.invalid" not in repr(observation)
    asyncio.run(run())


@pytest.mark.parametrize("path", ["https://other.invalid", "//other.invalid", "/\\other.invalid", "/health#x", "/h\n"])
def test_unsafe_probe_path_is_rejected_without_resolving_secrets(path):
    with pytest.raises(ValueError):
        HttpProviderProbe(client=None, source=None, base_url="https://provider.invalid/v1", path=path, secret_ref="provider-key")


def test_disabled_probe_projection_creates_no_policy_and_missing_config_is_not_enabled():
    content = {"providers": {"provider": {"health": {"active_probe": None}}}}
    loaded = replace(snapshot(), revision="1", snapshot_json=json.dumps(content).encode())
    assert project_probe_policy(loaded, "provider") is None
    content["providers"]["provider"]["health"]["active_probe"] = {"path": "/health", "interval_seconds": 10, "timeout_seconds": 1}
    assert project_probe_policy(replace(loaded, snapshot_json=json.dumps(content).encode()), "provider") == POLICY
    with pytest.raises(KeyError):
        project_probe_policy(loaded, "missing")


def test_per_provider_probe_concurrency_and_interval_are_shared_across_revisions():
    async def run():
        now, entered, release = [0], asyncio.Event(), asyncio.Event()
        runner = ProviderProbeRunner(clock=lambda: now[0])
        class Port:
            calls = 0
            async def probe(self, context):
                self.calls += 1
                entered.set()
                await release.wait()
                return ProbeOutcome("available")
        port = Port()
        task = asyncio.create_task(runner.run_due(POLICY, port))
        await entered.wait()
        now[0] = 20
        assert await runner.run_due(replace(POLICY, configuration_revision="2"), port) is None
        release.set()
        await task
        assert port.calls == 1 and not runner._busy
        assert (await runner.run_due(POLICY, port)).outcome.code == "available"
        assert await runner.run_due(POLICY, port) is None
        assert (await runner.run_due(replace(POLICY, provider_id="other"), port)).outcome.code == "available"
        runner.stop()
        now[0] = 100
        assert await runner.run_due(POLICY, port) is None
    asyncio.run(run())


@pytest.mark.parametrize("cancel", [False, True])
def test_probe_timeout_or_cancel_closes_credential_lease_and_releases_isolated_slot(cancel):
    async def run():
        now, entered, leases = [0], asyncio.Event(), []
        sync_source = environment_source({"EXPLICIT_RECORD": record(value="test-only")})
        class Source:
            async def resolve(self, reference):
                lease = sync_source.resolve(reference)
                leases.append(lease)
                return lease
        async def handler(request):
            entered.set()
            await asyncio.Future()
        runner = ProviderProbeRunner(clock=lambda: now[0])
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            port = HttpProviderProbe(client=client, source=Source(), base_url="https://provider.invalid/v1", path="/health", secret_ref="provider-key")
            task = asyncio.create_task(runner.run_due(POLICY, port))
            await asyncio.wait_for(entered.wait(), 2)
            if cancel:
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
            else:
                assert (await task).outcome == ProbeOutcome("upstream_timeout")
            with pytest.raises(CredentialUnavailable):
                leases[0].bearer_value()
            assert not runner._busy
            assert await runner.run_due(POLICY, port) is None  # No immediate retry, even after cancellation.
    asyncio.run(run())


@pytest.mark.parametrize("kind,code", [("missing", "provider_credentials_unavailable"), ("certificate", "transport_nonretryable"),
    ("network", "provider_unavailable"), ("unexpected", "internal")])
def test_probe_failures_are_safe_and_non_availability_causes_are_excluded(kind, code):
    async def run():
        def handler(request):
            if kind == "certificate":
                raise httpx.ConnectError("secret endpoint") from ssl.SSLCertVerificationError("certificate data")
            if kind == "network":
                raise httpx.ConnectError("secret endpoint")
            raise RuntimeError("raw confidential diagnostic")
        with closing(AsyncCredentialResolver(environment_source({} if kind == "missing" else {"EXPLICIT_RECORD": record()}), max_concurrent_reads=1)) as source:
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                port = HttpProviderProbe(client=client, source=source, base_url="https://provider.invalid/v1", path="/health", secret_ref="provider-key")
                observation = await ProviderProbeRunner().run_due(POLICY, port)
        assert observation.outcome.code == code
        assert "secret endpoint" not in repr(observation) and "confidential" not in repr(observation)
        if kind != "network":
            assert observation.outcome.availability_sample is None
    asyncio.run(run())


def test_credential_resolution_timeout_is_not_provider_availability_failure():
    async def run():
        class Source:
            async def resolve(self, reference):
                await asyncio.Future()
        port = HttpProviderProbe(client=None, source=Source(), base_url="https://provider.invalid/v1",
            path="/health", secret_ref="provider-key")
        runner = ProviderProbeRunner()
        observation = await runner.run_due(POLICY, port)
        assert observation.outcome == ProbeOutcome("provider_credentials_unavailable")
        assert observation.outcome.availability_sample is None and not runner._busy
    asyncio.run(run())


def test_local_response_cleanup_timeout_does_not_become_provider_health_failure():
    async def run():
        class Body(httpx.AsyncByteStream):
            async def __aiter__(self):
                yield b""
            async def aclose(self):
                await asyncio.Future()
        def handler(request):
            return httpx.Response(200, stream=Body())
        with closing(AsyncCredentialResolver(environment_source({"EXPLICIT_RECORD": record()}), max_concurrent_reads=1)) as source:
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                port = HttpProviderProbe(client=client, source=source, base_url="https://provider.invalid/v1", path="/health", secret_ref="provider-key")
                observation = await ProviderProbeRunner().run_due(POLICY, port)
        assert observation.outcome == ProbeOutcome("internal")
        assert observation.outcome.availability_sample is None
    asyncio.run(run())
