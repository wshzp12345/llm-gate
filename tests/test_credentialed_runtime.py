import asyncio
from contextlib import asynccontextmanager
from uuid import uuid4

import httpx
import pytest

from llm_gateway.adapters.credentialed_runtime import CredentialBinding, CredentialedCandidateRuntime
from llm_gateway.adapters.provider_credentials import CredentialUnavailable
from llm_gateway.application.attempt_execution import ExecutionCandidate, FailureRecovery, SynchronousAttemptExecutor
from llm_gateway.domain.recovery import RetryPolicy
from tests.test_completion import REQUEST, envelope
from tests.test_development_secrets import environment_source, record


class Harness:
    def __init__(self, statuses=(503, 200), fail_at=None):
        self.events, self.leases = [], []
        self.env = {"EXPLICIT_RECORD": record()}
        self.statuses = iter(statuses)
        self.fail_at = fail_at

    async def resolve(self, secret_ref):
        self.events.append("resolve")
        lease = environment_source(self.env).resolve(secret_ref)
        self.leases.append(lease)
        return lease

    @asynccontextmanager
    async def gates(self, binding, metadata):
        self.events.append("gate")
        assert self.leases[-1].bearer_value()
        try:
            yield self.fail_at != "gate"
        finally:
            self.events.append("release")

    async def reject(self, binding, code):
        assert code == "provider_credentials_unavailable"
        self.events.append("reject")

    async def started(self, *args):
        self.events.append("start")
        assert self.leases[-1].bearer_value()
        if self.fail_at == "start":
            raise RuntimeError("checkpoint failed")

    async def finished(self, *args):
        self.events.append("finish")
        assert self.leases[-1].bearer_value()
        if self.fail_at == "finish":
            raise RuntimeError("checkpoint failed")

    async def sleep(self, seconds):
        self.events.append("sleep")
        assert all(not lease._material for lease in self.leases)

    def handler(self, request):
        self.events.append("io")
        assert request.headers["authorization"] == "Bearer " + self.leases[-1].bearer_value()
        if self.fail_at == "cancel":
            raise asyncio.CancelledError()
        return httpx.Response(next(self.statuses), json=envelope())

    def runtime(self, client):
        return CredentialedCandidateRuntime(source=self, gates=self.gates, reject=self.reject,
            bindings={"a": CredentialBinding("provider-key", "https://provider.invalid/v1", client)})

    async def run(self):
        async with httpx.AsyncClient(transport=httpx.MockTransport(self.handler), trust_env=False) as client:
            executor = SynchronousAttemptExecutor(runtime=self.runtime(client), journal=self,
                classify=lambda failure: FailureRecovery(failure.retryable, True),
                draw_jitter=lambda maximum: 0, sleep=self.sleep)
            return await executor.execute((ExecutionCandidate("a", REQUEST),), policy=RetryPolicy(),
                                          deadline=asyncio.get_running_loop().time() + 5)


def test_executor_resolves_before_each_start_and_releases_before_backoff():
    harness = Harness()
    result = asyncio.run(harness.run())
    assert result.output.text == "你好"
    assert harness.events == ["resolve", "gate", "start", "io", "finish", "release", "sleep",
                              "resolve", "gate", "start", "io", "finish", "release"]
    assert len(harness.leases) == 2 and harness.leases[0] is not harness.leases[1]
    assert all(not lease._material for lease in harness.leases)


@pytest.mark.parametrize("fail_at,error,io_count", [("start", RuntimeError, 0),
    ("finish", RuntimeError, 1), ("cancel", asyncio.CancelledError, 1)])
def test_checkpoint_failure_or_cancellation_closes_credential_without_retry(fail_at, error, io_count):
    harness = Harness(fail_at=fail_at)
    with pytest.raises(error):
        asyncio.run(harness.run())
    assert harness.events.count("io") == io_count
    assert "sleep" not in harness.events
    assert all(not lease._material for lease in harness.leases)


def test_missing_credential_records_rejection_without_provider_or_attempt():
    from llm_gateway.application.attempt_execution import NoEligibleCandidate
    harness = Harness()
    harness.env.clear()
    with pytest.raises(NoEligibleCandidate):
        asyncio.run(harness.run())
    assert harness.events == ["resolve", "reject"]


def test_remaining_gate_cannot_be_bypassed_by_valid_credentials():
    from llm_gateway.application.attempt_execution import NoEligibleCandidate
    harness = Harness(fail_at="gate")
    with pytest.raises(NoEligibleCandidate):
        asyncio.run(harness.run())
    assert harness.events == ["resolve", "gate", "release"]
    assert not harness.leases[0]._material


@pytest.mark.parametrize("fail_at", [None, "gate", "initialize", "retired"])
def test_lazy_client_initialization_is_after_gates_before_attempt_and_local_faults_do_not_retry(fail_at):
    from llm_gateway.application.attempt_execution import NoEligibleCandidate
    from llm_gateway.adapters.egress_pool import ProviderTransportRetired
    async def run():
        harness = Harness(statuses=(200,), fail_at=fail_at)
        async def reject(binding, code):
            assert code == "security_invalidated"
            harness.events.append("security_reject")
        async with httpx.AsyncClient(transport=httpx.MockTransport(harness.handler), trust_env=False) as client:
            def lazy():
                harness.events.append("initialize")
                if fail_at == "initialize":
                    raise RuntimeError("local transport initialization defect")
                if fail_at == "retired":
                    raise ProviderTransportRetired()
                return client
            runtime = CredentialedCandidateRuntime(source=harness, gates=harness.gates, reject=reject,
                bindings={"a": CredentialBinding("provider-key", "https://provider.invalid/v1", lazy)})
            executor = SynchronousAttemptExecutor(runtime=runtime, journal=harness,
                classify=lambda failure: pytest.fail("No Provider failure may be sampled"),
                draw_jitter=lambda maximum: 0, sleep=harness.sleep)
            async def execute():
                return await executor.execute((ExecutionCandidate("a", REQUEST),), policy=RetryPolicy(),
                                              deadline=asyncio.get_running_loop().time() + 5)
            if fail_at in {"gate", "retired"}:
                with pytest.raises(NoEligibleCandidate):
                    await execute()
            elif fail_at == "initialize":
                with pytest.raises(RuntimeError, match="initialization"):
                    await execute()
            else:
                assert (await execute()).output.text == "你好"
        expected = {
            None: ["resolve", "gate", "initialize", "start", "io", "finish", "release"],
            "gate": ["resolve", "gate", "release"],
            "initialize": ["resolve", "gate", "initialize", "release"],
            "retired": ["resolve", "gate", "initialize", "security_reject", "release"],
        }
        assert harness.events == expected[fail_at]
        assert all(not lease._material for lease in harness.leases)
    asyncio.run(run())


@pytest.mark.parametrize("status", [401, 403])
def test_rejected_version_is_blocked_but_new_version_can_be_acquired(status):
    async def run():
        harness = Harness(statuses=(status, 200))
        async with httpx.AsyncClient(transport=httpx.MockTransport(harness.handler), trust_env=False) as client:
            runtime = harness.runtime(client)
            async with runtime.acquire("a") as provider:
                result = await provider.complete(REQUEST)
                assert not result.retryable
            async with runtime.acquire("a") as provider:
                assert provider is None
            harness.env["EXPLICIT_RECORD"] = record(secret_version=str(uuid4()), value="new-test-token")
            async with runtime.acquire("a") as provider:
                assert provider is not None
                await provider.complete(REQUEST)
        assert harness.events.count("io") == 2
        assert harness.events.count("reject") == 1
        assert all(not lease._material for lease in harness.leases)
    asyncio.run(run())


def test_completion_handle_cannot_outlive_lease_or_be_reused():
    async def run():
        harness = Harness(statuses=(200,))
        async with httpx.AsyncClient(transport=httpx.MockTransport(harness.handler), trust_env=False) as client:
            runtime = harness.runtime(client)
            async with runtime.acquire("a") as escaped:
                pass
            with pytest.raises(CredentialUnavailable):
                await escaped.complete(REQUEST)
            async with runtime.acquire("a") as provider:
                await provider.complete(REQUEST)
                with pytest.raises(RuntimeError):
                    await provider.complete(REQUEST)
        assert harness.events.count("io") == 1
    asyncio.run(run())


@pytest.mark.parametrize("fail_at", ["gate", "rejection"])
def test_gate_or_rejection_persistence_failure_propagates_without_io(fail_at):
    async def run():
        harness = Harness()
        @asynccontextmanager
        async def broken_gate(binding, metadata):
            raise RuntimeError("gate checkpoint failed")
            yield True
        async def broken_rejection(binding, code):
            raise RuntimeError("rejection checkpoint failed")
        if fail_at == "rejection":
            harness.env.clear()
        async with httpx.AsyncClient(transport=httpx.MockTransport(harness.handler), trust_env=False) as client:
            runtime = CredentialedCandidateRuntime(source=harness, gates=broken_gate, reject=broken_rejection,
                bindings={"a": CredentialBinding("provider-key", "https://provider.invalid/v1", client)})
            with pytest.raises(RuntimeError, match="checkpoint failed"):
                async with runtime.acquire("a"):
                    pytest.fail("Checkpoint failure must not yield a Candidate")
        assert "io" not in harness.events
        assert all(not lease._material for lease in harness.leases)
    asyncio.run(run())
