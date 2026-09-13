import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace

import psycopg
import pytest

from llm_gateway.application.configuration import ConfigurationCommands
from llm_gateway.application.fingerprint_keys import FingerprintFence, FingerprintKeys
from llm_gateway.infrastructure.fingerprint_protection import PostgresFingerprintProtection
from tests.test_fingerprint_leases import RING, Source, deadline
from tests.test_text_fingerprints import QUERY, LIMITS
from llm_gateway.adapters.text_fingerprints import request_fingerprint, execution_fingerprint
from llm_gateway.application.active_configuration import LoadedConfiguration
from llm_gateway.domain.fingerprints import FingerprintIdentity
from llm_gateway.domain.invocation import InvocationPersistenceUnavailable
from llm_gateway.infrastructure.configuration import PostgresConfigurationUnitOfWork
from llm_gateway.infrastructure.fingerprint_admission import PostgresFingerprintAdmission
from llm_gateway.infrastructure.invocation import PostgresInvocationStore
from llm_gateway.infrastructure.routing_evidence import PostgresRoutingEvidence
from tests.test_configuration import database, run, scalar, identity, PreparedFixture
from tests.test_configuration_preparation import draft
from tests.test_invocation import admission
from tests.test_routing_evidence import preselection


FENCE = FingerprintFence("fp", "v1", 0, 0)
REQUEST = FingerprintIdentity("request-test/v1", "fp", "v1", "1"*64)
EXECUTION = FingerprintIdentity("execution-test/v1", "fp", "v1", "2"*64)


async def prepare(database, *, body=None):
    commands = ConfigurationCommands(PostgresConfigurationUnitOfWork(database))
    value = draft(body)
    prepared = PreparedFixture()
    prepared.candidate = replace(prepared.candidate, snapshot_json=value.snapshot_json, snapshot_digest=value.snapshot_digest)
    created = await commands.create(identity(), None, prepared, timeout_seconds=5)
    published = await commands.publish(identity("publish"), created.revision.revision, None,
                                       created.revision.snapshot_digest, "test publication", timeout_seconds=5)
    assert published.error is None
    return PostgresInvocationStore(database), admission(created.revision.revision)


@asynccontextmanager
async def scope(fence):
    # Transaction component fixture only, NOT production revocation coordination.
    yield lambda actual: None


async def admit(store, record, *, fence=FENCE, security_scope=scope, policy="route-a"):
    return await PostgresFingerprintAdmission(store).admit(record, REQUEST, EXECUTION, fence,
        routing_policy=policy, routing_seed="a"*64, security_scope=security_scope)


@pytest.mark.postgres
def test_admission_commits_identity_and_protection_together_with_database_time(database):
    async def scenario():
        store, record = await prepare(database)
        released = []
        @asynccontextmanager
        async def committed_scope(fence):
            yield lambda actual: None
            assert scalar(database, "SELECT count(*) FROM invocation_fingerprints") == 1
            released.append(True)
        accepted = await admit(store, record, security_scope=committed_scope)
        assert accepted == scalar(database, "SELECT accepted_at FROM model_invocation")
        assert released == [True]
        assert scalar(database, """SELECT p.protected_until >= i.expires_at FROM fingerprint_key_protection p
                                  JOIN invocation_fingerprints i USING(key_id,key_version)""")
        assert scalar(database, "SELECT count(*) FROM provider_attempt") == 0
        with psycopg.connect(database) as connection:
            data = connection.execute("SELECT to_jsonb(f) FROM invocation_fingerprints f").fetchone()[0]
        assert data["request_digest"] == REQUEST.digest and data["execution_digest"] == EXECUTION.digest
        assert not {"messages", "secret_ref", "key_bytes", "canonical_request"} & set(data)
        with pytest.raises(InvocationPersistenceUnavailable):
            await PostgresRoutingEvidence(store).preselect(record.call_id, preselection())  # wrong policy
        correct = replace(preselection(), routing_policy="route-a")
        with pytest.raises(InvocationPersistenceUnavailable):
            await PostgresRoutingEvidence(store).preselect(record.call_id, replace(correct, seed_hex="b"*64))
        await PostgresRoutingEvidence(store).preselect(record.call_id, correct)
    run(scenario())


@pytest.mark.postgres
def test_active_pointer_remains_locked_through_admission_commit(database):
    async def scenario():
        store, record = await prepare(database)
        entered, release = asyncio.Event(), asyncio.Event()
        original = store.insert_shell
        async def paused(connection, value):
            result = await original(connection, value)
            entered.set()
            await release.wait()
            return result
        store.insert_shell = paused
        task = asyncio.create_task(admit(store, record))
        try:
            await asyncio.wait_for(entered.wait(), 2)
            with pytest.raises(psycopg.errors.LockNotAvailable), psycopg.connect(database) as connection:
                connection.execute("SELECT revision FROM config_active WHERE singleton FOR UPDATE NOWAIT")
        finally:
            release.set()
            await task
        with psycopg.connect(database) as connection:
            assert connection.execute("SELECT revision FROM config_active WHERE singleton FOR UPDATE NOWAIT").fetchone()[0] == int(record.configuration_revision)
    run(scenario())
    for statement in ("DELETE FROM invocation_fingerprints", "UPDATE invocation_fingerprints SET routing_seed=repeat('b',64)"):
        with pytest.raises(psycopg.Error), psycopg.connect(database) as connection:
            connection.execute(statement)


@pytest.mark.postgres
@pytest.mark.parametrize("overlapping_fault", [False, True])
def test_real_key_scope_and_database_admission_share_one_operation_identity(database, overlapping_fault):
    async def scenario():
        store, record = await prepare(database)
        source = Source()
        keys = FingerprintKeys(RING, source, PostgresFingerprintProtection(store))
        assert await keys.validate_active()
        value = draft()
        snapshot = LoadedConfiguration(record.configuration_revision, value.snapshot_digest,
                                       value.snapshot_json, value.validation_resources)
        failures = []
        if overlapping_fault:
            original = store.insert_shell
            async def observed_during_transaction(connection, admission_value):
                result = await original(connection, admission_value)
                source.fail.add(RING.active.identity)
                observed = asyncio.Event()
                original_resolve = source.resolve
                async def failing_resolve(member):
                    try:
                        return await original_resolve(member)
                    except Exception:
                        observed.set()
                        raise
                source.resolve = failing_resolve
                async def fault():
                    with pytest.raises(InvocationPersistenceUnavailable):
                        async with keys.active(deadline=deadline()):
                            pass
                    # Fault observation is completed only after the earlier effect.
                    assert scalar(database, "SELECT count(*) FROM invocation_fingerprints") == 1
                failures.append(asyncio.create_task(fault()))
                await asyncio.wait_for(observed.wait(), 2)
                assert not keys.ready and not failures[0].done()
                return result
            store.insert_shell = observed_during_transaction
        async with keys.active(deadline=deadline()) as lease:
            request = request_fingerprint(QUERY, record.authorization, lease)
            execution = execution_fingerprint(QUERY, snapshot, lease, effective_limits=LIMITS)
            seed = lease.routing_seed(record.call_id, "route-a", record.configuration_revision)
            accepted = await PostgresFingerprintAdmission(store).admit(record, request, execution, lease.fence,
                routing_policy="route-a", routing_seed=seed, security_scope=keys.security_scope)
            assert accepted == scalar(database, "SELECT accepted_at FROM model_invocation")
        for failure in failures:
            await failure
        assert len(source.calls) == (3 if overlapping_fault else 2)
        assert all(material._closed for material in source.materials)
        assert keys.in_use == 0
        assert scalar(database, "SELECT request_digest FROM invocation_fingerprints") == request.digest
        assert scalar(database, "SELECT routing_seed FROM invocation_fingerprints") == seed
    run(scenario())


@pytest.mark.postgres
@pytest.mark.parametrize("fault", ["revision", "generation", "local_fence", "write", "policy"])
def test_failed_admission_leaves_no_call_or_new_protection(database, fault):
    async def scenario():
        store, record = await prepare(database)
        fence, selected_scope, policy = FENCE, scope, "route-a"
        if fault == "revision":
            record = replace(record, configuration_revision="2")
        elif fault == "generation":
            fence = replace(fence, invalidation_generation=1)
        elif fault == "local_fence":
            checks = 0
            def reject_late(value):
                nonlocal checks
                checks += 1
                if checks == 6:
                    raise InvocationPersistenceUnavailable()
            @asynccontextmanager
            async def rejected_scope(value):
                yield reject_late
            selected_scope = rejected_scope
        elif fault == "write":
            with psycopg.connect(database) as connection:
                connection.execute("ALTER TABLE invocation_fingerprints ADD CONSTRAINT reject_test_write CHECK(false)")
        else:
            policy = "unrelated"
        with pytest.raises(InvocationPersistenceUnavailable):
            await admit(store, record, fence=fence, security_scope=selected_scope, policy=policy)
        for table in ("model_invocation", "invocation_fingerprints", "fingerprint_key_protection", "provider_attempt"):
            assert scalar(database, "SELECT count(*) FROM "+table) == 0
    run(scenario())
