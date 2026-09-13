import asyncio
from dataclasses import replace

import psycopg
import pytest

from llm_gateway.application.fingerprint_keys import FingerprintFence, FingerprintVersionInvalidated
from llm_gateway.domain.fingerprint_keys import FingerprintKeyMember, FingerprintKeyRing
from llm_gateway.domain.invocation import InvocationPersistenceUnavailable
from llm_gateway.infrastructure.fingerprint_protection import PostgresFingerprintProtection
from llm_gateway.infrastructure.invocation import PostgresInvocationStore
from tests.test_configuration import database, run, scalar


ACTIVE = FingerprintKeyMember("fp", "v2", "active", "ref2")
OLD = FingerprintKeyMember("fp", "v1", "verification_only", "ref1")
FENCE = FingerprintFence("fp", "v2", 0, 0)


def observed(fence):
    assert fence.observed_generation == 0


@pytest.mark.postgres
def test_startup_reads_no_rows_and_horizon_is_atomic_monotonic_database_time(database):
    async def scenario():
        store = PostgresInvocationStore(database)
        protection = PostgresFingerprintProtection(store)
        await protection.check_ring(FingerprintKeyRing((ACTIVE,)))
        assert await protection.generation(ACTIVE) == 0
        assert scalar(database, "SELECT count(*) FROM fingerprint_key_protection") == 0
        with pytest.raises(RuntimeError):
            async with store.transaction() as connection:
                await protection.extend(connection, FENCE, ttl_seconds=100, check_observed=observed)
                raise RuntimeError("dependent admission failed")
        assert scalar(database, "SELECT count(*) FROM fingerprint_key_protection") == 0
        async def extend(ttl):
            async with store.transaction() as connection:
                await protection.extend(connection, FENCE, ttl_seconds=ttl, check_observed=observed)
        await asyncio.gather(extend(100), extend(200), extend(50))
        first = scalar(database, "SELECT protected_until FROM fingerprint_key_protection")
        await extend(1)
        assert scalar(database, "SELECT protected_until FROM fingerprint_key_protection") == first
        assert scalar(database, "SELECT protected_until > statement_timestamp()+interval '190 seconds' FROM fingerprint_key_protection")
        async with store.transaction() as connection:
            await protection.guard(connection, FENCE, check_observed=observed)
    run(scenario())


@pytest.mark.postgres
def test_ring_cannot_omit_protected_version_or_include_invalidated_version(database):
    with psycopg.connect(database) as connection:
        connection.execute("""INSERT INTO fingerprint_key_protection
            (key_id,key_version,protected_until) VALUES ('fp','v1',statement_timestamp()+interval '1 day')""")
    async def scenario():
        protection = PostgresFingerprintProtection(PostgresInvocationStore(database))
        with pytest.raises(InvocationPersistenceUnavailable):
            await protection.check_ring(FingerprintKeyRing((ACTIVE,)))
        await protection.check_ring(FingerprintKeyRing((ACTIVE, OLD)))
    run(scenario())
    # Preexisting invalidated/expired versions are fixture data, not a mutation API.
    with psycopg.connect(database) as connection:
        connection.execute("""INSERT INTO fingerprint_key_protection
            (key_id,key_version,protected_until,invalidated_at,invalidation_generation)
            VALUES ('fp','revoked',statement_timestamp()+interval '1 day',statement_timestamp(),1),
                   ('fp','expired',statement_timestamp()-interval '1 day',NULL,0)""")
    async def invalidated():
        protection = PostgresFingerprintProtection(PostgresInvocationStore(database))
        await protection.check_ring(FingerprintKeyRing((ACTIVE, OLD)))
        revoked = replace(OLD, key_version="revoked", secret_ref="revoked-ref")
        with pytest.raises(FingerprintVersionInvalidated):
            await protection.check_ring(FingerprintKeyRing((ACTIVE, OLD, revoked)))
        with pytest.raises(FingerprintVersionInvalidated):
            await protection.generation(revoked)
    run(invalidated())


@pytest.mark.postgres
def test_generation_and_local_fence_failure_roll_back_new_protection(database):
    async def scenario():
        store = PostgresInvocationStore(database)
        protection = PostgresFingerprintProtection(store)
        for fence in (FENCE, replace(FENCE, invalidation_generation=1)):
            with pytest.raises(InvocationPersistenceUnavailable):
                async with store.transaction() as connection:
                    await protection.guard(connection, fence, check_observed=observed)
        with pytest.raises(InvocationPersistenceUnavailable):
            async with store.transaction() as connection:
                await protection.extend(connection, replace(FENCE, invalidation_generation=1),
                                        ttl_seconds=100, check_observed=observed)
        checks = 0
        def revoked_during_write(fence):
            nonlocal checks
            checks += 1
            if checks == 4:
                raise InvocationPersistenceUnavailable()
        with pytest.raises(InvocationPersistenceUnavailable):
            async with store.transaction() as connection:
                await protection.extend(connection, FENCE, ttl_seconds=100, check_observed=revoked_during_write)
        assert checks == 4
        assert scalar(database, "SELECT count(*) FROM fingerprint_key_protection") == 0
    run(scenario())


@pytest.mark.postgres
@pytest.mark.parametrize("statement", [
    "DELETE FROM fingerprint_key_protection",
    "UPDATE fingerprint_key_protection SET protected_until=statement_timestamp()-interval '1 day'",
    "UPDATE fingerprint_key_protection SET key_version='replacement'",
    "UPDATE fingerprint_key_protection SET invalidation_generation=1",
    "UPDATE fingerprint_key_protection SET invalidated_at=statement_timestamp()",
])
def test_no_shrink_delete_or_v01_invalidation_mutation(database, statement):
    with psycopg.connect(database) as connection:
        connection.execute("""INSERT INTO fingerprint_key_protection
            (key_id,key_version,protected_until) VALUES ('fp','v2',statement_timestamp()+interval '1 day')""")
    with pytest.raises(psycopg.Error), psycopg.connect(database) as connection:
        connection.execute(statement)
