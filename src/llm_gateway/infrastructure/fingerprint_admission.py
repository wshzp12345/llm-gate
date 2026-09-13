"""Atomic PostgreSQL portion of unkeyed fingerprinted admission.

The mandatory security_scope(fence) must serialize local revocation observations
through COMMIT and yield a synchronous observed-fence checker. It is an outer
application dependency, not a permissive default. Full admission composition,
including authorization, capacity and routing preparation, remains caller-owned.
"""

from datetime import timedelta

from llm_gateway.domain.fingerprints import FingerprintIdentity
from llm_gateway.domain.invocation import InvocationAdmission, InvocationPersistenceUnavailable
from llm_gateway.domain.routing_seed import routing_seed_message
from llm_gateway.infrastructure.fingerprint_protection import PostgresFingerprintProtection
from llm_gateway.infrastructure.safety_refusal import lock_safety_policy


class PostgresFingerprintAdmission:
    def __init__(self, store):
        self._store = store

    async def admit(self, admission: InvocationAdmission, request: FingerprintIdentity,
                    execution: FingerprintIdentity, fence, *, routing_policy: str,
                    routing_seed: str, security_scope):
        # Validate all supplied non-content identities before connecting.
        routing_seed_message(admission.call_id, routing_policy, admission.configuration_revision)
        if (type(request) is not FingerprintIdentity or type(execution) is not FingerprintIdentity
                or (request.key_id, request.key_version) != (fence.key_id, fence.key_version)
                or (execution.key_id, execution.key_version) != (fence.key_id, fence.key_version)
                or type(routing_seed) is not str or len(routing_seed) != 64
                or any(char not in "0123456789abcdef" for char in routing_seed)):
            raise ValueError("Consistent fingerprinted admission identities required")
        async with security_scope(fence) as check_observed:
            async with self._store.transaction() as connection:
                check_observed(fence)
                # Publication updates this singleton: hold its lock until COMMIT.
                cursor = await connection.execute("SELECT revision FROM config_active WHERE singleton FOR SHARE")
                row = await cursor.fetchone()
                if row is None or row[0] != int(admission.configuration_revision):
                    raise InvocationPersistenceUnavailable()
                cursor = await connection.execute("""
                    SELECT snapshot->'model_aliases'->%s->>'routing_policy'
                    FROM config_revision WHERE revision=%s
                    """, (admission.requested_model, row[0]))
                alias = await cursor.fetchone()
                if alias is None or alias[0] != routing_policy:
                    raise InvocationPersistenceUnavailable()
                accepted_at, ttl = await self._store.insert_shell(connection, admission)
                await lock_safety_policy(connection, admission.call_id)
                await PostgresFingerprintProtection.extend(connection, fence, ttl_seconds=ttl,
                                                           check_observed=check_observed)
                await connection.execute("""
                    INSERT INTO invocation_fingerprints
                    (call_id,key_id,key_version,invalidation_generation,algorithm,request_profile,request_digest,
                     execution_profile,execution_digest,routing_policy,routing_seed,expires_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """, (admission.call_id, fence.key_id, fence.key_version, fence.invalidation_generation,
                          request.algorithm, request.canonicalization, request.digest,
                          execution.canonicalization, execution.digest, routing_policy, routing_seed,
                          accepted_at+timedelta(seconds=ttl)))
                check_observed(fence)
            return accepted_at
