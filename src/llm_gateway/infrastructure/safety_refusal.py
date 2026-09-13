"""Non-content Safety Policy admission reference and valid refusal Evidence."""

from llm_gateway.adapters.canonical_json import canonical_digest
from llm_gateway.domain.invocation import InvocationPersistenceUnavailable


async def lock_safety_policy(connection, call_id):
    cursor = await connection.execute("""
        SELECT i.configuration_revision,c.snapshot->'model_aliases'->i.requested_model->>'safety_policy',c.snapshot
        FROM model_invocation i JOIN config_revision c ON c.revision=i.configuration_revision
        WHERE i.call_id=%s AND i.state='accepted' FOR UPDATE OF i
        """, (call_id,))
    row = await cursor.fetchone()
    if row is None or row[1] is None:
        raise InvocationPersistenceUnavailable()
    policy = row[2].get("safety_policies", {}).get(row[1])
    if policy != {"mode": "provider_refusal_terminal"}:
        raise InvocationPersistenceUnavailable()
    await connection.execute("""
        INSERT INTO invocation_safety_policy(call_id,configuration_revision,resource_id,content_digest,enforcement_identity)
        VALUES (%s,%s,%s,%s,'provider_refusal_terminal')
        """, (call_id, row[0], row[1], canonical_digest(policy)))


async def record_refusal(connection, call_id, number, result):
    has_refusal = result.output.refusal is not None
    filtered = result.finish_reason == "content_filter"
    signal = "both" if has_refusal and filtered else "message_refusal" if has_refusal else "content_filter"
    await connection.execute("INSERT INTO safety_refusal(call_id,attempt_number,signal) VALUES (%s,%s,%s)",
                             (call_id, number, signal))
