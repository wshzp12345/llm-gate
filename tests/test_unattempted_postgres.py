import asyncio
from dataclasses import replace

import psycopg
import pytest

from llm_gateway.domain.invocation import InvocationPersistenceUnavailable, InvocationTerminalConflict
from llm_gateway.domain.routing_evidence import EvidenceCandidate
from llm_gateway.domain.routing_eligibility import StaticRoutingReason
from llm_gateway.infrastructure.routing_evidence import PostgresRoutingEvidence
from llm_gateway.infrastructure.settlement import PostgresInvocationSettlement
from tests.test_configuration import database, run, scalar
from tests.test_invocation import admission
from tests.test_invocation_postgres import setup
from tests.test_routing_evidence import preselection


@pytest.mark.postgres
@pytest.mark.parametrize("case", ["unvisited", "allowed", "started", "missing_candidate"])
def test_incomplete_exhaustion_evidence_never_becomes_terminal(database, case):
    async def scenario():
        store, record = await setup(database)
        evidence = PostgresRoutingEvidence(store)
        if case == "missing_candidate":
            record = replace(record, call_id=admission().call_id)
            await store.admit_unkeyed_shell(record)
            # Valid local ordering alone does not prove the full Alias batch.
            partial = replace(preselection(), candidates=preselection().candidates[:1])
            await evidence.preselect(record.call_id, partial)
            await evidence.gate(record.call_id, "a", (StaticRoutingReason("concurrency_exhausted"),))
        elif case in {"allowed", "started"}:
            await evidence.gate(record.call_id, "a")
            if case == "started":
                await store.journal(record.call_id).started(1, "a", 1)
        with pytest.raises(InvocationPersistenceUnavailable):
            await PostgresInvocationSettlement(store, record.call_id).settle_exhausted()
    run(scenario())
    assert scalar(database, "SELECT count(*) FROM invocation_settlement") == 0
    assert scalar(database, "SELECT count(*) FROM routing_terminal") == 0


@pytest.mark.postgres
def test_unattempted_terminal_has_one_owner_and_fixed_ttl(database):
    async def scenario():
        store, record = await setup(database)
        evidence = PostgresRoutingEvidence(store)
        for name in ("a", "b", "c"):
            await evidence.gate(record.call_id, name, (StaticRoutingReason("concurrency_exhausted"),))
        settlement = PostgresInvocationSettlement(store, record.call_id)
        results = await asyncio.gather(settlement.settle_exhausted(), settlement.settle_exhausted(), return_exceptions=True)
        assert sum(isinstance(result, InvocationTerminalConflict) for result in results) == 1
        assert sum(getattr(result, "code", None) == "rate_limited" for result in results) == 1
    run(scenario())
    assert scalar(database, "SELECT count(*) FROM invocation_settlement") == 1
    assert scalar(database, "SELECT extract(epoch FROM expires_at-terminal_at) FROM routing_terminal") == 2592000
    assert scalar(database, "SELECT t.terminal_at=s.terminal_at FROM routing_terminal t JOIN invocation_settlement s USING(call_id)")


@pytest.mark.postgres
def test_all_static_rejections_commit_no_route_without_usage_or_cost(database):
    async def scenario():
        store, previous = await setup(database)
        record = replace(previous, call_id=admission().call_id)
        await store.admit_unkeyed_shell(record)
        selected = preselection()
        selected = replace(selected, candidates=tuple(EvidenceCandidate(item.candidate, None,
            (StaticRoutingReason("provider_disabled"),)) for item in selected.candidates))
        await PostgresRoutingEvidence(store).preselect(record.call_id, selected)
        result = await PostgresInvocationSettlement(store, record.call_id).settle_exhausted()
        assert result.code == "no_route"
        with psycopg.connect(database) as connection:
            row = connection.execute("SELECT attempt_count,input_tokens,output_tokens,resolved_model FROM invocation_settlement WHERE call_id=%s", (record.call_id,)).fetchone()
        assert row == (0, None, None, None)
    run(scenario())
