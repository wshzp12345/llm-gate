from uuid import uuid4

import pytest

from llm_gateway.infrastructure.invocation_observation import PostgresInvocationObservation, _model, _usage
from tests.test_configuration import database, run, scalar
from tests.test_fingerprint_admission_postgres import prepare, admit


@pytest.mark.parametrize("values,expected", [
    ((None, None), (None, False)),
    ((None, "actual", "actual"), ("actual", False)),
    (("first", "second"), (None, True)),
])
def test_model_evidence_does_not_invent_or_hide_conflicts(values, expected):
    assert _model(*values) == expected


def test_partial_total_is_not_split_into_invented_parent_counts():
    usage = _usage(dict(input_tokens=None, output_tokens=None, cached_tokens=None,
                        reasoning_tokens=None, provider_reported_total=12))
    assert usage.total_tokens is None and usage.provider_reported_total == 12


@pytest.mark.postgres
def test_missing_and_accepted_invocations_are_not_fabricated_terminals(database):
    async def scenario():
        reader = PostgresInvocationObservation(database)
        assert await reader.read(uuid4()) is None
        store, record = await prepare(database)
        await admit(store, record)
        observation = await reader.read(record.call_id)
        assert observation.state == "accepted" and observation.terminal_outcome is None
        assert observation.attempts == () and observation.costs == ()
        assert observation.settled_usage.total_tokens is None
        assert observation.actual_model is None
        assert scalar(database, "SELECT count(*) FROM invocation_settlement") == 0
        assert scalar(database, "SELECT count(*) FROM provider_attempt") == 0
    run(scenario())
