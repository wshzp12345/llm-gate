from uuid import uuid4

import psycopg
import pytest

from llm_gateway.application.cancellation import CancellationCleanup
from llm_gateway.application.late_provider_outcome import LateTextOutcome
from llm_gateway.domain.model import FailureCode, ProviderFailure, Usage
from llm_gateway.infrastructure.migrate import migrate
from tests.test_configuration import database, run
from tests.test_late_provider_outcome import pending, RESULT


@pytest.mark.postgres
@pytest.mark.parametrize("database", ["0016_stream_commit.sql"], indirect=True)
def test_upgrade_retains_late_history_and_failed_observation_cannot_rebill(database):
    async def scenario():
        store, record, cancellation, port = await pending(database)
        await port.record(LateTextOutcome.from_result(uuid4(), 1, RESULT))
        await cancellation.finalize(record, CancellationCleanup(False, None, False))
        tables = ("late_provider_outcome", "model_invocation", "provider_attempt_outcome", "invocation_settlement",
                  "cost_accrual", "invocation_cost_summary")
        def snapshot(connection, table):
            # Compare all pre-existing fields while allowing new nullable identity columns.
            return connection.execute(f"SELECT to_jsonb(t) - ARRAY['task_id','turn_id','step_id'] FROM {table} t").fetchall()
        with psycopg.connect(database) as connection:
            before = {table: snapshot(connection, table) for table in tables}
        await migrate(database)
        with psycopg.connect(database) as connection:
            assert {table: snapshot(connection, table) for table in tables} == before
        failed = LateTextOutcome.from_result(uuid4(), 1, ProviderFailure(FailureCode.UNCERTAIN, False,
            observed_usage=Usage(10, 6, 0, 0, 16), observed_model="actual"))
        await port.record(failed)
        await port.record(failed)
        with psycopg.connect(database) as connection:
            assert connection.execute("SELECT count(*) FROM late_provider_outcome").fetchone() == (2,)
            for table in tables[1:]:
                assert snapshot(connection, table) == before[table]
            assert connection.execute("SELECT outcome,resolved_model,input_tokens,output_tokens FROM late_provider_outcome WHERE event_id=%s", (failed.event_id,)).fetchone() == (
                "uncertain", "actual", 10, 6)
    run(scenario())
