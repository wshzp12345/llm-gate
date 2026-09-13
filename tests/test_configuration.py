import asyncio
import os
from dataclasses import replace
from uuid import uuid4

import psycopg
import pytest
from psycopg.conninfo import make_conninfo

from llm_gateway.adapters.canonical_json import canonical_bytes, canonical_digest
from llm_gateway.application.configuration import ConfigurationCommands
from llm_gateway.domain.configuration import (
    CommandIdentity, ConfigurationDeadlineExceeded, PreparedCandidate, RevisionState,
    revision_number, ConfigurationDiagnostic, ConfigurationValidationFailed,
)
from llm_gateway.infrastructure.configuration import PostgresConfigurationUnitOfWork
from llm_gateway.infrastructure.migrate import migrate
from llm_gateway.infrastructure import migrate as migration_module


def run(coroutine):
    return asyncio.run(coroutine, loop_factory=asyncio.SelectorEventLoop)


@pytest.fixture
def database(request, monkeypatch):
    dsn = os.environ.get("GATEWAY_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("Set GATEWAY_TEST_DATABASE_URL to a disposable PostgreSQL database")
    schema = "gateway_test_" + uuid4().hex
    with psycopg.connect(dsn, autocommit=True) as connection:
        connection.execute(psycopg.sql.SQL("CREATE SCHEMA {} ").format(psycopg.sql.Identifier(schema)))
    isolated = make_conninfo(dsn, options=f"-csearch_path={schema}")
    try:
        version = getattr(request, "param", None)
        if version is None:
            run(migrate(isolated))
        else:
            manifest = migration_module._manifest()
            assert version in {entry[0] for entry in manifest}
            with monkeypatch.context() as patch:
                patch.setattr(migration_module, "_manifest", lambda: tuple(entry for entry in manifest if entry[0] <= version))
                run(migrate(isolated))
        yield isolated
    finally:
        # Only this fixture's randomly named schema is removed, never public or the database.
        with psycopg.connect(dsn, autocommit=True) as connection:
            connection.execute(psycopg.sql.SQL("DROP SCHEMA {} CASCADE").format(psycopg.sql.Identifier(schema)))


def identity(operation="create", body=None, **changes):
    return replace(CommandIdentity("test-tenant", "test-subject", operation, str(uuid4()), canonical_digest(body or {})), **changes)


class PreparedFixture:
    """Trusted component-test fixture; not a valid production Configuration Bundle."""

    def __init__(self, base=None, text="one"):
        self.calls = 0
        snapshot = {"schema_version": "gateway.config/v1", "test_fixture": text}
        self.candidate = PreparedCandidate(base, canonical_bytes(snapshot), canonical_digest(snapshot), b'{"operations":[]}', "test")

    async def prepare(self, transaction):
        self.calls += 1
        return self.candidate


def scalar(database, query):
    with psycopg.connect(database) as connection:
        return connection.execute(query).fetchone()[0]


@pytest.mark.postgres
def test_repeatable_migration(database):
    run(migrate(database))
    assert scalar(database, "SELECT count(*) FROM gateway_schema_migration") == 18


@pytest.mark.postgres
def test_create_replay_and_digest_conflict_before_preparation(database):
    async def scenario():
        commands = ConfigurationCommands(PostgresConfigurationUnitOfWork(database))
        command, prepared = identity(), PreparedFixture()
        first = await commands.create(command, None, prepared, timeout_seconds=5)
        repeated = await commands.create(command, None, prepared, timeout_seconds=5)
        conflict = await commands.create(replace(command, digest=canonical_digest({"changed": True})), None, prepared, timeout_seconds=5)
        assert first == repeated
        assert first.revision.state == RevisionState.CANDIDATE
        assert conflict.error == "conflict"
        assert prepared.calls == 1
    run(scenario())
    assert scalar(database, "SELECT count(*) FROM config_revision") == 1
    assert scalar(database, "SELECT count(*) FROM config_command_index") == 1
    assert scalar(database, "SELECT count(*) FROM config_command_audit") == 1


@pytest.mark.postgres
def test_concurrent_first_publish_has_one_winner_and_replay(database):
    async def scenario():
        commands = ConfigurationCommands(PostgresConfigurationUnitOfWork(database))
        candidates = []
        for _ in range(2):
            outcome = await commands.create(identity(), None, PreparedFixture(), timeout_seconds=5)
            candidates.append(outcome.revision)
        ids = [identity("publish", {"revision": candidate.revision}) for candidate in candidates]
        async def publish(index):
            candidate = candidates[index]
            return await commands.publish(ids[index], candidate.revision, None, candidate.snapshot_digest, "publish", timeout_seconds=5)
        results = await asyncio.gather(publish(0), publish(1))
        assert sum(result.error is None for result in results) == 1
        assert sum(result.error == "conflict" for result in results) == 1
        assert await publish(0) == results[0]
        assert await publish(1) == results[1]
    run(scenario())
    assert scalar(database, "SELECT count(*) FROM config_revision WHERE state='active'") == 1
    assert scalar(database, "SELECT count(*) FROM config_revision WHERE state='stale'") == 1
    assert scalar(database, "SELECT count(*) FROM config_command_index") == 4


@pytest.mark.postgres
def test_same_command_concurrently_creates_once(database):
    async def scenario():
        commands = ConfigurationCommands(PostgresConfigurationUnitOfWork(database))
        command, prepared = identity(), PreparedFixture()
        results = await asyncio.gather(*(commands.create(command, None, prepared, timeout_seconds=5) for _ in range(6)))
        assert all(result == results[0] for result in results)
        assert prepared.calls == 1
    run(scenario())
    assert scalar(database, "SELECT count(*) FROM config_revision") == 1


@pytest.mark.postgres
def test_failure_rolls_back_revision_index_and_audit(database):
    async def scenario():
        uow = PostgresConfigurationUnitOfWork(database)
        with pytest.raises(RuntimeError):
            async with uow.transaction(5) as tx:
                await tx.lock_active()
                await tx.insert_candidate(PreparedFixture().candidate)
                raise RuntimeError("injected before command record")
    run(scenario())
    assert scalar(database, "SELECT count(*) FROM config_revision") == 0
    assert scalar(database, "SELECT count(*) FROM config_command_index") == 0
    assert scalar(database, "SELECT count(*) FROM config_command_audit") == 0


@pytest.mark.postgres
def test_index_wait_respects_request_deadline(database):
    async def scenario():
        uow = PostgresConfigurationUnitOfWork(database)
        command = identity()
        async with uow.transaction(5) as leader:
            await leader.indexed_outcome(command)
            with pytest.raises(ConfigurationDeadlineExceeded):
                async with uow.transaction(0.15) as waiter:
                    await waiter.indexed_outcome(command)
    run(scenario())
    assert scalar(database, "SELECT count(*) FROM config_command_index") == 0


@pytest.mark.postgres
def test_revision_content_is_immutable(database):
    async def scenario():
        return await ConfigurationCommands(PostgresConfigurationUnitOfWork(database)).create(identity(), None, PreparedFixture(), timeout_seconds=5)
    result = run(scenario())
    with psycopg.connect(database) as connection:
        with pytest.raises(psycopg.Error):
            connection.execute("UPDATE config_revision SET snapshot='{}' WHERE revision=%s", (int(result.revision.revision),))


@pytest.mark.parametrize("value", ["0", "01", "-1", "1.0", 1, "9223372036854775808"])
def test_revision_string_profile(value):
    with pytest.raises(ValueError):
        revision_number(value)


def test_revision_preserves_bigint_precision():
    assert revision_number("9223372036854775807") == 9223372036854775807


def test_canonical_command_order_and_numbers():
    assert canonical_digest({"b": 1.0, "a": "文本"}) == canonical_digest({"a": "文本", "b": 1})
    with pytest.raises(ValueError):
        canonical_bytes({"bad": float("nan")})


@pytest.mark.postgres
def test_second_publish_supersedes_active_without_changing_original_replay(database):
    async def scenario():
        commands = ConfigurationCommands(PostgresConfigurationUnitOfWork(database))
        original_id, prepared = identity(), PreparedFixture()
        first = await commands.create(original_id, None, prepared, timeout_seconds=5)
        await commands.publish(identity("publish"), first.revision.revision, None, first.revision.snapshot_digest, "first", timeout_seconds=5)
        second = await commands.create(identity(), first.revision.revision, PreparedFixture(first.revision.revision, "two"), timeout_seconds=5)
        await commands.publish(identity("publish"), second.revision.revision, first.revision.revision, second.revision.snapshot_digest, "second", timeout_seconds=5)
        # Index lookup wins over a now-invalid null base and changed Revision state.
        assert await commands.create(original_id, None, prepared, timeout_seconds=5) == first
    run(scenario())
    assert scalar(database, "SELECT count(*) FROM config_revision WHERE state='superseded'") == 1
    assert scalar(database, "SELECT count(*) FROM config_revision WHERE state='active'") == 1


@pytest.mark.postgres
def test_deterministic_conflict_is_replayed_without_new_preparation(database):
    async def scenario():
        commands = ConfigurationCommands(PostgresConfigurationUnitOfWork(database))
        command, prepared = identity(), PreparedFixture("999")
        first = await commands.create(command, "999", prepared, timeout_seconds=5)
        assert first.error == "conflict"
        assert await commands.create(command, "999", prepared, timeout_seconds=5) == first
        assert prepared.calls == 0
    run(scenario())
    assert scalar(database, "SELECT count(*) FROM config_revision") == 0
    assert scalar(database, "SELECT count(*) FROM config_command_index") == 1


@pytest.mark.postgres
def test_cancelled_leader_releases_reservation_for_waiter(database):
    async def scenario():
        uow = PostgresConfigurationUnitOfWork(database)
        command = identity()
        locked, wait_forever = asyncio.Event(), asyncio.Event()
        async def leader():
            async with uow.transaction(5) as tx:
                await tx.indexed_outcome(command)
                await tx.lock_active()
                await tx.insert_candidate(PreparedFixture().candidate)
                locked.set()
                await wait_forever.wait()
        task = asyncio.create_task(leader())
        await asyncio.wait_for(locked.wait(), 3)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        result = await ConfigurationCommands(uow).create(command, None, PreparedFixture(), timeout_seconds=5)
        assert result.error is None
    run(scenario())
    assert scalar(database, "SELECT count(*) FROM config_revision") == 1
    assert scalar(database, "SELECT count(*) FROM config_command_index") == 1


@pytest.mark.postgres
def test_migration_checksum_mismatch_fails_closed(database):
    with psycopg.connect(database) as connection:
        connection.execute("UPDATE gateway_schema_migration SET checksum='changed-test-value'")
    with pytest.raises(RuntimeError, match="checksum mismatch"):
        run(migrate(database))


@pytest.mark.postgres
def test_command_identity_is_scoped_to_subject(database):
    async def scenario():
        commands = ConfigurationCommands(PostgresConfigurationUnitOfWork(database))
        command = identity()
        first = await commands.create(command, None, PreparedFixture(), timeout_seconds=5)
        second = await commands.create(replace(command, subject="other-subject"), None, PreparedFixture(), timeout_seconds=5)
        assert first.revision.revision != second.revision.revision
    run(scenario())


@pytest.mark.postgres
def test_semantic_validation_failure_is_durably_replayed(database):
    class InvalidFixture:
        calls = 0
        async def prepare(self, transaction):
            self.calls += 1
            raise ConfigurationValidationFailed((ConfigurationDiagnostic("reference_not_found", "/bundle/model_aliases/a/routing_policy"),))
    async def scenario():
        commands = ConfigurationCommands(PostgresConfigurationUnitOfWork(database))
        command, invalid = identity(), InvalidFixture()
        outcome = await commands.create(command, None, invalid, timeout_seconds=5)
        assert outcome.error == "configuration_invalid"
        assert await commands.create(command, None, invalid, timeout_seconds=5) == outcome
        assert invalid.calls == 1
    run(scenario())
    assert scalar(database, "SELECT count(*) FROM config_revision") == 0
    assert scalar(database, "SELECT count(*) FROM config_command_index") == 1
    assert scalar(database, "SELECT count(*) FROM config_command_audit") == 1


@pytest.mark.postgres
def test_typed_bundle_preparation_round_trips_through_postgresql(database):
    from llm_gateway.adapters.configuration_dto import BundleSubmission
    from llm_gateway.adapters.configuration_mapping import CanonicalConfigurationCodec, bundle_draft
    from llm_gateway.application.configuration_preparation import BundlePreparer
    from llm_gateway.application.configuration_validation import LocalConfigurationValidator
    from tests.config_fixtures import bundle_submission
    from tests.test_configuration_validation import POLICY, NoTrustBundleExpected

    async def scenario():
        commands = ConfigurationCommands(PostgresConfigurationUnitOfWork(database))
        validator = LocalConfigurationValidator(POLICY, NoTrustBundleExpected())
        body = bundle_submission()
        original = bundle_draft(BundleSubmission.model_validate(body).bundle)
        first = await commands.create(identity(), None, BundlePreparer(original, validator, CanonicalConfigurationCodec()), timeout_seconds=5)
        await commands.publish(identity("publish"), first.revision.revision, None, original.snapshot_digest, "publish typed bundle", timeout_seconds=5)
        body["bundle"]["base_revision"] = first.revision.revision
        repeated = bundle_draft(BundleSubmission.model_validate(body).bundle)
        second = await commands.create(identity(), repeated.base_revision, BundlePreparer(repeated, validator, CanonicalConfigurationCodec()), timeout_seconds=5)
        assert second.revision.snapshot_digest == first.revision.snapshot_digest
        assert second.revision.revision != first.revision.revision
    run(scenario())
    assert scalar(database, "SELECT change_set->'operations' FROM config_revision ORDER BY revision DESC LIMIT 1") == []
