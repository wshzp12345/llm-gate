import asyncio

import psycopg
import pytest
from psycopg.conninfo import make_conninfo

from llm_gateway.domain.configuration import ConfigurationDeadlineExceeded, ConfigurationPersistenceUnavailable
from llm_gateway.infrastructure.migrate import verify_schema
from tests.test_configuration import database, run, scalar


VERSION = "0018_invocation_correlation.sql"


@pytest.mark.parametrize("timeout", [0, -1, float("inf"), float("nan"), True])
def test_invalid_deadline_prevents_connection(timeout):
    with pytest.raises(ConfigurationDeadlineExceeded):
        asyncio.run(verify_schema("must not connect", expected_version=VERSION, timeout_seconds=timeout))


@pytest.mark.parametrize("version", ["", "gateway.config/v1", "0000_unknown.sql", "9999_future.sql"])
def test_bootstrap_expected_version_must_match_binary_before_connection(version):
    with pytest.raises(ConfigurationPersistenceUnavailable):
        asyncio.run(verify_schema("must not connect", expected_version=version, timeout_seconds=1))


@pytest.mark.postgres
def test_schema_verification_succeeds_in_read_only_session(database):
    dsn = make_conninfo(database, options=psycopg.conninfo.conninfo_to_dict(database)["options"] + " -cdefault_transaction_read_only=on")
    run(verify_schema(dsn, expected_version=VERSION, timeout_seconds=3))
    assert scalar(database, "SELECT count(*) FROM config_revision") == 0
    assert scalar(database, "SELECT count(*) FROM gateway_schema_migration") == 18


@pytest.mark.postgres
@pytest.mark.parametrize("fault", ["missing_table", "missing_version", "checksum", "future"])
def test_incompatible_history_is_rejected_without_repair(database, fault):
    with psycopg.connect(database) as connection:
        if fault == "missing_table":
            # This fixture owns a disposable random schema, never a user schema.
            connection.execute("DROP TABLE gateway_schema_migration")
        elif fault == "missing_version":
            connection.execute("DELETE FROM gateway_schema_migration")
        elif fault == "checksum":
            connection.execute("UPDATE gateway_schema_migration SET checksum='corrupt'")
        else:
            connection.execute("INSERT INTO gateway_schema_migration(version,checksum) VALUES ('9999_future.sql','future')")
    with pytest.raises(ConfigurationPersistenceUnavailable):
        run(verify_schema(database, expected_version=VERSION, timeout_seconds=3))
    if fault == "missing_table":
        assert scalar(database, "SELECT to_regclass('gateway_schema_migration')") is None
    elif fault == "missing_version":
        assert scalar(database, "SELECT count(*) FROM gateway_schema_migration") == 0
    elif fault == "checksum":
        assert scalar(database, "SELECT checksum FROM gateway_schema_migration") == "corrupt"
    else:
        assert scalar(database, "SELECT count(*) FROM gateway_schema_migration") == 19
