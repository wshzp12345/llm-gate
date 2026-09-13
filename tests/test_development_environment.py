import asyncio

import pytest

from llm_gateway.infrastructure.development_environment import load_development_environment, DevelopmentEnvironmentError, gateway_database_url
from llm_gateway.infrastructure import development_check
from llm_gateway.domain.configuration import ConfigurationPersistenceUnavailable, ConfigurationDeadlineExceeded


def test_explicit_file_parsing_does_not_interpolate_or_disclose(tmp_path):
    path = tmp_path / ".env.local"
    path.write_text('AGENT_RUNTIME_POSTGRES_URL="postgresql://fixture/database"\nDEEPSEEK_API_KEY=literal-${OTHER}\n', encoding="utf-8")
    environment = {"OTHER": "do-not-expand"}
    result = load_development_environment(path, environ=environment)
    assert result.database_url == "postgresql://fixture/database"
    assert result.deepseek_api_key == "literal-${OTHER}"
    assert "literal" not in repr(result) and "postgresql" not in repr(result)
    assert environment == {"OTHER": "do-not-expand"}


def test_process_environment_overrides_file_across_database_aliases(tmp_path):
    path = tmp_path / ".env.local"
    path.write_text("GATEWAY_DATABASE_URL=file\nDEEPSEEK_API_KEY=file-key\n", encoding="utf-8")
    result = load_development_environment(path, environ={"AGENT_RUNTIME_POSTGRES_URL": "process", "DEEPSEEK_API_KEY": "process-key"})
    assert result.database_url == "process" and result.deepseek_api_key == "process-key"


def test_environment_only_and_gateway_specific_name_precedence(tmp_path):
    result = load_development_environment(tmp_path / "missing", environ={
        "GATEWAY_DATABASE_URL": "specific", "AGENT_RUNTIME_POSTGRES_URL": "fallback"})
    assert result.database_url == "specific" and result.deepseek_api_key is None


def test_sqlalchemy_psycopg_url_changes_only_driver_prefix(tmp_path):
    suffix = "user:p%40ss@localhost/database?sslmode=require&application_name=gateway"
    result = load_development_environment(tmp_path / "absent", environ={
        "AGENT_RUNTIME_POSTGRES_URL": "postgresql+psycopg://" + suffix})
    assert result.database_url == "postgresql://" + suffix


@pytest.mark.parametrize("body", [
    "DEEPSEEK_API_KEY=only-key\n", "AGENT_RUNTIME_POSTGRES_URL=\n",
    "AGENT_RUNTIME_POSTGRES_URL=one\nAGENT_RUNTIME_POSTGRES_URL=two\n",
    'AGENT_RUNTIME_POSTGRES_URL="unterminated',
    "AGENT_RUNTIME_POSTGRES_URL=valid\nDEEPSEEK_API_KEY\n",
])
def test_invalid_file_fails_without_content_disclosure(tmp_path, body):
    path = tmp_path / ".env.local"
    path.write_text(body, encoding="utf-8")
    with pytest.raises(DevelopmentEnvironmentError) as failure:
        load_development_environment(path, environ={})
    assert str(failure.value) == "Development environment is missing or invalid"


def test_empty_process_override_does_not_resurrect_file_credentials(tmp_path):
    path = tmp_path / ".env.local"
    path.write_text("GATEWAY_DATABASE_URL=file\n", encoding="utf-8")
    with pytest.raises(DevelopmentEnvironmentError):
        load_development_environment(path, environ={"GATEWAY_DATABASE_URL": ""})


@pytest.mark.parametrize("fault,expected,code", [
    (None, "verified", 0), (ConfigurationPersistenceUnavailable(), "unavailable_or_incompatible", 1),
    (ConfigurationDeadlineExceeded(), "deadline_exceeded", 1),
])
def test_preflight_is_safe_and_never_claims_gateway_readiness(tmp_path, monkeypatch, fault, expected, code):
    monkeypatch.setenv("GATEWAY_DATABASE_URL", "postgresql://fixture:synthetic-sensitive-connection@localhost/example")
    monkeypatch.setenv("DEEPSEEK_API_KEY", "synthetic-sensitive-key")
    calls = []

    async def verify(dsn, **kwargs):
        calls.append((dsn, kwargs))
        if fault:
            raise fault

    monkeypatch.setattr(development_check, "verify_schema", verify)
    actual, result = asyncio.run(development_check.check(tmp_path / "absent"))
    assert actual == code and result["schema"] == expected
    assert result["gateway_ready"] is False and result["deepseek_key_configured"] is True
    assert "synthetic" not in repr(result)
    assert len(calls) == 1 and calls[0][1]["timeout_seconds"] == 5


def test_gateway_schema_override_preserves_other_connection_options():
    from psycopg.conninfo import conninfo_to_dict
    original = "host=localhost dbname=fixture options='-cstatement_timeout=3000 -csearch_path=public'"
    scoped = conninfo_to_dict(gateway_database_url(original))
    assert scoped["options"] == "-cstatement_timeout=3000 -csearch_path=public -csearch_path=llm_gateway"
    assert scoped["dbname"] == "fixture"


def test_invalid_database_url_is_not_echoed():
    with pytest.raises(DevelopmentEnvironmentError) as failure:
        gateway_database_url("sensitive-invalid-connection")
    assert "sensitive" not in str(failure.value)
