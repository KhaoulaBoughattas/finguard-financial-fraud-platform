import os

import pytest

from src.common.config import GeneratorSettings, PostgresSettings


def test_postgres_settings_defaults_when_no_env_vars():
    for key in list(os.environ):
        if key.startswith("POSTGRES_"):
            del os.environ[key]
    settings = PostgresSettings()
    assert settings.host == "localhost"
    assert settings.port == 5432
    assert settings.db == "finguard"


def test_postgres_settings_reads_from_environment(monkeypatch):
    monkeypatch.setenv("POSTGRES_HOST", "db.internal")
    monkeypatch.setenv("POSTGRES_PORT", "6543")
    monkeypatch.setenv("POSTGRES_DB", "custom_db")
    monkeypatch.setenv("POSTGRES_USER", "custom_user")
    monkeypatch.setenv("POSTGRES_PASSWORD", "secret")

    settings = PostgresSettings()
    assert settings.host == "db.internal"
    assert settings.port == 6543
    assert settings.db == "custom_db"
    assert settings.dsn == "postgresql://custom_user:secret@db.internal:6543/custom_db"


def test_generator_settings_defaults():
    for key in list(os.environ):
        if key.startswith("GENERATOR_"):
            del os.environ[key]
    settings = GeneratorSettings()
    assert settings.num_customers == 2_000
    assert settings.seed == 42
    assert 0 < settings.fraud_rate < 1


def test_generator_settings_override_via_environment(monkeypatch):
    monkeypatch.setenv("GENERATOR_NUM_CUSTOMERS", "10")
    monkeypatch.setenv("GENERATOR_SEED", "999")
    settings = GeneratorSettings()
    assert settings.num_customers == 10
    assert settings.seed == 999


def test_postgres_settings_rejects_non_integer_port(monkeypatch):
    monkeypatch.setenv("POSTGRES_PORT", "not-a-port")
    # broad on purpose: we only care that construction fails, not the exact
    # pydantic-settings exception type/wrapping across versions
    with pytest.raises(Exception):  # noqa: B017
        PostgresSettings()
