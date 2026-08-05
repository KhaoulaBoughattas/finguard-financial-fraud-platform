"""Unit tests for API key authentication -- pure function tests, no
Postgres/MLflow dependency needed since require_api_key only touches
config and the provided header value."""
import pytest
from fastapi import HTTPException

from src.api import auth
from src.common.config import get_settings


@pytest.fixture(autouse=True)
def _reset_warned_flag():
    """auth.py warns only once per process (module-level flag) -- reset it
    between tests so each test's warning-or-not behavior is independent."""
    auth._warned_no_key_configured = False
    yield
    auth._warned_no_key_configured = False


def test_no_key_configured_allows_request_through(monkeypatch):
    monkeypatch.setenv("SECURITY_API_KEY", "")
    get_settings.cache_clear()
    # should not raise
    auth.require_api_key(provided_key=None)
    get_settings.cache_clear()


def test_configured_key_rejects_missing_header(monkeypatch):
    monkeypatch.setenv("SECURITY_API_KEY", "correct-key")
    get_settings.cache_clear()
    with pytest.raises(HTTPException) as exc_info:
        auth.require_api_key(provided_key=None)
    assert exc_info.value.status_code == 401
    get_settings.cache_clear()


def test_configured_key_rejects_wrong_header(monkeypatch):
    monkeypatch.setenv("SECURITY_API_KEY", "correct-key")
    get_settings.cache_clear()
    with pytest.raises(HTTPException) as exc_info:
        auth.require_api_key(provided_key="wrong-key")
    assert exc_info.value.status_code == 401
    get_settings.cache_clear()


def test_configured_key_accepts_correct_header(monkeypatch):
    monkeypatch.setenv("SECURITY_API_KEY", "correct-key")
    get_settings.cache_clear()
    # should not raise
    auth.require_api_key(provided_key="correct-key")
    get_settings.cache_clear()


def test_empty_string_provided_key_is_rejected_when_key_configured(monkeypatch):
    """An empty string is not the same as 'no key sent' -- both should be
    rejected when a real key is configured, not accidentally treated as a
    match via falsy-value confusion."""
    monkeypatch.setenv("SECURITY_API_KEY", "correct-key")
    get_settings.cache_clear()
    with pytest.raises(HTTPException) as exc_info:
        auth.require_api_key(provided_key="")
    assert exc_info.value.status_code == 401
    get_settings.cache_clear()
