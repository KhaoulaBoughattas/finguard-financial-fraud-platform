"""Shared configuration for all FinGuard Airflow DAGs.

Kept minimal and boring on purpose: default_args (retries, retry delay,
owner, alerting hooks) and the environment dict BashOperator tasks need to
reach Postgres -- both read from the SAME .env-driven settings the rest of
the codebase uses (src.common.config), not duplicated Airflow Variables,
so there is exactly one place credentials/hosts are configured.
"""
from __future__ import annotations

from datetime import timedelta

from src.common.config import get_settings

DEFAULT_ARGS = {
    "owner": "finguard-data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,  # no mail server configured locally; a real
    # deployment would set this True and wire in alerting (Phase 8 scope).
    "depends_on_past": False,
}


def postgres_env() -> dict[str, str]:
    """Environment variables BashOperator tasks (dbt) need to reach
    Postgres -- sourced from the same PostgresSettings the rest of the
    app uses, not hardcoded or duplicated as separate Airflow Variables."""
    settings = get_settings()
    return {
        "POSTGRES_HOST": settings.postgres.host,
        "POSTGRES_PORT": str(settings.postgres.port),
        "POSTGRES_DB": settings.postgres.db,
        "POSTGRES_USER": settings.postgres.user,
        "POSTGRES_PASSWORD": settings.postgres.password,
    }
