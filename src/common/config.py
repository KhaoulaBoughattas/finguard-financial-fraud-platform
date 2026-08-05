"""Centralized configuration for FinGuard Financial.

All configuration is loaded from environment variables (via a .env file in
local development). Nothing here should ever contain a hardcoded secret --
see .env.example for the variables this module expects.
"""
from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class PostgresSettings(BaseSettings):
    """Connection settings for the serving-layer PostgreSQL database."""

    model_config = SettingsConfigDict(
        env_prefix="POSTGRES_", env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    host: str = Field(default="localhost")
    port: int = Field(default=5432)
    db: str = Field(default="finguard")
    user: str = Field(default="finguard")
    password: str = Field(default="")

    @property
    def dsn(self) -> str:
        return (
            f"postgresql://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.db}"
        )


class GeneratorSettings(BaseSettings):
    """Settings controlling the synthetic data generator."""

    model_config = SettingsConfigDict(
        env_prefix="GENERATOR_", env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    num_customers: int = Field(default=2_000)
    num_accounts: int = Field(default=2_500)
    num_merchants: int = Field(default=300)
    num_transactions: int = Field(default=50_000)
    fraud_rate: float = Field(default=0.015, description="Target overall fraud rate")
    seed: int = Field(default=42)
    output_dir: str = Field(default="data/raw")


class KafkaSettings(BaseSettings):
    """Kafka connection settings.

    bootstrap_servers defaults to localhost:9092 -- the HOST listener
    published by docker-compose.yml (see docs/kafka.md for the full
    host-vs-internal-listener explanation). Code running INSIDE a compose
    service (e.g. a future Spark container in Phase 3) should instead set
    KAFKA_BOOTSTRAP_SERVERS=kafka:19092 to reach the internal PLAINTEXT
    listener -- never hardcode either address in application code.
    """

    model_config = SettingsConfigDict(
        env_prefix="KAFKA_", env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    bootstrap_servers: str = Field(default="localhost:9092")


class DeltaSettings(BaseSettings):
    """Delta Lake table and checkpoint locations for the Phase 3 streaming
    pipeline. Local paths default under ./data/; the Azure target replaces
    these with ADLS Gen2 paths (abfss://...) -- application code should
    always read the path from here, never hardcode a scheme or root."""

    model_config = SettingsConfigDict(
        env_prefix="DELTA_", env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    lake_root: str = Field(default="data/delta")
    checkpoint_root: str = Field(default="data/checkpoints")

    @property
    def bronze_transactions_path(self) -> str:
        return f"{self.lake_root}/bronze/transactions"

    @property
    def silver_transactions_path(self) -> str:
        return f"{self.lake_root}/silver/transactions"

    @property
    def bronze_checkpoint_path(self) -> str:
        return f"{self.checkpoint_root}/bronze/transactions"

    @property
    def silver_checkpoint_path(self) -> str:
        return f"{self.checkpoint_root}/silver/transactions"


class MLSettings(BaseSettings):
    """Settings for the Phase 5 ML pipeline (training, MLflow tracking)."""

    model_config = SettingsConfigDict(
        env_prefix="ML_", env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    # SQLite, not the plain filesystem store: MLflow 3.x deprecated the
    # filesystem backend (see docs/ml.md) -- sqlite is still fully local,
    # no network or server process required, and unlike the filesystem
    # store it supports the Model Registry features this phase uses.
    mlflow_tracking_uri: str = Field(default="sqlite:///data/mlflow/mlflow.db")
    experiment_name: str = Field(default="finguard-fraud-detection")
    random_seed: int = Field(default=42)


class SecuritySettings(BaseSettings):
    """API authentication settings (Phase 9).

    `api_key` gates the sensitive FastAPI endpoints (see src/api/auth.py)
    -- required via an `X-API-Key` header, checked with a constant-time
    comparison. Left empty by default (auth effectively disabled) ONLY to
    keep local development/testing frictionless without a mandatory secret
    in .env.example; src/api/auth.py logs a clear warning at startup when
    this is empty, and docs/security.md documents this as a local-dev-only
    posture, not a production recommendation -- the same "may be
    unauthenticated locally, TLS+SASL+ACLs in production" pattern already
    used for Kafka (docs/kafka.md §9).
    """

    model_config = SettingsConfigDict(
        env_prefix="SECURITY_", env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    api_key: str = Field(default="")


class AppSettings(BaseSettings):
    """Top-level application settings."""

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    env: str = Field(default="local")
    log_level: str = Field(default="INFO")

    postgres: PostgresSettings = Field(default_factory=PostgresSettings)
    generator: GeneratorSettings = Field(default_factory=GeneratorSettings)
    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    delta: DeltaSettings = Field(default_factory=DeltaSettings)
    ml: MLSettings = Field(default_factory=MLSettings)
    security: SecuritySettings = Field(default_factory=SecuritySettings)


@lru_cache
def get_settings() -> AppSettings:
    """Return a cached, process-wide settings instance."""
    return AppSettings()
