"""Structured logging configuration shared by every FinGuard service.

Uses stdlib logging with a JSON formatter so logs are machine-parseable
(important later for correlating logs with Prometheus/Grafana and for
running in containers where stdout is scraped).
"""
from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any


class JsonFormatter(logging.Formatter):
    """Renders each log record as a single JSON line."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        # allow callers to pass extra={"context": {...}}
        context = getattr(record, "context", None)
        if context:
            payload["context"] = context
        return json.dumps(payload, default=str)


def configure_logging(level: str = "INFO") -> None:
    """Configure the root logger once, idempotently, for the whole process."""
    root = logging.getLogger()
    if root.handlers:
        # Already configured (e.g. re-imported in tests) -- avoid duplicate handlers.
        root.setLevel(level.upper())
        return

    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(JsonFormatter())
    root.addHandler(handler)
    root.setLevel(level.upper())


def get_logger(name: str) -> logging.Logger:
    """Convenience accessor used by application modules."""
    return logging.getLogger(name)
