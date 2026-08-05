"""API key authentication for the FastAPI scoring service.

Simple, deliberately: a single shared API key checked via the `X-API-Key`
header, using a constant-time comparison (`secrets.compare_digest`) to
avoid a timing side-channel that could let an attacker guess the key
character-by-character. This is NOT the production target -- docs/security.md
documents OAuth2/JWT or an API gateway as the real target, matching the
project's established "document the production target, implement what's
appropriate locally" pattern (Kafka's TLS+SASL, the Azure Key Vault target
for secrets).
"""
from __future__ import annotations

import secrets

from fastapi import Depends, HTTPException, Security
from fastapi.security import APIKeyHeader

from src.common.config import get_settings
from src.common.logging import get_logger

logger = get_logger(__name__)

_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

_warned_no_key_configured = False


def require_api_key(provided_key: str | None = Security(_api_key_header)) -> None:
    """FastAPI dependency: raises 401 unless `provided_key` matches the
    configured SECURITY_API_KEY.

    If no key is configured (empty string, the .env.example default), auth
    is effectively disabled -- logged once as a warning, not silently, so
    a developer who forgets to set SECURITY_API_KEY notices in their logs
    rather than assuming the endpoint is protected when it isn't. See
    docs/security.md §2 for why this default exists and why it is
    explicitly NOT the production posture.
    """
    global _warned_no_key_configured

    configured_key = get_settings().security.api_key
    if not configured_key:
        if not _warned_no_key_configured:
            logger.warning(
                "SECURITY_API_KEY is not set -- API authentication is DISABLED. "
                "This is acceptable for local development only; see docs/security.md §2."
            )
            _warned_no_key_configured = True
        return

    if provided_key is None or not secrets.compare_digest(provided_key, configured_key):
        raise HTTPException(status_code=401, detail="Missing or invalid API key")


# Convenience alias for route decorators: `dependencies=[require_auth]`
require_auth = Depends(require_api_key)
