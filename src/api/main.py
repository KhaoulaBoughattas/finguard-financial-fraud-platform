"""FinGuard Financial scoring API (Phase 7).

Endpoints (per the Phase 0 spec):
    GET  /health
    GET  /customers/{customer_id}/risk
    GET  /transactions/{transaction_id}
    GET  /alerts
    POST /score-transaction

Every endpoint delegates to already-tested logic (src/ml/serving.py,
src/api/repository.py) -- route handlers here are thin: validate input
(via Pydantic), call the service/repository layer, map results/errors to
HTTP responses. No business logic lives in this file.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from time import perf_counter

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from prometheus_fastapi_instrumentator import Instrumentator

from src.api import repository
from src.api.auth import require_auth
from src.api.metrics import (
    ALERTS_LISTED_TOTAL,
    CUSTOMER_RISK_LOOKUPS_TOTAL,
    RISK_LEVEL_TOTAL,
    SCORING_ERRORS_TOTAL,
    SCORING_LATENCY_SECONDS,
    SCORING_REQUESTS_TOTAL,
)
from src.api.schemas import (
    AlertResponse,
    CustomerRiskResponse,
    ErrorResponse,
    HealthResponse,
    ScoreTransactionRequest,
    ScoreTransactionResponse,
    TransactionResponse,
)
from src.common.config import get_settings
from src.common.logging import configure_logging, get_logger
from src.ml.serving import ScoringInput, UnknownCustomerError, score_transaction

logger = get_logger(__name__)


@asynccontextmanager
async def _lifespan(_app: FastAPI):
    settings = get_settings()
    configure_logging(settings.log_level)
    logger.info("FinGuard API starting up")
    yield


app = FastAPI(
    title="FinGuard Financial Fraud Scoring API",
    description=(
        "Real-time transaction fraud scoring and risk lookup. "
        "See docs/api.md for the full architecture, including what this "
        "API does and does not guarantee."
    ),
    version="1.0.0",
    lifespan=_lifespan,
)

# Generic HTTP metrics (request count/latency/status per route) via a
# well-established library, rather than hand-rolling middleware for
# something this common -- exposes GET /metrics automatically, reading
# from the same global prometheus_client registry the domain-specific
# counters in src/api/metrics.py also register to, so both show up
# together at one scrape endpoint. See docs/observability.md §2.
Instrumentator().instrument(app).expose(app, endpoint="/metrics", include_in_schema=True)


def _decision_for_level(risk_level: str) -> str:
    return {
        "LOW": "APPROVE",
        "MEDIUM": "APPROVE",
        "HIGH": "REVIEW",
        "CRITICAL": "BLOCK",
    }.get(risk_level, "REVIEW")


@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    settings = get_settings()
    db_ok = repository.check_database_health(settings.postgres)

    model_status = "not_loaded"
    try:
        from src.ml.serving import _get_production_model

        _get_production_model()
        model_status = "loaded"
    except Exception as exc:  # noqa: BLE001 -- health check: report status, don't raise
        model_status = f"unavailable: {exc}"

    overall = "healthy" if db_ok and model_status == "loaded" else "degraded"
    return HealthResponse(
        status=overall,
        database="reachable" if db_ok else "unreachable",
        model=model_status,
    )


@app.get(
    "/customers/{customer_id}/risk",
    response_model=CustomerRiskResponse,
    responses={401: {"model": ErrorResponse}, 404: {"model": ErrorResponse}},
    dependencies=[require_auth],
)
def get_customer_risk(customer_id: str) -> CustomerRiskResponse:
    settings = get_settings()
    row = repository.get_customer_risk(settings.postgres, customer_id)
    CUSTOMER_RISK_LOOKUPS_TOTAL.labels(found=str(row is not None)).inc()
    if row is None:
        raise HTTPException(status_code=404, detail=f"No risk record found for customer_id={customer_id!r}")
    return CustomerRiskResponse(**row)


@app.get(
    "/transactions/{transaction_id}",
    response_model=TransactionResponse,
    responses={401: {"model": ErrorResponse}, 404: {"model": ErrorResponse}},
    dependencies=[require_auth],
)
def get_transaction(transaction_id: str) -> TransactionResponse:
    settings = get_settings()
    row = repository.get_transaction(settings.postgres, transaction_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Transaction {transaction_id!r} not found")
    return TransactionResponse(**row)


@app.get(
    "/alerts",
    response_model=list[AlertResponse],
    responses={401: {"model": ErrorResponse}},
    dependencies=[require_auth],
)
def list_alerts(
    customer_id: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
) -> list[AlertResponse]:
    settings = get_settings()
    rows = repository.list_alerts(settings.postgres, customer_id=customer_id, limit=limit)
    ALERTS_LISTED_TOTAL.inc(len(rows))
    return [AlertResponse(**row) for row in rows]


@app.post(
    "/score-transaction",
    response_model=ScoreTransactionResponse,
    responses={401: {"model": ErrorResponse}, 404: {"model": ErrorResponse}, 503: {"model": ErrorResponse}},
    dependencies=[require_auth],
)
def post_score_transaction(request: ScoreTransactionRequest) -> ScoreTransactionResponse:
    SCORING_REQUESTS_TOTAL.inc()
    scoring_input = ScoringInput(
        transaction_id=request.transaction_id,
        account_id=request.account_id,
        customer_id=request.customer_id,
        merchant_id=request.merchant_id,
        amount=request.amount,
        country=request.country,
        device_id=request.device_id,
        status=request.status,
        timestamp=request.timestamp,
    )
    start = perf_counter()
    try:
        assessment = score_transaction(scoring_input)
    except UnknownCustomerError as exc:
        SCORING_ERRORS_TOTAL.labels(error_type="unknown_customer").inc()
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        SCORING_ERRORS_TOTAL.labels(error_type=type(exc).__name__).inc()
        logger.exception("Scoring failed", extra={"context": {"transaction_id": request.transaction_id}})
        raise HTTPException(status_code=503, detail=f"Scoring temporarily unavailable: {exc}") from exc
    finally:
        SCORING_LATENCY_SECONDS.observe(perf_counter() - start)

    RISK_LEVEL_TOTAL.labels(risk_level=assessment.risk_level).inc()

    return ScoreTransactionResponse(
        transaction_id=request.transaction_id,
        risk_score=round(assessment.risk_score, 4),
        risk_level=assessment.risk_level,
        decision=_decision_for_level(assessment.risk_level),
        reasons=assessment.reasons,
    )


@app.exception_handler(Exception)
def unhandled_exception_handler(request, exc: Exception) -> JSONResponse:
    logger.exception("Unhandled exception", extra={"context": {"path": str(request.url)}})
    return JSONResponse(status_code=500, content={"detail": "Internal server error"})
