# Observability (Phase 8)

## 1. What was actually run vs. what's documented

```text
FastAPI /metrics endpoint:          VERIFIED (real requests, real counter/histogram values)
Prometheus scraping /metrics:       NOT RUN -- no Docker daemon in this sandbox
Grafana rendering dashboards:       NOT RUN -- no Docker daemon in this sandbox
```
Same honesty pattern as Kafka (Phase 2) and Delta (Phase 3): the
instrumentation code itself is real and tested; the containers that would
scrape/visualize it could not be started here. Unlike those two, the
*application-level* half of this phase (the metrics themselves) is fully
verified, not just implemented -- Prometheus/Grafana are documented,
reviewed, syntax-validated infrastructure sitting on top of already-proven
instrumentation, not the whole of what "observability" means here.

## 2. Architecture

```
FastAPI app (Phase 7)
    │
    ├── prometheus-fastapi-instrumentator   -- generic HTTP metrics
    │     (request count, latency, size, status -- per route)
    │
    └── src/api/metrics.py                  -- domain-specific counters
          finguard_scoring_requests_total
          finguard_scoring_errors_total{error_type}
          finguard_scoring_latency_seconds (histogram)
          finguard_risk_level_total{risk_level}
          finguard_customer_risk_lookups_total{found}
          finguard_alerts_listed_total
    │
    ▼
GET /metrics  (Prometheus text-exposition format)
    │
    ▼ (documented, not executed here)
Prometheus (docker-compose.yml, monitoring/prometheus.yml)
    │
    ▼
Grafana (docker-compose.yml, monitoring/grafana/dashboards/api-observability.json)
```

Both metric sources share Prometheus client's global default registry, so
they appear together at one `/metrics` endpoint without any extra wiring
-- confirmed directly (`tests/integration/test_metrics.py`), not assumed
from reading the library docs.

## 3. Why prometheus-fastapi-instrumentator, not hand-rolled middleware
Generic per-route HTTP metrics (request count, latency histogram, request
size, status code) are a well-solved problem with an established library;
hand-writing that middleware would just reproduce it with more surface
for bugs, for zero domain-specific value. The domain-specific metrics
(scoring outcomes, risk-level distribution, customer-risk lookup hit
rate) are exactly what a generic library CAN'T know to add, and are
custom `prometheus_client` counters instead.

## 4. What each domain metric answers

| Metric | Question it answers |
|---|---|
| `finguard_scoring_requests_total` | How much scoring traffic is the API handling? |
| `finguard_scoring_latency_seconds` | Is scoring fast enough (feature computation + model inference)? |
| `finguard_scoring_errors_total{error_type}` | What's failing, and how often -- unknown customers vs. genuine scoring failures? |
| `finguard_risk_level_total{risk_level}` | Is the model's output distribution stable, or suddenly flagging everything/nothing? (an early, cheap proxy for the kind of drift Phase 6's `model_monitoring` DAG checks more deliberately against the database) |
| `finguard_customer_risk_lookups_total{found}` | Are callers requesting risk data for customers that don't exist (client bug) vs. real lookups? |
| `finguard_alerts_listed_total` | How much alert volume is actually being consumed by callers? |

## 5. Real Verification

`tests/integration/test_metrics.py` (8 tests, all passing against live
Postgres and the live MLflow model) does not just check the endpoint
returns 200 -- it makes real requests through the app and asserts the
SPECIFIC counter changed by the SPECIFIC amount expected:
```text
test_scoring_request_increments_scoring_counter        -- counter += 1 after one POST
test_unknown_customer_scoring_increments_error_counter -- error_type="unknown_customer" += 1
test_alerts_listed_counter_increases_by_result_count    -- += exactly len(response)
test_scoring_latency_histogram_has_observations         -- histogram count >= 1 after a request
```
Sample real output from an actual run (not hand-written):
```text
finguard_scoring_requests_total 1.0
finguard_scoring_latency_seconds_sum 0.6307195379999939
finguard_risk_level_total{risk_level="LOW"} 1.0
finguard_customer_risk_lookups_total{found="True"} 1.0
finguard_alerts_listed_total 3.0
```

## 6. Grafana Dashboard

`monitoring/grafana/dashboards/api-observability.json` -- 6 panels
(scoring requests/sec, latency p50/p95/p99, errors by type, risk-level
distribution, HTTP requests by endpoint/status, customer-risk lookup hit
rate), all querying the metrics verified in §5. Validated as syntactically
correct JSON (`json.load()` succeeds, 6 panels present) -- **not**
verified to render correctly in an actual Grafana instance, since Grafana
itself could not be started here. Provisioned automatically via
`monitoring/grafana/provisioning/` (datasource + dashboard-folder config)
so a real deployment doesn't need manual dashboard import.

**Deliberately NOT built**: dashboards for Kafka/Spark/model-training
metrics. Those components either aren't instrumented with Prometheus at
all yet (Kafka's producer/consumer metrics from Phase 2 are in-process
dataclasses, not exported) or aren't running continuously in this sandbox
to have live metrics to visualize (Spark, Airflow). Building a dashboard
with no real metric source behind it would violate this project's "no
fake results" principle just as much as fabricating a number would --
better to build one real, fully-verified dashboard than several
unverifiable ones.

## 7. Local Setup

```bash
# Application-level (verified in this sandbox):
python -m uvicorn src.api.main:app --host 0.0.0.0 --port 8000
curl http://localhost:8000/metrics

# Full stack (requires Docker -- not run here):
cp .env.example .env   # set POSTGRES_PASSWORD, GRAFANA_ADMIN_PASSWORD
docker compose up -d prometheus grafana api
# Prometheus UI:  http://localhost:9090
# Grafana UI:     http://localhost:3000 (admin / $GRAFANA_ADMIN_PASSWORD)
```

## 8. Known Limitations

- Prometheus/Grafana containers never ran in this sandbox (no Docker) --
  the scrape config and dashboard JSON are written and syntax-validated,
  not confirmed to render/scrape correctly against a live Prometheus
  server.
- No alerting rules (Prometheus Alertmanager) defined -- out of scope for
  this pass; would layer on top of the same metrics already verified
  here.
- Kafka/Spark/Airflow have no Prometheus instrumentation yet -- Phase 2's
  producer/consumer metrics remain in-process counters (`ProducerMetrics`/
  `ConsumerMetrics` dataclasses), not exported to `/metrics`. Extending
  them would follow the exact pattern established here (a metrics module
  + explicit `.inc()`/`.observe()` calls at the relevant points).
- The `api` service's Dockerfile (`infra/docker/Dockerfile.api`) was
  written but never built -- syntax-reasonable, not build-tested.
