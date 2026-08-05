# ADR 010: Observability (Prometheus + Grafana)

## Context
Phase 0 specifies Prometheus + Grafana monitoring Kafka, Spark, data
quality, ML metrics, and API latency/errors/throughput, explicitly
deferred until this phase ("Do NOT implement the full Prometheus/Grafana
stack yet. That belongs to Phase 8."). Phase 8 had to decide: which
components can realistically be instrumented given this sandbox's
existing, already-documented limitations (no Docker since Phase 2, no
continuously-running Spark/Kafka/Airflow), and how to avoid the
observability equivalent of "no fake results" -- building dashboards for
metrics that don't actually exist yet.

## Decision: instrument the FastAPI service, not Kafka/Spark/Airflow, in
this pass
The API (Phase 7) is the one component that both (a) runs synchronously
and testably in this sandbox (via `TestClient`, no persistent daemon
needed, same pattern established for Airflow's `dags test` and this
phase's own testing) and (b) has meaningful per-request metrics to
export. Kafka's producer/consumer already have in-process metrics
(`ProducerMetrics`/`ConsumerMetrics` dataclasses, Phase 2) but exporting
them to Prometheus would only be verifiable against a running broker,
which remains unavailable. Instrumenting them anyway, un-verified, would
produce dashboard panels with no real data behind them -- the same
category of problem as fabricating a metric value outright. Scoped this
phase to what could be built AND proven end-to-end.

## Decision: prometheus-fastapi-instrumentator for generic HTTP metrics,
custom prometheus_client counters for domain metrics
Generic per-route metrics (request count, latency, size, status) are a
solved problem; a purpose-built library is less error-prone than
hand-rolled middleware and adds no risk the domain-specific metrics don't
already carry. Domain metrics (`finguard_scoring_requests_total`,
`finguard_risk_level_total`, etc.) use `prometheus_client` directly,
since they encode business logic a generic library can't infer. Both
share the same global default registry -- confirmed directly (not
assumed) that they appear together at one `/metrics` endpoint with no
extra wiring required.

## Decision: build ONE real, verified Grafana dashboard (API metrics),
not placeholder dashboards for every planned component
Phase 0 lists dashboards for streaming, data quality, ML, and API metrics.
Only the API layer has real, tested metrics behind it in this sandbox.
Building dashboard JSON for Kafka lag or model drift with no live metric
source would be indistinguishable, in spirit, from fabricating those
numbers directly -- this project's "no fake results" principle applies to
dashboard scaffolding pointed at nonexistent data just as much as it
applies to a training-run metric. One dashboard, fully real and
verifiable end-to-end (metric → scrape config → panel query, all
reviewed), was chosen over several dashboards of unknown correctness.

## Decision: Prometheus/Grafana added to docker-compose.yml, documented
as NOT RUN
Consistent with every prior Docker-dependent decision since Phase 2: the
correct target-architecture configuration is written, reviewed carefully
(scrape target hostnames use the container-network hostname `api:8000`,
not `localhost`, mirroring the Kafka host-vs-internal-listener lesson from
Phase 2/`docs/kafka.md §1`), and explicitly reported as unexecuted rather
than silently implied to have been tested. `monitoring/prometheus.yml`
and the Grafana provisioning/dashboard files were validated for syntax
correctness (YAML/JSON parse successfully) -- not for runtime correctness
against a live Prometheus/Grafana instance.

## Local vs. Production
| | Local (this repo) | Production target |
|---|---|---|
| Metrics source | FastAPI `/metrics` only | All components (Kafka, Spark, Airflow, API) instrumented |
| Prometheus | Not run (no Docker) | Real scraping, retention, alerting rules |
| Grafana | Not run (no Docker); one dashboard JSON, syntax-validated only | Multiple dashboards per Phase 0's spec, live-rendering, alerting integrations |
| Alerting | None | Prometheus Alertmanager wired to on-call/paging |

## Known Limitations
- Prometheus/Grafana never ran against real traffic in this sandbox.
- Kafka/Spark/Airflow remain uninstrumented for Prometheus -- their
  existing in-process metrics (Phase 2's dataclasses) are a natural
  starting point for a future pass, following the exact pattern
  established here.
- No alerting rules defined.
- `infra/docker/Dockerfile.api` was written but never built.

## Consequences
- Any future phase adding Prometheus instrumentation to another component
  should follow this phase's pattern: a dedicated `metrics.py` module,
  explicit `.inc()`/`.observe()` calls at meaningful points, and a test
  that asserts real counter movement from a real action -- not just that
  `/metrics` returns 200.
- `monitoring/prometheus.yml`'s scrape target (`api:8000`) and
  `docker-compose.yml`'s `api` service's `POSTGRES_HOST: postgres`
  setting are the canonical examples of container-network hostnames in
  this repo, alongside Kafka's `kafka:19092` -- any new containerized
  service should follow the same host-vs-internal-listener pattern.
