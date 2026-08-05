# Airflow Orchestration (Phase 6)

## 1. Why Airflow lives in its own virtualenv

Airflow 2.10.5 is installed in `/home/claude/airflow_venv`, NOT the main
project environment `requirements.txt` installs into. This was a real
decision forced by a real conflict found during Phase 6, not preemptive
caution: installing `apache-airflow` alongside the accumulated ML/Spark/
dbt stack in the shared environment surfaced version conflicts (see §5).
Isolating Airflow avoids destabilizing every other phase's already-working
dependency set, at the cost of needing to install the project's own
`requirements.txt` into the Airflow venv too (so DAG task code, which
imports `src.*`, has what it needs) -- done once, documented in §4.

Airflow 2.10.x, not 3.x, was chosen deliberately: Airflow 3 restructures
the architecture (separate API server, DAG processor, task execution
model) in ways that are less battle-tested and less universally deployed
as of this writing than the 2.x line, which is still what most real
organizations run. Same reasoning as choosing Spark 3.5 over 4.x and
Delta 3.x over 4.x in earlier phases: the widely-deployed, defensible
choice over the newest available one.

## 2. DAGs

| DAG | Schedule | What it does |
|---|---|---|
| `gold_layer_refresh` | `@daily` | Generate fresh reference data -> `dbt run` -> `dbt test` |
| `daily_data_quality` | `@daily` | Real SQL-computed data-quality report + gate (fails below 99%) |
| `model_training` | `@weekly` | Runs the Phase 5 training pipeline; fails if the model doesn't beat baseline |
| `model_validation` | `@daily` | Reloads the current PRODUCTION model, re-evaluates it, fails on serious degradation |
| `model_monitoring` | `@hourly` | Compares recent fraud rate to the historical baseline, fails on excessive drift |

None of these duplicate logic that already exists elsewhere in the
codebase -- every DAG is a thin PythonOperator/BashOperator wrapper around
an already-implemented, already-tested pipeline (`src.generator.generate`,
`dbt run`/`dbt test`, `src.ml.train`, `src.ml.evaluate`). Airflow's job here
is scheduling, retries, and failure signaling -- not reimplementing
business logic.

**Deliberately NOT implemented**: a Kafka/streaming-triggered DAG. Per the
Phase 0 architecture, Airflow orchestrates batch/training/monitoring
workflows; continuous Kafka consumption is Phase 2/3's job (a long-running
streaming query, not a scheduled batch task) and should not be
force-fitted into Airflow's batch-scheduling model.

## 3. Real Results (from an actual `airflow dags test` run of every DAG)

```text
gold_layer_refresh:  generate -> dbt run (14/14 models) -> dbt test (68/68) -- SUCCESS
daily_data_quality:  10,050 records processed, 100.0% quality score -- SUCCESS
model_training:      XGBoost PR-AUC 0.9546, 39.78x baseline, promoted to production -- SUCCESS
model_validation:    reloaded production model, PR-AUC 0.9355 on current data -- SUCCESS
model_monitoring:    baseline fraud rate 0.0211, recent 0.0240, drift 1.14x (< 3.0x threshold) -- SUCCESS
```
Every number above came from an actual `airflow dags test <dag_id> <date>`
invocation against live Postgres/MLflow data -- not a description of
intended behavior. See the Phase 6 close-out report for the exact commands.

## 4. Local Setup

```bash
# One-time: install Airflow in its own venv (see §1 for why)
python3 -m venv /home/claude/airflow_venv
/home/claude/airflow_venv/bin/pip install "apache-airflow==2.10.5" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.12.txt"
/home/claude/airflow_venv/bin/pip install -r requirements.txt psycopg2-binary

# One-time: create a dedicated Postgres database for Airflow's own metadata
# (separate from the finguard application database)
psql -c "CREATE DATABASE airflow OWNER finguard;"

# Every session: source the correct environment (see §5 for why this is a
# script and not just "set AIRFLOW_HOME")
export POSTGRES_PASSWORD=<your password>
. scripts/airflow_env.sh
/home/claude/airflow_venv/bin/airflow db migrate

# Run a DAG for real, synchronously, without a scheduler daemon:
/home/claude/airflow_venv/bin/airflow dags test gold_layer_refresh 2026-01-01
```

## 5. Two Real Bugs Found and Fixed This Phase

### Bug 1: cross-phase dependency conflict (`typing_extensions`)
Installing the ML stack (Phase 5: mlflow, xgboost, scikit-learn) into the
shared environment had silently upgraded `typing_extensions` to a version
incompatible with `dbt`'s `mashumaro` dependency -- `dbt --version` started
failing with `ImportError: cannot import name 'evaluate_forward_ref'`,
discovered only when Phase 6's `gold_layer_refresh` DAG tried to invoke
`dbt run`. Fixed by pinning `typing_extensions==4.16.0` in
`requirements.txt` (the version both ecosystems are compatible with) and
re-verifying the ENTIRE main test suite (120 tests) plus `dbt --version`
afterward, not just the specific failure.

### Bug 2: `AIRFLOW_HOME` named `airflow` inside the repo shadows the real
package
Setting `AIRFLOW_HOME=<repo>/airflow` (a seemingly natural default) creates
a directory literally named `airflow` inside the project. Python's
implicit namespace-package mechanism (PEP 420) treats ANY directory on
`sys.path` as an importable module if no real package by that name is
found first -- so `from airflow import DAG` inside this project's own test
suite (which has the repo root on `sys.path`) would silently resolve to
that empty directory instead of the real, pip-installed `apache-airflow`
package, producing a confusing `ImportError: cannot import name 'DAG'
from 'airflow' (unknown location)`.

**Compounding factor**: even after moving `AIRFLOW_HOME` outside the repo
(`/home/claude/airflow_home`), some Airflow log-path settings
(`dag_processor_manager_log_location`,
`scheduler.child_process_log_directory`) do not fully derive from
`AIRFLOW_HOME` in every code path and silently fell back to a
CWD-relative default -- recreating the exact same `<repo>/airflow`
directory and the exact same shadowing bug, even with `AIRFLOW_HOME` set
correctly everywhere else. Fixed by setting every relevant log-path
environment variable explicitly (see `scripts/airflow_env.sh`), not just
`AIRFLOW_HOME` -- verified by running Airflow commands repeatedly and
confirming the stray directory does not reappear.

**Why this matters beyond "annoying to debug"**: this is exactly the kind
of environment-configuration bug that silently breaks CI or a
teammate's machine differently depending on their current working
directory -- worth documenting precisely since the same class of bug (a
local file/directory shadowing a real package name) can recur with any
tool that defaults its home directory to something generic.

## 6. Testing Strategy

Two distinct test layers, matching the "DAGs need Airflow, business logic
doesn't" reality:

- **DAG structure** (`tests/dags/test_dag_integrity.py`): import
  correctness, expected tasks present, no cycles, retries configured,
  sane schedule relationships. Runs via the isolated Airflow venv, NOT the
  main `pytest -v` command -- `pytest.importorskip("airflow")` makes it
  skip cleanly (not error) when run from the main environment, verified
  directly.
- **Task business logic** (everything in `src/ml/`, `src/generator/`, dbt
  models): already covered by the main test suite from Phases 1-5,
  unchanged by Phase 6 -- DAGs call into that tested code, they don't
  reimplement it.
- **End-to-end DAG execution**: `airflow dags test <dag_id> <date>`, run
  for real for all five DAGs (see §3) -- the closest equivalent to
  "integration test" for a DAG, since it runs actual task code against
  actual infrastructure without needing a live scheduler daemon.

## 7. Idempotency

- `gold_layer_refresh`: generation uses `ON CONFLICT DO NOTHING` (Phase 1);
  dbt recomputes each model's full result on every run (none of the Gold
  models are configured as incremental).
- `model_training`: NOT idempotent by nature, and not claimed to be --
  each run trains a genuinely new model version. This is correct behavior
  for a training DAG, not a gap.
- `daily_data_quality` / `model_monitoring`: pure read-only queries, safe
  to rerun any number of times.
- `model_validation`: read-only against the registry and Postgres, safe to
  rerun.

## 8. Known Limitations

- `model_validation` evaluates the production model against the FULL
  current dataset, which includes rows it was trained on -- a degradation
  smoke check, not true held-out validation against genuinely new data
  (documented in the DAG's own docstring).
- No Airflow scheduler/webserver was run as a persistent process in this
  environment -- every DAG was validated via `airflow dags test`, which
  runs synchronously without needing either. A real deployment would run
  both.
- Alerting on task failure (`email_on_failure`) is disabled locally (no
  mail server configured) -- documented as a Phase 8 concern, not silently
  omitted.
- `model_monitoring`'s "recent window" is a row-count window (most recent
  N transactions), not a true wall-clock window, since this sandbox's
  synthetic data doesn't arrive on a real clock.
