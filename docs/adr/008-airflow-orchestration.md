# ADR 008: Airflow Orchestration

## Context
Phase 0 specifies Airflow for batch/training/monitoring workflows
(`daily_data_quality`, `daily_feature_pipeline`, `model_training`,
`model_validation`, `model_monitoring`, `gold_layer_refresh`), explicitly
NOT for continuous Kafka streaming. Phase 6 had to decide: which Airflow
version, how to avoid destabilizing the accumulated dependency set from
Phases 1-5, how to map the six named workflows onto what actually exists
in this repo, and how to test DAGs meaningfully without a persistent
scheduler.

## Decision: Airflow 2.10.5, not 3.x
Airflow 3 restructures the architecture (separate API server, DAG
processor, new task execution model). As of this writing it is newer and
less universally deployed than the 2.x line, which most real
organizations still run. Consistent with this project's established
pattern (Spark 3.5 over 4.x in Phase 3, Delta 3.x over 4.x, dbt without
optional packages in Phase 4): choosing the widely-deployed, interview-
defensible version over the newest available one.

## Decision: isolated virtualenv for Airflow
Installing `apache-airflow` directly into the shared project environment
(where Phases 3-5 had already installed pyspark, delta-spark, dbt-core,
scikit-learn, xgboost, mlflow) risked -- and, in a related but distinct
way, DID cause -- real dependency conflicts (see Bug 1 below). Isolating
Airflow in its own venv (`/home/claude/airflow_venv`, with the project's
own `requirements.txt` installed into it separately) means Airflow's
dependency resolution never has to be reconciled against five phases'
worth of accumulated ML/Spark/dbt packages, and vice versa. **Alternative
considered**: Docker Compose service for Airflow (matches how Kafka/
Postgres are run). Rejected for the same reason Docker itself has been
unusable since Phase 2 -- no Docker daemon in this sandbox; a Python
virtualenv is the correct fallback, not a workaround pretending to be the
real thing.

## Decision: map the six Phase 0 workflow names onto five DAGs
`daily_feature_pipeline` is not a separate DAG -- its function (fresh
data feeding the Gold layer) is what `gold_layer_refresh` already does;
naming both separately would be two DAGs doing overlapping work for no
added clarity. The other five map directly: `daily_data_quality`,
`model_training`, `model_validation`, `model_monitoring`,
`gold_layer_refresh`. Each DAG is a thin orchestration wrapper -- retries,
scheduling, failure signaling -- around an already-implemented, already-
tested pipeline from an earlier phase; no business logic is duplicated
into DAG files.

## Decision: `model_validation` is a distinct DAG from `model_training`,
not a task within it
`model_training` trains and (conditionally) promotes a new model version.
`model_validation` does not train anything -- it reloads whichever model
currently holds the `production` alias and re-evaluates it, on a much
tighter schedule (`@daily` vs `@weekly`), specifically to catch
degradation BETWEEN training runs. Collapsing these into one DAG would
lose that distinction: a production model can degrade due to data drift
without anyone having retrained it, and that's exactly the gap
`model_validation` exists to catch on its own cadence.

## Bug found and fixed: `typing_extensions` conflict across the shared
environment (Bug 1)
Installing Phase 5's ML stack (mlflow, xgboost, scikit-learn) had
silently upgraded `typing_extensions` system-wide to a version
incompatible with `dbt`'s `mashumaro` dependency. This surfaced only when
Phase 6's `gold_layer_refresh` DAG tried to actually invoke `dbt run` --
`dbt --version` itself was already broken and nobody had re-run it since
Phase 5. Fixed by pinning `typing_extensions==4.16.0` in
`requirements.txt` and re-verifying the FULL main test suite (120 tests)
plus `dbt --version` directly, not just the DAG that happened to expose
it. **Lesson generalized**: every phase from here forward should re-run
`dbt --version` (and any other cross-phase tool) after installing new
dependencies, not just the current phase's own test suite -- a passing
test suite for Phase N doesn't guarantee Phase N's install didn't quietly
break Phase N-1's tooling.

## Bug found and fixed: `AIRFLOW_HOME` named `airflow` inside the repo
shadows the real package (Bug 2)
`AIRFLOW_HOME=<repo>/airflow` -- a natural-seeming default -- creates a
directory literally named `airflow` inside the project. Python's implicit
namespace-package mechanism (PEP 420) then treats that directory as an
importable module for anything with the repo root on `sys.path`,
including this project's own test suite -- `from airflow import DAG`
inside a DAG file silently resolved to the empty shadow directory instead
of the real, pip-installed package, producing a confusing `ImportError:
cannot import name 'DAG' from 'airflow' (unknown location)`. Compounded by
a second finding: some Airflow log-path settings do not fully derive from
`AIRFLOW_HOME` in every code path, and silently recreated the exact same
directory (and bug) even after `AIRFLOW_HOME` itself was moved outside the
repo. Fixed by (1) relocating `AIRFLOW_HOME` to `/home/claude/airflow_home`
and (2) setting every relevant log-path environment variable explicitly
(`scripts/airflow_env.sh`), verified by repeated runs confirming the
directory does not reappear. Full detail in `docs/airflow.md §5`.

## Decision: DAG testing via `airflow dags test`, not a running scheduler
No scheduler/webserver was run as a persistent process -- `airflow dags
test <dag_id> <date>` runs a DAG's tasks synchronously, in dependency
order, against real infrastructure (Postgres, MLflow), without needing
either daemon. This is the correct tool for validating "does this DAG's
logic actually work end-to-end," which is what Phase 6 needed to prove; a
persistent scheduler would additionally prove "does this DAG get
triggered on schedule reliably over time," which needs sustained runtime
this environment doesn't provide.

## Local vs. Production
| | Local (this repo) | Production target |
|---|---|---|
| Airflow deployment | Isolated venv, `airflow dags test` | Managed Airflow (e.g. Astronomer, MWAA) or a proper Docker/K8s deployment with a real scheduler |
| Metadata DB | Dedicated local Postgres database (`airflow`) | Same pattern, managed instance |
| Alerting | Disabled (`email_on_failure=False`) | Real alerting (email/Slack/PagerDuty) wired to the same task failures already implemented here |
| DAG triggering | Manual (`airflow dags test`) | Scheduler-triggered on the configured cadence |

## Known Limitations
- `model_validation` evaluates against the full current dataset (includes
  training data), not genuinely held-out new data -- see `docs/airflow.md
  §8`.
- No persistent scheduler was run; DAG correctness was proven via
  synchronous `dags test` runs, not sustained scheduled operation.
- Alerting is a documented gap, deferred to a later phase's monitoring
  scope, not silently omitted.

## Consequences
- Any future phase adding a new batch workflow should add a DAG file
  under `dags/`, following the existing pattern (thin wrapper around
  tested `src.*`/dbt logic, `DEFAULT_ARGS` from `dags/common.py`) -- not
  duplicate orchestration logic inline elsewhere.
- `scripts/airflow_env.sh` is the one place Airflow's environment should
  be configured; no future documentation or script should re-derive
  `AIRFLOW_HOME`/log paths independently, given the bugs found here.
