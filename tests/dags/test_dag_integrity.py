"""DAG integrity tests.

IMPORTANT: these run against the Airflow venv (/home/claude/airflow_venv),
NOT the main project's Python environment. Airflow was deliberately
installed in an isolated virtualenv during Phase 6 after discovering real
version conflicts when mixed into the shared environment -- see
docs/adr/008-airflow-orchestration.md for the full incident (which also
covers a second, related discovery: AIRFLOW_HOME must NOT be a directory
named `airflow` inside the repo itself, since Python's implicit namespace
packages would then shadow the real `airflow` package for anything with
the repo root on sys.path -- AIRFLOW_HOME lives at
/home/claude/airflow_home, outside the repo, for exactly this reason).
Run with:

    AIRFLOW_HOME=/home/claude/airflow_home \
    AIRFLOW__CORE__DAGS_FOLDER=/home/claude/finguard-fraud-platform/dags \
    AIRFLOW__CORE__LOAD_EXAMPLES=False \
    PYTHONPATH=/home/claude/finguard-fraud-platform \
    /home/claude/airflow_venv/bin/python -m pytest tests/dags/ -v

These tests check DAG STRUCTURE (imports cleanly, expected tasks exist,
dependencies wired correctly, sane default_args) -- they do not execute
task logic. End-to-end task execution is covered separately by
`airflow dags test <dag_id> <date>`, run for real for all five DAGs
during Phase 6 (see the close-out report for actual results).
"""
import os
import sys

import pytest

pytest.importorskip(
    "airflow",
    reason=(
        "Airflow is intentionally installed in an isolated venv "
        "(/home/claude/airflow_venv), not the main project environment -- "
        "see docs/adr/008-airflow-orchestration.md. Run these tests with: "
        "AIRFLOW_HOME=/home/claude/airflow_home PYTHONPATH=... "
        "/home/claude/airflow_venv/bin/python -m pytest tests/dags/ -v"
    ),
)

REPO_ROOT = "/home/claude/finguard-fraud-platform"
DAGS_FOLDER = f"{REPO_ROOT}/dags"

os.environ.setdefault("AIRFLOW_HOME", "/home/claude/airflow_home")
os.environ.setdefault("AIRFLOW__CORE__DAGS_FOLDER", DAGS_FOLDER)
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
sys.path.insert(0, REPO_ROOT)


@pytest.fixture(scope="module")
def dagbag():
    from airflow.models import DagBag

    return DagBag(dag_folder=DAGS_FOLDER, include_examples=False)


def test_dagbag_has_no_import_errors(dagbag):
    assert dagbag.import_errors == {}, f"DAG import errors: {dagbag.import_errors}"


def test_all_five_expected_dags_are_present(dagbag):
    expected = {
        "gold_layer_refresh", "daily_data_quality",
        "model_training", "model_validation", "model_monitoring",
    }
    assert expected.issubset(set(dagbag.dag_ids))


@pytest.mark.parametrize("dag_id", [
    "gold_layer_refresh", "daily_data_quality",
    "model_training", "model_validation", "model_monitoring",
])
def test_dag_has_retries_configured(dagbag, dag_id):
    dag = dagbag.get_dag(dag_id)
    assert dag is not None
    for task in dag.tasks:
        assert task.retries >= 1, f"{dag_id}.{task.task_id} has no retries configured"


@pytest.mark.parametrize("dag_id", [
    "gold_layer_refresh", "daily_data_quality",
    "model_training", "model_validation", "model_monitoring",
])
def test_dag_has_no_cycles(dagbag, dag_id):
    dag = dagbag.get_dag(dag_id)
    sorted_tasks = dag.topological_sort()
    assert len(sorted_tasks) == len(dag.tasks)


def test_gold_layer_refresh_has_expected_task_order():
    from dags.gold_layer_refresh_dag import dag

    task_ids = [t.task_id for t in dag.topological_sort()]
    assert task_ids == [
        "generate_and_persist_reference_data", "dbt_run", "dbt_test",
    ]


def test_daily_data_quality_has_one_task():
    from dags.daily_data_quality_dag import dag

    assert len(dag.tasks) == 1
    assert dag.tasks[0].task_id == "run_quality_checks"


def test_model_training_has_one_task():
    from dags.model_training_dag import dag

    assert len(dag.tasks) == 1


def test_model_validation_has_one_task():
    from dags.model_validation_dag import dag

    assert len(dag.tasks) == 1


def test_model_monitoring_has_one_task():
    from dags.model_monitoring_dag import dag

    assert len(dag.tasks) == 1


def test_gold_layer_refresh_is_daily_and_data_quality_gate_is_daily():
    from dags.daily_data_quality_dag import dag as dq_dag
    from dags.gold_layer_refresh_dag import dag as refresh_dag

    assert refresh_dag.schedule_interval == "@daily"
    assert dq_dag.schedule_interval == "@daily"


def test_model_monitoring_runs_more_frequently_than_model_training():
    """Monitoring should run at least as often as training -- catching
    drift between training runs is the whole point of a separate
    monitoring DAG (see its own module docstring)."""
    from dags.model_monitoring_dag import dag as monitoring_dag
    from dags.model_training_dag import dag as training_dag

    assert monitoring_dag.schedule_interval == "@hourly"
    assert training_dag.schedule_interval == "@weekly"
