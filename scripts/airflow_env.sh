#!/bin/bash
# Sources the correct Airflow environment for this project. Usage:
#   source scripts/airflow_env.sh
#   airflow dags test daily_data_quality 2026-01-01
#
# AIRFLOW_HOME and all log paths are deliberately set OUTSIDE the repo
# (/home/claude/airflow_home) and explicitly for every log-path setting
# Airflow has -- both are real bugs found and fixed during Phase 6, not
# preemptive caution:
#   1. AIRFLOW_HOME must not be a directory literally named "airflow"
#      inside the repo -- Python's implicit namespace packages treat any
#      directory on sys.path as an importable module, which silently
#      shadowed the real `airflow` package for anything importing it with
#      the repo root on sys.path (i.e. this project's own test suite).
#   2. Some Airflow log paths (dag_processor_manager,
#      scheduler.child_process_log_directory) are not fully derived from
#      AIRFLOW_HOME in all versions/code paths and can fall back to a
#      CWD-relative default -- which silently recreated the very
#      <repo>/airflow directory problem #1 fixes, even with AIRFLOW_HOME
#      set correctly. Setting every log path explicitly closes that gap.
# See docs/adr/008-airflow-orchestration.md for the full incident writeup.

export AIRFLOW_HOME=/home/claude/airflow_home
export AIRFLOW__CORE__DAGS_FOLDER=/home/claude/finguard-fraud-platform/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__EXECUTOR=LocalExecutor
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://finguard:${POSTGRES_PASSWORD:-change_me_locally}@localhost:5432/airflow"
export AIRFLOW__LOGGING__BASE_LOG_FOLDER=/home/claude/airflow_home/logs
export AIRFLOW__LOGGING__DAG_PROCESSOR_MANAGER_LOG_LOCATION=/home/claude/airflow_home/logs/dag_processor_manager/dag_processor_manager.log
export AIRFLOW__SCHEDULER__CHILD_PROCESS_LOG_DIRECTORY=/home/claude/airflow_home/logs/scheduler
export PYTHONPATH=/home/claude/finguard-fraud-platform
