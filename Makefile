.PHONY: install up down logs test test-cov generate kafka-topics train terraform-validate airflow-dag-test airflow-dag-test-list clean

install:
	pip install --break-system-packages -r requirements.txt

up:
	docker compose up -d
	@echo "Waiting for postgres healthcheck..."
	@until [ "$$(docker inspect -f '{{.State.Health.Status}}' finguard_postgres 2>/dev/null)" = "healthy" ]; do sleep 1; done
	@echo "postgres is healthy."
	@echo "Waiting for kafka healthcheck..."
	@until [ "$$(docker inspect -f '{{.State.Health.Status}}' finguard_kafka 2>/dev/null)" = "healthy" ]; do sleep 1; done
	@echo "kafka is healthy."

down:
	docker compose down

logs:
	docker compose logs -f postgres kafka

test:
	pytest -v

test-cov:
	pytest -v --cov=src --cov-report=term-missing

generate:
	python -m src.generator.generate --customers 1000 --transactions 10000 --seed 42

kafka-topics:
	python -m scripts.create_kafka_topics

train:
	python -m src.ml.train

terraform-validate:
	bash scripts/validate_terraform.sh
	python3 scripts/check_terraform_composition.py

# Airflow lives in its own venv -- see docs/airflow.md §1 for why.
# Usage: make airflow-dag-test DAG=gold_layer_refresh DATE=2026-01-01
airflow-dag-test:
	. scripts/airflow_env.sh && /home/claude/airflow_venv/bin/airflow dags test $(DAG) $(DATE)

airflow-dag-list:
	. scripts/airflow_env.sh && /home/claude/airflow_venv/bin/airflow dags list

clean:
	rm -rf data/raw/*.parquet .pytest_cache **/__pycache__ .coverage
