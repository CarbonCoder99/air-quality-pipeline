# ─────────────────────────────────────────────────────────────
# Air Quality Pipeline — Makefile
# Run commands with: make <target>
# ─────────────────────────────────────────────────────────────

# Build the custom Airflow image
build:
	docker compose build

# First time setup — build + init + start
setup: build
	docker compose up airflow-init
	docker compose up -d
	@echo "✅ Stack is up. Visit http://localhost:8080"
	@echo "   Login: admin / admin"

# Start all services in the background
up:
	docker compose up -d

# Stop all services (keeps data volumes)
down:
	docker compose down

# Stop and wipe everything including volumes
# USE WITH CAUTION — deletes all Airflow metadata
destroy:
	docker compose down --volumes --remove-orphans

# View logs for all services
logs:
	docker compose logs -f

# View logs for a specific service
# Usage: make logs-worker
logs-worker:
	docker compose logs -f airflow-worker

logs-scheduler:
	docker compose logs -f airflow-scheduler

# Check status of all containers
status:
	docker compose ps

# Shell into the worker container
# Useful for debugging dbt runs manually
shell:
	docker compose exec airflow-worker bash

# Run dbt commands inside the worker container
# Usage: make dbt-run
dbt-run:
	docker compose exec airflow-worker bash -c \
		"cd /opt/airflow/dbt && dbt run --profiles-dir /opt/airflow/dbt --target prod"

dbt-test:
	docker compose exec airflow-worker bash -c \
		"cd /opt/airflow/dbt && dbt test --profiles-dir /opt/airflow/dbt --target prod"

dbt-docs:
	docker compose exec airflow-worker bash -c \
		"cd /opt/airflow/dbt && dbt docs generate --profiles-dir /opt/airflow/dbt --target prod"

# Trigger a manual backfill for a specific date range
# Usage: make backfill START=2024-01-01 END=2024-01-07
backfill:
	docker compose exec airflow-scheduler bash -c \
		"airflow dags backfill ingest_openaq \
		--start-date $(START) \
		--end-date $(END) \
		--reset-dagruns"

# Restart just the scheduler (useful after DAG changes)
restart-scheduler:
	docker compose restart airflow-scheduler

.PHONY: build setup up down destroy logs logs-worker logs-scheduler \
        status shell dbt-run dbt-test dbt-docs backfill restart-scheduler
