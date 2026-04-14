.PHONY: help infra up down restart status logs \
        trigger-dag dbt-run dbt-test dbt-docs \
        psql-staging clean

# ── Load .env automatically ────────────────────────────────────────────
ifneq (,$(wildcard .env))
  include .env
  export
endif

help:
	@echo ""
	@echo "  London Infrastructure Spatial Analytics"
	@echo "  ========================================"
	@echo ""
	@echo "  Setup"
	@echo "    make infra          Provision GCP with Terraform"
	@echo "    make up             Build images + start all containers"
	@echo "    make down           Stop all containers"
	@echo "    make restart        Down then up"
	@echo ""
	@echo "  Pipeline"
	@echo "    make trigger-dag    Trigger Airflow DAG manually"
	@echo "    make dbt-run        Run dbt models inside container"
	@echo "    make dbt-test       Run dbt tests inside container"
	@echo "    make dbt-docs       Generate + serve dbt docs (port 8081)"
	@echo ""
	@echo "  Debug"
	@echo "    make status         Show container health"
	@echo "    make logs           Tail Airflow scheduler logs"
	@echo "    make psql-staging   Open psql in staging Postgres"
	@echo ""
	@echo "  Cleanup"
	@echo "    make clean          Remove containers + volumes + build cache"
	@echo ""

# ── GCP Infrastructure ─────────────────────────────────────────────────
infra:
	@echo "→ Provisioning GCP with Terraform..."
	cd ../terraform && \
	  terraform init && \
	  terraform plan && \
	  terraform apply -auto-approve
	@echo ""
	@echo "→ Copy these values into your .env:"
	@cd ../terraform && terraform output

# ── Docker ─────────────────────────────────────────────────────────────
up:
	@echo "→ Building images and starting containers..."
	docker compose up -d --build
	@echo ""
	@echo "  Waiting 30s for Airflow to initialise..."
	@sleep 30
	@echo ""
	@echo "  ✓ Airflow UI  →  http://localhost:8080  (airflow / airflow)"
	@echo "  ✓ PostGIS     →  localhost:5432"
	@echo ""
	@echo "  In Codespace: open the PORTS tab and click port 8080"

down:
	docker compose down

restart: down up

status:
	docker compose ps

logs:
	docker compose logs -f airflow-scheduler

# ── Pipeline ───────────────────────────────────────────────────────────
trigger-dag:
	docker compose exec airflow-scheduler \
	  airflow dags trigger london_pipeline
	@echo "→ DAG triggered. Open http://localhost:8080 to monitor."

dbt-run:
	docker compose exec airflow-scheduler bash -c \
	  "cd /opt/airflow/dbt && dbt deps && dbt run --profiles-dir ."

dbt-test:
	docker compose exec airflow-scheduler bash -c \
	  "cd /opt/airflow/dbt && dbt test --profiles-dir ."

dbt-docs:
	docker compose exec airflow-scheduler bash -c \
	  "cd /opt/airflow/dbt && dbt docs generate --profiles-dir . && \
	   dbt docs serve --profiles-dir . --port 8081"

# ── Debug ──────────────────────────────────────────────────────────────
psql-staging:
	docker compose exec postgres \
	  psql -U $${STAGING_DB_USER:-airflow} -d $${STAGING_DB_NAME:-airflow}

# ── Cleanup ────────────────────────────────────────────────────────────
clean:
	docker compose down -v --remove-orphans
	docker builder prune -f
	rm -rf ../dbt/target ../dbt/dbt_packages logs/*
	@echo "→ Cleaned. Run 'make up' to start fresh."
