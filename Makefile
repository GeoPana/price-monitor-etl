PYTHON ?= python
PIP ?= pip
APP ?= pricemonitor
ALEMBIC ?= alembic
UVICORN ?= uvicorn
DOCKER_COMPOSE ?= docker compose
CONFIG ?= configs/settings.yaml
message ?= describe_schema_change
dag_id ?= pricemonitor_daily_pipeline

.PHONY: install install-dev init-db show-config scrape process export alert run test migrate revision downgrade history stamp-head airflow-init airflow-up airflow-down airflow-ps airflow-logs airflow-dags airflow-unpause airflow-trigger

install:
	$(PIP) install -e .

install-dev:
	$(PIP) install -e .[dev]

init-db:
	$(APP) --config $(CONFIG) init-db

show-config:
	$(APP) --config $(CONFIG) show-config

scrape:
	$(APP) --config $(CONFIG) scrape --source site_a

process:
	$(APP) --config $(CONFIG) process --source site_a
	
export:
	$(APP) --config $(CONFIG) export --source site_a

alert:
	$(APP) --config $(CONFIG) alert --source site_a
	
run:
	$(APP) --config $(CONFIG) run --source site_a
	
api:
	$(UVICORN) pricemonitor.api.app:create_app --factory --reload

airflow-init:
	$(DOCKER_COMPOSE) up airflow-init

airflow-up:
	$(DOCKER_COMPOSE) up -d db airflow-db redis airflow-api-server airflow-scheduler airflow-dag-processor airflow-worker airflow-triggerer

airflow-down:
	$(DOCKER_COMPOSE) down

airflow-ps:
	$(DOCKER_COMPOSE) ps

airflow-logs:
	$(DOCKER_COMPOSE) logs -f airflow-api-server airflow-scheduler airflow-worker

airflow-dags:
	$(DOCKER_COMPOSE) exec airflow-api-server airflow dags list

airflow-unpause:
	$(DOCKER_COMPOSE) exec airflow-api-server airflow dags unpause $(dag_id)

airflow-trigger:
	$(DOCKER_COMPOSE) exec airflow-api-server airflow dags trigger $(dag_id)

test:
	pytest

migrate:
	$(ALEMBIC) -x app_config=$(CONFIG) upgrade head

revision:
	$(ALEMBIC) -x app_config=$(CONFIG) revision --autogenerate -m "$(message)"

downgrade:
	$(ALEMBIC) -x app_config=$(CONFIG) downgrade -1

history:
	$(ALEMBIC) -x app_config=$(CONFIG) history

stamp-head:
	$(ALEMBIC) -x app_config=$(CONFIG) stamp head
