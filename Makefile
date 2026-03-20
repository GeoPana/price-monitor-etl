PYTHON ?= python
PIP ?= pip
APP ?= pricemonitor
ALEMBIC ?= alembic
CONFIG ?= configs/settings.yaml
message ?= describe_schema_change

.PHONY: install install-dev init-db show-config scrape test migrate revision downgrade history stamp-head

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
