PYTHON ?= python
PIP ?= pip
APP ?= pricemonitor
CONFIG ?= configs/settings.yaml

.PHONY: install install-dev init-db show-config scrape test

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
