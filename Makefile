SHELL := /bin/bash
.DEFAULT_GOAL := help

PROJECT := mysql-interceptor

COMPOSE := docker-compose
COMPOSE_FILE := tests/integration/docker-compose.yml

PYMYSQL_DIR := tests/integration/pymysql
ALCHEMY_DIR := tests/integration/sqlalchemy

PYMYSQL_VENV := $(PYMYSQL_DIR)/.venv
ALCHEMY_VENV := $(ALCHEMY_DIR)/.venv

PYTHON ?= python3

.PHONY: help
help:
	@echo ""
	@echo "$(PROJECT) - dev helpers"
	@echo ""
	@echo "Docker (dependencies):"
	@echo "  make up                         Start mysql + kafka via docker-compose"
	@echo "  make down                       Stop and remove containers"
	@echo "  make ps                         Show container status"
	@echo "  make logs                        Tail all logs"
	@echo "  make logs-kafka                  Tail kafka logs"
	@echo "  make logs-mysql                  Tail mysql logs"
	@echo ""
	@echo "Integration tests (auto venv):"
	@echo "  make pymysql-test                Create venv (if needed), install deps, run pytest (PyMySQL)"
	@echo "  make alchemysql-test             Create venv (if needed), install deps, run pytest (SQLAlchemy)"
	@echo ""

.PHONY: _check-compose
_check-compose:
	@command -v $(COMPOSE) >/dev/null 2>&1 || (echo "$(COMPOSE) not found. Run: ./scripts/install-docker-compose.sh" && exit 1)

.PHONY: up
up: _check-compose
	$(COMPOSE) -f $(COMPOSE_FILE) up -d

.PHONY: down
down: _check-compose
	$(COMPOSE) -f $(COMPOSE_FILE) down -v

.PHONY: ps
ps: _check-compose
	$(COMPOSE) -f $(COMPOSE_FILE) ps

.PHONY: logs
logs: _check-compose
	$(COMPOSE) -f $(COMPOSE_FILE) logs -f --tail=200

.PHONY: logs-kafka
logs-kafka: _check-compose
	$(COMPOSE) -f $(COMPOSE_FILE) logs -f --tail=200 kafka

.PHONY: logs-mysql
logs-mysql: _check-compose
	$(COMPOSE) -f $(COMPOSE_FILE) logs -f --tail=200 mysql

define ensure_venv_and_run
	@set -euo pipefail; \
	dir="$(1)"; \
	venv="$(2)"; \
	req="$(3)"; \
	testfile="$(4)"; \
	reporoot="$$(pwd)"; \
	PYTHONPATH="$$reporoot"; \
	export PYTHONPATH; \
	if [ ! -d "$$venv" ]; then \
		echo "Creating venv: $$venv"; \
		$(PYTHON) -m venv "$$venv"; \
	fi; \
	source "$$venv/bin/activate"; \
	python -m pip install -U pip setuptools wheel >/dev/null; \
	python -m pip install -r "$$req"; \
	python -m pip install -e "$$reporoot"; \
	pytest -m integration -q "$$dir/$$testfile"
endef

.PHONY: pymysql-test
pymysql-test:
	@echo "Tip: ensure deps are up: make up"
	$(call ensure_venv_and_run,$(PYMYSQL_DIR),$(PYMYSQL_VENV),$(PYMYSQL_DIR)/requirements.txt,test_integration_pymysql_docker.py)

.PHONY: alchemysql-test
alchemysql-test:
	@echo "Tip: ensure deps are up: make up"
	$(call ensure_venv_and_run,$(ALCHEMY_DIR),$(ALCHEMY_VENV),$(ALCHEMY_DIR)/requirements.txt,test_integration_sqlalchemy_docker.py)


.PHONY: inspect-results
inspect-results:
	@echo "Reading interceptor events from Kafka (CTRL+C to stop)."
	@echo "Optional: TOPIC=... PATTERN=... DEBUG=... (default TOPIC=MYSQL_EVENTS)"
	@echo "Example: make inspect-results PATTERN='MYSQL_EVENTS_TEST_.*'"
	@if [ -d "$(PYMYSQL_VENV)" ]; then \
		source "$(PYMYSQL_VENV)/bin/activate"; \
	else \
		echo "PyMySQL venv missing. Run: make pymysql-test (creates venv + installs deps)."; \
		exit 1; \
	fi; \
	python scripts/inspect_kafka.py --bootstrap localhost:9092 $${TOPIC:+--topic $$TOPIC} $${PATTERN:+--pattern $$PATTERN} $${DEBUG:+--debug $$DEBUG}

