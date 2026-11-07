PYTHON ?= python3
VENV ?= .venv
PY := $(VENV)/bin/python
PIP := $(VENV)/bin/pip

.PHONY: setup venv install dev generate up down logs etl dashboard test psql clean

setup: install generate
	@echo "Setup complete. Next: make up && make etl && make dashboard"

venv:
	@test -d $(VENV) || $(PYTHON) -m venv $(VENV)

install: venv
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt

dev: install
	$(PIP) install -r requirements-dev.txt

generate:
	$(PY) -m finance_dwh.generate_data

up:
	docker compose up -d
	@echo "Waiting for PostgreSQL healthcheck..."
	@until [ "$$(docker inspect --format='{{.State.Health.Status}}' helix-finance-dwh-postgres 2>/dev/null)" = "healthy" ]; do sleep 1; done
	@echo "PostgreSQL is healthy on localhost:$${POSTGRES_PORT:-5433}"

down:
	docker compose down

logs:
	docker compose logs -f postgres

etl:
	$(PY) -m finance_dwh.pipeline

dashboard:
	$(PY) -m streamlit run app.py

test:
	$(PY) -m pytest -q

psql:
	docker compose exec postgres psql -U $${POSTGRES_USER:-dwh_user} -d $${POSTGRES_DB:-finance_dwh}

clean:
	rm -rf .pytest_cache
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
