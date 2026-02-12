.PHONY: setup install clean run test-api test-pipeline

# Variables
PYTHON = python3
PIP = pip3
VENV = venv

setup: install
	@echo "Project setup successfully."

install:
	$(PYTHON) -m venv $(VENV)
	./$(VENV)/bin/$(PIP) install --upgrade pip
	./$(VENV)/bin/$(PIP) install -e .
	./$(VENV)/bin/playwright install chromium
	@if [ ! -f .env ]; then cp .env.example .env && echo "Created .env from .env.example"; fi

run:
	./$(VENV)/bin/uvicorn api.main:app --host 0.0.0.0 --port 8000

pipeline:
	./$(VENV)/bin/$(PYTHON) run_pipeline.py

verify:
	./$(VENV)/bin/$(PYTHON) verify_data.py

tunnel:
	cloudflared tunnel --url http://localhost:8000

test-api:
	./$(VENV)/bin/pytest

test-pipeline:
	./$(VENV)/bin/$(PYTHON) test_main.py

clean:
	rm -rf $(VENV)
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
