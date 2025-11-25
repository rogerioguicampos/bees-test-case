# Variables
IMAGE_NAME = bees-pipeline
VENV = .venv
PYTHON = $(VENV)/bin/python
PIP = $(VENV)/bin/pip
PYTEST = $(VENV)/bin/pytest

# Detect OS to handle volume mounting (Linux/Mac vs Windows)
PWD := $(shell pwd)

.PHONY: help setup clean run test docker-build docker-run docker-shell docker-up docker-logs format

# --- DEFAULT GOAL ---
help: ## Shows this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Targets:'
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# --- LOCAL DEVELOPMENT ---
setup: ## Sets up the environment (Python 3.9 + uv + deps)
	@echo "Setting up Python 3.9 environment..."
	@echo "3.9" > .python-version
	@uv venv
	@echo "Installing dependencies..."
	@uv pip install -r requirements.txt
	@echo "Environment ready! Run 'source .venv/bin/activate' to enter."

run: ## Runs the pipeline locally (using venv)
	@echo "Running pipeline locally..."
	@$(PYTHON) main.py

test: ## Runs unit tests locally
	@echo "Running tests..."
	@$(PYTEST) tests/ -v

clean: ## Cleans up generated data, cache and venv
	@echo "Cleaning up..."
	@rm -rf data/ logs/
	@rm -rf .venv
	@rm -rf .pytest_cache
	@rm -rf __pycache__
	@find . -type d -name "__pycache__" -exec rm -rf {} +
	@echo "Cleaned."

# --- DOCKER OPERATIONS ---
docker-build: ## Builds the Docker image
	@echo "Building Docker image..."
	@docker build -t $(IMAGE_NAME) .

docker-run: ## Runs the pipeline inside Docker (manual trigger)
	@echo "Running pipeline in Docker..."
	@docker run --rm -v $(PWD)/data:/app/data $(IMAGE_NAME) python main.py

docker-shell: ## Opens a bash shell inside the container (Bind Mount for Dev)
	@echo "Entering Docker Dev Shell..."
	@docker run --rm -it -v $(PWD):/app $(IMAGE_NAME) /bin/bash

docker-up: ## Runs the pipeline in background (Cron Schedule)
	@echo "Starting Cron Scheduler..."
	@docker run -d --name bees-etl -v $(PWD)/data:/app/data -v $(PWD)/logs:/var/log $(IMAGE_NAME)
	@echo "Type 'make docker-logs' to see output."

docker-logs: ## Follows the logs of the background container
	@docker exec -it bees-etl tail -f /var/log/cron.log
