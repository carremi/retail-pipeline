.PHONY: help up down demo init-db gen-data seed pipeline test lint clean build

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

# ---------- Docker ----------

build: ## Build the Docker image
	docker compose build

up: build ## Start DB + pgAdmin (schemas auto-initialized)
	docker compose up -d postgres pgadmin
	docker compose run --rm db-init

down: ## Stop all services and remove containers
	docker compose --profile demo down

demo: build ## Full demo: DB + sample data + pipeline run
	docker compose --profile demo up

# ---------- Pipeline commands (via Docker) ----------

init-db: ## Initialize database schemas
	docker compose run --rm app init-db

gen-data: ## Generate sample data with simulators
	docker compose run --rm app generate-data

seed: ## Seed FX rates and SKU mappings
	docker compose run --rm app seed

pipeline: ## Run the full ETL pipeline
	docker compose run --rm app pipeline

# ---------- Local development ----------

test: ## Run unit tests (local, requires pip install -e ".[dev]")
	pytest tests/ -m "not integration" -v

lint: ## Run ruff linter
	ruff check src/ tests/ scripts/

# ---------- Cleanup ----------

clean: ## Remove Docker volumes, generated data, and caches
	docker compose --profile demo down -v
	rm -rf postgres-data/ pgadmin-data/
	rm -f data/drops/*.json data/drops/*.tsv data/drops/*.xlsx
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	rm -rf logs/*.log
