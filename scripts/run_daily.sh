#!/bin/bash
# Wrapper to invoke the daily pipeline from launchd / cron / CI.
# Self-contained: activates venv, sets PATH, runs the pipeline, logs everything.

set -euo pipefail

# Resolve project root (this script's parent's parent)
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

# Ensure Docker is reachable (Docker Desktop installs it under /usr/local/bin
# or /opt/homebrew/bin on Apple Silicon).
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:$PATH"

# Activate the virtual environment
source "$PROJECT_ROOT/venv/bin/activate"

# Optional: ensure Postgres container is up (no-op if already running)
docker compose up -d postgres >/dev/null 2>&1 || true

# Wait briefly for Postgres to accept connections
for i in {1..10}; do
    if docker compose exec -T postgres pg_isready -U retail_user -d retail_pipeline >/dev/null 2>&1; then
        break
    fi
    sleep 2
done

# Run the pipeline
exec python scripts/run_daily.py
