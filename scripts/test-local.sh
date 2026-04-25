#!/usr/bin/env bash
# Local test runner — two modes:
#
#   bash scripts/test-local.sh         # fast: unit tests only (no Frappe needed)
#   bash scripts/test-local.sh --full  # full: also builds Docker image + starts compose
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
MODE="${1:-}"

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

pass() { echo -e "${GREEN}✓ $*${NC}"; }
fail() { echo -e "${RED}✗ $*${NC}"; exit 1; }

echo "=== Airflow Manager — Local Test ==="
echo ""

# ─── Step 1: Unit tests in a clean Python container ───────────────────────────
echo "── Step 1: Unit tests (Python 3.11, no Frappe required)"

# Generate a throwaway Fernet key for tests using Docker
TEST_FERNET_KEY=$(docker run --rm python:3.11-slim \
    bash -c "pip install -q cryptography && python -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\"")

docker run --rm \
    -v "$REPO_ROOT/apps/airflow_manager":/app \
    -e AIRFLOW_FERNET_KEY="$TEST_FERNET_KEY" \
    -e AIRFLOW_DB_URL="postgresql+psycopg2://dummy:dummy@localhost/dummy" \
    -w /app \
    python:3.11-slim \
    bash -c "
        pip install -q sqlalchemy psycopg2-binary cryptography pytest &&
        PYTHONPATH=/app python -m pytest tests/ -v --tb=short
    "

pass "All unit tests passed"
echo ""

if [[ "$MODE" != "--full" ]]; then
    echo "── Skipping Docker build (run with --full to include)"
    echo ""
    echo "Done. Run 'bash scripts/test-local.sh --full' for a complete test."
    exit 0
fi

# ─── Step 2: Docker image build ───────────────────────────────────────────────
echo "── Step 2: Docker build (this takes 5-10 min on first run — Frappe is downloaded)"

docker build -t airflow-frappe:local "$REPO_ROOT"

pass "Docker image built: airflow-frappe:local"
echo ""

# ─── Step 3: Compose up ───────────────────────────────────────────────────────
echo "── Step 3: docker compose up"
echo "   Requires .env file — copying from .env.example if missing..."

if [[ ! -f "$REPO_ROOT/.env" ]]; then
    cp "$REPO_ROOT/.env.example" "$REPO_ROOT/.env"
    echo ""
    echo "   ⚠  .env was created from .env.example."
    echo "   Set AIRFLOW_DB_URL and AIRFLOW_FERNET_KEY before running --full."
    echo ""
    fail "Edit .env with real Airflow credentials, then re-run with --full"
fi

if ! grep -q "AIRFLOW_FERNET_KEY=." "$REPO_ROOT/.env" 2>/dev/null; then
    fail "AIRFLOW_FERNET_KEY is empty in .env — fill it in first"
fi

cd "$REPO_ROOT"
docker compose up -d frappe-mariadb frappe-redis

echo "   Waiting for MariaDB..."
until docker compose exec frappe-mariadb mysqladmin ping -h localhost --silent 2>/dev/null; do
    sleep 2
done
pass "MariaDB ready"

docker compose up -d frappe
echo ""
echo "   Waiting for Frappe to start (first run initialises the site — ~2 min)..."

for i in $(seq 1 60); do
    if curl -sf http://localhost:${FRAPPE_PORT:-8000}/api/method/ping >/dev/null 2>&1; then
        break
    fi
    sleep 3
done

if curl -sf http://localhost:${FRAPPE_PORT:-8000}/api/method/ping >/dev/null 2>&1; then
    pass "Frappe is up → http://localhost:${FRAPPE_PORT:-8000}"
else
    echo "Frappe not responding — check logs:"
    docker compose logs --tail=40 frappe
    fail "Frappe did not start in time"
fi

echo ""
echo "=== All steps passed ==="
echo "Login: http://localhost:${FRAPPE_PORT:-8000}  admin / (ADMIN_PASSWORD from .env)"
