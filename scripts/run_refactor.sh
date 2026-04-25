#!/usr/bin/env bash
# Refactor airflow-frappe → frappe-airflow base via Codex.
#
# Usage (run from repo root):
#   bash scripts/run_refactor.sh
#
# What it does:
#   Runs Codex agent through codex-migration/TASK.md (10 tasks):
#   1-6  — rename package, update imports/hooks/Dockerfile/entrypoint
#   7    — move Client/Cabinet/TableConfig to /mnt/Soft/Work/Projects/fldrspro/airflow-manager
#   8-9  — fix workspace (3 shortcuts), homepage hook, docker-compose plugin pattern
#   10   — smoke test full rebuild
#
# Requirements:
#   - codex CLI installed: npm install -g @openai/codex
#   - logged in via: codex login
#   - Docker running (for Task 10)
#   - /mnt/Soft/Work/Projects/fldrspro/airflow-manager directory accessible
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TASK_FILE="$REPO_ROOT/codex-migration/TASK.md"

if [[ ! -f "$TASK_FILE" ]]; then
  echo "ERROR: Task file not found at $TASK_FILE" >&2
  exit 1
fi

if ! command -v codex &>/dev/null; then
  echo "ERROR: codex CLI not found. Install with: npm install -g @openai/codex" >&2
  exit 1
fi

if ! docker info &>/dev/null; then
  echo "WARNING: Docker is not running. Task 10 (smoke test) will fail." >&2
fi

EXT_REPO="/mnt/Soft/Work/Projects/fldrspro/airflow-manager"
if [[ ! -d "$(dirname "$EXT_REPO")" ]]; then
  echo "ERROR: Parent directory $(dirname "$EXT_REPO") does not exist." >&2
  exit 1
fi

echo "============================================"
echo " frappe-airflow — Base Refactor via Codex"
echo "============================================"
echo "Task file   : $TASK_FILE"
echo "Repo root   : $REPO_ROOT"
echo "Ext repo    : $EXT_REPO"
echo "Tasks       : 10"
echo ""
echo "Starting..."
echo ""

codex exec \
  --full-auto \
  --cd "$REPO_ROOT" \
  "$(cat "$TASK_FILE")"
