#!/usr/bin/env bash
# Build the Airflow Manager Frappe app via Codex.
#
# Usage (run from the NEW airflow-frappe repo root):
#   bash scripts/run_codex.sh
#
# Codex works through codex-migration/TASK.md task by task,
# running tests and committing after each one.
#
# Requirements:
#   - codex CLI installed: npm install -g @openai/codex
#   - logged in via: codex login
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TASK_FILE="$REPO_ROOT/codex-migration/TASK.md"

if [[ ! -f "$TASK_FILE" ]]; then
  echo "ERROR: Task file not found at $TASK_FILE" >&2
  echo "Copy docs/superpowers/frappe-tasks/TASK.md to codex-migration/TASK.md first." >&2
  exit 1
fi

if ! command -v codex &>/dev/null; then
  echo "ERROR: codex CLI not found. Install with: npm install -g @openai/codex" >&2
  exit 1
fi

echo "============================================"
echo " Airflow Manager Frappe App — Codex Build"
echo "============================================"
echo "Task file : $TASK_FILE"
echo "Repo root : $REPO_ROOT"
echo "Tasks     : 13"
echo ""
echo "Starting..."
echo ""

codex exec \
  --full-auto \
  --cd "$REPO_ROOT" \
  "$(cat "$TASK_FILE")"
