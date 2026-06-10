#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

AUTO_TRADER_ENV_FILE="${AUTO_TRADER_ENV_FILE:-${ROOT_DIR}/.env}"
AUTO_TRADER_PYTHON="${AUTO_TRADER_PYTHON:-${ROOT_DIR}/.venv/bin/python}"

if [[ ! -f "$AUTO_TRADER_ENV_FILE" ]]; then
  echo "AUTO_TRADER_ENV_FILE not found: $AUTO_TRADER_ENV_FILE" >&2
  exit 1
fi

if [[ ! -x "$AUTO_TRADER_PYTHON" ]]; then
  echo "AUTO_TRADER_PYTHON not executable: $AUTO_TRADER_PYTHON" >&2
  exit 1
fi

export AUTO_TRADER_ENV_FILE
exec "$AUTO_TRADER_PYTHON" -m auto_trader.ai_postmortem_review "$@"
