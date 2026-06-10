#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${AUTO_TRADER_ENV_FILE:-${ROOT_DIR}/.env}"
PYTHON_BIN="${AUTO_TRADER_PYTHON:-${PYTHON_BIN:-${ROOT_DIR}/.venv/bin/python}}"

if [[ ! -x "${PYTHON_BIN}" ]]; then
  echo "Python virtualenv not found at ${PYTHON_BIN}" >&2
  exit 1
fi

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "Env file not found at ${ENV_FILE}" >&2
  exit 1
fi

export AUTO_TRADER_ENV_FILE="${ENV_FILE}"

exec "${PYTHON_BIN}" -m auto_trader.edge_report --brief "$@"
