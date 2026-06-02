#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${AUTO_TRADER_ENV_FILE:-${ROOT_DIR}/.env}"
PYTHON_BIN="${AUTO_TRADER_PYTHON:-${ROOT_DIR}/.venv/bin/python}"

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "Missing environment file: ${ENV_FILE}" >&2
  echo "Copy .env.example to .env and fill the required paper-trading values." >&2
  exit 1
fi

if [[ ! -x "${PYTHON_BIN}" ]]; then
  echo "Missing executable Python: ${PYTHON_BIN}" >&2
  echo "Create the virtualenv or set AUTO_TRADER_PYTHON to the desired interpreter." >&2
  exit 1
fi

cd "${ROOT_DIR}"
export AUTO_TRADER_ENV_FILE="${ENV_FILE}"

exec "${PYTHON_BIN}" -m auto_trader
