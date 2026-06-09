#!/usr/bin/env bash
set -euo pipefail

: "${ORACLE_HOST:?Set ORACLE_HOST}"
: "${ORACLE_USER:?Set ORACLE_USER}"
: "${ORACLE_KEY:?Set ORACLE_KEY}"

REMOTE_DIR="${ORACLE_REMOTE_DIR:-/opt/auto-trader}"
REMOTE_CMD="cd '${REMOTE_DIR}' && sudo -u auto-trader AUTO_TRADER_ENV_FILE='${REMOTE_DIR}/.env' '${REMOTE_DIR}/scripts/edge_report.sh'"
for arg in "$@"; do
  REMOTE_CMD+=" $(printf '%q' "${arg}")"
done

ssh -i "${ORACLE_KEY}" -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new \
  "${ORACLE_USER}@${ORACLE_HOST}" \
  "${REMOTE_CMD}"
