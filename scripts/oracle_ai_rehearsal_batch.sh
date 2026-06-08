#!/usr/bin/env bash
set -euo pipefail

: "${ORACLE_HOST:?Set ORACLE_HOST}"
: "${ORACLE_USER:?Set ORACLE_USER}"
: "${ORACLE_KEY:?Set ORACLE_KEY}"

REMOTE_DIR="${ORACLE_REMOTE_DIR:-/opt/auto-trader}"
if [[ "$#" -eq 0 ]]; then
  set -- --limit 10
fi
printf -v REMOTE_ARGS "%q " "$@"

ssh -i "${ORACLE_KEY}" -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new \
  "${ORACLE_USER}@${ORACLE_HOST}" \
  "cd '${REMOTE_DIR}' && sudo -u auto-trader AUTO_TRADER_ENV_FILE='${REMOTE_DIR}/.env' '${REMOTE_DIR}/scripts/ai_rehearsal_batch.sh' ${REMOTE_ARGS}"
