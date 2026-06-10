#!/usr/bin/env bash
set -euo pipefail

: "${ORACLE_HOST:?ORACLE_HOST is required}"
: "${ORACLE_USER:?ORACLE_USER is required}"
: "${ORACLE_KEY:?ORACLE_KEY is required}"

REMOTE_DIR="${ORACLE_REMOTE_DIR:-/opt/auto-trader}"
REMOTE_ARGS=""
for arg in "$@"; do
  printf -v quoted " %q" "$arg"
  REMOTE_ARGS+="$quoted"
done

ssh -i "$ORACLE_KEY" -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new \
  "$ORACLE_USER@$ORACLE_HOST" \
  "cd '${REMOTE_DIR}' && sudo -u auto-trader AUTO_TRADER_ENV_FILE='${REMOTE_DIR}/.env' '${REMOTE_DIR}/scripts/ai_postmortem_review.sh'${REMOTE_ARGS}"
