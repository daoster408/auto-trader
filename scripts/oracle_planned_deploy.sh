#!/usr/bin/env bash
set -euo pipefail

: "${ORACLE_HOST:?set ORACLE_HOST}"
: "${ORACLE_USER:=ubuntu}"
: "${ORACLE_KEY:?set ORACLE_KEY}"

REMOTE_DIR="${REMOTE_DIR:-/opt/auto-trader}"
REASON="${REASON:-planned deployment}"
TTL_SECONDS="${TTL_SECONDS:-300}"
ALLOW_LIVE="${ALLOW_LIVE:-false}"

SSH=(ssh -i "$ORACLE_KEY")
RSYNC_RSH="ssh -i $ORACLE_KEY"

echo "Syncing code to ${ORACLE_USER}@${ORACLE_HOST}:${REMOTE_DIR}"
rsync -az --delete --no-owner --no-group \
  --exclude .git \
  --exclude .venv \
  --exclude .env \
  --exclude auto_trader.db \
  --exclude ssh-key-2026-06-03.key \
  --exclude __pycache__ \
  --exclude '*.pyc' \
  -e "$RSYNC_RSH" \
  --rsync-path='sudo -u auto-trader rsync' \
  ./ "${ORACLE_USER}@${ORACLE_HOST}:${REMOTE_DIR}/"

echo "Arming planned maintenance shutdown marker"
printf -v QUOTED_REASON "%q" "$REASON"
REMOTE_REQUEST_CMD="cd ${REMOTE_DIR} && sudo -u auto-trader AUTO_TRADER_ENV_FILE=${REMOTE_DIR}/.env ${REMOTE_DIR}/.venv/bin/python -m auto_trader.maintenance request-shutdown --ttl-seconds ${TTL_SECONDS} --reason ${QUOTED_REASON}"

if [[ "$ALLOW_LIVE" == "true" ]]; then
  "${SSH[@]}" "${ORACLE_USER}@${ORACLE_HOST}" "${REMOTE_REQUEST_CMD} --allow-live"
else
  "${SSH[@]}" "${ORACLE_USER}@${ORACLE_HOST}" "${REMOTE_REQUEST_CMD}"
fi

echo "Restarting auto-trader service with normal SIGTERM"
"${SSH[@]}" "${ORACLE_USER}@${ORACLE_HOST}" "sudo systemctl restart auto-trader"

echo "Verifying service state"
"${SSH[@]}" "${ORACLE_USER}@${ORACLE_HOST}" "sudo systemctl is-active auto-trader"
"${SSH[@]}" "${ORACLE_USER}@${ORACLE_HOST}" "sudo systemctl show auto-trader -p MainPID -p ActiveState -p SubState --no-pager"
