#!/usr/bin/env bash
set -euo pipefail

: "${ORACLE_HOST:?set ORACLE_HOST}"
: "${ORACLE_USER:=ubuntu}"
: "${ORACLE_KEY:?set ORACLE_KEY}"

REMOTE_DIR="${REMOTE_DIR:-/opt/auto-trader}"
REASON="${REASON:-planned deployment}"
TTL_SECONDS="${TTL_SECONDS:-300}"
ALLOW_LIVE="${ALLOW_LIVE:-false}"
BOOTSTRAP_HARD_KILL="${BOOTSTRAP_HARD_KILL:-false}"

SSH=(ssh -i "$ORACLE_KEY")
RSYNC_RSH="ssh -i $ORACLE_KEY"
REMOTE="${ORACLE_USER}@${ORACLE_HOST}"

REMOTE_PYTHON="cd ${REMOTE_DIR} && sudo -u auto-trader AUTO_TRADER_ENV_FILE=${REMOTE_DIR}/.env ${REMOTE_DIR}/.venv/bin/python"
REMOTE_SUPPORT_CMD="${REMOTE_PYTHON} -m auto_trader.maintenance request-shutdown --help >/dev/null 2>&1"

if "${SSH[@]}" "$REMOTE" "$REMOTE_SUPPORT_CMD"; then
  REMOTE_SUPPORTS_MAINTENANCE=true
else
  REMOTE_SUPPORTS_MAINTENANCE=false
fi

if [[ "$REMOTE_SUPPORTS_MAINTENANCE" != "true" ]]; then
  if [[ "$BOOTSTRAP_HARD_KILL" != "true" ]]; then
    cat >&2 <<'EOF'
Remote service does not yet support planned maintenance markers.

Refusing to run a normal planned deploy because the currently running process
would receive SIGTERM without knowing how to consume the marker, which can
HALT/flatten positions when SHUTDOWN_FLATTEN_ON_EXIT=true.

For the one-time paper-mode bootstrap rollout, rerun with:
  BOOTSTRAP_HARD_KILL=true

Only use that bootstrap path while the remote .env is paper mode.
EOF
    exit 1
  fi

  if [[ "$ALLOW_LIVE" == "true" ]]; then
    echo "BOOTSTRAP_HARD_KILL is paper-only; refusing with ALLOW_LIVE=true" >&2
    exit 1
  fi

  REMOTE_PAPER_CHECK="sudo grep -Eq '^ALPACA_PAPER=(true|True|TRUE)$' ${REMOTE_DIR}/.env"
  if ! "${SSH[@]}" "$REMOTE" "$REMOTE_PAPER_CHECK"; then
    echo "BOOTSTRAP_HARD_KILL requires remote ${REMOTE_DIR}/.env to contain ALPACA_PAPER=true" >&2
    exit 1
  fi
fi

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
  ./ "${REMOTE}:${REMOTE_DIR}/"

if [[ "$REMOTE_SUPPORTS_MAINTENANCE" != "true" ]]; then
  echo "One-time paper bootstrap: restarting with SIGKILL to avoid old SIGTERM flatten path"
  "${SSH[@]}" "$REMOTE" "sudo systemctl kill -s SIGKILL auto-trader && sleep 2 && sudo systemctl start auto-trader"
else
  echo "Arming planned maintenance shutdown marker"
  printf -v QUOTED_REASON "%q" "$REASON"
  REMOTE_REQUEST_CMD="${REMOTE_PYTHON} -m auto_trader.maintenance request-shutdown --ttl-seconds ${TTL_SECONDS} --reason ${QUOTED_REASON}"

  if [[ "$ALLOW_LIVE" == "true" ]]; then
    "${SSH[@]}" "$REMOTE" "${REMOTE_REQUEST_CMD} --allow-live"
  else
    "${SSH[@]}" "$REMOTE" "${REMOTE_REQUEST_CMD}"
  fi

  echo "Restarting auto-trader service with normal SIGTERM"
  "${SSH[@]}" "$REMOTE" "sudo systemctl restart auto-trader"
fi

echo "Verifying service state"
"${SSH[@]}" "$REMOTE" "sudo systemctl is-active auto-trader"
"${SSH[@]}" "$REMOTE" "sudo systemctl show auto-trader -p MainPID -p ActiveState -p SubState --no-pager"

echo "REQUIRED POST-DEPLOY CHECK: send Telegram /status and confirm the state is not unexpectedly HALTED before resuming or leaving it unattended."
