#!/usr/bin/env bash
set -euo pipefail

: "${ORACLE_HOST:?set ORACLE_HOST}"
: "${ORACLE_USER:=ubuntu}"
: "${ORACLE_KEY:?set ORACLE_KEY}"

REMOTE_DIR="${REMOTE_DIR:-/opt/auto-trader}"
REMOTE_SERVICE="${REMOTE_SERVICE:-auto-trader}"
REASON="${REASON:-planned safe restart}"
TTL_SECONDS="${TTL_SECONDS:-300}"
ALLOW_LIVE="${ALLOW_LIVE:-false}"
DRY_RUN="${DRY_RUN:-false}"

SSH=(ssh -i "$ORACLE_KEY")
REMOTE="${ORACLE_USER}@${ORACLE_HOST}"
REMOTE_PYTHON="cd ${REMOTE_DIR} && sudo -u auto-trader AUTO_TRADER_ENV_FILE=${REMOTE_DIR}/.env ${REMOTE_DIR}/.venv/bin/python"

run_ssh() {
  "${SSH[@]}" "${REMOTE}" "$@"
}

run_remote() {
  if [[ "${DRY_RUN}" == "true" ]]; then
    printf 'DRY RUN remote: %s\n' "$*"
    return 0
  fi
  run_ssh "$@"
}

echo "Oracle safe restart"
echo "Remote: ${REMOTE} ${REMOTE_DIR}"
echo "Service: ${REMOTE_SERVICE}"
echo "Reason: ${REASON}"

if [[ "${ALLOW_LIVE}" == "true" ]]; then
  echo "ALLOW_LIVE=true; planned-maintenance marker will permit live restart."
else
  echo "ALLOW_LIVE=false; live restart marker will be rejected by the bot."
fi

if [[ "${DRY_RUN}" != "true" ]]; then
  REMOTE_MAIN_PID="$(run_ssh "systemctl show '${REMOTE_SERVICE}' -p MainPID --value")"
  REMOTE_RUNTIME_SUPPORT_CMD="cd ${REMOTE_DIR} && sudo -u auto-trader AUTO_TRADER_ENV_FILE=${REMOTE_DIR}/.env AUTO_TRADER_EXPECTED_PID=${REMOTE_MAIN_PID} ${REMOTE_DIR}/.venv/bin/python -c 'import os, sqlite3; from auto_trader.config.settings import get_settings; con = sqlite3.connect(get_settings().db_path); rows = dict(con.execute(\"SELECT key, value FROM runtime_config WHERE key IN (?, ?)\", (\"runtime_capability_planned_maintenance_shutdown\", \"runtime_capability_planned_maintenance_pid\")).fetchall()); con.close(); raise SystemExit(0 if rows.get(\"runtime_capability_planned_maintenance_shutdown\") == \"true\" and rows.get(\"runtime_capability_planned_maintenance_pid\") == os.environ[\"AUTO_TRADER_EXPECTED_PID\"] else 1)'"
  if [[ ! "${REMOTE_MAIN_PID}" =~ ^[0-9]+$ || "${REMOTE_MAIN_PID}" == "0" ]] || ! run_ssh "${REMOTE_RUNTIME_SUPPORT_CMD}"; then
    cat >&2 <<'EOF'
Remote service does not currently advertise planned-maintenance restart support.

Refusing safe restart because a normal SIGTERM could persist HALTED and queue
flatten orders while SHUTDOWN_FLATTEN_ON_EXIT=true.

Use scripts/oracle_planned_deploy.sh for reviewed deploys, or inspect /status
and broker state before choosing any manual recovery action.
EOF
    exit 1
  fi
fi

printf -v QUOTED_REASON "%q" "${REASON}"
REQUEST_CMD="${REMOTE_PYTHON} -m auto_trader.maintenance request-shutdown --ttl-seconds ${TTL_SECONDS} --reason ${QUOTED_REASON}"
if [[ "${ALLOW_LIVE}" == "true" ]]; then
  REQUEST_CMD="${REQUEST_CMD} --allow-live"
fi

echo "Arming planned maintenance shutdown marker"
run_remote "${REQUEST_CMD}"

echo "Restarting ${REMOTE_SERVICE} with planned-maintenance marker armed"
run_remote "sudo systemctl restart '${REMOTE_SERVICE}'"

echo "Verifying service state"
run_remote "sudo systemctl is-active '${REMOTE_SERVICE}'"
run_remote "sudo systemctl show '${REMOTE_SERVICE}' -p MainPID -p ActiveState -p SubState --no-pager"

echo "SAFE RESTART COMPLETE: send Telegram /status and confirm state/positions/open orders before leaving unattended."
