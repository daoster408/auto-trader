#!/usr/bin/env bash
set -euo pipefail

REMOTE_HOST="${ORACLE_HOST:-}"
REMOTE_USER="${ORACLE_USER:-ubuntu}"
REMOTE_KEY="${ORACLE_KEY:-}"
REMOTE_DIR="${ORACLE_REMOTE_DIR:-/opt/auto-trader}"
REMOTE_SERVICE="${ORACLE_SERVICE:-auto-trader}"

failures=0
ssh_base=()

say() {
  printf '%s\n' "$*"
}

fail() {
  failures=$((failures + 1))
  say "FAIL: $*"
}

pass() {
  say "PASS: $*"
}

build_ssh() {
  ssh_base=(ssh -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new)
  if [[ -n "${REMOTE_KEY}" ]]; then
    ssh_base+=(-i "${REMOTE_KEY}")
  fi
  ssh_base+=("${REMOTE_USER}@${REMOTE_HOST}")
}

remote() {
  "${ssh_base[@]}" "$@"
}

require_inputs() {
  if [[ -z "${REMOTE_HOST}" ]]; then
    fail "ORACLE_HOST is required"
  fi
  if [[ -n "${REMOTE_KEY}" && ! -f "${REMOTE_KEY}" ]]; then
    fail "ORACLE_KEY does not exist: ${REMOTE_KEY}"
  fi
}

check_service() {
  say "== Oracle service =="
  local active
  active="$(remote "systemctl is-active '${REMOTE_SERVICE}' 2>/dev/null || true")"
  if [[ "${active}" == "active" ]]; then
    pass "${REMOTE_SERVICE} is active"
  else
    fail "${REMOTE_SERVICE} is not active: ${active:-unknown}"
  fi
  remote "systemctl show '${REMOTE_SERVICE}' --property=MainPID,ActiveState,SubState --no-pager"
}

run_recovery_check() {
  say "== Friday recovery snapshot =="
  set +e
  remote "cd '${REMOTE_DIR}' && sudo -u auto-trader AUTO_TRADER_ENV_FILE='${REMOTE_DIR}/.env' '${REMOTE_DIR}/scripts/friday_recovery_check.sh'"
  local code=$?
  set -e
  if [[ "${code}" == "0" ]]; then
    pass "Friday recovery check is ready to resume"
  elif [[ "${code}" == "1" ]]; then
    pass "Friday recovery check is safely waiting"
  else
    fail "Friday recovery check failed"
  fi
}

main() {
  say "Oracle Friday recovery check"
  say "Remote: ${REMOTE_USER}@${REMOTE_HOST:-<missing>} ${REMOTE_DIR}"
  require_inputs
  if (( failures > 0 )); then
    say "Overall: FAIL"
    exit 2
  fi

  build_ssh
  remote "true" && pass "SSH connection works" || fail "SSH connection failed"
  if (( failures > 0 )); then
    say "Overall: FAIL"
    exit 2
  fi

  check_service
  run_recovery_check

  if (( failures > 0 )); then
    say "Overall: FAIL"
    exit 2
  fi
  say "Overall: PASS"
}

main "$@"
