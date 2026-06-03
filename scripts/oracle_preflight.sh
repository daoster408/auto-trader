#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

REMOTE_HOST="${ORACLE_HOST:-}"
REMOTE_USER="${ORACLE_USER:-ubuntu}"
REMOTE_KEY="${ORACLE_KEY:-}"
REMOTE_DIR="${ORACLE_REMOTE_DIR:-/opt/auto-trader}"
REMOTE_SERVICE="${ORACLE_SERVICE:-auto-trader}"
SYMBOL="${1:-}"

failures=0
warnings=0

say() {
  printf '%s\n' "$*"
}

pass() {
  say "PASS: $*"
}

warn() {
  warnings=$((warnings + 1))
  say "WARN: $*"
}

fail() {
  failures=$((failures + 1))
  say "FAIL: $*"
}

require_value() {
  local name="$1"
  local value="$2"

  if [[ -z "${value}" ]]; then
    fail "${name} is required"
  fi
}

ssh_base=()

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

check_local_runner() {
  say "== Local runner =="
  local matches
  matches="$(pgrep -fl "python.*-m auto_trader" || true)"

  if [[ -n "${matches}" ]]; then
    warn "local auto_trader process is running"
    say "${matches}"
  else
    pass "no local auto_trader process found"
  fi
}

check_remote_service() {
  say "== Oracle service =="
  local enabled
  local active

  enabled="$(remote "systemctl is-enabled ${REMOTE_SERVICE} 2>/dev/null || true")"
  active="$(remote "systemctl is-active ${REMOTE_SERVICE} 2>/dev/null || true")"

  if [[ "${enabled}" == "disabled" ]]; then
    pass "${REMOTE_SERVICE} service is disabled"
  else
    warn "${REMOTE_SERVICE} service is not disabled: ${enabled:-unknown}"
  fi

  if [[ "${active}" == "inactive" ]]; then
    pass "${REMOTE_SERVICE} service is inactive"
  else
    warn "${REMOTE_SERVICE} service is not inactive: ${active:-unknown}"
  fi
}

check_remote_env() {
  say "== Oracle env safety =="
  remote "sudo test -d '${REMOTE_DIR}'" \
    && pass "${REMOTE_DIR} exists" \
    || fail "${REMOTE_DIR} is missing"

  remote "sudo test -f '${REMOTE_DIR}/.env'" \
    && pass "remote .env exists" \
    || fail "remote .env is missing"

  local mode
  mode="$(remote "sudo stat -c '%a %U:%G' '${REMOTE_DIR}/.env' 2>/dev/null || true")"
  if [[ "${mode}" == "600 auto-trader:auto-trader" ]]; then
    pass "remote .env permissions are 600 auto-trader:auto-trader"
  else
    warn "remote .env permissions are ${mode:-unreadable}"
  fi

  local flags
  flags="$(remote "sudo awk -F= '
    /^[[:space:]]*(ALPACA_PAPER|AUTO_ENTRY_ENABLED|AUTO_EXIT_ENABLED|SHUTDOWN_FLATTEN_ON_EXIT|MAX_NEW_POSITIONS_PER_DAY)=/ {
      key=\$1
      sub(/^[[:space:]]+/, \"\", key)
      sub(/[[:space:]]+\$/, \"\", key)
      print key \"=\" \$2
    }
  ' '${REMOTE_DIR}/.env'")"
  say "${flags}"

  grep -q '^ALPACA_PAPER=true$' <<<"${flags}" \
    && pass "remote ALPACA_PAPER=true" \
    || fail "remote ALPACA_PAPER is not true"
  grep -q '^AUTO_ENTRY_ENABLED=false$' <<<"${flags}" \
    && pass "remote AUTO_ENTRY_ENABLED=false" \
    || warn "remote AUTO_ENTRY_ENABLED is not false"
  grep -q '^AUTO_EXIT_ENABLED=true$' <<<"${flags}" \
    && pass "remote AUTO_EXIT_ENABLED=true" \
    || warn "remote AUTO_EXIT_ENABLED is not true"
  grep -q '^SHUTDOWN_FLATTEN_ON_EXIT=true$' <<<"${flags}" \
    && pass "remote SHUTDOWN_FLATTEN_ON_EXIT=true" \
    || fail "remote SHUTDOWN_FLATTEN_ON_EXIT is not true"
  grep -q '^MAX_NEW_POSITIONS_PER_DAY=1$' <<<"${flags}" \
    && pass "remote MAX_NEW_POSITIONS_PER_DAY=1" \
    || warn "remote MAX_NEW_POSITIONS_PER_DAY is not 1"
}

check_remote_validation() {
  say "== Broker validation =="
  if [[ -z "${SYMBOL}" ]]; then
    warn "no symbol supplied; skipping day3 broker validation"
    return
  fi

  remote "cd '${REMOTE_DIR}' && sudo -u auto-trader AUTO_TRADER_ENV_FILE='${REMOTE_DIR}/.env' .venv/bin/python -m auto_trader.day3_validate --symbol '${SYMBOL}'"
  pass "remote day3 validation completed for ${SYMBOL}"
}

main() {
  say "Oracle preflight"
  say "Remote: ${REMOTE_USER}@${REMOTE_HOST:-<missing>} ${REMOTE_DIR}"

  require_value "ORACLE_HOST" "${REMOTE_HOST}"

  if [[ -n "${REMOTE_KEY}" && ! -f "${REMOTE_KEY}" ]]; then
    fail "ORACLE_KEY does not exist: ${REMOTE_KEY}"
  fi

  if (( failures > 0 )); then
    say "Overall: FAIL"
    exit 2
  fi

  build_ssh

  remote "true" \
    && pass "SSH connection works" \
    || fail "SSH connection failed"

  if (( failures > 0 )); then
    say "Overall: FAIL"
    exit 2
  fi

  check_local_runner
  check_remote_service
  check_remote_env
  check_remote_validation

  if (( failures > 0 )); then
    say "Overall: FAIL"
    exit 2
  fi

  if (( warnings > 0 )); then
    say "Overall: WARN"
    exit 0
  fi

  say "Overall: PASS"
}

main "$@"
