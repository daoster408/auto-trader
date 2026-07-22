#!/usr/bin/env bash
set -euo pipefail

: "${ORACLE_HOST:?set ORACLE_HOST}"
: "${ORACLE_USER:=ubuntu}"
: "${ORACLE_KEY:?set ORACLE_KEY}"

REMOTE_SERVICE="auto-trader.service"
REMOTE_CONFIG="${NEEDRESTART_CONFIG:-/etc/needrestart/conf.d/auto-trader.conf}"
CHECK_ONLY="${CHECK_ONLY:-false}"
DRY_RUN="${DRY_RUN:-false}"

SSH=(ssh -i "$ORACLE_KEY" -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new)
REMOTE="${ORACLE_USER}@${ORACLE_HOST}"
CONFIG_LINE="\$nrconf{override_rc}{qr(^auto-trader\\.service\$)} = 0;"

remote() {
  "${SSH[@]}" "$REMOTE" "$@"
}

check_guard() {
  remote "sudo test -f '${REMOTE_CONFIG}' && sudo grep -Fqx '${CONFIG_LINE}' '${REMOTE_CONFIG}' && sudo test \"\$(sudo stat -c '%U:%G:%a' '${REMOTE_CONFIG}')\" = 'root:root:644' && sudo perl -c '${REMOTE_CONFIG}' >/dev/null"
  if ! remote "sudo needrestart -b -r l >/dev/null"; then
    echo "WARN: needrestart list-mode scan reported unrelated pending restart work; guard file checks passed." >&2
  fi
}

echo "Oracle needrestart guard"
echo "Remote: ${REMOTE}"
echo "Service: ${REMOTE_SERVICE}"
echo "Config: ${REMOTE_CONFIG}"

if [[ "$DRY_RUN" == "true" ]]; then
  echo "DRY RUN: would install a needrestart override that defers ${REMOTE_SERVICE}."
  echo "DRY RUN: no service restart would be performed."
  exit 0
fi

if [[ "$CHECK_ONLY" == "true" ]]; then
  check_guard
  echo "PASS: needrestart guard is installed and syntactically valid."
  exit 0
fi

main_pid_before="$(remote "systemctl show '${REMOTE_SERVICE}' -p MainPID --value")"
if [[ ! "$main_pid_before" =~ ^[1-9][0-9]*$ ]]; then
  echo "FAIL: ${REMOTE_SERVICE} does not have a running MainPID." >&2
  exit 1
fi

printf '%s\n' "$CONFIG_LINE" | remote \
  "sudo install -d -m 0755 /etc/needrestart/conf.d && sudo tee '${REMOTE_CONFIG}' >/dev/null && sudo chown root:root '${REMOTE_CONFIG}' && sudo chmod 0644 '${REMOTE_CONFIG}'"

check_guard
remote "systemctl is-active '${REMOTE_SERVICE}'"
main_pid_after="$(remote "systemctl show '${REMOTE_SERVICE}' -p MainPID --value")"
if [[ "$main_pid_after" != "$main_pid_before" ]]; then
  echo "FAIL: ${REMOTE_SERVICE} restarted during guard installation (${main_pid_before} -> ${main_pid_after})." >&2
  exit 1
fi

echo "PASS: needrestart will defer automatic restarts of ${REMOTE_SERVICE}."
echo "PASS: ${REMOTE_SERVICE} MainPID remained ${main_pid_after}."
echo "Security updates remain enabled. This command did not restart the service."
echo "Apply any deferred application restart later with scripts/oracle_safe_restart.sh."
