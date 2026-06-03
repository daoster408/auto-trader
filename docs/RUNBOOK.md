# RUNBOOK

Operational notes for supervised paper-trading runs. Keep live-money changes explicit and reviewed.

## Current Mode

- Day 2 paper burn-in is supervised paper mode.
- `AUTO_ENTRY_ENABLED=false` means the supervisor will not open new positions.
- `AUTO_EXIT_ENABLED=true` is enabled locally after close-path hardening approval so exit rules can close existing paper positions.
- Current AMPX close order is expected to remain accepted/queued while the market is closed.
- `/kill` remains the emergency path: cancel all orders, flatten all positions, and persist `HALTED`.
- RiskEngine remains the only path to any order.

## Local Laptop Run

Use this while watching the bot and Telegram:

```bash
scripts/run_bot.sh
```

Expected startup checks:

- Alpaca paper health succeeds.
- Single-instance lock is acquired before Telegram polling starts.
- State is restored from SQLite.
- Telegram polling starts.
- Supervisor starts with the configured `auto_entry` and `auto_exit` flags.

Telegram checks after startup:

- Send `/status` and confirm account, state, and warnings.
- Send `/status` and confirm pending exits show the close order ID, reason, status, and duplicate-exit suppression.
- Send `/report` and confirm positions, orders, pending exits, and latest journal entries are visible.
- Expected pending-close suppression is log-only; Telegram alerts are reserved for submitted exits, failures, unresolved pending exits, and operator actions.
- A duplicate local bot process should fail fast on startup instead of creating Telegram `getUpdates` conflicts.
- Do not use `/resume <token>` unless the restored state is intentionally ready to trade.
- Use `/kill` only when you intend to flatten paper positions and persist `HALTED`.

API budget behavior:

- Alpaca API calls are counted internally in a rolling 60-second window.
- Normal usage is log-only and should not produce Telegram noise.
- Safety-critical calls are counted but not blocked: `/kill`, account/clock/position reads, order reconciliation, and exit/close paths.
- Nonessential discovery work is the first thing deferred if API usage reaches the internal critical threshold.
- The intent is smooth live operation, not another operator report.

For supervised laptop burn-in, `SHUTDOWN_FLATTEN_ON_EXIT=false` is allowed only while `ALPACA_PAPER=true`. This lets Ctrl+C or process termination stop the local process without flattening. The setting is forbidden in live mode.

## Server Run: Oracle VM Or Raspberry Pi

The preferred always-on deployment is a small Linux host using systemd. Oracle Always Free ARM is the default target; Raspberry Pi is acceptable for experimentation if power, network, clock sync, and uptime are reliable.

Tonight's Oracle VM stance: prepare the host, but do not migrate the active bot yet. The laptop bot is currently carrying the accepted AMPX paper close order and local pending-exit marker. The AMPX close lifecycle should validate on Wednesday, 2026-06-03 before Oracle becomes the active runner.

Only one active bot host should poll Telegram and trade the paper account at a time. The local single-instance lock protects one SQLite DB path on one machine; it does not coordinate across a laptop and Oracle VM. If Oracle is started with the same Telegram token and Alpaca account while the laptop bot is also running, cross-host duplicate polling and duplicate trading decisions are possible.

Example install shape:

```bash
sudo useradd --system --create-home --shell /usr/sbin/nologin auto-trader
sudo mkdir -p /opt/auto-trader
sudo chown -R auto-trader:auto-trader /opt/auto-trader
```

Copy the repo to `/opt/auto-trader`, create the virtualenv, install dependencies, and place the server `.env` at `/opt/auto-trader/.env`. The server `.env` should keep:

- `ALPACA_PAPER=true` during burn-in.
- `SHUTDOWN_FLATTEN_ON_EXIT=true` for unattended operation.
- `AUTO_ENTRY_ENABLED=false` until entry automation is explicitly promoted.
- `AUTO_EXIT_ENABLED=true` only after the close path has been reviewed, pushed, and validated on the supervised laptop run.
- `TELEGRAM_ALLOWED_IDS` populated so commands fail closed.

Recommended install commands from `/opt/auto-trader`:

```bash
sudo -u auto-trader python3.12 -m venv .venv
sudo -u auto-trader .venv/bin/pip install -e .
sudo chown auto-trader:auto-trader /opt/auto-trader/.env
sudo chmod 600 /opt/auto-trader/.env
```

The systemd template uses `Restart=always` so the host restarts the bot if the Python process exits after a recoverable runtime failure. An explicit `sudo systemctl stop auto-trader` remains the manual way to stop the service.
The service points `AUTO_TRADER_ENV_FILE` at `/opt/auto-trader/.env`; Python/Pydantic parses secrets, not systemd.
The template keeps `PrivateTmp=false` because the app writes `/tmp/auto_trader_healthy` for host-visible health checks.

Install the systemd template:

```bash
sudo cp deploy/systemd/auto-trader.service.example /etc/systemd/system/auto-trader.service
sudo systemctl daemon-reload
sudo systemctl enable auto-trader
sudo systemctl start auto-trader
```

Operational checks:

```bash
sudo systemctl status auto-trader
sudo journalctl -u auto-trader -n 100 --no-pager
sudo journalctl -u auto-trader -f
```

Before making Oracle the active runner, stop the laptop bot cleanly or otherwise confirm it is not polling Telegram. Run the Day 3 validation command on the host that owns the current DB/env before promoting new entries.

Manual stop:

```bash
sudo systemctl stop auto-trader
```

## Wednesday Market-Open Checklist

Use this on Wednesday, 2026-06-03, before enabling new entries:

```bash
scripts/day3_validate.sh --symbol AMPX
```

- Exit code `0` means the validation has no hard failures. `Overall: WARN` is expected if the market is still closed or the close order is still legitimately pending.
- Exit code `2` means a hard gate failed and new entries should stay disabled.
- Confirm the accepted AMPX close either fills after market open or remains visible as an open/pending broker order.
- Confirm `/status` shows pending exits and does not hide duplicate-exit suppression.
- Confirm `/report` shows the AMPX close order, pending-exit state, and latest journal entry.
- Confirm no second AMPX close order was submitted.
- Confirm `pending_exits` clears only after Alpaca position snapshot shows AMPX is gone.
- Keep `AUTO_ENTRY_ENABLED=false` until the full close lifecycle is verified.

## Promotion Gates

Before keeping auto-exit enabled unattended:

- Confirm `/status` and `/report` work during market hours.
- Confirm reconciliation sees filled orders and open positions.
- Confirm no duplicate close orders are produced in tests.
- Confirm pending exits are visible in Telegram and clear after the broker position disappears.
- Confirm shutdown behavior is understood for the run target.

Before enabling auto-entry:

- Complete at least one paper burn-in session with stable Telegram visibility.
- Review latest risk settings and document any threshold changes in `docs/DECISIONS_LOG.md`.
- Run the visible Reviewer/Optimizer cycle for the promotion.
- Commit and push approved changes before unattended operation.

Before live money:

- Keep `SHUTDOWN_FLATTEN_ON_EXIT=true`.
- Keep Telegram allowlist populated.
- Start with minimal capital.
- Confirm paper evidence covers entries, exits, rejects, reconnects, `/kill`, restart, and reporting.
- Get explicit live cutover approval.
