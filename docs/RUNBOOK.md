# RUNBOOK

Operational notes for supervised paper-trading runs. Keep live-money changes explicit and reviewed.

## Current Mode

- Day 2 paper burn-in is alert-only by default.
- `AUTO_ENTRY_ENABLED=false` means the supervisor will not open new positions.
- `AUTO_EXIT_ENABLED=false` means the supervisor will not close positions from exit rules.
- `/kill` remains the emergency path: cancel all orders, flatten all positions, and persist `HALTED`.
- RiskEngine remains the only path to any order.

## Local Laptop Run

Use this while watching the bot and Telegram:

```bash
scripts/run_bot.sh
```

Expected startup checks:

- Alpaca paper health succeeds.
- State is restored from SQLite.
- Telegram polling starts.
- Supervisor starts with the configured `auto_entry` and `auto_exit` flags.

Telegram checks after startup:

- Send `/status` and confirm account, state, and warnings.
- Send `/report` and confirm positions/orders are visible.
- Do not use `/resume <token>` unless the restored state is intentionally ready to trade.
- Use `/kill` only when you intend to flatten paper positions and persist `HALTED`.

For supervised laptop burn-in, `SHUTDOWN_FLATTEN_ON_EXIT=false` is allowed only while `ALPACA_PAPER=true`. This lets Ctrl+C or process termination stop the local process without flattening. The setting is forbidden in live mode.

## Server Run: Oracle VM Or Raspberry Pi

The preferred always-on deployment is a small Linux host using systemd. Oracle Always Free ARM is the default target; Raspberry Pi is acceptable for experimentation if power, network, clock sync, and uptime are reliable.

Example install shape:

```bash
sudo useradd --system --create-home --shell /usr/sbin/nologin auto-trader
sudo mkdir -p /opt/auto-trader
sudo chown -R auto-trader:auto-trader /opt/auto-trader
```

Copy the repo to `/opt/auto-trader`, create the virtualenv, install dependencies, and place the server `.env` at `/opt/auto-trader/.env`. The server `.env` should keep:

- `ALPACA_PAPER=true` during burn-in.
- `SHUTDOWN_FLATTEN_ON_EXIT=true` for unattended operation.
- `AUTO_ENTRY_ENABLED=false` and `AUTO_EXIT_ENABLED=false` until explicitly promoted.
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

Manual stop:

```bash
sudo systemctl stop auto-trader
```

## Promotion Gates

Before enabling auto-exit:

- Confirm `/status` and `/report` work during market hours.
- Confirm reconciliation sees filled orders and open positions.
- Confirm no duplicate close orders are produced in tests.
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
