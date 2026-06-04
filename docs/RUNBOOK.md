# RUNBOOK

Operational notes for supervised paper-trading runs. Keep live-money changes explicit and reviewed.

## Current Mode

- Day 2 paper burn-in is supervised paper mode.
- As of 2026-06-03 09:35 PDT, Oracle VM is the single active paper runner.
- The laptop bot is stopped; do not start it unless intentionally migrating back from Oracle.
- `AUTO_ENTRY_ENABLED=false` is the service env default. Runtime Telegram config can override it in SQLite; trust `/status` and `/config` for the effective active-runner entry state.
- `AUTO_EXIT_ENABLED=true` is enabled on the active runner after close-path hardening approval.
- AMPX and POET paper lifecycles are complete; current expected open positions are none.
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
- Send `/config` and confirm runtime auto-entry state before changing entry automation.
- Send `/status` and confirm pending exits show the close order ID, reason, status, and duplicate-exit suppression.
- Send `/report` and confirm positions, orders, pending exits, and latest journal entries are visible.
- Expected pending-close suppression is log-only; Telegram alerts are reserved for submitted exits, failures, unresolved pending exits, and operator actions.
- A duplicate local bot process should fail fast on startup instead of creating Telegram `getUpdates` conflicts.
- Do not use `/resume <token>` unless the restored state is intentionally ready to trade.
- Use `/kill` only when you intend to flatten paper positions and persist `HALTED`.

Runtime config:

- `/config` shows runtime switches.
- `/config auto_entry on` enables new-entry automation without a service restart.
- `/config auto_entry off` disables new-entry automation without a service restart.
- Runtime `auto_entry_enabled` is persisted in SQLite and overrides the env default.
- `/status` reports `Runtime auto-entry` plus the effective new-entry status.

API budget behavior:

- Alpaca API calls are counted internally in a rolling 60-second window.
- Normal usage is log-only and should not produce Telegram noise.
- Safety-critical calls are counted but not blocked: `/kill`, account/clock/position reads, order reconciliation, and exit/close paths.
- Nonessential discovery work is the first thing deferred if API usage reaches the internal critical threshold.
- The intent is smooth live operation, not another operator report.

For supervised laptop burn-in, `SHUTDOWN_FLATTEN_ON_EXIT=false` is allowed only while `ALPACA_PAPER=true`. This lets Ctrl+C or process termination stop the local process without flattening. The setting is forbidden in live mode.

## Server Run: Oracle VM Or Raspberry Pi

The preferred always-on deployment is a small Linux host using systemd. Oracle Always Free ARM is the default target; Raspberry Pi is acceptable for experimentation if power, network, clock sync, and uptime are reliable.

Current Oracle VM stance: Oracle is the active paper runner. The service is active and enabled, with `AUTO_ENTRY_ENABLED=false`, `AUTO_EXIT_ENABLED=true`, `ALPACA_PAPER=true`, `MAX_NEW_POSITIONS_PER_DAY=1`, and `SHUTDOWN_FLATTEN_ON_EXIT=true`. Runtime Telegram config may promote paper-only entry controls without editing `.env`; as of Day 3, `auto_entry_enabled=true` and `max_new_positions_per_day=3` are runtime values, not service env defaults.

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
- `FINNHUB_API_KEY` blank unless Finnhub enrichment is deliberately being tested.

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

Run the migration preflight before any Oracle cutover attempt:

```bash
ORACLE_HOST=<host> ORACLE_USER=ubuntu ORACLE_KEY=<ssh-key> scripts/oracle_preflight.sh POET
```

Expected result while the laptop remains the active runner is `Overall: WARN`, with the local runner warning explaining why Oracle should stay inactive. A true migration-ready result requires no local `python -m auto_trader` process, Oracle systemd still inactive before cutover, paper-mode safety flags present, and broker validation passing for the latest lifecycle symbol.

After cutover, verify Oracle remains the only active runner:

```bash
pgrep -fl "python.*-m auto_trader"
ssh -i <ssh-key> ubuntu@<host> "systemctl is-active auto-trader; systemctl is-enabled auto-trader; sudo journalctl -u auto-trader -n 80 --no-pager"
```

Expected current Oracle state is `active` and `enabled`. Expected current laptop state is no local `python -m auto_trader` process.

Manual stop:

```bash
sudo systemctl stop auto-trader
```

Because Oracle uses `SHUTDOWN_FLATTEN_ON_EXIT=true`, an unmarked manual stop or restart intentionally persists `HALTED` and may flatten positions before shutdown.

Planned maintenance deploys should use the one-shot maintenance marker instead of a hard kill or an unmarked restart. The marker is short-lived, consumed once by the old process on `SIGTERM`, and lets a normal `systemctl restart` preserve the active paper lifecycle:

```bash
ORACLE_HOST=<host> ORACLE_USER=ubuntu ORACLE_KEY=<ssh-key> scripts/oracle_planned_deploy.sh
```

The currently running process must already support the maintenance marker. The script checks a runtime capability marker written by the active systemd `MainPID`; checking files on disk is not enough. If the script reports that the remote service does not support planned maintenance, it will refuse the normal path before syncing code. For the one-time first rollout of the maintenance feature in paper mode only, use the guarded bootstrap path:

```bash
BOOTSTRAP_HARD_KILL=true ORACLE_HOST=<host> ORACLE_USER=ubuntu ORACLE_KEY=<ssh-key> scripts/oracle_planned_deploy.sh
```

The bootstrap path requires the remote `.env` to contain `ALPACA_PAPER=true` and refuses `ALLOW_LIVE=true`. After that first rollout, future deploys should use the normal planned maintenance command above.

For live mode, the maintenance helper refuses preserve-position restarts unless `ALLOW_LIVE=true` is set explicitly. Use that only for a reviewed live deploy window where preserving positions through a fast restart is intentional. After any deploy, inspect `/status`; send `/resume <token>` only if the state is intentionally ready to trade.

### Optional Finnhub Enrichment

Finnhub is optional market-data enrichment for candidate audit context. It must not be treated as trade approval, sizing, or a risk override. Orders still require `RiskEngine -> OrderManager -> AlpacaAdapter`.

Enable it only after code deployment and a clean service restart/resume:

```bash
FINNHUB_API_KEY=...
```

To disable it, remove or blank `FINNHUB_API_KEY` in `/opt/auto-trader/.env` and use the planned maintenance deploy path above. Resume manually only after `/status` is clean if the restart lands in `HALTED`.

Before leaving Finnhub enabled unattended:

- Confirm `/status` has no warnings after one supervisor tick.
- Confirm `signals.features_json` contains Finnhub context for new candidates.
- Confirm supervisor ticks do not time out.
- Watch API-budget logs; Finnhub calls are nonessential and should be disabled if free-tier limits or latency become noisy.

### Optional FRED Macro Context

FRED can help the AI research packet with macro regime context: interest rates, inflation, credit stress, unemployment, liquidity, and broad risk-on/risk-off backdrop. It is not ticker-specific market data and must not be treated as a standalone buy/sell signal.

```bash
FRED_API_KEY=...
```

FRED context is added to the `macro` lane in AI research packets through a cached daily Core Risk Pack: short/long Treasury yields, 10Y-2Y curve spread, high-yield credit spread, CPI, unemployment, VIX, and Fed balance sheet assets. Missing keys, API errors, and API-budget deferrals are recorded as macro context errors; they must not crash the supervisor or approve/block trades by themselves.

### Optional AI Research Preflight

AI research is advisory only. It may write research memos for candidates, but it must not approve trades, size orders, override risk gates, or place orders.

`AI_ENTRY_GATE_ENABLED=false` is the safe default. When enabled, real-provider AI research becomes a fail-closed entry filter before `RiskEngine`: only a valid `approve` can continue to RiskEngine and OrderManager. `watch`, `reject`, invalid output, budget exhaustion, provider failure, disabled AI research, or shadow-only research blocks the entry and records an audit journal note. AI still cannot size orders, submit orders, override RiskEngine, bypass `/kill`, bypass `HALTED`, bypass broker/account blocks, or override account loss/drawdown halts.

The gate can also be toggled at runtime from Telegram without a deploy:

```text
/config ai_gate on
/config ai_gate off
```

Use `/config` to confirm whether `ai_entry_gate_enabled` is coming from runtime config or the env default. Runtime `ai_gate on` only changes the pre-RiskEngine filter; it does not enable paid AI by itself, change models, change budget, submit orders, or bypass any halt.

Run the read-only activation preflight before enabling any paid provider:

```bash
scripts/ai_research_preflight.sh
```

On Oracle, run it as the service user so it can read the locked `/opt/auto-trader/.env`:

```bash
sudo -u auto-trader AUTO_TRADER_ENV_FILE=/opt/auto-trader/.env /opt/auto-trader/scripts/ai_research_preflight.sh
```

From the laptop, use the Oracle readiness wrapper before market open:

```bash
ORACLE_HOST=<host> ORACLE_USER=ubuntu ORACLE_KEY=<ssh-key> scripts/oracle_ai_ready.sh
```

This checks SSH, the Oracle service state, whether Finnhub/FRED key slots are set, and the read-only AI research preflight. It does not call paid AI providers.

The preflight performs no provider API calls. `READY` requires `AI_RESEARCH_ENABLED=true`, a real provider such as `anthropic`, an explicit model, the matching provider key present, a positive `AI_RESEARCH_MAX_CALLS_PER_DAY`, a working DB budget count, enough calls remaining for the UTC day, and a bounded timeout. It also prints the effective `AI entry gate enabled` state after runtime config is applied. It prints only `key_present=true/false`; never key values, prefixes, or lengths.

After preflight is `READY`, run a no-order rehearsal against the real current candidate packet:

```bash
scripts/ai_entry_gate_rehearsal.sh
```

On Oracle:

```bash
sudo -u auto-trader AUTO_TRADER_ENV_FILE=/opt/auto-trader/.env /opt/auto-trader/scripts/ai_entry_gate_rehearsal.sh
```

The rehearsal can call paid AI providers and consume the configured daily AI budget. It uses live discovery plus Finnhub/FRED/risk context, logs AI research memos for audit, and prints whether the gate would block before `RiskEngine` or would continue to `RiskEngine`. It never imports `OrderManager`, never calls `RiskEngine`, and never submits an order.

For Claude/Anthropic testing, set pricing assumptions explicitly in `.env` before trusting the estimate:

```bash
AI_RESEARCH_PROVIDER=anthropic
AI_RESEARCH_MODEL=claude-opus-4-8
AI_RESEARCH_MAX_CALLS_PER_DAY=1
AI_RESEARCH_EST_INPUT_TOKENS=15000
AI_RESEARCH_EST_OUTPUT_TOKENS=2000
AI_RESEARCH_INPUT_PRICE_PER_MTOK=5.0
AI_RESEARCH_OUTPUT_PRICE_PER_MTOK=25.0
```

For the multi-provider advisory committee, keep single-provider mode parked and set the provider list plus provider-specific models:

```bash
AI_RESEARCH_PROVIDERS=anthropic,openai,xai
AI_RESEARCH_ANTHROPIC_MODEL=claude-opus-4-8
AI_RESEARCH_OPENAI_MODEL=<chosen-openai-model>
AI_RESEARCH_XAI_MODEL=<chosen-grok-model>
AI_RESEARCH_MAX_CALLS_PER_DAY=3
```

One committee round consumes one chargeable call per selected real provider, and preflight reports how many full rounds remain from the current daily budget. The v1 aggregate is deterministic: at least two valid provider approvals with confidence >= 0.65 and no valid reject can produce an AI advisory `approve`; any valid reject produces `reject`; everything else is `watch`. Invalid provider output, timeouts, and failures are audited but cannot force approval. RiskEngine remains the only execution and sizing authority.

The default `AI_RESEARCH_MAX_CALLS_PER_DAY=0` intentionally reports `NOT_READY`, even when a key is present.

### Future Risk Profiles / YOLO Mode

Future risk profiles should be explicit, for example `conservative | aggressive | yolo`. `conservative` is the current default. `aggressive` and `yolo` must be introduced behind paper-first controls and audit labels. `yolo` must remain paper-only by default and must never bypass kill switch, HALTED state, broker/account blocks, daily/weekly/drawdown halts, duplicate-position guards, the AI entry gate when enabled, or RiskEngine.

### Account Risk Halt Rehearsal

Before a burn-in session or deploy window, validate both the account-risk threshold math and the real supervisor halt path:

```bash
scripts/account_risk_validate.sh --base-equity 400
scripts/account_risk_validate.sh --base-equity 400 --rehearse-supervisor-halt
```

Both commands should return `Overall: PASS`. The rehearsal uses a temp SQLite DB and a fake broker adapter; it does not submit, cancel, or flatten real broker state. It proves the production supervisor path persists `HALTED`, calls cancel-all, calls flatten-all, emits an alert, and writes a journal entry when a synthetic equity shock breaches the account-risk threshold.

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
- Run the live cutover preflight from the intended runner:

```bash
scripts/live_preflight.sh --max-equity 500 --max-new-positions 3
```

- The preflight is read-only against Alpaca and the production DB. Its embedded halt drill and account-risk rehearsal use temp SQLite DBs plus fake broker adapters; they do not submit, cancel, or flatten real broker state.
- Default preflight should run while `ALPACA_PAPER=true` and should fail if there are open positions, open orders, pending exits, unsafe shutdown config, missing planned-deploy capability for the active systemd `MainPID`, failed account-risk rehearsal, or failed halt drill.
- The preflight also requires an explicit runtime `auto_entry_enabled` value so live cutover cannot depend on an accidental settings default.
- After a reviewed switch to `ALPACA_PAPER=false`, rerun only with `--allow-current-live`; use `--allow-open-positions` only for a reviewed in-position validation window.
- Get explicit live cutover approval.
