# RUNBOOK

Operational notes for supervised paper-trading runs. Keep live-money changes explicit and reviewed.

## Current Mode

- On 2026-07-16 the project adopted a simplification target. This documentation pass does not change the deployed Oracle code, configuration, service state, positions, or orders.
- The prior multi-provider/postmortem runtime may still be installed on Oracle. Perform a read-only state and configuration audit before any restart or reactivation.
- As of 2026-06-03 09:35 PDT, Oracle VM is the single active paper runner.
- The laptop bot is stopped; do not start it unless intentionally migrating back from Oracle.
- `AUTO_ENTRY_ENABLED=false` is the service env default. Runtime Telegram config can override it in SQLite; trust `/status` and `/config` for the effective active-runner entry state.
- `AUTO_EXIT_ENABLED=true` is enabled on the active runner after close-path hardening approval.
- Current positions, open orders, runtime max entries, AI gate state, and AI budget usage are live operational state; check `/status`, `/config`, or the Week 2 launchpad instead of trusting a static doc snapshot.
- `/kill` remains the emergency path: cancel all orders, flatten all positions, and persist `HALTED`.
- RiskEngine remains the only path to any order.

## Simplified Target (Not Yet Deployed)

```text
scanner -> deterministic prefilter -> one configured real AI decision -> RiskEngine -> OrderManager -> deterministic exits
```

Keep Oracle/Alpaca operations, state persistence, RiskEngine, halt/kill behavior, duplicate-order protection, reconciliation, deterministic exits, journaling, and concise Telegram alerts.

Park multi-provider voting, Gemini/DeepSeek/Fable escalation, FRED-in-entry, and postmortem-bias prompt injection. Existing commands and environment settings for those features are documented below only because the pre-pivot runtime may still contain them; they are not the active target.

Replace profile labels in the target control surface with explicit numeric settings for entries/day, position size, gross exposure, daily/weekly loss halts, drawdown halt, stop/profit/trailing/stagnation exits, and AI spend.

The primary score is net realized dollars after losses and attributable API costs. Also track dollar expectancy/trade, average dollar win/loss, profit factor, drawdown, and incremental AI-added dollars versus the deterministic baseline. Win rate is secondary and cannot rescue negative net dollars.

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
- If an open-order lookup misses a persisted pending close, the supervisor reconciles the exact broker/client order identity against recent all-status orders. A filled or failed exact match clears the marker; a different same-symbol order never does. A genuinely unmatched marker pauses only after `PENDING_EXIT_UNRESOLVED_GRACE_SECONDS` (default `360`), and forced reconciliation is throttled to the normal reconciliation interval.
- `SYMBOL_REENTRY_COOLDOWN_MINUTES` defaults to `60`. A symbol with a durable filled exit inside that window is skipped before signal persistence, filtering, paid AI, or order submission. Canceled, rejected, expired, and merely submitted exits do not start the cooldown.
- `/ai` labels its rows as historical whenever the state is `PAUSED` or `HALTED`; no new entry research runs in those states.
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

Current Oracle VM stance: Oracle is the active paper runner. The service is active and enabled, with paper mode and safe shutdown semantics in `.env`; runtime Telegram config may promote entry controls without editing `.env`. Treat `/config`, `/status`, and the Week 2 launchpad as the source of truth for effective `auto_entry_enabled`, `ai_entry_gate_enabled`, `risk_profile`, and `max_new_positions_per_day`.

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

Because Oracle uses `SHUTDOWN_FLATTEN_ON_EXIT=true`, an unmarked manual stop or restart intentionally persists `HALTED` and may flatten positions before shutdown. Do not run raw `sudo systemctl restart auto-trader` while this setting is enabled.

Planned maintenance deploys should use the one-shot maintenance marker instead of a hard kill or an unmarked restart. The marker is short-lived, consumed once by the old process on `SIGTERM`, and lets a normal `systemctl restart` preserve the active paper lifecycle:

```bash
ORACLE_HOST=<host> ORACLE_USER=ubuntu ORACLE_KEY=<ssh-key> scripts/oracle_planned_deploy.sh
```

For env reloads or restarts without syncing code, use the safe restart helper, which arms the same planned-maintenance marker before sending SIGTERM:

```bash
ORACLE_HOST=<host> ORACLE_USER=ubuntu ORACLE_KEY=<ssh-key> scripts/oracle_safe_restart.sh
```

Ubuntu's `unattended-upgrades` may invoke `needrestart` after replacing a library used by the bot. An automatic `systemctl restart auto-trader.service` bypasses the maintenance marker and is therefore unsafe while `SHUTDOWN_FLATTEN_ON_EXIT=true`. Install and verify the Oracle guard once:

```bash
ORACLE_HOST=<host> ORACLE_USER=ubuntu ORACLE_KEY=<ssh-key> scripts/oracle_install_needrestart_guard.sh
CHECK_ONLY=true ORACLE_HOST=<host> ORACLE_USER=ubuntu ORACLE_KEY=<ssh-key> scripts/oracle_install_needrestart_guard.sh
```

The guard leaves security updates enabled but tells `needrestart` to defer only `auto-trader.service`. It does not restart the bot. Apply a deferred application restart later with `scripts/oracle_safe_restart.sh`, followed by a read-only launchpad check of state, positions, and open orders.

The currently running process must already support the maintenance marker. The script checks a runtime capability marker written by the active systemd `MainPID`; checking files on disk is not enough. If the script reports that the remote service does not support planned maintenance, it will refuse the normal path before syncing code. For the one-time first rollout of the maintenance feature in paper mode only, use the guarded bootstrap path:

```bash
BOOTSTRAP_HARD_KILL=true ORACLE_HOST=<host> ORACLE_USER=ubuntu ORACLE_KEY=<ssh-key> scripts/oracle_planned_deploy.sh
```

The bootstrap path requires the remote `.env` to contain `ALPACA_PAPER=true` and refuses `ALLOW_LIVE=true`. After that first rollout, future deploys should use the normal planned maintenance command above.

For live mode, the maintenance helper refuses preserve-position restarts unless `ALLOW_LIVE=true` is set explicitly. Use that only for a reviewed live deploy window where preserving positions through a fast restart is intentional. After any deploy, inspect `/status`; send `/resume <token>` only if the state is intentionally ready to trade.

When recovering a false safety pause, first verify broker positions, open orders, and local pending exits are mutually consistent. Persist `ACTIVE` only after that check and immediately before a planned-maintenance restart. Never add or rely on automatic resume behavior.

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

### Legacy Optional FRED Macro Context (Parked)

FRED support remains documented for historical operation and possible later experiments, but FRED-in-entry is parked in the simplified target. It is not ticker-specific market data and must not be treated as a standalone buy/sell signal.

```bash
FRED_API_KEY=...
```

FRED context is added to the `macro` lane in AI research packets through a cached daily Core Risk Pack: short/long Treasury yields, 10Y-2Y curve spread, high-yield credit spread, CPI, unemployment, VIX, and Fed balance sheet assets. Missing keys, API errors, and API-budget deferrals are recorded as macro context errors; they must not crash the supervisor or approve/block trades by themselves.

### Legacy AI Research Operations

The following commands describe capabilities that may still exist in the deployed pre-pivot runtime. Do not interpret them as proof that the simplified one-provider target has been deployed.

AI research is advisory only. It may return a pre-RiskEngine `approve`, `watch`, or `reject` recommendation, but it cannot authorize final trade execution, size orders, override risk gates, or place orders.

`AI_ENTRY_GATE_ENABLED=false` is the safe default and an explicit AI bypass: entries use the deterministic scanner/prefilter/RiskEngine path without an AI decision. The simplified one-provider target is active only when the gate is enabled. When enabled, real-provider AI research becomes a fail-closed entry filter before `RiskEngine`: only a valid `approve` can continue to RiskEngine and OrderManager. `watch`, `reject`, invalid output, budget exhaustion, provider setup/runtime failure, disabled AI research, or shadow/unavailable research blocks the entry and records an audit journal note. AI still cannot size orders, submit orders, override RiskEngine, bypass `/kill`, bypass `HALTED`, bypass broker/account blocks, or override account loss/drawdown halts.

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

### Friday HALTED Recovery Check

After the 2026-06-04 unmarked Oracle restart, the expected paper recovery posture is:

- persisted state remains `HALTED`;
- `M`, `SPCE`, and `TECS` may still be open before regular market open;
- accepted sell market orders should remain queued at the broker;
- no cancel, resume, or paid AI rehearsal should run until regular-hours flattening is verified.

Use the read-only Oracle recovery wrapper before and after market open:

```bash
ORACLE_HOST=<host> ORACLE_USER=ubuntu ORACLE_KEY=<ssh-key> scripts/oracle_friday_recovery_check.sh
```

The wrapper checks the Oracle service and runs the remote `scripts/friday_recovery_check.sh` as the `auto-trader` service user. The recovery check reads persisted state, paper/live mode, account status, market clock, positions, open broker orders, and pending exits. It does not submit orders, cancel orders, reconcile broker orders, resume the bot, or call paid AI providers.

Expected outputs:

- `WAITING_QUEUED_FLATTEN` means open positions still have queued close orders; keep waiting and do not resume.
- `WAITING_OPEN_ORDERS_CLEAR` means positions are flat but open orders remain; keep waiting and do not resume.
- `WAITING_MARKET_OPEN` means positions and orders are clear but regular market hours are not open; keep waiting and do not resume.
- `READY_TO_RESUME` means persisted state is `HALTED`, paper/account checks passed, positions are flat, open orders are gone, and no pending exits remain.
- `FAIL` means operator attention is required before resume.

Resume is allowed only after `READY_TO_RESUME`. After intentional resume and a clean `/status`, run AI readiness/rehearsal separately; do not enable `AI_ENTRY_GATE_ENABLED` until the no-order rehearsal path is clean.

The preflight performs no provider API calls. `READY` requires `AI_RESEARCH_ENABLED=true`, a real provider such as `anthropic`, an explicit model, the matching provider key present, a positive `AI_RESEARCH_MAX_CALLS_PER_DAY`, a working DB budget count, enough calls remaining for the UTC day, and a bounded timeout. It also prints the effective `AI entry gate enabled` state after runtime config is applied. It prints only `key_present=true/false`; never key values, prefixes, or lengths.

Before any paid provider call, the deterministic paid-AI prefilter can block obvious low-conviction candidates such as weak relative-volume moves, high-chase setups without catalyst/news, or additional leveraged/inverse exposure when the account already holds inverse/volatility positions. Prefilter blocks are audited as `ai_paid_prefilter/v0`, do not consume paid budget, and cannot approve trades. Disable only for a deliberate experiment:

```bash
AI_PAID_PREFILTER_ENABLED=false
```

Legacy risk profiles control how wide the currently implemented experiment funnel is. The simplified target will use explicit numeric settings instead:

- `conservative`: current live-readiness posture; 5% early notional cap, strict discovery and paid-AI prefilter thresholds.
- `aggressive`: paper-only wider opportunity search; 7.5% early notional cap, moderately looser discovery/prefilter thresholds.
- `risky`: paper-only experiment mode; 10% early notional cap, wider discovery/prefilter thresholds. If `ALPACA_PAPER=false`, `risky` normalizes back to `conservative`.

Entry capacity is an explicit operator setting, not a risk-profile side effect. `/config max_entries N` accepts a positive integer, records the old/new value in the journal, and is shown in `/config`, `/status`, and launchpad. Changing `risk_profile` does not clamp or mutate `max_new_positions_per_day`.

The passive AI candidate outcome ledger registers the first validated real single-provider decision for each provider/model/policy/symbol/market-session. It excludes shadow, prefilter, multi-provider aggregate, budget, failure, postmortem, and invalid rows. A bounded background resolver fetches completed Alpaca/IEX daily bars in batches and records:

- `D0`: the decision session close;
- `D1`: the next completed trading-session close;
- `D3`: the third completed trading-session close after D0;
- `D5`: the fifth completed trading-session close after D0.

The reference price is the price in the model packet, not an order fill. Returns and `$30 hypothetical P/L` are observation-only evidence for comparing approved versus watch/reject decisions. They never enter realized P/L, RiskEngine, sizing, order submission, exits, runtime config, or AI prompts. Missing bars remain pending and retry silently; resolver failures do not block or alert trading.

If `ALPACA_PAPER=false`, all experiment profiles normalize back to `conservative`. No risk profile bypasses `HALTED`, kill switch, broker/account blocks, daily/weekly loss halts, drawdown halts, duplicate-position guards, AI entry gate, or RiskEngine. Runtime changes:

```text
/config risk_profile conservative
/config risk_profile aggressive
/config risk_profile risky
/config max_entries <positive integer>
```

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

The following multi-provider configuration is retained for historical diagnosis only. The committee is parked and should not be reactivated as part of the simplified target:

```bash
AI_RESEARCH_PROVIDERS=anthropic,openai,xai
AI_RESEARCH_ANTHROPIC_MODEL=claude-opus-4-8
AI_RESEARCH_OPENAI_MODEL=<chosen-openai-model>
AI_RESEARCH_XAI_MODEL=<chosen-grok-model>
AI_RESEARCH_MAX_CALLS_PER_DAY=72
AI_HIGH_EXPOSURE_UNANIMOUS_THRESHOLD_PCT=60
```

One committee round consumes one chargeable call per selected real provider; with a 72-call daily budget, the three-provider committee gets 24 full candidate rounds. Preflight reports how many full rounds remain from the current daily budget. The v1 aggregate is deterministic: aggressive mode can approve with one valid provider approval at confidence >= 0.65 and no valid reject while projected gross exposure is at or below the high-exposure threshold. Above `AI_HIGH_EXPOSURE_UNANIMOUS_THRESHOLD_PCT`, every configured provider must return a valid `approve`; any watch, reject, invalid output, timeout, or provider failure blocks the AI advisory approval. RiskEngine remains the only execution and sizing authority.

The default `AI_RESEARCH_MAX_CALLS_PER_DAY=0` intentionally reports `NOT_READY`, even when a key is present.

### AI Cost Report Calibration

Run the read-only persisted-usage report locally:

```bash
scripts/ai_cost_report.sh --days 30
```

Or against Oracle:

```bash
scripts/oracle_ai_cost_report.sh --days 30
```

For `grok-latest`, the configured xAI rates verified on 2026-08-06 are `$1.25` per million regular input tokens, `$0.20` per million cached input tokens, and `$2.50` per million completion or reasoning tokens. Future provider responses persist the explicit cached and reasoning token buckets when supplied.

Older xAI memos stored only aggregate prompt, completion, and total tokens. Their cache split is reconstructed with `AI_RESEARCH_XAI_LEGACY_CACHED_INPUT_RATIO=0.06245255`, calibrated against the Jul 8-Aug 6 xAI dashboard snapshot: `$2.20`, 1,555,199 tokens, 1.2M regular prompt tokens, 79.8K cached prompt tokens, 65.9K completion tokens, and 207.8K reasoning tokens. The report labels these rows `legacy_estimated`; this is a historical estimate, not a provider invoice.

The report's `persisted_calls` count is the number of stored bot research decisions. It does not equal xAI's portal request counter because one research decision can make multiple HTTP requests or retries. Invalid cost-only settings fall back to defaults and cannot prevent the trading runtime from starting.

### Legacy Future Risk Profile Notes (Parked)

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

Optional stagnation exit:

- `POSITION_STAGNATION_EXIT_ENABLED=false` by default.
- When enabled, the supervisor can close dead-money positions after `POSITION_STAGNATION_MIN_HOLD_DAYS` if P/L remains between `POSITION_STAGNATION_MIN_PNL_PCT` and `POSITION_STAGNATION_MAX_PNL_PCT`, relative volume is at or below `POSITION_STAGNATION_MAX_REL_VOLUME`, and the daily range is at or below `POSITION_STAGNATION_MAX_DAILY_RANGE_PCT`.
- Stagnation evaluation only runs during regular market hours after the configured last-risk-sweep time, default `12:55` in `REPORT_TIMEZONE`, to avoid early-session volume/range false positives.
- The rule is deterministic and does not call AI.
- Missing, stale, or timestampless Alpaca snapshot, relative-volume, or daily-range data means hold; do not sell on incomplete stagnation evidence.

Before enabling auto-entry:

- Complete at least one paper burn-in session with stable Telegram visibility.
- Review latest risk settings and document any threshold changes in `docs/DECISIONS_LOG.md`.
- Run the visible Reviewer/Optimizer cycle for the promotion.
- Commit and push approved changes before unattended operation.

Before live money:

- Keep `SHUTDOWN_FLATTEN_ON_EXIT=true`.
- Keep Telegram allowlist populated.
- Start with minimal capital.
- Confirm active-sample evidence covers entries, exits, rejects, reconnects, `/kill`, restart, and reporting. Idle weeks do not count.
- Confirm positive net realized dollars after losses and attributable AI costs, positive dollar expectancy, acceptable profit factor/drawdown, and incremental AI-added dollars versus the deterministic baseline. Win rate alone is insufficient.
- Measure rejected candidates using observed prices over predefined comparable windows; do not invent fills or choose the horizon after seeing the outcome.
- Run the live cutover preflight from the intended runner:

```bash
scripts/live_preflight.sh --max-equity 500 --max-new-positions 3
```

- The preflight is read-only against Alpaca and the production DB. Its embedded halt drill and account-risk rehearsal use temp SQLite DBs plus fake broker adapters; they do not submit, cancel, or flatten real broker state.
- Default preflight should run while `ALPACA_PAPER=true` and should fail if there are open positions, open orders, pending exits, unsafe shutdown config, missing planned-deploy capability for the active systemd `MainPID`, failed account-risk rehearsal, or failed halt drill.
- The preflight also requires an explicit runtime `auto_entry_enabled` value so live cutover cannot depend on an accidental settings default.
- After a reviewed switch to `ALPACA_PAPER=false`, rerun only with `--allow-current-live`; use `--allow-open-positions` only for a reviewed in-position validation window.
- Get explicit live cutover approval.

### Week 2 Launchpad And Batch Rehearsal

Use the Week 2 launchpad when you need one read-only cockpit view before a market session or deploy decision:

```bash
scripts/week2_launchpad.sh
```

On Oracle:

```bash
ORACLE_HOST=<host> ORACLE_USER=ubuntu ORACLE_KEY=<ssh-key> scripts/oracle_week2_launchpad.sh
```

The launchpad reads service/broker state through the configured environment and prints bot state, account status, market clock, positions, open orders, pending exits, risk profile, runtime auto-entry, runtime AI gate, paid AI budget usage, resume eligibility, entry-pressure diagnostics, and the next expected bot behavior. Entry pressure is best-effort and read-only: it summarizes persisted candidates, prefilter blocks, AI watch/reject/invalid/approve counts, RiskEngine blocks, latest entry, capacity, and the likely blocker. It does not submit, cancel, reconcile, resume, or call paid AI providers.

For Sunday-night or premarket candidate learning without orders, run the AI rehearsal batch:

```bash
scripts/ai_rehearsal_batch.sh --limit 10
```

On Oracle:

```bash
ORACLE_HOST=<host> ORACLE_USER=ubuntu ORACLE_KEY=<ssh-key> scripts/oracle_ai_rehearsal_batch.sh --limit 10
```

Default batch mode is `SHADOW`, which generates live candidates, adds risk/Finnhub/FRED context when configured, runs the deterministic paid-AI prefilter, then uses the zero-cost shadow committee for candidates that pass. It never imports `OrderManager`, never calls `RiskEngine`, and never submits orders. Use paid mode only during an explicit provider-budget experiment:

```bash
scripts/ai_rehearsal_batch.sh --limit 5 --paid
```

Paid mode can consume real provider budget. Keep it off for broad Sunday batches unless the goal is specifically to test provider behavior.
## Edge Execution-Mode Safety

Every persisted order records `paper`, `live`, or `unknown`. Historical
backfill uses only explicit `(PAPER, ...)` or `(LIVE, ...)` entry-journal
evidence; unknown rows are never inferred from the current environment.

Use `/edge paper` or `/edge live` to isolate P/L. A proven entry supplies
the trade mode when its paired historical exit is unknown. If a report
window contains both proven paper and proven live trades, the unfiltered
scorecard is withheld and the report must be rerun with a mode filter.

## Decision Provenance

Each supervisor process creates a captured `runtime_session`. Every entry
decision then creates an immutable `decision_context` recording the effective
AI gate value and whether it came from an environment setting or runtime
override, AI research and simplified-runtime state, paper/live mode, risk
profile, provider/model/prompt identifiers, host/process session identity, and
a hash of a secret-safe configuration snapshot. Signals, AI memos, risk
decisions, and orders reference that context.

Configuration snapshots use an explicit non-secret allowlist. All other
configured values are stored as `<redacted>`, and the hash is computed from
that redacted snapshot. API keys, broker secrets, Telegram credentials, and
resume tokens must never be persisted in provenance rows.

Pre-provenance trading rows are attached once to a synthetic
`legacy_supervisor_entry` context with `inferred=1`. They remain visible in the
default report for historical continuity and can be isolated with
`/edge legacy`, but they are labeled `legacy inferred` and never count as
captured or proven provenance. Smoke tests, rehearsals, postmortems, broker
reconciliation, and other offline sources are tagged explicitly and excluded
from Edge trading-decision counts.
