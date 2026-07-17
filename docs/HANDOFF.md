# HANDOFF

Quick resume file for any AI session. Update at end of every work session.

## Current Snapshot

- Last updated UTC: 2026-06-03T16:43:07Z
- Last updated local (`America/Los_Angeles`): 2026-06-03 09:43:07 PDT
- Updated by: openai/gpt-5-codex
- Active role: Engineer
- Project phase: 
  - Kill + persistence foundation: **Clean APPROVED**
  - First paper order path implemented (real submit_order, OrderManager)
  - Dynamic stock discovery implemented and market-data parser fixed for Alpaca top-level snapshot payloads
  - First paper order submitted and filled into an open paper position
  - Reconciliation foundation and duplicate-entry protection implemented
  - Reviewer/Optimizer BLOCK findings addressed; re-review requested
  - Automatic visible Reviewer/Optimizer polling/fix/re-review workflow clarified in docs
  - GitHub sync rule added: approved major milestones and safety fixes should be committed and pushed automatically
  - Telegram `/status` and `/report` visibility implemented
  - Telegram visibility Reviewer/Optimizer BLOCK findings addressed; re-review returned `APPROVE WITH CHANGES`
  - Final pre-commit Telegram visibility changes applied; final Reviewer and Optimizer verdicts: **APPROVE**
  - Day 2 supervised trading loop implemented; final Reviewer and Optimizer verdicts: **APPROVE**
  - Supervised local shutdown mode implemented; final Reviewer and Optimizer verdicts: **APPROVE**
  - Automatic Reviewer/Optimizer cycle completed after major Engineer work
  - Final Reviewer verdict for tomorrow readiness: **APPROVE**
  - Day 3 entry hardening added: auto-entry now suppresses if Alpaca already reports an open buy/long order before candidate generation.
  - Account-level risk halt layer added: daily loss, weekly loss, and peak drawdown breaches now persist `HALTED` and call cancel/flatten through the existing kill-grade path.
  - Account-risk dry-run validator added for operator proof without touching broker cancel/flatten paths.
  - Oracle migration rehearsal promoted to active paper runner: local laptop bot stopped; Oracle systemd service is active and enabled.
  - Runtime config switch built locally: `/config auto_entry on|off` persists `auto_entry_enabled` in SQLite and supervisor reads it every tick.
- System status:
  - RiskEngine remains the **only** path to any real order.
  - Real order submission code is written and gated.
  - First paper trade is now possible in code (via `run_first_paper_trade_test()` helper) and guarded by system state, market clock, account status, live equity, dynamic discovery, RiskEngine, and OrderManager.
  - First market-hours attempt on 2026-06-02 initially submitted no orders because Alpaca paper account reported zero buying power. After user reset/funded the paper account, first order was submitted.
  - First paper order: `AMPX`, quantity `0.832986`, order id `eaf99d3e-c577-4b2d-8f4f-74cd74be4178`, RiskEngine trace `ed4e33f9`.
  - Broker verification after submission returned no open orders and an open `AMPX` position with quantity `0.832986`.
  - SQLite reconciliation now has the filled AMPX order persisted: status `filled`, avg fill price `24.134`.
  - Duplicate-protection verification reran the helper and submitted no second order; RiskEngine rejected with `Symbol already has an open position`, trace `7b41038c`.
  - RiskEngine now sizes first paper trades fractionally under the existing 5% early notional cap; cap was not weakened.
  - Fail-closed blocker fixes applied after Reviewer/Optimizer:
    - fresh DB defaults `HALTED`;
    - missing durable entry count rejects;
    - DB count failures raise;
    - reconciliation reports successful persisted rows only;
    - strict broker position read is required before pre-trade checks;
    - one-shot helper refuses before discovery when open-position or same-day entry limits are reached.
  - Latest verification: `13 passed`; live helper reconciled AMPX and refused early with `Open position limit already reached`, no second order submitted.
  - Reviewer re-review returned `APPROVE`.
  - Optimizer re-review returned `APPROVE WITH CHANGES`; Engineer addressed the non-blocking recommendation by pausing the running state machine if broker submit succeeds but local order persistence fails.
  - Latest verification after Optimizer recommendation fix: `14 passed`.
  - Final visible Reviewer verdict: `APPROVE`.
  - Final visible Optimizer verdict: `APPROVE`.
  - GitHub sync complete:
    - Commit `b30d690` (`Add reconciliation and duplicate trade guards`) pushed to `origin/main`.
  - Telegram visibility now reports:
    - live Alpaca equity/cash/buying power,
    - AMPX open position,
    - latest filled order,
    - reconciliation count,
    - durable same-day entry count,
    - whether new entries are blocked by open-position or daily-entry limits.
  - Latest Telegram preview was read-only and submitted no orders.
  - Telegram is now fail-closed with `TELEGRAM_ALLOWED_IDS`; local `.env` has one allowed private Telegram ID configured and remains ignored.
  - All Telegram handlers require authorization before data reads or controls.
  - Telegram app enables concurrent update processing so `/kill` is not queued behind status/report broker reads.
  - Telegram user-ID allowlist only authorizes private chats; group chats must be explicitly allowlisted by chat ID.
  - Status/report snapshots are bounded and surface account/clock/reconciliation warnings, including adapter returned error dictionaries.
  - Latest verification: `25 passed`; compileall passed.
  - Final visible Reviewer verdict for Telegram visibility: `APPROVE`.
  - Final visible Optimizer verdict for Telegram visibility: `APPROVE`.
  - GitHub sync complete:
    - Commit `89c2af8` (`Add authorized Telegram visibility reports`) pushed to `origin/main`.
  - Day 2 supervisor loop now implemented in code:
    - periodic broker order reconciliation,
    - position monitoring,
    - Telegram supervisor alerts,
    - HALTED-with-open-position warning for kill/flatten validation,
    - optional auto-entry via `RiskEngine -> OrderManager`,
    - optional auto-exit close-position execution,
    - default alert-only mode with `AUTO_ENTRY_ENABLED=false` and `AUTO_EXIT_ENABLED=false`.
  - Supervisor blocker fixes applied after Reviewer/Optimizer:
    - duplicate auto-exit close submissions suppressed per symbol;
    - pending exits survive transient position snapshot failures and only clear after a successful snapshot proves the symbol is gone;
    - Alpaca close quantities are capped to broker-reported position size;
    - supervisor auto-exit is suppressed while `HALTED`;
    - shutdown emergency halt covers both `ACTIVE` and `PAUSED`;
    - supervisor interval/timeout settings have Pydantic bounds to prevent hot loops.
  - Latest verification for supervisor implementation: `33 passed`; compileall passed; direct `auto_trader.__main__.main` import check passed.
  - Final visible Reviewer verdict for Day 2 supervisor loop: `APPROVE`.
  - Final visible Optimizer verdict for Day 2 supervisor loop: `APPROVE`.
  - GitHub sync complete:
    - Commit `9fe40e1` (`Add Day 2 trading supervisor loop`) pushed to `origin/main`.
  - Local supervised shutdown mode now exists:
    - `SHUTDOWN_FLATTEN_ON_EXIT=true` remains production/live safety default.
    - `SHUTDOWN_FLATTEN_ON_EXIT=false` is allowed only in paper mode and prevents Ctrl+C/SIGTERM process exit from flattening during supervised local tests.
    - Settings fail fast if `ALPACA_PAPER=false` and shutdown flattening is disabled.
    - `/kill` remains independent and still halts/flattens.
  - Local ignored `.env` is currently set to `SHUTDOWN_FLATTEN_ON_EXIT=false` for the next supervised laptop run; do not commit `.env`.
  - Latest verification for shutdown mode: `39 passed`; compileall passed; `git diff --check` clean.
  - Final visible Reviewer verdict for shutdown mode: `APPROVE`.
  - Final visible Optimizer verdict for shutdown mode: `APPROVE`.
  - Local alert-only bot run started successfully on 2026-06-02:
    - Alpaca paper health OK.
    - Telegram polling active.
    - Supervisor running with `AUTO_ENTRY_ENABLED=false` and `AUTO_EXIT_ENABLED=false`.
    - Telegram `/status` and `/report` were verified by the user.
    - AMPX paper position is being monitored; no new orders are being placed by the supervisor.
  - Repeatable run/service package added:
    - `scripts/run_bot.sh` points `AUTO_TRADER_ENV_FILE` at `.env` and execs `python -m auto_trader` without sourcing secrets as shell.
    - `deploy/systemd/auto-trader.service.example` documents Oracle/Pi service shape with `Restart=always`, restrictive `UMask=0077`, host-visible `/tmp` health checks, and Pydantic-only `.env` parsing via `AUTO_TRADER_ENV_FILE`.
    - `docs/RUNBOOK.md` documents laptop burn-in, server operation, Telegram checks, and promotion gates.
  - Repeatable run/service package final Reviewer verdict: `APPROVE`.
  - Repeatable run/service package final Optimizer verdict: `APPROVE`.
  - Latest verification for run/service package: `39 passed`; compileall passed; `bash -n scripts/run_bot.sh` passed; `git diff --check` clean.
  - Auto-exit close-path hardening implemented; final Reviewer and Optimizer verdicts: **APPROVE**:
    - broker open close-order check before any supervisor close submission;
    - durable `pending_exits` table so submitted/pending closes survive process restart;
    - pending exits clear only after a trusted broker position snapshot proves the symbol is gone;
    - close submissions persist both the order record and pending-exit marker;
    - pending markers for canceled/rejected/expired close orders clear during broker reconciliation so the supervisor can retry;
    - unresolved persisted pending exits with no matching broker/client close-order ID now pause and alert for operator review;
    - broker-open-close persistence failures now explicitly alert and pause the state machine;
    - persisted pending exits are covered by a regression proving cleanup after a trusted empty position snapshot;
    - supervisor pauses if a broker close exists/submits but local pending-exit persistence fails;
    - single-position close submission is intentionally not retry-wrapped so response-path failures cannot duplicate a broker close inside the adapter.
  - Latest verification for close-path hardening: `48 passed`; compileall passed; `git diff --check` clean.
  - Local supervised paper mode now has `AUTO_EXIT_ENABLED=true` and `AUTO_ENTRY_ENABLED=false`.
  - AMPX auto-exit was submitted while the market was closed:
    - close order id `d08fb2a8-7df4-4da5-b3b5-d4c939be1fde`;
    - side `sell`, quantity `0.832986`;
    - status `accepted`;
    - pending-exit marker persisted so duplicate AMPX exits are suppressed.
  - Telegram `/status` and `/report` now include pending exits, matched broker/order status, duplicate-exit suppression language, and latest journal entries.
  - Supervisor now appends a lightweight journal entry when an auto-exit is submitted.
  - Latest verification for pending-exit visibility and journaling: `49 passed`; compileall passed; `git diff --check` clean.
  - Normal pending-close suppression is now log-only so Telegram does not repeat expected AMPX pending-close notices.
  - Startup now enforces a single local bot process per SQLite DB with a `/tmp/auto_trader_*.lock`.
  - Duplicate startup was verified to fail before Telegram polling with a clear fatal message naming the existing lock holder.
  - Day 3 validation command added:
    - `scripts/day3_validate.sh --symbol AMPX`;
    - verifies paper mode, auto-entry off, auto-exit on, broker account tradability, market state, broker reconciliation, AMPX position status, close order visibility, duplicate close count, and pending-exit marker state;
    - exits `0` for PASS/WARN and `2` for hard FAIL gates.
  - Latest Day 3 validation result:
    - initial market-open validation found AMPX close filled and position gone, but failed because the stale pending-exit marker remained;
    - supervisor reconciliation now clears matched filled pending exits as completed, appends a journal entry, and sends one Telegram completion alert;
    - final validation is `Overall: PASS`;
    - broker reconciliation found 2 orders;
    - one filled AMPX close order is visible;
    - AMPX position is gone;
    - duplicate close count passed;
    - pending-exit marker is clear.
  - Oracle VM stance:
    - Oracle can be prepared tonight;
    - do not migrate the active bot until the Day 3 AMPX close lifecycle validates;
    - keep exactly one active bot host polling Telegram and trading the Alpaca account because the local single-instance lock does not coordinate across machines.
  - Day 3 one-entry promotion is active:
    - local ignored `.env` was temporarily set to `AUTO_ENTRY_ENABLED=true`, with `AUTO_EXIT_ENABLED=true`, `MAX_NEW_POSITIONS_PER_DAY=1`, and `ALPACA_PAPER=true`;
    - local bot restarted with `auto_entry=True, auto_exit=True`;
    - supervisor selected `POET`;
    - RiskEngine approved trace `c47fe60c`;
    - paper order id `a6168fe9-b518-4d08-b9b1-106308138c6c`, client id `1e27fb4b-afcd-40ef-a064-29b9658ed929`, quantity `1.36532`;
    - next monitor saw open POET position quantity `1.365320`, value about `$19.87`, unrealized P/L about `$-0.09`.
    - after the one allowed entry opened, local ignored `.env` was set back to `AUTO_ENTRY_ENABLED=false`;
    - local bot restarted with `auto_entry=False, auto_exit=True`;
    - latest monitor saw POET still open, quantity `1.365320`, value about `$20.24`, unrealized P/L about `$0.28`.
    - POET later auto-exited by trailing stop:
      - reason `position trailing stop reached`;
      - trailing drawdown about `-6.06%`;
      - close order id `8d620fa9-a1f6-448d-ac1e-e64a87c32f58`;
      - buy filled at `$14.62`;
      - sell filled at `$14.10`;
      - position is gone and pending exits are clear.
  - Quiet API budget guard added:
    - Alpaca calls are counted by endpoint in a rolling 60-second window;
    - normal behavior is log-only, not Telegram reporting;
    - safety-critical calls are counted but not blocked;
    - nonessential discovery/snapshot scanning is deferred first if the budget becomes hot;
    - latest verification: `57 passed`, compileall passed, `git diff --check` clean;
    - local bot restarted onto this code with `auto_entry=False`, `auto_exit=True`.
  - Oracle VM prep completed to safe staging point:
    - SSH works with user `ubuntu`;
    - VM is Ubuntu 24.04.4 LTS with Python 3.12, about 45GB disk, about 1GB RAM;
    - source tree copied to `/opt/auto-trader` without `.env`, `.git`, local DB, venv, key files, or caches;
    - dedicated `auto-trader` system user created;
    - virtualenv created and project installed;
    - private server `.env` installed as `auto-trader:auto-trader` with mode `600`;
    - server-safe flags set: `ALPACA_PAPER=true`, `AUTO_ENTRY_ENABLED=false`, `AUTO_EXIT_ENABLED=true`, `SHUTDOWN_FLATTEN_ON_EXIT=true`, `MAX_NEW_POSITIONS_PER_DAY=1`;
    - systemd service installed at `/etc/systemd/system/auto-trader.service`;
    - service is `disabled` and `inactive`;
    - read-only POET validation on Oracle passed without starting Telegram polling or supervisor.
  - Entry duplicate protection now checks open broker entry orders before signal generation:
    - protects restart/migration timing where an accepted buy exists but no position has appeared yet;
    - latest verification: `58 passed`.
  - Account risk halt enforcement now runs every supervisor tick after a connected account snapshot:
    - durable account-risk state tracks daily start equity, weekly start equity, and peak equity;
    - breaches of `DAILY_LOSS_HALT_PCT`, `WEEKLY_LOSS_HALT_PCT`, or `PEAK_DRAWDOWN_HALT_PCT` notify Telegram once, append a journal entry, persist `HALTED`, cancel open orders, and flatten positions;
    - latest verification: `60 passed`, compileall passed, `git diff --check` clean.
  - Account-risk validation command:
    - `scripts/account_risk_validate.sh --base-equity 400`;
    - dry-runs healthy, daily-loss-breach, and peak-drawdown-breach scenarios using configured thresholds;
    - does not call Alpaca cancel/flatten;
    - latest verification: `63 passed`, compileall passed, `git diff --check` clean.
  - Active hosting state:
    - laptop runner stopped; no local `python -m auto_trader` process remains;
    - latest code and SQLite DB were synced to `/opt/auto-trader`;
    - Oracle service `auto-trader` is `active` and `enabled`;
    - Oracle startup health passed against Alpaca paper;
    - Telegram polling is live from Oracle;
    - supervisor is running `auto_entry=False`, `auto_exit=True`, `monitor_interval=60s`;
    - repeated Oracle ticks updated account-risk state with equity about `$398.10` and no loss/drawdown breach.
  - Runtime config switch:
    - new table: `runtime_config`;
    - Telegram command: `/config`, `/config auto_entry on`, `/config auto_entry off`;
    - `/status` now shows `Runtime auto-entry`;
    - supervisor uses runtime `auto_entry_enabled` override without requiring future service restarts;
    - latest local verification: `68 passed`, compileall passed, `git diff --check` clean.
  - Deployment note:
    - Oracle is still running the prior active code until this commit is synced/restarted;
    - restarting Oracle service with `SHUTDOWN_FLATTEN_ON_EXIT=true` is expected to persist `HALTED`;
    - after deploy restart, operator should send `/resume <token>` once state is reviewed, then `/config auto_entry on` can promote entries without another restart.
  - Discovery now pulls Alpaca active/tradable/fractionable US equities and free IEX snapshots, then ranks by liquidity, spread, relative volume, constructive momentum, and non-parabolic behavior.
  - `.env` now exists with Alpaca paper keys, Telegram bot token, and generated RESUME_TOKEN. Do not print or commit secrets.
  - Safe preflight passed: Alpaca paper account connected, Telegram bot token valid, dynamic tradable universe fetch works.
  - Market-data discovery now successfully scans snapshots and found candidates during market hours.
  - AI committee design is documented but not active for the first paper trade. It will start later in journal-only mode.
  - GitHub is connected and pushed: `origin` -> `https://github.com/daoster408/auto-trader` (private repo). Local `main` tracks `origin/main`. `.env` is ignored and was not committed.
  - Workflow automation policy active: Major Engineer work triggers automatic Reviewer/Optimizer launches.

## Locked Requirements

- US equities only
- Swing strategy, flexible hold period
- Weekend holds allowed
- Cash account
- Fractional shares for `fractionable=true` assets
- Paper trading starts now; initial paper trades in week 1
- Live trading by end of month 1
- On halt/kill: flatten all positions
- Telegram-first controls and reporting
- Hosting goal: free or near-free (Oracle-first preference)

## Immediate Next Actions

1. Keep Oracle as the single active paper runner; do not restart the laptop bot unless deliberately migrating back.
2. Confirm Telegram `/status` from Oracle shows no open positions, `Today new entries: 1 / 1`, and pending exits clear.
3. Deploy the runtime config switch to Oracle, then resume after the intentional restart HALT.
4. Use `/config auto_entry on` for the next paper entry promotion after `/status` is clean.
5. After the rules-only paper loop is proven: implement AI committee in journal-only mode using `docs/ARCHITECTURE.md` section `9.1`.
6. Maintain automatic visible Reviewer/Optimizer polling/fix/re-review loop and automatic GitHub push for future major milestones.

## Risks To Watch

- Over-accelerating without risk gates can cause unsafe live behavior. (Mitigated: ARCHITECTURE makes RiskEngine the ONLY order path.)
- Free-tier infrastructure may have provisioning limits. (Python slim chosen; watch memory on ARM.)
- Fractional support must be validated per asset before order placement. (Enforced in RiskEngine + AlpacaAdapter.)
- Skipping role cycle or architecture will violate contract and user trust.

## Required Files To Read First

- `docs/SOURCE_OF_TRUTH.md`
- `docs/OPERATING_RULES.md`
- `docs/DAILY_PLAN.md`
- `docs/DECISIONS_LOG.md`
- `docs/MODEL_REGISTRY.md`
- `docs/AGENT_WORKFLOW.md`
- `docs/ARCHITECTURE.md` (NEW - binding design for all implementation)

## 2026-06-01 Optimizer Session (xai/grok-build-0.1)
- Role: Optimizer (post-Reviewer BLOCK)
- Changes:
  - Fixed syntax error in risk_engine.py (from __future__ must be first).
  - Created utils/logging.py (structlog, UTC ISO, model_tag injection, trace context).
  - Created utils/retry.py (tenacity wrappers: retry_external + retry_kill_critical with jitter).
  - Created persistence/db.py + wired to __init__ (aiosqlite, load/save system_state for HALTED durability across restarts).
  - Hardened state_machine.py: async-aware halt/persist, supports async flatten, fire-and-forget safe.
  - Hardened telegram_bot.py: _kill_handler now 100% async + @retry_kill_critical on real adapter calls. No run_until_complete (eliminates deadlock). Added shutdown() + stop_event support.
  - Hardened alpaca_adapter.py: real alpaca-py SDK (lazy client) for health_check, get_account, get_clock, cancel_all_orders, flatten_all_positions. All wrapped in tenacity. submit_order remains blocked. /kill now does real cancel+flatten.
  - Hardened __main__.py: proper graceful shutdown with integrated stop_event + bot.shutdown(), modern signal handlers, DB init + state restore on startup, persist_hook wired, basic /tmp health file observability, structured logging at entry.
  - Updated all modules to use new loggers (no weakening of any gates).
- /kill now: highest priority, async end-to-end, retried 3x on transient, real broker calls, HALTED persisted immediately to SQLite before/after flatten.
- No risk thresholds, no kill logic, no safety gates touched or weakened.
- Performance: lazy init (fast ARM startup), to_thread for sync SDK, minimal polling overhead, structured logs cheap.
- Evidence: syntax fixed, code paths reviewed for deadlock-free kill, persistence for HALTED added.
- Next: Reviewer re-review or Engineer to wire OrderManager + first paper order (now safe on hardened base). Then full 4-role cycle.

(End of file - total ~80 lines, append-only)

## 2026-07-16 Simplification Pivot Handoff

- Role: Architect/Engineer
- Session AI/model: openai/gpt-5
- Timestamp: 2026-07-17T05:30:41Z / 2026-07-16 22:30:41 PDT
- Status: Documentation target approved by Reviewer and Optimizer; runtime implementation not started.
- Target flow: scanner -> deterministic prefilter -> one configured real AI decision -> RiskEngine -> OrderManager -> deterministic exits.
- Keep: Oracle/Alpaca, state persistence, RiskEngine-only order authority, `/kill`, `HALTED`, broker/account blocks, duplicate-order protection, reconciliation, deterministic exits, audit journal, and concise Telegram operations.
- Park: multi-provider voting, Gemini/DeepSeek/Fable escalation, FRED-in-entry, postmortem bias injection, and profile-label-driven target configuration.
- Primary evidence: net realized dollars after losses and attributable AI costs, dollar expectancy/trade, average dollar win/loss, profit factor, drawdown, and incremental AI-added dollars versus the deterministic baseline.
- Secondary evidence: win rate. A high win rate with negative net dollars is failure.
- Counterfactual rule: measure rejected candidates from observed prices over predefined comparable windows; do not invent fills or choose horizons after seeing results.
- Live gate: requires an active sample of closed trades and comparable rejected candidates. Three inactive weeks do not count.
- Runtime warning: do not assume Oracle is already in one-provider mode. Audit service, state, positions, orders, runtime config, provider config, and deployed commit before any mutation or restart.
- Next implementation owner: Engineer, after a fresh intended-delta review with the standing Reviewer and Optimizer.

## 2026-07-17 Simplified Runtime Implementation Handoff

- Role: Architect/Engineer
- Session AI/model: openai/gpt-5
- Timestamp: 2026-07-17T15:18:22Z / 2026-07-17 08:18:22 PDT
- Status: Simplified runtime implementation completed, tested, approved by Reviewer and Optimizer, and committed locally as `c60003e`; Oracle deployment has not occurred.
- Implemented: one configured AI provider in simplified gate-on mode, no entry-memory/FRED loading, explicit discovery/prefilter/notional controls, and dollar-first edge reporting with estimated AI cost.
- Gate-off behavior: explicitly bypasses all AI research and uses the deterministic prefilter -> RiskEngine -> OrderManager path.
- Fail-closed behavior: invalid provider/key/model preserves service liveness but blocks AI-gated entries as `ai_research_provider_setup_failed`; it cannot fall back to shadow.
- Oracle read-only audit: service active; bot `HALTED`; paper account connected and flat; no open orders or pending exits; no resume, config mutation, restart, or paid AI call performed.
- Evidence: `377 passed`; Ruff passed; compileall passed; `git diff --check` passed; Reviewer `APPROVE`; Optimizer `APPROVE` after required fixes.
- Blocker: local `main` is ahead of `origin/main`, but HTTPS push cannot authenticate and GitHub SSH authentication is unavailable. Do not deploy until the reviewed commits are pushed or the source-sync policy is explicitly changed.
