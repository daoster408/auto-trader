# ARCHITECTURE

**Project**: AUTO-TRADER  
**Version**: v1 simplification target (preserves v0 history below)
**Date**: 2026-07-16
**Role**: Architect/Engineer
**Session AI/model**: openai/gpt-5
**Status**: Documentation target only. The Oracle runtime has not yet been changed to this design.

---

## Current Pivot

The active target is intentionally small:

```text
scanner -> deterministic prefilter -> one configured real AI decision -> RiskEngine -> OrderManager -> deterministic exits
```

Keep the broker adapter, Oracle runner, state machine, RiskEngine, kill/halt behavior, duplicate-order protection, reconciliation, deterministic exits, persistence, and audit journal.

Park multi-provider committee voting, Gemini/DeepSeek/Fable escalation, FRED-in-entry, and postmortem bias injection. They remain in history and may remain in code until the implementation pass, but they are not part of the target runtime path.

The primary optimization objective is net dollars after losses and attributable AI cost, with drawdown constrained by explicit risk limits. Win rate is diagnostic only.

## 1. Executive Summary

Fully automated swing-trading bot for US equities on Alpaca (paper → live $100-$400 ramp).  
Primary controls and reporting via Telegram.  
Zero separate UI/dashboard for v1.  
Strict risk engine is the only path to order submission.  
`/kill` is non-bypassable and always flattens + halts.  
Single-process Python application optimized for free/near-free Oracle ARM VPS hosting.  
Provider-agnostic single-provider AI research layer with fail-closed validation.
All actions, signals, risk decisions, and outcomes fully audited (append-only, UTC canonical).

**Key Guarantees (Non-Negotiable)**:
- No order ever submitted without passing RiskEngine.
- On any HALTED transition or `/kill`: cancel all + flatten all + incident report.
- System state persisted and reconciled on startup.
- Time: storage=UTC ISO8601, display=America/Los_Angeles, scheduler uses Alpaca market clock.

---

## 2. High-Level Architecture

```
Telegram (commands + alerts)
          ↕
   [Comms Layer]
          ↕
   [Core Orchestrator / Scheduler]
          ↕
   [Signal Engine]  <->  [One Configured LLM Provider]
          ↕
   [Risk Engine]  (the gate)
          ↕
   [Order Manager]  ←→  [Alpaca Broker Adapter]
          ↕
   [Persistence (SQLite)] + [Journal Service]
```

Single long-running async process.  
Market-aware scheduling via Alpaca `/clock`.  
Background reconciliation loop for order/position state.  
Fast-path for kill (signal handler + command priority queue).

---

## 3. Technology Stack (v1)

| Layer            | Choice                          | Rationale |
|------------------|---------------------------------|-----------|
| Language         | Python 3.12+                    | Mature Alpaca SDK, rich data libs, low cognitive overhead for rapid safe iteration |
| Async Runtime    | asyncio + anyio                 | Non-blocking I/O for Telegram + broker polling |
| Broker SDK       | alpaca-py (official)            | First-class support, types, paper/live parity |
| Telegram         | python-telegram-bot (v20+ async)| Mature, easy command routing, webhook/polling |
| Config/Validation| pydantic-settings + pydantic    | Type-safe env, runtime validation |
| DB               | aiosqlite (SQLite)              | Zero-ops, atomic, sufficient for single writer, easy backup |
| Scheduler        | APScheduler (async)             | Cron + interval jobs, timezone aware |
| Retries/Backoff  | tenacity                        | Declarative, jitter, circuit-breaker friendly |
| Logging          | structlog (JSON) + std logging  | Structured, contextual, UTC forced |
| Testing          | pytest + pytest-asyncio, respx (http mock), freezegun | Fast unit + integration |
| Linting          | ruff + mypy (strict)            | Fast, modern |
| Packaging        | uv or pip-tools                 | Reproducible |
| Container        | python:3.12-slim Dockerfile     | ~80MB image, Oracle ARM compatible |
| Hosting          | Oracle Always Free (ARM) or cheap VPS | Docker + systemd; fallback: plain venv + nohup initially |

**Why not Node/TS?** Python wins for future quant features (TA-Lib, pandas, backtesting parity) and Alpaca ecosystem. Memory acceptable on 1-2GB free tier when using slim base + no heavy DS libs in hot path.

**Why not Go?** Slower iteration for v1; Python sufficient and safer for financial logic readability.

---

## 4. Module & Package Structure

```
auto_trader/
├── __main__.py                 # entrypoint, lifespan, signal handlers
├── cli.py                      # optional one-off commands (reconcile, journal)
├── config/
│   ├── __init__.py
│   └── settings.py             # Pydantic BaseSettings, risk params, tokens, env validation
├── core/
│   ├── __init__.py
│   ├── state_machine.py        # ACTIVE | PAUSED | HALTED + transitions + persistence
│   ├── risk_engine.py          # THE gate. Pure-ish validation + sizing. All checks logged.
│   └── models.py               # domain dataclasses / TypedDicts (TradeIntent, RiskDecision, etc.)
├── broker/
│   ├── __init__.py
│   ├── alpaca_adapter.py       # async wrapper over alpaca-py (orders, positions, clock, assets, account)
│   └── models.py               # broker DTOs
├── comms/
│   ├── __init__.py
│   └── telegram_bot.py         # command handlers, notifications, resume auth, rate-limit aware
├── intelligence/
│   ├── __init__.py
│   ├── signal_engine.py        # orchestrates rules + LLM calls, de-dupes
│   ├── llm_client.py           # ABC + concrete impls (OpenAI, Anthropic, XAI)
│   ├── prompt_templates.py
│   └── rules_fallback.py       # deterministic bootstrap signals (momentum, breakout, etc.)
├── execution/
│   ├── __init__.py
│   ├── order_manager.py        # submit, client_order_id idempotency, lifecycle tracking, reconciliation
│   └── position_sizer.py       # risk-based sizing (0.5% equity, exposure caps)
├── journal/
│   ├── __init__.py
│   ├── daily_journal.py        # EOD rationale + PnL + risk snapshot
│   └── weekly_summary.py
├── persistence/
│   ├── __init__.py
│   ├── db.py                   # connection pool, migrations (simple .sql files or pragma)
│   ├── repositories.py         # trades, risk_checks, state, events
│   └── schema.sql
├── scheduler/
│   ├── __init__.py
│   └── daily_cycle.py          # pre-market, scan windows, EOD, overnight monitoring
├── utils/
│   ├── __init__.py
│   ├── time.py                 # tz helpers (UTC <-> LA), market calendar helpers
│   ├── logging.py              # structlog config, forced UTC, model tag injection
│   └── exceptions.py
├── risk/                       # (kept minimal; risk_engine owns most)
└── tests/                      # mirrors package layout
```

**Import rule**: Core and risk never import from broker/comms. Depend on ports (interfaces).

---

## 5. Data Flow (Detailed)

1. **Startup**
   - Load config + validate secrets
   - Init DB + run schema if needed
   - Reconcile: fetch open orders + positions from Alpaca → update local state
   - Restore system_state from DB (or default ACTIVE)
   - Start Telegram polling + scheduler jobs

2. **Daily Cycle (market day)**
   - Pre-open (via clock API): refresh universe (assets API + filters)
   - Signal window(s): SignalEngine.run(universe) → list[SignalCandidate]
   - For each candidate that passes the deterministic prefilter:
     - One configured real AI provider returns a validated `approve`, `watch`, or `reject` research decision.
     - Only a valid AI `approve` continues; failure, timeout, invalid output, `watch`, or `reject` fails closed before RiskEngine.
     - RiskEngine.evaluate(candidate, snapshot) → RiskDecision (approved/rejected + sized qty + reason)
     - If approved: OrderManager.submit(approved_order)
     - Log everything with model tag if AI involved
   - Background: order update poller (every 30-60s) → DB + Telegram fill alerts
   - Continuous risk monitor: equity snapshots → auto-halt on breach (daily loss, drawdown, consecutive SLs)
   - EOD (after close or 16:30 PT): JournalService.generate_daily() → persist + Telegram /report

3. **Telegram Commands (anytime)**
   - `/status` → current state, equity, open positions, risk metrics, last journal ref
   - `/pause` → state=PAUSED (no new entries)
   - `/resume <token>` → state=ACTIVE (token matches env)
   - `/kill` → **immediate** cancel_all + flatten_all (market + limit) + state=HALTED + full incident report
   - `/report` → latest daily or on-demand journal

4. **Halt / Kill Path** (highest priority)
   - Any trigger (manual, threshold, error storm) → atomic state write + broker flatten + broadcast alert
   - `/kill` uses dedicated handler that can preempt other coroutines via cancellation tokens or queue priority.

5. **Reconciliation & Recovery**
   - On every startup and periodically: match local trades vs broker reality
   - Orphan orders → cancel or adopt
   - Use client_order_id for idempotency on retries

---

## 6. Risk Engine (Core Safety Module)

**Location**: `core/risk_engine.py`

Responsibilities:
- Pre-trade validation (only gate for OrderManager)
- Position sizing (0.5% equity risk budget)
- Real-time exposure & loss monitoring
- Halt decisioning

**Inputs to evaluate()**:
- Proposed: symbol, side (long only), entry_price (or market), stop_price (optional for v1), rationale
- Current: account equity, cash, open positions (with unrealized), daily/weekly realized PnL, peak_equity, consecutive_losses

**Checks (v1 defaults from SOURCE_OF_TRUTH)**:
1. System state == ACTIVE
2. Per-trade risk <= 0.5% equity (using proposed stop distance or conservative default 5-8% stop assumption for swing)
3. Max new positions today <= 1 (initial)
4. Post-trade gross exposure <= 25% equity
5. Daily realized + unrealized loss from open <= -1.75%
6. Weekly <= -4.0%
7. Peak drawdown from high-water <= -6.0%
8. Consecutive stop-losses <= 2
9. No duplicate symbol open position
10. Asset is fractionable (if using fractional)
11. Fresh market data (< 5min old quote)

**Outputs**:
- `RiskDecision(approved: bool, reason: str, sized_quantity: float | None, risk_metrics: dict)`

**Logging**: Every call persisted with full input snapshot + decision + UTC + model (if any).

**Halt triggers** also call `state_machine.transition(HALTED, reason)` + flatten flow.

**Sizing formula (v1)**:
```python
risk_dollars = equity * 0.005
if stop_distance_pct:
    qty = risk_dollars / (entry * stop_distance_pct)
else:
    qty = min( (equity * 0.02), risk_dollars / (entry * 0.06) )  # conservative 6% default risk
qty = round_for_fractionable(qty)
```

---

## 7. State Machine

States: `ACTIVE` (full), `PAUSED` (monitor only, no entries), `HALTED` (flat, manual resume only).

Transitions:
- Any → HALTED on kill or auto-breach
- PAUSED → ACTIVE only via `/resume <token>`
- HALTED → ACTIVE only via `/resume <token>` (after manual review)

Persistence: single row `system_state` table with `state`, `halted_at`, `reason`, `resumed_at`, `last_equity`.

---

## 8. Persistence Schema (v1)

See `persistence/schema.sql` (to be created by Engineer).

Core tables:
- `system_state`
- `risk_decisions` (append-only)
- `signals` (raw + rationale)
- `orders` (client_order_id PK, broker_order_id, status history)
- `positions_history` (daily snapshots)
- `journal_entries` (daily + weekly)
- `audit_log` (generic for everything else)

All timestamps stored as TEXT ISO8601 UTC.

---

## 9. AI / Signal Layer

**Abstraction** (`intelligence/llm_client.py`):
```python
class LLMClient(ABC):
    async def generate_daily_thesis(self, context: dict) -> Thesis: ...
    async def rank_universe(self, symbols: list[str], features: dict) -> list[Signal]: ...
```

**v1 bootstrap**: `rules_fallback.py` provides 1-3 high-conviction names per day using dynamic discovery and simple rules. No LLM cost until the paper-trade loop is proven.

**Prompt discipline**: Small context, structured output (JSON), temperature low, model tag always recorded.

**Cost guard**: max 1-2 LLM calls per scan cycle initially.

### 9.1 Simplified Single-Provider Entry Decision

Required target flow:

```text
Dynamic scanner -> deterministic prefilter -> verified data packet -> one configured real AI provider -> output validator -> RiskEngine -> OrderManager -> Alpaca
```

Rules:

- The AI receives only pre-fetched, timestamped, source-labeled facts.
- The AI returns one structured research decision: `approve`, `watch`, or `reject`, with confidence and concise rationale.
- Any provider timeout, transport error, invalid structure, unsupported fact, `watch`, or `reject` blocks the candidate before RiskEngine.
- A valid AI `approve` is permission to ask RiskEngine, not permission to trade.
- RiskEngine alone decides approval, quantity, exposure, and whether an order can proceed.
- The provider, model, prompt version, input hash, validation result, cost, trace ID, and final trade outcome are audited.
- One runtime provider is configured at a time. Provider choice is based on valid-response reliability and incremental dollar edge, not brand or raw win rate.

### 9.2 Historical Multi-Provider Committee (Parked)

The committee design below records the previous experiment. It is parked and is not the active target or currently authorized implementation direction.

Required flow:

```
Dynamic scanner -> verified data packet -> AI committee -> output validator -> RiskEngine -> OrderManager -> Alpaca
```

Committee roles:

- Bull Analyst: argues why a candidate could work using only the verified data packet.
- Bear / Risk Analyst: argues why the trade should be rejected or watched only.
- Judge / Portfolio Manager: reads scanner data + Bull + Bear outputs and produces the final AI-side recommendation.
- Optional future News/Sentiment Analyst: can review verified news/sentiment packets only after a separate source adapter exists.

Hard rules:

- AI never supplies source-of-truth market facts such as price, volume, spread, market cap, earnings date, account state, or order size.
- AI receives only pre-fetched, timestamped, source-labeled data packets from broker/data adapters.
- AI output must be structured JSON and must include `used_only_provided_data=true`.
- If AI mentions numeric facts that conflict with the verified data packet, the response is rejected and logged.
- AI cannot submit orders, choose final quantity, bypass RiskEngine, override `/kill`, override `HALTED`, override stale-data blocks, or override loss/exposure limits.
- AI committee output is logged with provider/model tag, prompt version, input data hash, validation result, and trace ID.

Implementation phases:

1. Journal-only: AI reviews candidates and writes rationale, but does not affect trades.
2. Ranking influence: AI can reorder scanner-approved candidates, still RiskEngine-gated.
3. Veto authority: AI can reject candidates, but cannot force trades.
4. Approval required: AI approval becomes required before trade, but RiskEngine remains the final execution gate.

Historical policy: these phases describe the prior committee experiment and do not authorize reactivation without an explicit decision and a new Reviewer/Optimizer cycle.

---

## 10. Journaling & Reporting

- **Daily Journal**: Date, market regime note, signals considered + risk decisions, trades executed + rationale, realized PnL, open positions mark-to-market, risk metrics, model tags used.
- **Weekly Summary**: Net realized dollars, dollar expectancy after losses/costs, average dollar win/loss, profit factor, max drawdown, AI cost, incremental AI-added dollars versus the deterministic baseline, and secondary win rate.
- **Rejected-candidate comparison**: Observe predefined comparable holding windows from market data; never invent fills or choose a favorable horizon after the result.
- Delivered via Telegram EOD + on `/report`.
- Stored for audit + future backtesting.

---

## 11. Error Handling, Resilience, Observability

- **Transient failures**: tenacity retry with exponential backoff + jitter on all broker/comms calls.
- **Repeated rejects / API errors**: auto PAUSE + Telegram alert.
- **Stale data**: last_quote_age check blocks new signals.
- **Kill reliability**: dual path (Telegram handler + OS signal handler for SIGTERM/SIGINT that forces flatten best-effort).
- **Watchdog**: simple external cron that curls a local health file updated by main loop; restart container if stale.
- **Logging**: Every decision has `model=...`, `trace_id`, full context. JSON lines.
- **Metrics (v1)**: simple counters in DB (trades_today, risk_rejects, etc.) exposed in /status.

---

## 12. Security & Secrets

- All secrets via environment variables only (never committed).
- `.env.example` + validation at startup (fail fast if missing critical).
- Resume token: exact match against `RESUME_TOKEN` env (simple, upgrade later to TOTP or signed).
- No inbound HTTP in v1 (Telegram long-poll only).
- Audit logs are append-only; later can add hash chaining for tamper evidence.
- Docker: non-root user.

---

## 13. Deployment Topology

**Local dev**:
```
docker-compose up
# or uv run python -m auto_trader
```

**Production (Oracle ARM)**:
- Dockerfile multi-stage
- Deploy via `docker compose` on host
- Volume for SQLite + logs
- Systemd unit or `restart: unless-stopped`
- Secrets via host `.env` mounted or docker secrets
- Optional: Cloudflare tunnel or ngrok only if webhook needed later (polling preferred for free tier)
- Backup: daily `sqlite3 .backup` to object storage or git-crypt (small DB)

**Update path**: `git pull && docker compose build && docker compose up -d` (manual for v1)

**Rollback**: previous docker image tag + DB is append-only so safe.

---

## 14. Testing & Verification Strategy

- **Unit**: Risk engine with 100% branch coverage using property-based tests (hypothesis) for limit math.
- **Contract**: Broker adapter interface tested with respx + recorded Alpaca responses.
- **Critical paths**: 
  - Kill switch end-to-end (in-memory broker mock)
  - Risk gate blocks bad trades
  - Reconciliation after simulated crash
- **Paper-only**: All week 1-3 runs on paper. Live only after burn-in + explicit sign-off.
- **Chaos**: Manually trigger API 429, disconnects, bad data.

---

## 15. Phased Delivery Alignment (Week 1-4)

**Week 1 (Engineer focus)**:
- Scaffold + config + Alpaca + Telegram skeleton (`/status`, `/kill` stub)
- Basic universe + rules signal + first paper order through risk gate
- Reconciliation + basic journaling stub
- `/kill` proven on paper

**Week 2**:
- Full risk engine + all halt triggers + incident reports
- Order lifecycle + fill notifications
- Daily EOD journal to Telegram

**Week 3**:
- LLM abstraction + at least one provider wired
- Improved universe + signal quality
- Weekly summary

**Week 4**:
- Burn-in, edge cases, performance
- Live cutover prep (small capital)
- Full 4-role cycle complete (Architect/Engineer/Reviewer/Optimizer)

---

## 16. Open Questions / Future (Post v1)

- Webhook vs polling (when public URL cheap)
- Multi-account or prop?
- Backtesting harness reuse for signals
- Advanced stops / scaling out
- Local LLM (Ollama) for zero-cost signals
- Hash-chained audit log
- Grafana + Prometheus sidecar (if Oracle allows)
- Options / futures later (explicit scope out for v1)

---

## 17. Appendices (to be implemented by Engineer)

- `persistence/schema.sql` (full DDL)
- Sequence diagrams (mermaid):
  - Normal trade flow
  - Kill flow
  - Daily cycle
- Interface definitions (Python Protocols)
- Sample `.env.example`
- Risk parameter change process (must go through DECISIONS_LOG + restart)

---

**Architecture Sign-off**: This document is the binding design for all subsequent implementation. Any deviation requires explicit update here + entry in `DECISIONS_LOG.md`.

Next owner: Engineer (implement per this + SOURCE_OF_TRUTH + OPERATING_RULES).
```
