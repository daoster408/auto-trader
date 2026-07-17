# AUTO-TRADER

Fully automated US equities swing trading system.

**Status**: Simplification pivot documented on 2026-07-16. The target is one auditable AI research decision between the deterministic prefilter and RiskEngine. The existing Oracle runtime has not yet been changed by this documentation pass and must be audited before restart or reactivation.

**Primary outcome**: Make more dollars than the deterministic baseline after losses and API costs. Win rate is supporting context only; an 80% win rate with negative net P/L is failure.

## Non-Negotiables

- **RiskEngine is the ONLY path to any order.**
- `/kill` always works, preempts strategy logic, cancels orders, flattens positions, and sets `HALTED`.
- Paper trading only until explicit live cutover approval.
- All decisions audited. UTC canonical storage.
- One configured real AI provider may approve/block candidate flow before RiskEngine when the simplified AI entry gate is implemented and enabled. AI cannot override RiskEngine, sizing, `/kill`, `HALTED`, stale-data blocks, explicit exposure/loss limits, duplicate-position guards, or account/broker blocks.

See `docs/SOURCE_OF_TRUTH.md`, `docs/ARCHITECTURE.md`, and `docs/OPERATING_RULES.md`.

## Quick Start

1. Python 3.12+
2. Use the local virtualenv: `.venv/bin/python`
3. Fill `.env` from `.env.example` with Alpaca paper keys, Telegram token, `RESUME_TOKEN`, and any enabled AI/provider keys.
4. For a local supervised Telegram + supervisor loop, run:

```bash
scripts/run_bot.sh
```

5. For the current Oracle paper runner, use the read-only launchpad before making operational decisions:

```bash
ORACLE_HOST=<host> ORACLE_USER=ubuntu ORACLE_KEY=<ssh-key> scripts/oracle_week2_launchpad.sh
```

## Simplified Target Flow

```text
scanner -> deterministic prefilter -> one real AI decision -> RiskEngine -> OrderManager -> deterministic exits
```

- Keep Oracle, Alpaca, RiskEngine, halt/kill behavior, duplicate-order protection, deterministic exits, journaling, and Telegram operations.
- Park multi-provider voting, Gemini/DeepSeek/Fable escalation, FRED-in-entry, and postmortem-bias prompt injection.
- Configure risk with explicit numeric limits rather than relying on labels such as `conservative`, `aggressive`, or `risky`.
- Keep old components available for audit and possible later experiments; do not treat them as the active target.

## Implemented Capabilities

The repository still contains the pre-pivot multi-provider and postmortem machinery until the implementation phase removes it from the active runtime path.

- Alpaca paper account connectivity
- Telegram bot token validation
- Dynamic stock discovery from Alpaca tradable/fractionable universe
- Free Alpaca IEX snapshots for discovery inputs
- Risk-gated order submission through `OrderManager`
- Supervised Oracle/systemd paper runner with planned-maintenance restart helper
- Runtime Telegram config for auto-entry, AI gate, legacy risk profile, and explicit `max_entries`
- Legacy AI entry gate with paid-prefilter, budget accounting, and multi-provider committee support
- Legacy scoreboard memory, brain guidance, and paid postmortem tooling
- Launchpad entry-pressure diagnostics that explain likely blockers without placing orders or calling paid AI
- Deterministic exits for max loss, take profit, trailing stop, max hold, and stagnation
- Emergency `/kill` and OS-signal halt/flatten path
- SQLite system-state persistence with safe `HALTED` defaults
- Append-only operational docs

## Telegram Commands

- `/status` - health, state, risk/PnL snapshot
- `/pause` - stop new entries
- `/resume <token>` - resume after pause/halt
- `/kill` - emergency cancel all + flatten all + set `HALTED`
- `/report` - latest performance summary
- `/config` - inspect runtime config
- `/config auto_entry on|off` - enable or disable new entries
- `/config ai_gate on|off` - enable or disable the AI entry gate
- `/config risk_profile conservative|aggressive|risky` - switch experiment profile
- `/config max_entries <positive integer>` - explicitly set entry capacity

## Hosting Target

Oracle Always Free ARM tier with systemd is the active paper runner. Keep only one active bot host polling Telegram/trading the paper account at a time.
See `docs/RUNBOOK.md` and `deploy/systemd/auto-trader.service.example` for repeatable local/server runs.

## Important

This project follows an append-only documentation contract. Never delete history from `docs/`.

**Do not trade live until an active evidence sample proves entries, exits, rejects, reconnects, `/kill`, restart behavior, reporting, positive net dollars after costs, acceptable drawdown, and incremental AI-added dollar value versus the deterministic baseline. Idle calendar weeks do not count.**
