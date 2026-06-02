# AUTO-TRADER

Fully automated US equities swing trading system.

**Status**: Paper-trade readiness phase. Kill/persistence foundation is approved, dynamic discovery is implemented, and first paper order path is ready to run once market is open.

## Non-Negotiables

- **RiskEngine is the ONLY path to any order.**
- `/kill` always works, preempts strategy logic, cancels orders, flattens positions, and sets `HALTED`.
- Paper trading only until explicit live cutover approval.
- All decisions audited. UTC canonical storage.
- AI can rank/review candidates later, but cannot override RiskEngine, `/kill`, `HALTED`, stale-data blocks, exposure limits, or loss limits.

See `docs/SOURCE_OF_TRUTH.md`, `docs/ARCHITECTURE.md`, and `docs/OPERATING_RULES.md`.

## Quick Start

1. Python 3.12+
2. Use the local virtualenv: `.venv/bin/python`
3. Fill `.env` from `.env.example` with Alpaca paper keys, Telegram token, and `RESUME_TOKEN`
4. During market hours, run the one-shot paper trade helper:

```bash
.venv/bin/python -c "import asyncio; from auto_trader.__main__ import run_first_paper_trade_test; asyncio.run(run_first_paper_trade_test())"
```

## Current Capabilities

- Alpaca paper account connectivity
- Telegram bot token validation
- Dynamic stock discovery from Alpaca tradable/fractionable universe
- Free Alpaca IEX snapshots for discovery inputs
- Risk-gated order submission through `OrderManager`
- Emergency `/kill` and OS-signal halt/flatten path
- SQLite system-state persistence with safe `HALTED` defaults
- Append-only operational docs

## Telegram Commands

- `/status` - health, state, risk/PnL snapshot
- `/pause` - stop new entries
- `/resume <token>` - resume after pause/halt
- `/kill` - emergency cancel all + flatten all + set `HALTED`
- `/report` - latest performance summary

## Hosting Target

Oracle Always Free ARM tier (Docker/systemd), after local paper loop is validated.

## Important

This project follows an append-only documentation contract. Never delete history from `docs/`.

**Do not trade live until all risk gates, kill-switch, reconciliation, and journals are proven on paper across multiple days.**
