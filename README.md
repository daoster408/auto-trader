# AUTO-TRADER

Fully automated US equities swing trading system.

**Status**: v0 architecture complete. Implementation in progress following strict 4-agent (Architect → Engineer → Reviewer → Optimizer) workflow.

## Non-Negotiables (Read These First)

- **Risk engine is the ONLY path to any order.**
- `/kill` always works, preempts everything, flattens positions + cancels orders + sets HALTED.
- Paper trading only until explicit live cutover approval.
- All decisions audited. UTC canonical storage.

See `docs/SOURCE_OF_TRUTH.md`, `docs/ARCHITECTURE.md`, `docs/OPERATING_RULES.md`.

## Quick Start (Dev)

1. Python 3.12+
2. `uv sync` (or `pip install -e ".[dev]"`)
3. `cp .env.example .env` and fill secrets (Alpaca paper keys + Telegram bot token)
4. `python -m auto_trader` (once implemented)

## Current Phase

- Architecture delivered 2026-06-01 by xai/grok-build-0.1
- Next: Engineer scaffold + first paper trade path (Week 1 targets)

## Commands (Planned v1)

- `/status` - health + PnL + risk
- `/pause`, `/resume <token>`, `/kill`, `/report`

## Hosting Target

Oracle Always Free ARM tier (Docker) or equivalent low-cost VPS. Single-process async Python.

## Important

This project follows an append-only documentation contract. Never delete history from `docs/`.

**Do not trade live until all risk gates, kill-switch, and journals are proven on paper across multiple days.**
