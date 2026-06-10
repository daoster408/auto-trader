# SOURCE OF TRUTH

This file is the canonical reference for project goals, constraints, risk rules, and launch criteria.
If any other document conflicts with this one, this file wins.

## Project Identity

- Project: AUTO-TRADER
- Scope: Fully automated US equities swing trading system
- Execution venue: Alpaca (paper first, live by end of month 1)
- Primary operator interface: Telegram + Alpaca platform
- UI requirement: No separate dashboard required for v1

## Mission

Build an automated trading platform that can:

1. Trade US equities with minimal manual intervention.
2. Balance aggressiveness and conservatism via strict risk controls.
3. Produce daily and weekly journals with trade rationale and account performance.
4. Reach live trading within month 1 using small real capital.

## Team Operating Model

The build process uses four collaborating agents.

- Architect: Designs scalable system architecture.
- Engineer: Builds implementation from approved architecture.
- Reviewer: Performs senior-level code review and risk checks.
- Optimizer: Improves performance, reliability, and scalability.

Required workflow order:

1. Architect designs.
2. Engineer implements.
3. Reviewer critiques and requests improvements.
4. Optimizer production-hardens.

Required output bundle per major milestone:

- Complete architecture
- Full implementation
- Review feedback
- Final optimized version

## Non-Negotiables

- No order may bypass the risk engine.
- `/kill` must always work and take precedence over strategy logic.
- On halt/kill, system must flatten all positions and cancel open orders.
- All decisions and trades must be logged for auditability.
- Logging timestamps are canonical UTC; human reports show `America/Los_Angeles` time.

## Current Locked Decisions

- Market: US equities only
- Strategy style: Swing (flexible holding period)
- Weekend holds: Allowed
- Account type: Cash
- Fractional shares: Enabled where `fractionable=true`
- Paper trading: Start immediately; visible initial trades in week 1
- Live launch target: By end of month 1
- Live capital progression: Start at $100, then $200, then cap at $400
- Hosting preference: Free or near-free; Oracle first, fallback to low-cost VPS

## Risk Profile (v1 Defaults)

- Per-trade risk: 0.5% of equity
- Max new positions per day (initial): 1
- Max initial gross exposure: 25% of equity by default; active aggressive experiments may set the configured cap to 100% while preserving per-position sizing, halts, duplicate guards, and RiskEngine authority.
- Daily loss halt: -1.75%
- Weekly loss halt: -4.0%
- Peak drawdown halt: -6.0%
- Consecutive stop-loss halt: 2

All threshold changes must be logged in `docs/DECISIONS_LOG.md`.

## System States

- `ACTIVE`: Trading allowed
- `PAUSED`: No new entries; monitoring continues
- `HALTED`: No trading allowed, positions flattened, manual resume required

## Telegram Command Contract (v1)

- `/status`: Health, state, PnL snapshot, risk status
- `/pause`: Move system to `PAUSED`
- `/resume <token>`: Resume from `PAUSED` or `HALTED` with authorization token
- `/kill`: Cancel all orders, flatten all positions, set `HALTED`, send incident report
- `/report`: On-demand latest daily/weekly performance summary

## Phase Plan

- Week 1: First paper trades executing and visible
- Week 2: Risk engine hardening + kill-switch reliability
- Week 3: AI signal abstraction + stock universe discovery improvements
- Week 4: Burn-in + live cutover preparation + live launch

## Timekeeping Standard

- Canonical storage: ISO 8601 UTC (example: `2026-06-01T17:10:00Z`)
- User-facing display: `America/Los_Angeles`
- Scheduler: UTC

## Documentation Contract

Every AI session must update:

- `docs/DAILY_PLAN.md` (always)
- `docs/HANDOFF.md` (always)
- `docs/DECISIONS_LOG.md` (only when a decision changes)

Session entries must include model/provider tag (example: `openai/gpt-5.3-codex`).
The acting role for the session must also be declared (Architect, Engineer, Reviewer, or Optimizer).
