# SOURCE OF TRUTH

This file is the canonical reference for project goals, constraints, risk rules, and launch criteria.
If any other document conflicts with this one, this file wins.

## Project Identity

- Project: AUTO-TRADER
- Scope: Fully automated US equities swing trading system
- Execution venue: Alpaca (paper first; live only after the evidence gate passes)
- Primary operator interface: Telegram + Alpaca platform
- UI requirement: No separate dashboard required for v1

## Mission

Build an automated trading platform that can:

1. Trade US equities with minimal manual intervention.
2. Use explicit numeric risk controls rather than ambiguous profile labels.
3. Produce clear journals and scoreboards centered on dollars won or lost.
4. Prove that one AI research decision adds net dollar value versus the deterministic baseline before live trading.

## Team Operating Model

The build process uses four working roles.

- Architect: Inline design role in the current Codex thread.
- Engineer: Inline implementation role in the current Codex thread.
- Reviewer: Standing thread for senior-level correctness and risk review.
- Optimizer: Standing thread for performance, reliability, and maintainability review.

Required workflow order:

1. Architect designs inline when useful.
2. Intended non-trivial delta goes to the standing Reviewer and Optimizer.
3. Engineer implements.
4. Actual diff and verification go back to Reviewer and Optimizer.
5. Engineer applies required fixes until both approve.

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
- Live launch target: Evidence-gated; the former month-1 calendar target does not authorize a launch
- Live capital progression: Start at $100, then $200, then cap at $400
- Hosting preference: Free or near-free; Oracle first, fallback to low-cost VPS
- Simplified target flow: scanner -> deterministic prefilter -> one configured real AI decision -> RiskEngine -> OrderManager -> deterministic exits
- Parked experiments: multi-provider committee, Gemini/DeepSeek/Fable escalation, FRED-in-entry, and postmortem bias injection
- Live readiness: inactive calendar time does not count as evidence

## Explicit Risk Controls

The values below describe the documented baseline. This 2026-07-16 documentation pivot does not change deployed Oracle runtime values.

- Per-trade risk: 0.5% of equity
- Max new positions per day (initial): 1
- Max initial gross exposure: 25% of equity by default; active aggressive experiments may set the configured cap to 100% while preserving per-position sizing, halts, duplicate guards, and RiskEngine authority.
- Daily loss halt: -1.75%
- Weekly loss halt: -4.0%
- Peak drawdown halt: -6.0%
- Consecutive stop-loss halt: 2

All threshold changes must be logged in `docs/DECISIONS_LOG.md`.

Profile labels such as `conservative`, `aggressive`, and `risky` are legacy shorthand and are not the target control surface. The target exposes explicit values for max entries/day, per-position size, gross exposure, daily/weekly loss halts, drawdown halt, stop/profit/trailing/stagnation exits, and AI spend.

## Edge And Success Criteria

Primary measures:

- Net realized P/L in dollars after losses and attributable API costs
- Dollar expectancy per closed trade
- Average dollar win versus average dollar loss
- Profit factor
- Peak-to-trough drawdown
- Incremental AI-added dollars versus the deterministic scanner/prefilter baseline

Win rate is secondary. An 80% win rate with negative net dollars is failure.

Rejected candidates must be measured from observed market prices over predefined windows comparable to the actual holding policy. They are counterfactual evidence, not imaginary broker fills. Do not select the comparison window after seeing the outcome.

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

## Current Phase Plan

1. Audit the dormant Oracle runtime and preserve current state before changing it.
2. Remove parked intelligence layers from the active entry path while keeping history and code available for audit.
3. Run the simplified one-provider path and collect an active sample of closed trades plus comparably measured rejected candidates.
4. Consider live money only when dollar-first edge and operational readiness are both demonstrated. No calendar deadline overrides this gate.

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
