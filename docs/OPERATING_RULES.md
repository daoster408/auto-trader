# OPERATING RULES

Rules all AIs and contributors must follow.

## Priority Order

1. Capital safety and hard risk limits
2. Correctness and reliability
3. Speed of iteration

## Team Workflow Rules

- Architect and Engineer are inline roles in the current working thread. Reviewer and Optimizer are standing review threads.
- For non-trivial changes, use Architect design judgment when needed, implement as Engineer, then complete the Reviewer/Optimizer fix-and-re-review loop required by `AGENTS.md`.
- Every session must declare active role and model tag.
- Reviewer can require changes before optimization.
- Optimizer cannot remove or weaken risk controls to improve speed.
- After any major Engineer deliverable, visible Reviewer and Optimizer background threads must be launched automatically without asking the user each cycle. Reviewer focuses on safety/correctness; Optimizer focuses on reliability/performance without weakening risk controls.
- Engineer must proactively check Reviewer/Optimizer verdicts, apply required fixes, and send the updated work back for re-review without waiting for the user to ask.
- BLOCK and APPROVE WITH CHANGES verdicts require Engineer action or an explicit documented deferral before new feature work continues.
- Milestones are incomplete until the relevant deliverables are present:
  - complete architecture
  - full implementation
  - review feedback
  - final optimized version

## Hard Execution Rules

- All trade proposals must pass the risk engine before any order request.
- If risk validation fails, order must not be submitted.
- `/kill` has absolute priority and must preempt all trading tasks.
- In `HALTED` state, no new trades are allowed.
- Any halt event triggers cancel-all + flatten-all flow.

## Logging and Audit Rules

- Log every signal input, model output, risk check result, and order action.
- Store canonical timestamps in UTC.
- Include provider/model tag for AI-driven decisions.
- Keep append-only history in planning and decision docs.

## Documentation Update Rules

Per session, update:

- `docs/DAILY_PLAN.md` (required)
- `docs/HANDOFF.md` (required)
- `docs/DECISIONS_LOG.md` (required only if a decision changed)
- `docs/AGENT_WORKFLOW.md` (reference required for role handoffs)

## GitHub Sync Rules

- After every approved major milestone or safety fix, Engineer must commit and push to GitHub without waiting for the user to ask.
- Do not push while Reviewer or Optimizer has an unresolved `BLOCK`.
- `APPROVE WITH CHANGES` must be fixed or explicitly documented as deferred before push.
- Never commit `.env`, secrets, local databases, or generated private runtime artifacts.
- Commit messages should name the milestone and safety outcome.

## Model Experimentation Rules

- Model selection must remain provider-agnostic in code design.
- Track each model's usage and observed outcomes in `docs/MODEL_REGISTRY.md`.
- Do not change live-risk thresholds without logging rationale in `docs/DECISIONS_LOG.md`.
- The active target uses one configured real AI provider per entry decision. Multi-provider committees and escalation models are parked until evidence shows one-provider operation is insufficient.
- Model quality is judged by valid-response reliability, incremental net dollars versus the deterministic baseline, drawdown, and API cost. Win rate alone is not a promotion criterion.

## Strategy Simplicity And Evaluation Rules

- Active target flow: scanner -> deterministic prefilter -> one configured real AI decision -> RiskEngine -> OrderManager -> deterministic exits.
- Primary metrics: net realized dollars, dollar expectancy after losses and costs, average dollar win/loss, profit factor, drawdown, API cost, and incremental AI-added dollars.
- Track rejected candidates over predefined comparable windows using observed market data. Do not invent fills or cherry-pick a favorable horizon after the outcome is known.
- A high win rate with negative net dollars is failure.
- Do not count inactive calendar weeks toward the evidence sample.

## Deployment Rules

- Paper trading required during initial month build.
- Live cutover is evidence-gated, not calendar-gated. A prior month-1 target does not authorize live trading without positive active-sample evidence and operational readiness.
- Start live with minimal capital and step exposure only after stable behavior.

## Failure Handling Rules

- On broker/API outage, suspend new entries and alert via Telegram.
- On stale market data, block order generation.
- On repeated order rejects, move to safe/paused state and alert.
