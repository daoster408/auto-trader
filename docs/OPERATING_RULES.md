# OPERATING RULES

Rules all AIs and contributors must follow.

## Priority Order

1. Capital safety and hard risk limits
2. Correctness and reliability
3. Speed of iteration

## Team Workflow Rules

- Work must follow role order: Architect -> Engineer -> Reviewer -> Optimizer.
- Every session must declare active role and model tag.
- Reviewer can require changes before optimization.
- Optimizer cannot remove or weaken risk controls to improve speed.
- After any major Engineer deliverable, visible Reviewer and Optimizer background threads must be launched automatically without asking the user each cycle. Reviewer focuses on safety/correctness; Optimizer focuses on reliability/performance without weakening risk controls.
- Engineer must proactively check Reviewer/Optimizer verdicts, apply required fixes, and send the updated work back for re-review without waiting for the user to ask.
- BLOCK and APPROVE WITH CHANGES verdicts require Engineer action or an explicit documented deferral before new feature work continues.
- Milestones are incomplete until all four deliverables are present:
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

## Deployment Rules

- Paper trading required during initial month build.
- Live cutover required by end of month 1 unless explicit user override.
- Start live with minimal capital and step exposure only after stable behavior.

## Failure Handling Rules

- On broker/API outage, suspend new entries and alert via Telegram.
- On stale market data, block order generation.
- On repeated order rejects, move to safe/paused state and alert.
