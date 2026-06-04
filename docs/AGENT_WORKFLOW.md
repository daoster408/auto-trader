# AGENT WORKFLOW

Defines responsibilities and handoff rules for the 4-agent build team.

## Roles

- Architect
  - Owns system design, module boundaries, data flow, and deployment topology.
  - Produces architecture docs before implementation starts.
- Engineer
  - Implements features according to architecture and operating rules.
  - Adds tests, logging hooks, and risk-gate integration.
- Reviewer
  - Performs senior-level code review with focus on correctness, safety, and maintainability.
  - Blocks merges for risk bypass, missing tests, or unclear failure handling.
- Optimizer
  - Improves performance, scalability, reliability, and operational cost.
  - Targets runtime speed, API call efficiency, and failure recovery behavior.

## Required Sequence

1. Architect delivers design package.
2. Engineer builds implementation package.
3. Reviewer produces review package and required changes.
4. Engineer applies required changes.
5. Optimizer produces production-grade package.

## Standing Subagent Rule

- Use one standing Reviewer thread and one standing Optimizer thread per trading day; do not create new Reviewer/Optimizer threads for each implementation delta. Pinning is optional operator convenience, not a requirement.
- Create fresh standing threads only when starting a new trading day, when a thread is wedged/unavailable, or when context pollution makes the thread unsafe to reuse. Archive or clearly mark replaced threads so stale verdicts are not mistaken for current review.
- Before every non-trivial Engineer implementation pass, send the intended delta to the standing Reviewer and standing Optimizer without asking the user for permission again.
- After every major Engineer implementation pass, send the actual diff and verification evidence back to the standing Reviewer and standing Optimizer.
- Engineer must proactively poll/read Reviewer and Optimizer verdicts; the user should not need to ask whether agents are done or whether blockers exist.
- If Reviewer or Optimizer returns BLOCK or APPROVE WITH CHANGES, Engineer must apply required fixes or explicitly log why a recommendation is deferred, then automatically send the updated working tree back to the same standing threads for re-review.
- Continue the Engineer -> Reviewer/Optimizer -> Engineer fix loop until Reviewer is APPROVE and Optimizer is APPROVE or only has documented non-blocking follow-ups.
- Reviewer must prioritize capital safety, risk bypass checks, kill-switch reliability, and correctness.
- Optimizer must not remove, weaken, bypass, or defer any risk control in pursuit of performance.
- If Reviewer returns BLOCK, Engineer fixes required changes and then automatically re-runs Reviewer/Optimizer as appropriate in the same standing threads.

## Mandatory Deliverables

For each major milestone, provide:

1. Complete architecture
2. Full implementation
3. Review feedback
4. Final optimized version

## Handoff Contract

Each role handoff must include:

- Current branch or working state summary
- Files changed and why
- Risks identified
- Verification evidence (tests, logs, sample outputs)
- Next owner role and concrete next actions
- GitHub sync status for approved milestones (commit hash and push status when available)

## Quality Gates

- No order path without risk engine validation.
- `/kill` flow tested and operational before live cutover.
- Journaling and audit logs must be end-to-end functional.
- Any threshold changes must be recorded in `docs/DECISIONS_LOG.md`.
- Approved major milestones must be committed and pushed to GitHub before moving to the next feature.

## Session Metadata Standard

Every planning/build entry should include:

- Role: Architect | Engineer | Reviewer | Optimizer
- AI model tag: provider/model
- Timestamp: UTC canonical + local display (`America/Los_Angeles`)
