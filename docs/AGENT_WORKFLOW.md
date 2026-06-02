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

## Automatic Subagent Rule

- After every major Engineer implementation pass, launch Reviewer and Optimizer subagents automatically without asking the user for permission again.
- Reviewer must prioritize capital safety, risk bypass checks, kill-switch reliability, and correctness.
- Optimizer must not remove, weaken, bypass, or defer any risk control in pursuit of performance.
- If Reviewer returns BLOCK, Engineer fixes required changes and then automatically re-runs Reviewer/Optimizer as appropriate.

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

## Quality Gates

- No order path without risk engine validation.
- `/kill` flow tested and operational before live cutover.
- Journaling and audit logs must be end-to-end functional.
- Any threshold changes must be recorded in `docs/DECISIONS_LOG.md`.

## Session Metadata Standard

Every planning/build entry should include:

- Role: Architect | Engineer | Reviewer | Optimizer
- AI model tag: provider/model
- Timestamp: UTC canonical + local display (`America/Los_Angeles`)
