# AUTO-TRADER Codex Instructions

## Iron-Clad Reviewer/Optimizer Rule

- For any non-trivial AUTO-TRADER code change, send the intended delta to the standing Reviewer and standing Optimizer threads before implementation starts.
- Reuse the existing standing Reviewer and Optimizer threads for the trading day. Do not create new Reviewer/Optimizer threads unless the existing thread is unavailable, wedged, or unsafe to reuse.
- After implementation, send the actual diff, verification output, and commit/deploy state back to the same Reviewer and Optimizer.
- If either returns `BLOCK` or `APPROVE WITH CHANGES`, apply the required fixes before moving to the next feature, then resend to the same threads for re-review.
- Do not treat a change as done until Reviewer is `APPROVE` and Optimizer is `APPROVE`, or any remaining Optimizer notes are explicitly documented as non-blocking follow-ups.
- If the user asks why Reviewer/Optimizer were not used, stop feature work, inspect the standing threads, and repair the workflow before continuing.

## Safety Priority

- Capital safety beats speed.
- AI research is advisory only. RiskEngine remains the only authority for approval, sizing, and order flow.
- Any config change that can increase live or paper trading risk must be reviewed and logged.
