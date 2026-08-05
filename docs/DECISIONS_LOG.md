# DECISIONS LOG

Append-only decision history. Do not delete or rewrite old decisions.

## Entry Template

- UTC timestamp:
- Local timestamp (`America/Los_Angeles`):
- Role:
- Session AI/model:
- Decision:
- Rationale:
- Impact:
- Confidence: high | medium | low

---

## Decisions

- UTC timestamp: 2026-06-01T16:55:00Z
- Local timestamp (`America/Los_Angeles`): 2026-06-01 09:55:00 PDT
- Session AI/model: openai/gpt-5.3-codex
- Decision: Trade US equities only
- Rationale: Keep scope tight and execution reliable for month-1 live target
- Impact: Excludes crypto/options for v1
- Confidence: high

- UTC timestamp: 2026-06-01T16:56:00Z
- Local timestamp (`America/Los_Angeles`): 2026-06-01 09:56:00 PDT
- Session AI/model: openai/gpt-5.3-codex
- Decision: Swing trading focus with flexible holding period
- Rationale: User preference for less intraday pressure and operational breathing room
- Impact: Daily cadence, not high-frequency execution loop
- Confidence: high

- UTC timestamp: 2026-06-01T16:57:00Z
- Local timestamp (`America/Los_Angeles`): 2026-06-01 09:57:00 PDT
- Session AI/model: openai/gpt-5.3-codex
- Decision: Weekend holds allowed
- Rationale: User approved overnight/weekend position carrying
- Impact: Must model gap risk and weekend exposure
- Confidence: high

- UTC timestamp: 2026-06-01T16:58:00Z
- Local timestamp (`America/Los_Angeles`): 2026-06-01 09:58:00 PDT
- Session AI/model: openai/gpt-5.3-codex
- Decision: On halt/kill, flatten all positions
- Rationale: User requested immediate flattening for safety
- Impact: Emergency flow includes cancel-all + flatten-all
- Confidence: high

- UTC timestamp: 2026-06-01T16:59:00Z
- Local timestamp (`America/Los_Angeles`): 2026-06-01 09:59:00 PDT
- Session AI/model: openai/gpt-5.3-codex
- Decision: Account type is cash
- Rationale: User selected cash for safer risk posture
- Impact: No margin leverage assumptions
- Confidence: high

- UTC timestamp: 2026-06-01T17:00:00Z
- Local timestamp (`America/Los_Angeles`): 2026-06-01 10:00:00 PDT
- Session AI/model: openai/gpt-5.3-codex
- Decision: Fractional trading enabled for fractionable assets
- Rationale: Supports small capital deployment ($100-$400)
- Impact: Must validate `fractionable=true` before notional/fractional orders
- Confidence: high

- UTC timestamp: 2026-06-01T17:01:00Z
- Local timestamp (`America/Los_Angeles`): 2026-06-01 10:01:00 PDT
- Session AI/model: openai/gpt-5.3-codex
- Decision: Paper phase starts immediately, with initial trades visible in week 1
- Rationale: User wants fast execution feedback
- Impact: Week 1 build must include active paper order flow
- Confidence: high

- UTC timestamp: 2026-06-01T17:02:00Z
- Local timestamp (`America/Los_Angeles`): 2026-06-01 10:02:00 PDT
- Session AI/model: openai/gpt-5.3-codex
- Decision: Live launch by end of month 1
- Rationale: User deadline preference
- Impact: Aggressive delivery cadence; risk controls required early
- Confidence: high

- UTC timestamp: 2026-06-01T17:03:00Z
- Local timestamp (`America/Los_Angeles`): 2026-06-01 10:03:00 PDT
- Session AI/model: openai/gpt-5.3-codex
- Decision: Use UTC canonical + local time display (`America/Los_Angeles`)
- Rationale: Avoid scheduling errors while keeping reports readable
- Impact: All logs in UTC; Telegram reports in local timezone
- Confidence: high

- UTC timestamp: 2026-06-01T17:04:00Z
- Local timestamp (`America/Los_Angeles`): 2026-06-01 10:04:00 PDT
- Role: Architect
- Session AI/model: openai/gpt-5.3-codex
- Decision: Documentation style is append-only daily history with evolving same-day NEXT
- Rationale: Preserve complete audit trail while allowing active planning
- Impact: `docs/DAILY_PLAN.md` structure fixed
- Confidence: high

- UTC timestamp: 2026-06-01T17:25:00Z
- Local timestamp (`America/Los_Angeles`): 2026-06-01 10:25:00 PDT
- Role: Architect
- Session AI/model: openai/gpt-5.3-codex
- Decision: Adopt mandatory 4-agent workflow (Architect, Engineer, Reviewer, Optimizer)
- Rationale: User requires startup-grade collaborative build process across multiple AI models
- Impact: Added role-based sequencing, handoff contract, and milestone deliverable bundle requirements
- Confidence: high

- UTC timestamp: 2026-06-01T18:50:00Z
- Local timestamp (`America/Los_Angeles`): 2026-06-01 11:50:00 PDT
- Role: Architect
- Session AI/model: xai/grok-build-0.1
- Decision: Select Python 3.12 + alpaca-py + aiosqlite + python-telegram-bot + pydantic as v1 stack
- Rationale: Best balance of Alpaca ecosystem maturity, async safety, low memory for Oracle free ARM tier, rapid safe iteration, and future quant/AI extensibility. Node/TS and Go evaluated implicitly; Python wins for this domain.
- Impact: Architecture, pyproject.toml, .env.example, and all code examples now bound to this choice. Dockerfile will target python:3.12-slim.
- Confidence: high

- UTC timestamp: 2026-06-01T21:10:00Z
- Local timestamp (`America/Los_Angeles`): 2026-06-01 14:10:00 PDT
- Role: Engineer + Coordinator
- Session AI/model: xai/grok-build-0.1
- Decision: Automate subagent role handoffs — after major Engineer deliverables, automatically launch Reviewer (and Optimizer when appropriate) via the Task tool without prompting the user each cycle.
- Rationale: User explicitly requested that manual prompting for every Reviewer/Optimizer launch was inefficient. This aligns with the 4-agent contract while improving iteration speed on safety-critical work.
- Impact: Future sessions will auto-trigger the next role after Engineer work. DAILY_PLAN and HANDOFF will still be updated by the coordinating agent. This is an operating process change, not a risk or architecture decision.
- Confidence: high

- UTC timestamp: 2026-06-02T04:35:00Z
- Local timestamp (`America/Los_Angeles`): 2026-06-01 21:35:00 PDT
- Role: Architect/Engineer
- Session AI/model: openai/gpt-5.5
- Decision: Adopt an AI committee as an advisory/ranking layer after the rules-only paper-trade loop is proven.
- Rationale: User wants multiple AI viewpoints (e.g., ChatGPT, Claude, Grok/Gemini) while avoiding hallucinated market facts and unsafe autonomous execution. Separating AI judgment from deterministic risk authorization preserves automation without weakening capital safety.
- Impact: Architecture now defines Bull Analyst, Bear/Risk Analyst, and Judge/Portfolio Manager roles; AI starts in journal-only mode, can later influence ranking/veto, and can never override RiskEngine, `/kill`, `HALTED`, stale-data blocks, or exposure/loss limits.
- Confidence: high

- UTC timestamp: 2026-06-02T20:06:52Z
- Local timestamp (`America/Los_Angeles`): 2026-06-02 13:06:56 PDT
- Role: Engineer
- Session AI/model: openai/gpt-5-codex
- Decision: Use Pydantic-only secret parsing and systemd watchdog restarts for server runs.
- Rationale: Avoid executing or systemd-parsing `.env` secrets while keeping Oracle/Pi service runs resilient to clean exits after recoverable runtime failures.
- Impact: `scripts/run_bot.sh` and systemd set `AUTO_TRADER_ENV_FILE`; `get_settings()` reads that env file; the systemd template uses `Restart=always`, `UMask=0077`, and host-visible `/tmp` health checks.
- Confidence: high

- UTC timestamp: 2026-06-02T20:13:56Z
- Local timestamp (`America/Los_Angeles`): 2026-06-02 13:14:01 PDT
- Role: Engineer
- Session AI/model: openai/gpt-5-codex
- Decision: Require broker open close-order checks and durable pending-exit markers before enabling auto-exit.
- Rationale: AMPX produced real dry-run max-loss exit alerts, so duplicate close prevention must survive both one-process ticks and process restarts before any autonomous exit execution.
- Impact: Supervisor close execution now checks open broker orders, persists pending exits, clears filled pending state only after a trusted broker position snapshot proves the symbol is gone, clears failed pending close orders during reconciliation so they can be retried, pauses for review when a persisted pending close cannot be matched by broker/client order ID to an open broker close order, and keeps single-position close submission un-retried to avoid duplicate closes after response-path failures.
- Confidence: high

- UTC timestamp: 2026-06-02T21:01:16Z
- Local timestamp (`America/Los_Angeles`): 2026-06-02 14:01:16 PDT
- Role: Engineer
- Session AI/model: openai/gpt-5-codex
- Decision: Keep auto-entry disabled while enabling supervised paper auto-exit and surfacing pending exits in Telegram.
- Rationale: The approved close path needs a real market-open lifecycle test, but new entries should not restart until AMPX exit fill, duplicate-close suppression, pending-exit cleanup, and operator reporting are verified.
- Impact: Local supervised mode uses `AUTO_ENTRY_ENABLED=false` and `AUTO_EXIT_ENABLED=true`; `/status` and `/report` include pending exits and duplicate-exit suppression; `/report` includes latest journal notes; Wednesday market-open validation is documented in the runbook.
- Confidence: high

- UTC timestamp: 2026-06-02T22:08:55Z
- Local timestamp (`America/Los_Angeles`): 2026-06-02 15:08:55 PDT
- Role: Engineer
- Session AI/model: openai/gpt-5-codex
- Decision: Enforce a single local bot process per SQLite trading database.
- Rationale: Duplicate bot instances caused Telegram `getUpdates` conflicts and can confuse supervised trading state. The second process should fail before Telegram polling or supervisor execution.
- Impact: Startup now acquires a non-blocking `/tmp/auto_trader_*.lock` keyed by the resolved DB path; duplicate startup exits with a clear fatal message naming the existing holder.
- Confidence: high

- UTC timestamp: 2026-06-03T00:29:10Z
- Local timestamp (`America/Los_Angeles`): 2026-06-02 17:29:10 PDT
- Role: Engineer
- Session AI/model: openai/gpt-5-codex
- Decision: Prepare Oracle VM after the local bot is stable, but do not make Oracle the active runner until the Day 3 AMPX close lifecycle validates.
- Rationale: Oracle improves uptime, but a second host can bypass the local single-instance lock and create cross-host Telegram polling or trading conflicts if laptop and VM run at the same time.
- Impact: Added a read-only Day 3 validation command and documented that Oracle migration must be single-runner: one active host, one Telegram bot token, one Alpaca paper account.
- Confidence: high

- UTC timestamp: 2026-06-03T14:37:56Z
- Local timestamp (`America/Los_Angeles`): 2026-06-03 07:37:56 PDT
- Role: Engineer
- Session AI/model: openai/gpt-5-codex
- Decision: Treat matched filled close orders as completed pending exits during supervisor reconciliation.
- Rationale: Day 3 market-open validation proved the AMPX close filled and the broker position disappeared, but the durable pending-exit marker remained. A matched filled broker close is enough evidence to clear the marker and complete the exit lifecycle.
- Impact: Supervisor reconciliation now clears matched filled pending exits, removes them from memory, appends a completion journal entry, and sends one Telegram completion alert; rejected/canceled/expired closes still clear as retry-able failures.
- Confidence: high

- UTC timestamp: 2026-07-17T05:30:41Z
- Local timestamp (`America/Los_Angeles`): 2026-07-16 22:30:41 PDT
- Role: Architect/Engineer
- Session AI/model: openai/gpt-5
- Decision: Pivot the active target from a multi-provider AI committee and recursive postmortem memory to one configured real AI research decision between the deterministic prefilter and RiskEngine.
- Rationale: The system accumulated provider voting, escalation, macro, postmortem, and profile machinery faster than it accumulated evidence of dollar edge. Three inactive calendar weeks are not a trading result, but the operational complexity itself reduced trust and usability.
- Impact: Multi-provider voting, Gemini/DeepSeek/Fable escalation, FRED-in-entry, and postmortem bias injection are parked. Oracle/Alpaca, RiskEngine authority, kill/halts, duplicate protection, deterministic exits, persistence, and audit remain. This entry documents a target only; deployed Oracle runtime/config is unchanged until a separate reviewed implementation.
- Confidence: high

- UTC timestamp: 2026-07-17T05:30:41Z
- Local timestamp (`America/Los_Angeles`): 2026-07-16 22:30:41 PDT
- Role: Architect/Engineer
- Session AI/model: openai/gpt-5
- Decision: Make dollar outcomes the primary definition of edge and live readiness.
- Rationale: Win rate counts a small gain and a large gain equally and can hide a losing payoff structure. An 80% win rate with negative net dollars is failure.
- Impact: Primary evaluation becomes net realized dollars after losses and attributable AI costs, dollar expectancy/trade, average dollar win/loss, profit factor, drawdown, and incremental AI-added dollars versus a comparably measured deterministic baseline. Win rate remains secondary. Idle calendar time does not count toward the active evidence sample.
- Confidence: high

- UTC timestamp: 2026-07-17T15:18:22Z
- Local timestamp (`America/Los_Angeles`): 2026-07-17 08:18:22 PDT
- Role: Architect/Engineer
- Session AI/model: openai/gpt-5
- Decision: Implement the simplified runtime as the repository default while requiring an explicit AI-entry-gate enablement for the one-provider AI decision.
- Rationale: Complexity must not masquerade as edge. Gate-on mode should test one real model's incremental dollar value; gate-off mode should be an exact, visible deterministic baseline with no paid or shadow AI calls.
- Impact: Simplified mode ignores legacy provider lists and profile-driven behavior, omits memory/FRED entry side effects, blocks configured-provider setup failures fail-closed, and reports dollars/cost/payoff before win rate. Oracle remains unchanged until reviewed commits can be pushed and intentionally deployed.
- Confidence: high

- UTC timestamp: 2026-07-22T06:53:00Z
- Local timestamp (`America/Los_Angeles`): 2026-07-21 23:53:00 PDT
- Role: Architect/Engineer
- Session AI/model: openai/gpt-5
- Decision: Defer `auto-trader.service` from Ubuntu `needrestart` automatic service restarts while keeping unattended security updates enabled.
- Rationale: A SQLite security update caused `needrestart` to issue an unmarked `systemctl restart auto-trader.service`. The bot correctly treated SIGTERM as an emergency, persisted `HALTED`, and queued closes for all open positions, even though no trading-risk threshold had fired.
- Impact: Oracle installs a narrow `override_rc` rule for `auto-trader.service`; deferred application restarts must use `scripts/oracle_safe_restart.sh`. The recovery canceled all 10 queued close orders, retained all 10 positions, verified zero open orders, and restored `ACTIVE` before this prevention change.
- Confidence: high

- UTC timestamp: 2026-07-28T06:56:09Z
- Local timestamp (`America/Los_Angeles`): 2026-07-27 23:56:09 PDT
- Role: Architect/Engineer
- Session AI/model: openai/gpt-5
- Decision: Measure every validated single-provider AI decision against completed D0/D1/D3/D5 market-session outcomes before changing provider or voting policy.
- Rationale: Realized trades alone cannot show whether Grok adds selection edge because rejected opportunities were not graded. Model choice and committee structure should be decided from comparable dollar evidence, not intuition or win rate.
- Impact: A passive, model-agnostic ledger stores one provider/model/policy/symbol/session decision, a model-packet reference price, and fixed `$30` hypothetical outcomes. Resolution is bounded, batched, silent, and outside trading authority. Realized and hypothetical dollars remain separate. Runtime entry capacity may be raised to 12, but existing RiskEngine sizing, gross-exposure, halt, duplicate, and exit controls remain authoritative.
- Confidence: high

- UTC timestamp: 2026-08-05T20:02:38Z
- Local timestamp (`America/Los_Angeles`): 2026-08-05 13:02:38 PDT
- Role: Architect/Engineer
- Session AI/model: openai/gpt-5
- Decision: Harden fast-filled pending-exit recovery and add a 60-minute filled-exit symbol re-entry cooldown.
- Rationale: On 2026-08-03, NVDL close orders filled and disappeared from the open-order endpoint before the local pending marker reconciled. The supervisor interpreted the temporary mismatch as unresolved and persisted `PAUSED`. The same symbol also re-entered three times in roughly seven minutes.
- Impact: Missing open orders now trigger throttled all-status reconciliation using exact broker/client identity, with a 360-second grace before a genuine unmatched marker pauses. Malformed marker timestamps remain fail-closed. Filled exits block same-symbol entry work for 60 minutes before signal persistence or AI spend. RiskEngine, exit rules, sizing, halts, and operator-only recovery authority are unchanged.
- Confidence: high
