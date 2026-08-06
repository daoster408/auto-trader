-- AUTO-TRADER SQLite schema (v0)
-- All timestamps stored as TEXT in ISO8601 UTC (per SOURCE_OF_TRUTH)
-- Append-only where possible for audit

PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS system_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),  -- singleton row
    state TEXT NOT NULL CHECK (state IN ('ACTIVE','PAUSED','HALTED')),
    halted_at TEXT,
    halt_reason TEXT,
    resumed_at TEXT,
    last_equity REAL,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS runtime_sessions (
    id TEXT PRIMARY KEY,
    started_at TEXT NOT NULL,
    ended_at TEXT,
    host_name TEXT NOT NULL,
    process_id INTEGER NOT NULL,
    process_role TEXT NOT NULL,
    execution_mode TEXT NOT NULL CHECK (execution_mode IN ('paper','live','unknown')),
    config_hash TEXT NOT NULL,
    config_snapshot_json TEXT NOT NULL,
    inferred INTEGER NOT NULL DEFAULT 0 CHECK (inferred IN (0,1))
);

CREATE TABLE IF NOT EXISTS decision_contexts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    context_key TEXT UNIQUE,
    runtime_session_id TEXT NOT NULL REFERENCES runtime_sessions(id),
    captured_at TEXT NOT NULL,
    decision_source TEXT NOT NULL,
    inferred INTEGER NOT NULL DEFAULT 0 CHECK (inferred IN (0,1)),
    ai_entry_gate_enabled INTEGER CHECK (ai_entry_gate_enabled IN (0,1)),
    ai_entry_gate_source TEXT,
    ai_research_enabled INTEGER CHECK (ai_research_enabled IN (0,1)),
    simplified_runtime_enabled INTEGER CHECK (simplified_runtime_enabled IN (0,1)),
    execution_mode TEXT NOT NULL CHECK (execution_mode IN ('paper','live','unknown')),
    provider TEXT,
    model_tag TEXT,
    prompt_version TEXT,
    risk_profile TEXT,
    config_hash TEXT NOT NULL,
    config_snapshot_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS risk_decisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    signal_id INTEGER REFERENCES signals(id),
    approved INTEGER NOT NULL CHECK (approved IN (0,1)),
    reason TEXT NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    proposed_qty REAL,
    sized_qty REAL,
    equity_snapshot REAL NOT NULL,
    metrics_json TEXT,  -- serialized risk snapshot
    model_tag TEXT,
    trace_id TEXT,
    decision_context_id INTEGER REFERENCES decision_contexts(id)
);

CREATE TABLE IF NOT EXISTS signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    symbol TEXT NOT NULL,
    thesis TEXT,
    confidence REAL,
    source TEXT NOT NULL,  -- 'rules' | 'openai/...' etc.
    model_tag TEXT,
    features_json TEXT,
    decision_context_id INTEGER REFERENCES decision_contexts(id)
);

CREATE INDEX IF NOT EXISTS idx_signals_created_julianday
ON signals (julianday(created_at));

CREATE TABLE IF NOT EXISTS ai_research_memos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    signal_id INTEGER REFERENCES signals(id),
    symbol TEXT NOT NULL,
    provider TEXT NOT NULL,
    model_tag TEXT NOT NULL,
    prompt_version TEXT NOT NULL,
    input_hash TEXT NOT NULL,
    verdict TEXT NOT NULL,
    confidence REAL,
    used_only_provided_data INTEGER NOT NULL CHECK (used_only_provided_data IN (0,1)),
    validation_passed INTEGER NOT NULL CHECK (validation_passed IN (0,1)),
    memo_json TEXT NOT NULL,
    decision_context_id INTEGER REFERENCES decision_contexts(id)
);

CREATE INDEX IF NOT EXISTS idx_ai_research_memos_created_julianday
ON ai_research_memos (julianday(created_at));

CREATE INDEX IF NOT EXISTS idx_ai_research_memos_signal_id
ON ai_research_memos (signal_id);

CREATE TABLE IF NOT EXISTS ai_candidate_outcomes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    resolved_at TEXT,
    memo_id INTEGER NOT NULL UNIQUE REFERENCES ai_research_memos(id),
    signal_id INTEGER REFERENCES signals(id),
    symbol TEXT NOT NULL,
    provider TEXT NOT NULL,
    model_tag TEXT NOT NULL,
    policy_tag TEXT NOT NULL,
    decision_at TEXT NOT NULL,
    decision_session_date TEXT NOT NULL,
    verdict TEXT NOT NULL CHECK (verdict IN ('approve','watch','reject')),
    confidence REAL,
    reference_price REAL,
    price_source TEXT NOT NULL,
    comparison_notional REAL NOT NULL DEFAULT 30.0,
    d0_session_date TEXT,
    d0_close REAL,
    d0_return_pct REAL,
    d0_hypothetical_pnl REAL,
    d1_session_date TEXT,
    d1_close REAL,
    d1_return_pct REAL,
    d1_hypothetical_pnl REAL,
    d3_session_date TEXT,
    d3_close REAL,
    d3_return_pct REAL,
    d3_hypothetical_pnl REAL,
    d5_session_date TEXT,
    d5_close REAL,
    d5_return_pct REAL,
    d5_hypothetical_pnl REAL,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending','partial','resolved','invalid_reference')),
    last_error TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_ai_candidate_outcome_session
ON ai_candidate_outcomes (provider, model_tag, policy_tag, symbol, decision_session_date);

CREATE INDEX IF NOT EXISTS idx_ai_candidate_outcome_pending
ON ai_candidate_outcomes (status, decision_session_date);

CREATE INDEX IF NOT EXISTS idx_ai_candidate_outcome_decision_julianday
ON ai_candidate_outcomes (julianday(decision_at));

CREATE TABLE IF NOT EXISTS orders (
    client_order_id TEXT PRIMARY KEY,
    broker_order_id TEXT,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    qty REAL NOT NULL,
    order_type TEXT,
    status TEXT NOT NULL,
    filled_qty REAL DEFAULT 0,
    avg_fill_price REAL,
    submitted_at TEXT,
    filled_at TEXT,
    risk_decision_id INTEGER REFERENCES risk_decisions(id),
    rationale TEXT,
    execution_mode TEXT NOT NULL DEFAULT 'unknown'
        CHECK (execution_mode IN ('paper','live','unknown')),
    decision_context_id INTEGER REFERENCES decision_contexts(id)
);

CREATE INDEX IF NOT EXISTS idx_orders_filled_exit_time
ON orders (lower(status), lower(side), julianday(COALESCE(filled_at, submitted_at)));

CREATE TABLE IF NOT EXISTS pending_exits (
    symbol TEXT PRIMARY KEY,
    broker_order_id TEXT,
    client_order_id TEXT,
    reason TEXT,
    qty REAL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS account_risk_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),  -- singleton row
    day_date TEXT NOT NULL,
    day_start_equity REAL NOT NULL,
    week_start_date TEXT NOT NULL,
    week_start_equity REAL NOT NULL,
    peak_equity REAL NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS runtime_config (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS journal_entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL,  -- YYYY-MM-DD local report date
    kind TEXT NOT NULL CHECK (kind IN ('daily','weekly')),
    content TEXT NOT NULL,  -- markdown or structured
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Initial state: safe default. Manual /resume is required after a fresh DB.
INSERT OR IGNORE INTO system_state (id, state, updated_at)
VALUES (1, 'HALTED', datetime('now'));
