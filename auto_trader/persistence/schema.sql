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
    trace_id TEXT
);

CREATE TABLE IF NOT EXISTS signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    symbol TEXT NOT NULL,
    thesis TEXT,
    confidence REAL,
    source TEXT NOT NULL,  -- 'rules' | 'openai/...' etc.
    model_tag TEXT,
    features_json TEXT
);

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
    memo_json TEXT NOT NULL
);

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
    rationale TEXT
);

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
