"""Lightweight async SQLite persistence layer (aiosqlite).

Hardens:
- HALTED state survives process restart (single-row system_state)
- Append-only risk/audit tables ready for later wiring
- Low memory / fast startup friendly (no connection pooling beyond one writer)
- All timestamps UTC ISO8601 TEXT
- Used ONLY for state machine persistence + future journal (no trading logic added here)

Wires directly to existing persistence/schema.sql .
"""
import asyncio
import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import aiosqlite

from auto_trader.core.models import SystemState
from auto_trader.utils.logging import get_logger

log = get_logger("auto_trader.persistence.db")

_DB_PATH: Path = Path("auto_trader.db")
_DB_LOCK = asyncio.Lock()  # single writer guarantee (simple & cheap for v1)
CHARGEABLE_AI_RESEARCH_PROMPT_VERSIONS = (
    "ai_research_committee/v0",
    "ai_research_committee/v1",
    "ai_research_committee/v2",
    "ai_research_committee/v3",
    "ai_research_single/v1",
    "ai_research_failure/v0",
)
CHARGEABLE_AI_POSTMORTEM_PROMPT_VERSIONS = (
    "ai_postmortem_review/v0",
    "ai_postmortem_failure/v0",
)
CHARGEABLE_AI_POSTMORTEM_ESCALATION_PROMPT_VERSIONS = (
    "ai_postmortem_escalation/v0",
    "ai_postmortem_escalation_failure/v0",
)
EXIT_SIDES = {"sell", "close", "sell_short", "buy_to_cover"}
ADMINISTRATIVE_EXIT_RATIONALES = {"", "broker_reconciliation", "unknown"}
AUTO_EXIT_JOURNAL_RE = re.compile(
    r"Auto-exit submitted for (?P<symbol>[A-Z0-9.\-]+): (?P<reason>[^.]+)\. "
    r"Order (?P<order_id>[A-Za-z0-9._:\-]+)",
    re.IGNORECASE,
)


def _utc_iso(dt: datetime) -> str:
    return dt.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _clean_rationale(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _is_exit_side(side: Any) -> bool:
    return str(side or "").strip().lower() in EXIT_SIDES


def _is_administrative_exit_rationale(value: Any) -> bool:
    return str(value or "").strip().lower() in ADMINISTRATIVE_EXIT_RATIONALES


async def _pending_exit_reason_for_order(
    db: aiosqlite.Connection,
    *,
    symbol: str,
    client_order_id: str,
    broker_order_id: str,
) -> str | None:
    """Return a pending-exit reason only when the order ID proves the match."""
    cur = await db.execute(
        """
        SELECT reason
        FROM pending_exits
        WHERE upper(symbol) = ?
          AND status = 'pending'
          AND reason IS NOT NULL
          AND reason != ''
          AND (
                broker_order_id = ?
             OR client_order_id = ?
             OR broker_order_id = ?
             OR client_order_id = ?
          )
        LIMIT 1
        """,
        (symbol.upper(), broker_order_id, broker_order_id, client_order_id, client_order_id),
    )
    row = await cur.fetchone()
    return _clean_rationale(row["reason"]) if row else None


def configure_db_path(path: str | Path) -> None:
    """Called at startup from settings so DB location is configurable."""
    global _DB_PATH
    _DB_PATH = Path(path).expanduser().resolve()


def get_configured_db_path() -> Path:
    """Return the currently configured SQLite DB path for read-only reports."""
    return _DB_PATH


async def init_db() -> None:
    """Create tables from schema.sql if missing. Idempotent. Fast on ARM."""
    schema_path = Path(__file__).parent / "schema.sql"
    if not schema_path.exists():
        log.warning("schema.sql missing - state persistence degraded")
        return

    async with aiosqlite.connect(_DB_PATH) as db:
        schema = schema_path.read_text()
        await db.executescript(schema)
        await _ensure_column(
            db,
            table="risk_decisions",
            column="signal_id",
            definition="INTEGER REFERENCES signals(id)",
        )
        await db.commit()
    log.info("db_initialized", path=str(_DB_PATH), size_kb=round(_DB_PATH.stat().st_size / 1024, 1) if _DB_PATH.exists() else 0)


async def _ensure_column(
    db: aiosqlite.Connection,
    *,
    table: str,
    column: str,
    definition: str,
) -> None:
    cur = await db.execute(f"PRAGMA table_info({table})")
    rows = await cur.fetchall()
    if any(str(row[1]) == column for row in rows):
        return
    await db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


async def _get_conn() -> aiosqlite.Connection:
    conn = await aiosqlite.connect(_DB_PATH)
    conn.row_factory = aiosqlite.Row
    return conn


async def _get_read_conn() -> aiosqlite.Connection:
    conn = await aiosqlite.connect(f"{_DB_PATH.resolve().as_uri()}?mode=ro", uri=True)
    conn.row_factory = aiosqlite.Row
    return conn


async def load_system_state() -> tuple[SystemState, dict[str, Any]]:
    """
    Restore last known state + metadata.

    SAFETY RULE (per SOURCE_OF_TRUTH + Reviewer):
    On any error, missing row, or corrupt data → default to HALTED.
    This forces manual /resume <token> after any kill or crash.
    """
    async with _DB_LOCK:
        try:
            await init_db()
            db = await _get_conn()
            try:
                cur = await db.execute(
                    "SELECT state, halted_at, halt_reason, resumed_at, last_equity, updated_at FROM system_state WHERE id=1"
                )
                row = await cur.fetchone()
                if row:
                    state = SystemState(row["state"])
                    meta = {
                        "halted_at": row["halted_at"],
                        "halt_reason": row["halt_reason"],
                        "resumed_at": row["resumed_at"],
                        "last_equity": row["last_equity"],
                        "updated_at": row["updated_at"],
                    }
                    log.info("state_restored_from_db", state=state.value, meta=meta)
                    return state, meta
                else:
                    # Clean first run or no prior state — safe default but not a "failure"
                    log.info("no_prior_state_found_defaulting_halted")
                    return SystemState.HALTED, {"reason": "no_prior_state"}
            finally:
                await db.close()
        except Exception as e:
            log.critical("state_load_failed_defaulting_halted", error=str(e))

    # Only real errors reach here
    log.critical("state_defaulted_to_halted_for_safety")
    return SystemState.HALTED, {"reason": "load_failed_or_missing"}


async def save_system_state(state: SystemState, reason: str | None = None, equity: float | None = None) -> None:
    """Persist state change using UPSERT. Critical for HALTED durability across restarts."""
    now = datetime.now(UTC).isoformat() + "Z"
    async with _DB_LOCK:
        try:
            await init_db()  # symmetry with load; safe if already initialized
            db = await _get_conn()
            try:
                # Proper UPSERT for the singleton row (id=1)
                await db.execute(
                    """
                    INSERT INTO system_state (id, state, halted_at, halt_reason, resumed_at, last_equity, updated_at)
                    VALUES (1, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        state=excluded.state,
                        halted_at=COALESCE(excluded.halted_at, system_state.halted_at),
                        halt_reason=COALESCE(excluded.halt_reason, system_state.halt_reason),
                        resumed_at=COALESCE(excluded.resumed_at, system_state.resumed_at),
                        last_equity=COALESCE(excluded.last_equity, system_state.last_equity),
                        updated_at=excluded.updated_at
                    """,
                    (
                        state.value,
                        now if state == SystemState.HALTED else None,
                        reason if state == SystemState.HALTED else None,
                        now if state == SystemState.ACTIVE else None,
                        equity,
                        now,
                    ),
                )
                await db.commit()
            finally:
                await db.close()
            log.info("state_persisted", state=state.value, reason=reason)
        except Exception as e:
            log.critical("state_persist_failed", state=state.value, error=str(e))
            # Never raise — in-memory HALTED is still effective for this process


async def log_risk_decision(**kwargs: Any) -> int | None:
    """Persist a risk decision audit row and return its row id."""
    async with _DB_LOCK:
        try:
            await init_db()
            db = await _get_conn()
            try:
                cur = await db.execute(
                    """
                    INSERT INTO risk_decisions (
                        signal_id, approved, reason, symbol, side, proposed_qty, sized_qty,
                        equity_snapshot, metrics_json, model_tag, trace_id
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        kwargs.get("signal_id"),
                        1 if kwargs.get("approved") else 0,
                        str(kwargs.get("reason", "")),
                        str(kwargs.get("symbol", "")).upper(),
                        str(kwargs.get("side", "")),
                        kwargs.get("proposed_qty"),
                        kwargs.get("sized_qty"),
                        float(kwargs.get("equity_snapshot") or 0.0),
                        json.dumps(kwargs.get("risk_metrics") or {}, sort_keys=True),
                        kwargs.get("model_tag"),
                        kwargs.get("trace_id"),
                    ),
                )
                await db.commit()
                return int(cur.lastrowid) if cur.lastrowid is not None else None
            finally:
                await db.close()
        except Exception as e:
            log.error("risk_decision_log_failed", error=str(e))
            return None


async def log_signal(
    *,
    symbol: str,
    thesis: str | None,
    confidence: float | None,
    source: str,
    model_tag: str | None = None,
    features: dict[str, Any] | None = None,
) -> int | None:
    """Persist a pre-risk candidate signal with optional enrichment features."""
    async with _DB_LOCK:
        try:
            await init_db()
            db = await _get_conn()
            try:
                cur = await db.execute(
                    """
                    INSERT INTO signals (
                        symbol, thesis, confidence, source, model_tag, features_json
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(symbol).upper(),
                        thesis,
                        confidence,
                        source,
                        model_tag,
                        json.dumps(features or {}, sort_keys=True),
                    ),
                )
                await db.commit()
                return int(cur.lastrowid) if cur.lastrowid is not None else None
            finally:
                await db.close()
        except Exception as e:
            log.error("signal_log_failed", symbol=symbol, source=source, error=str(e))
            return None


async def log_ai_research_memo(
    *,
    signal_id: int | None,
    symbol: str,
    provider: str,
    model_tag: str,
    prompt_version: str,
    input_hash: str,
    verdict: str,
    confidence: float | None,
    used_only_provided_data: bool,
    validation_passed: bool,
    memo: dict[str, Any],
) -> int | None:
    """Persist an AI/shadow committee research memo. Advisory only; never an order gate."""
    async with _DB_LOCK:
        try:
            await init_db()
            db = await _get_conn()
            try:
                cur = await db.execute(
                    """
                    INSERT INTO ai_research_memos (
                        signal_id, symbol, provider, model_tag, prompt_version, input_hash,
                        verdict, confidence, used_only_provided_data, validation_passed, memo_json
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        signal_id,
                        str(symbol).upper(),
                        str(provider),
                        str(model_tag),
                        str(prompt_version),
                        str(input_hash),
                        str(verdict),
                        confidence,
                        1 if used_only_provided_data else 0,
                        1 if validation_passed else 0,
                        json.dumps(memo, sort_keys=True),
                    ),
                )
                await db.commit()
                memo_id = int(cur.lastrowid) if cur.lastrowid is not None else None
                log.info(
                    "ai_research_memo_logged",
                    symbol=symbol,
                    provider=provider,
                    verdict=verdict,
                    validation_passed=validation_passed,
                )
                return memo_id
            finally:
                await db.close()
        except Exception as e:
            log.error("ai_research_memo_log_failed", symbol=symbol, error=str(e))
            return None


async def get_latest_ai_research_memos(
    limit: int = 10,
    *,
    symbol: str | None = None,
    exclude_providers: tuple[str, ...] = (),
) -> list[dict[str, Any]]:
    """Return latest AI/shadow committee memos for audit/status surfaces."""
    return await _latest_ai_research_memos(
        limit=limit,
        symbol=symbol,
        exclude_providers=exclude_providers,
        initialize=True,
    )


async def read_latest_ai_research_memos(
    limit: int = 10,
    *,
    symbol: str | None = None,
    exclude_providers: tuple[str, ...] = (),
) -> list[dict[str, Any]]:
    """Read latest AI research memos without creating or migrating the database."""
    return await _latest_ai_research_memos(
        limit=limit,
        symbol=symbol,
        exclude_providers=exclude_providers,
        initialize=False,
    )


async def _latest_ai_research_memos(
    *,
    limit: int,
    symbol: str | None,
    exclude_providers: tuple[str, ...],
    initialize: bool,
) -> list[dict[str, Any]]:
    async with _DB_LOCK:
        try:
            if initialize:
                await init_db()
            elif not _DB_PATH.exists():
                return []
            db = await _get_conn() if initialize else await _get_read_conn()
            try:
                conditions: list[str] = []
                params: list[Any] = []
                clean_symbol = str(symbol or "").strip().upper()
                if clean_symbol:
                    conditions.append("symbol = ?")
                    params.append(clean_symbol)
                clean_excluded = tuple(
                    str(provider or "").strip().lower()
                    for provider in exclude_providers
                    if str(provider or "").strip()
                )
                if clean_excluded:
                    placeholders = ", ".join("?" for _ in clean_excluded)
                    conditions.append(f"lower(provider) NOT IN ({placeholders})")
                    params.extend(clean_excluded)
                where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
                params.append(int(limit))
                cur = await db.execute(
                    f"""
                    SELECT id, created_at, signal_id, symbol, provider, model_tag,
                           prompt_version, input_hash, verdict, confidence,
                           used_only_provided_data, validation_passed, memo_json
                    FROM ai_research_memos
                    {where_clause}
                    ORDER BY id DESC
                    LIMIT ?
                    """,
                    tuple(params),
                )
                rows = await cur.fetchall()
                return [
                    {
                        "id": row["id"],
                        "created_at": row["created_at"],
                        "signal_id": row["signal_id"],
                        "symbol": row["symbol"],
                        "provider": row["provider"],
                        "model_tag": row["model_tag"],
                        "prompt_version": row["prompt_version"],
                        "input_hash": row["input_hash"],
                        "verdict": row["verdict"],
                        "confidence": row["confidence"],
                        "used_only_provided_data": bool(row["used_only_provided_data"]),
                        "validation_passed": bool(row["validation_passed"]),
                        "memo": json.loads(row["memo_json"] or "{}"),
                    }
                    for row in rows
                ]
            finally:
                await db.close()
        except Exception as e:
            log.error("ai_research_memos_list_failed", error=str(e))
            return []


async def get_latest_ai_research_memo_for_symbol(
    *,
    provider: str,
    symbol: str,
    model_tag: str | None = None,
    today_utc: bool = False,
    prompt_versions: tuple[str, ...] | None = None,
) -> dict[str, Any] | None:
    """Return the latest AI memo for a provider/symbol slice."""
    conditions = ["provider = ?", "upper(symbol) = ?"]
    params: list[Any] = [str(provider), str(symbol).upper()]
    if model_tag is not None:
        conditions.append("model_tag = ?")
        params.append(str(model_tag))
    if today_utc:
        conditions.append("date(created_at) = date('now')")
    if prompt_versions:
        conditions.append(f"prompt_version IN ({','.join('?' for _ in prompt_versions)})")
        params.extend(prompt_versions)
    where = " AND ".join(conditions)
    async with _DB_LOCK:
        try:
            await init_db()
            db = await _get_conn()
            try:
                cur = await db.execute(
                    f"""
                    SELECT id, created_at, signal_id, symbol, provider, model_tag,
                           prompt_version, input_hash, verdict, confidence,
                           used_only_provided_data, validation_passed, memo_json
                    FROM ai_research_memos
                    WHERE {where}
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    tuple(params),
                )
                row = await cur.fetchone()
                if row is None:
                    return None
                return {
                    "id": row["id"],
                    "created_at": row["created_at"],
                    "signal_id": row["signal_id"],
                    "symbol": row["symbol"],
                    "provider": row["provider"],
                    "model_tag": row["model_tag"],
                    "prompt_version": row["prompt_version"],
                    "input_hash": row["input_hash"],
                    "verdict": row["verdict"],
                    "confidence": row["confidence"],
                    "used_only_provided_data": bool(row["used_only_provided_data"]),
                    "validation_passed": bool(row["validation_passed"]),
                    "memo": json.loads(row["memo_json"] or "{}"),
                }
            finally:
                await db.close()
        except Exception as e:
            log.error("ai_research_memo_symbol_lookup_failed", provider=provider, symbol=symbol, error=str(e))
            raise


async def count_ai_research_memos(
    *,
    provider: str | None = None,
    input_hash: str | None = None,
    today_utc: bool = False,
) -> int | None:
    """Count AI research audit rows for budget and dedupe gates."""
    conditions: list[str] = []
    params: list[Any] = []
    if provider is not None:
        conditions.append("provider = ?")
        params.append(str(provider))
    if input_hash is not None:
        conditions.append("input_hash = ?")
        params.append(str(input_hash))
    if today_utc:
        conditions.append("date(created_at) = date('now')")
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    async with _DB_LOCK:
        try:
            await init_db()
            db = await _get_conn()
            try:
                cur = await db.execute(
                    f"SELECT COUNT(*) AS count FROM ai_research_memos {where}",
                    tuple(params),
                )
                row = await cur.fetchone()
                return int(row["count"] if row is not None else 0)
            finally:
                await db.close()
        except Exception as e:
            log.error("ai_research_memo_count_failed", provider=provider, input_hash=input_hash, error=str(e))
            return None


async def count_ai_research_chargeable_attempts(
    *,
    provider: str | None = None,
    input_hash: str | None = None,
    today_utc: bool = False,
) -> int | None:
    """Count real-provider AI attempts that should consume paid-call budget."""
    conditions: list[str] = [
        "provider NOT IN (?, ?)",
        f"prompt_version IN ({','.join('?' for _ in CHARGEABLE_AI_RESEARCH_PROMPT_VERSIONS)})",
    ]
    params: list[Any] = ["shadow", "multi", *CHARGEABLE_AI_RESEARCH_PROMPT_VERSIONS]
    if provider is not None:
        conditions.append("provider = ?")
        params.append(str(provider))
    if input_hash is not None:
        conditions.append("input_hash = ?")
        params.append(str(input_hash))
    if today_utc:
        conditions.append("date(created_at) = date('now')")
    where = f"WHERE {' AND '.join(conditions)}"
    async with _DB_LOCK:
        try:
            await init_db()
            db = await _get_conn()
            try:
                cur = await db.execute(
                    f"SELECT COUNT(*) AS count FROM ai_research_memos {where}",
                    tuple(params),
                )
                row = await cur.fetchone()
                return int(row["count"] if row is not None else 0)
            finally:
                await db.close()
        except Exception as e:
            log.error(
                "ai_research_chargeable_attempt_count_failed",
                provider=provider,
                input_hash=input_hash,
                error=str(e),
            )
            return None


async def count_ai_postmortem_chargeable_attempts(
    *,
    provider: str | None = None,
    input_hash: str | None = None,
    today_utc: bool = False,
) -> int | None:
    """Count real-provider AI postmortem attempts against the separate postmortem budget."""
    return await _count_ai_postmortem_chargeable_attempts(
        CHARGEABLE_AI_POSTMORTEM_PROMPT_VERSIONS,
        provider=provider,
        input_hash=input_hash,
        today_utc=today_utc,
        log_event="ai_postmortem_chargeable_attempt_count_failed",
    )


async def count_ai_postmortem_escalation_chargeable_attempts(
    *,
    provider: str | None = None,
    input_hash: str | None = None,
    today_utc: bool = False,
) -> int | None:
    """Count Fable/escalation-style postmortem attempts against the escalation cap."""
    return await _count_ai_postmortem_chargeable_attempts(
        CHARGEABLE_AI_POSTMORTEM_ESCALATION_PROMPT_VERSIONS,
        provider=provider,
        input_hash=input_hash,
        today_utc=today_utc,
        log_event="ai_postmortem_escalation_chargeable_attempt_count_failed",
    )


async def _count_ai_postmortem_chargeable_attempts(
    prompt_versions: tuple[str, ...],
    *,
    provider: str | None,
    input_hash: str | None,
    today_utc: bool,
    log_event: str,
) -> int | None:
    conditions: list[str] = [
        "provider NOT IN (?, ?)",
        f"prompt_version IN ({','.join('?' for _ in prompt_versions)})",
    ]
    params: list[Any] = ["shadow", "multi", *prompt_versions]
    if provider is not None:
        conditions.append("provider = ?")
        params.append(str(provider))
    if input_hash is not None:
        conditions.append("input_hash = ?")
        params.append(str(input_hash))
    if today_utc:
        conditions.append("date(created_at) = date('now')")
    where = f"WHERE {' AND '.join(conditions)}"
    async with _DB_LOCK:
        try:
            await init_db()
            db = await _get_conn()
            try:
                cur = await db.execute(
                    f"SELECT COUNT(*) AS count FROM ai_research_memos {where}",
                    tuple(params),
                )
                row = await cur.fetchone()
                return int(row["count"] if row is not None else 0)
            finally:
                await db.close()
        except Exception as e:
            log.error(
                log_event,
                provider=provider,
                input_hash=input_hash,
                error=str(e),
            )
            return None


async def upsert_order_record(order: dict[str, Any], risk_decision_id: int | None = None, rationale: str | None = None) -> bool:
    """Persist or refresh an order row from broker/manager normalized data."""
    client_order_id = str(order.get("client_order_id") or order.get("id") or order.get("broker_order_id") or "")
    broker_order_id = str(order.get("broker_order_id") or order.get("id") or client_order_id)
    if not client_order_id or not broker_order_id:
        log.warning("order_record_missing_id_skipped", order=order)
        return False
    symbol = str(order.get("symbol", "")).upper()
    side = str(order.get("side", ""))
    resolved_rationale = _clean_rationale(rationale or order.get("rationale"))

    async with _DB_LOCK:
        try:
            await init_db()
            db = await _get_conn()
            try:
                if _is_exit_side(side) and _is_administrative_exit_rationale(resolved_rationale):
                    pending_reason = await _pending_exit_reason_for_order(
                        db,
                        symbol=symbol,
                        client_order_id=client_order_id,
                        broker_order_id=broker_order_id,
                    )
                    if pending_reason:
                        resolved_rationale = pending_reason
                await db.execute(
                    """
                    INSERT INTO orders (
                        client_order_id, broker_order_id, symbol, side, qty, order_type,
                        status, filled_qty, avg_fill_price, submitted_at, filled_at,
                        risk_decision_id, rationale
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(client_order_id) DO UPDATE SET
                        broker_order_id=excluded.broker_order_id,
                        symbol=excluded.symbol,
                        side=excluded.side,
                        qty=excluded.qty,
                        order_type=excluded.order_type,
                        status=excluded.status,
                        filled_qty=excluded.filled_qty,
                        avg_fill_price=COALESCE(excluded.avg_fill_price, orders.avg_fill_price),
                        submitted_at=COALESCE(excluded.submitted_at, orders.submitted_at),
                        filled_at=COALESCE(excluded.filled_at, orders.filled_at),
                        risk_decision_id=COALESCE(excluded.risk_decision_id, orders.risk_decision_id),
                        rationale=CASE
                            WHEN excluded.rationale IS NULL OR excluded.rationale = '' THEN orders.rationale
                            WHEN lower(excluded.rationale) = 'broker_reconciliation'
                                 AND orders.rationale IS NOT NULL
                                 AND lower(orders.rationale) NOT IN ('', 'broker_reconciliation', 'unknown')
                                THEN orders.rationale
                            WHEN orders.rationale IS NULL
                                 OR orders.rationale = ''
                                 OR lower(orders.rationale) IN ('broker_reconciliation', 'unknown')
                                THEN excluded.rationale
                            ELSE orders.rationale
                        END
                    """,
                    (
                        client_order_id,
                        broker_order_id,
                        symbol,
                        side,
                        float(order.get("qty") or 0.0),
                        str(order.get("order_type") or "market"),
                        str(order.get("status") or "unknown"),
                        float(order.get("filled_qty") or 0.0),
                        order.get("avg_fill_price"),
                        order.get("submitted_at"),
                        order.get("filled_at"),
                        risk_decision_id,
                        resolved_rationale,
                    ),
                )
                await db.commit()
            finally:
                await db.close()
            log.info("order_record_upserted", broker_order_id=broker_order_id, symbol=order.get("symbol"))
            return True
        except Exception as e:
            log.error("order_record_upsert_failed", error=str(e), broker_order_id=broker_order_id)
            return False


async def count_entry_orders_since(start_utc_iso: str) -> int:
    """Count durable buy/long entry orders submitted since a UTC boundary."""
    async with _DB_LOCK:
        try:
            await init_db()
            db = await _get_conn()
            try:
                cur = await db.execute(
                    """
                    SELECT COUNT(*) AS count
                    FROM orders
                    WHERE COALESCE(submitted_at, '') >= ?
                      AND lower(side) IN ('long', 'buy')
                      AND lower(status) NOT IN ('canceled', 'cancelled', 'rejected', 'expired')
                    """,
                    (start_utc_iso,),
                )
                row = await cur.fetchone()
                return int(row["count"] if row else 0)
            finally:
                await db.close()
        except Exception as e:
            log.error("entry_order_count_failed", error=str(e))
            raise


async def get_entry_pressure_counts_since(start_utc_iso: str) -> dict[str, Any]:
    """Return bounded structured counts for read-only entry-pressure diagnostics."""
    async with _DB_LOCK:
        try:
            await init_db()
            db = await _get_conn()
            try:
                signal_cur = await db.execute(
                    """
                    SELECT COUNT(*) AS count, COUNT(DISTINCT upper(symbol)) AS symbols
                    FROM signals
                    WHERE datetime(created_at) >= datetime(?)
                    """,
                    (start_utc_iso,),
                )
                signal_row = await signal_cur.fetchone()

                latest_signal_cur = await db.execute(
                    """
                    SELECT id, created_at, symbol, source, model_tag
                    FROM signals
                    WHERE datetime(created_at) >= datetime(?)
                    ORDER BY datetime(created_at) DESC, id DESC
                    LIMIT 1
                    """,
                    (start_utc_iso,),
                )
                latest_signal = await latest_signal_cur.fetchone()

                memo_cur = await db.execute(
                    """
                    SELECT prompt_version, lower(verdict) AS verdict, validation_passed, COUNT(*) AS count
                    FROM ai_research_memos
                    WHERE datetime(created_at) >= datetime(?)
                    GROUP BY prompt_version, lower(verdict), validation_passed
                    ORDER BY count DESC, prompt_version ASC, verdict ASC
                    LIMIT 12
                    """,
                    (start_utc_iso,),
                )
                memo_rows = await memo_cur.fetchall()

                risk_cur = await db.execute(
                    """
                    SELECT approved, reason, COUNT(*) AS count
                    FROM risk_decisions
                    WHERE datetime(created_at) >= datetime(?)
                    GROUP BY approved, reason
                    ORDER BY count DESC, id DESC
                    LIMIT 8
                    """,
                    (start_utc_iso,),
                )
                risk_rows = await risk_cur.fetchall()

                latest_order_cur = await db.execute(
                    """
                    SELECT symbol, side, status, submitted_at, filled_at
                    FROM orders
                    WHERE datetime(COALESCE(submitted_at, filled_at, '')) >= datetime(?)
                      AND lower(side) IN ('long', 'buy')
                    ORDER BY datetime(COALESCE(submitted_at, filled_at, '')) DESC, rowid DESC
                    LIMIT 1
                    """,
                    (start_utc_iso,),
                )
                latest_order = await latest_order_cur.fetchone()

                return {
                    "since_utc": start_utc_iso,
                    "signals": {
                        "count": int(signal_row["count"] if signal_row else 0),
                        "symbols": int(signal_row["symbols"] if signal_row else 0),
                        "latest": dict(latest_signal) if latest_signal else None,
                    },
                    "memos": [
                        {
                            "prompt_version": row["prompt_version"],
                            "verdict": row["verdict"],
                            "validation_passed": bool(row["validation_passed"]),
                            "count": int(row["count"]),
                        }
                        for row in memo_rows
                    ],
                    "risk_decisions": [
                        {
                            "approved": bool(row["approved"]),
                            "reason": row["reason"],
                            "count": int(row["count"]),
                        }
                        for row in risk_rows
                    ],
                    "latest_entry_order": dict(latest_order) if latest_order else None,
                }
            finally:
                await db.close()
        except Exception as e:
            log.error("entry_pressure_counts_failed", error=str(e))
            raise


async def update_account_risk_state(
    *,
    equity: float,
    day_date: str,
    week_start_date: str,
) -> dict[str, Any]:
    """Persist account equity baselines and return loss/drawdown metrics."""
    clean_equity = float(equity)
    if clean_equity <= 0:
        raise ValueError("equity must be positive to update account risk state")
    now = datetime.now(UTC).isoformat() + "Z"
    async with _DB_LOCK:
        await init_db()
        db = await _get_conn()
        try:
            cur = await db.execute(
                """
                SELECT day_date, day_start_equity, week_start_date, week_start_equity, peak_equity
                FROM account_risk_state
                WHERE id = 1
                """
            )
            row = await cur.fetchone()
            if row is None:
                day_start_equity = clean_equity
                week_start_equity = clean_equity
                peak_equity = clean_equity
            else:
                day_start_equity = (
                    clean_equity
                    if str(row["day_date"]) != day_date
                    else float(row["day_start_equity"])
                )
                week_start_equity = (
                    clean_equity
                    if str(row["week_start_date"]) != week_start_date
                    else float(row["week_start_equity"])
                )
                peak_equity = max(float(row["peak_equity"]), clean_equity)

            await db.execute(
                """
                INSERT INTO account_risk_state (
                    id, day_date, day_start_equity, week_start_date, week_start_equity, peak_equity, updated_at
                )
                VALUES (1, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    day_date=excluded.day_date,
                    day_start_equity=excluded.day_start_equity,
                    week_start_date=excluded.week_start_date,
                    week_start_equity=excluded.week_start_equity,
                    peak_equity=excluded.peak_equity,
                    updated_at=excluded.updated_at
                """,
                (
                    day_date,
                    day_start_equity,
                    week_start_date,
                    week_start_equity,
                    peak_equity,
                    now,
                ),
            )
            await db.commit()
            daily_loss_pct = ((clean_equity - day_start_equity) / day_start_equity * 100.0) if day_start_equity else 0.0
            weekly_loss_pct = (
                ((clean_equity - week_start_equity) / week_start_equity * 100.0) if week_start_equity else 0.0
            )
            peak_drawdown_pct = ((clean_equity - peak_equity) / peak_equity * 100.0) if peak_equity else 0.0
            metrics = {
                "equity": clean_equity,
                "day_date": day_date,
                "day_start_equity": day_start_equity,
                "daily_loss_pct": daily_loss_pct,
                "week_start_date": week_start_date,
                "week_start_equity": week_start_equity,
                "weekly_loss_pct": weekly_loss_pct,
                "peak_equity": peak_equity,
                "peak_drawdown_pct": peak_drawdown_pct,
                "updated_at": now,
            }
            log.info(
                "account_risk_state_updated",
                equity=clean_equity,
                daily_loss_pct=daily_loss_pct,
                weekly_loss_pct=weekly_loss_pct,
                peak_drawdown_pct=peak_drawdown_pct,
            )
            return metrics
        finally:
            await db.close()


async def set_runtime_config_value(key: str, value: str) -> bool:
    """Persist a runtime configuration value controlled by Telegram/operator commands."""
    clean_key = str(key or "").strip().lower()
    clean_value = str(value).strip().lower()
    if not clean_key:
        raise ValueError("runtime config key is required")
    now = datetime.now(UTC).isoformat() + "Z"
    async with _DB_LOCK:
        try:
            await init_db()
            db = await _get_conn()
            try:
                await db.execute(
                    """
                    INSERT INTO runtime_config (key, value, updated_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(key) DO UPDATE SET
                        value=excluded.value,
                        updated_at=excluded.updated_at
                    """,
                    (clean_key, clean_value, now),
                )
                await db.commit()
            finally:
                await db.close()
            log.warning("runtime_config_updated", key=clean_key, value=clean_value)
            return True
        except Exception as e:
            log.error("runtime_config_update_failed", key=clean_key, error=str(e))
            return False


async def get_runtime_config_value(key: str) -> str | None:
    """Return a persisted runtime config value, if one exists."""
    clean_key = str(key or "").strip().lower()
    if not clean_key:
        return None
    async with _DB_LOCK:
        try:
            await init_db()
            db = await _get_conn()
            try:
                cur = await db.execute(
                    "SELECT value FROM runtime_config WHERE key = ?",
                    (clean_key,),
                )
                row = await cur.fetchone()
                return str(row["value"]) if row else None
            finally:
                await db.close()
        except Exception as e:
            log.error("runtime_config_lookup_failed", key=clean_key, error=str(e))
            raise


async def get_runtime_config_bool(key: str, *, default: bool) -> bool:
    """Return a runtime config value parsed as bool, with fail-closed defaults."""
    value = await get_runtime_config_value(key)
    if value is None:
        return bool(default)
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on", "enabled"}:
        return True
    if normalized in {"0", "false", "no", "off", "disabled"}:
        return False
    raise ValueError(f"invalid boolean runtime config for {key}: {value}")


async def get_runtime_config_int(
    key: str,
    *,
    default: int,
    minimum: int | None = None,
    maximum: int | None = None,
) -> int:
    """Return a runtime config value parsed as int and validated against explicit bounds."""
    value = await get_runtime_config_value(key)
    if value is None:
        parsed = int(default)
    else:
        try:
            parsed = int(value)
        except (TypeError, ValueError) as e:
            raise ValueError(f"invalid integer runtime config for {key}: {value}") from e
    if minimum is not None and parsed < minimum:
        raise ValueError(f"runtime config {key} below minimum {minimum}: {parsed}")
    if maximum is not None and parsed > maximum:
        raise ValueError(f"runtime config {key} above maximum {maximum}: {parsed}")
    return parsed


async def get_runtime_config_values() -> dict[str, str]:
    """Return all persisted runtime configuration values."""
    async with _DB_LOCK:
        try:
            await init_db()
            db = await _get_conn()
            try:
                cur = await db.execute("SELECT key, value FROM runtime_config ORDER BY key")
                rows = await cur.fetchall()
                return {str(row["key"]): str(row["value"]) for row in rows}
            finally:
                await db.close()
        except Exception as e:
            log.error("runtime_config_list_failed", error=str(e))
            raise


async def record_runtime_capabilities(*, pid: int) -> None:
    """Record capabilities supported by the currently running process."""
    now = _utc_iso(datetime.now(UTC))
    values = {
        "runtime_capability_planned_maintenance_shutdown": "true",
        "runtime_capability_planned_maintenance_pid": str(int(pid)),
    }
    async with _DB_LOCK:
        try:
            await init_db()
            db = await _get_conn()
            try:
                for key, value in values.items():
                    await db.execute(
                        """
                        INSERT INTO runtime_config (key, value, updated_at)
                        VALUES (?, ?, ?)
                        ON CONFLICT(key) DO UPDATE SET
                            value=excluded.value,
                            updated_at=excluded.updated_at
                        """,
                        (key, value, now),
                    )
                await db.commit()
            finally:
                await db.close()
            log.info(
                "runtime_capabilities_recorded",
                planned_maintenance_shutdown=True,
                pid=pid,
            )
        except Exception as e:
            log.error("runtime_capabilities_record_failed", error=str(e))
            raise


async def request_planned_maintenance_shutdown(
    *,
    reason: str,
    ttl_seconds: int = 300,
    allow_live: bool = False,
) -> dict[str, str]:
    """Arm a short-lived, one-shot shutdown marker for planned service restarts."""
    clean_reason = str(reason or "planned maintenance").strip()[:200]
    ttl = max(1, min(int(ttl_seconds), 900))
    now_dt = datetime.now(UTC)
    expires_at = _utc_iso(datetime.fromtimestamp(now_dt.timestamp() + ttl, UTC))
    now = _utc_iso(now_dt)
    values = {
        "planned_maintenance_shutdown": "true",
        "planned_maintenance_reason": clean_reason,
        "planned_maintenance_expires_at": expires_at,
        "planned_maintenance_allow_live": "true" if allow_live else "false",
    }
    async with _DB_LOCK:
        try:
            await init_db()
            db = await _get_conn()
            try:
                for key, value in values.items():
                    await db.execute(
                        """
                        INSERT INTO runtime_config (key, value, updated_at)
                        VALUES (?, ?, ?)
                        ON CONFLICT(key) DO UPDATE SET
                            value=excluded.value,
                            updated_at=excluded.updated_at
                        """,
                        (key, value, now),
                    )
                await db.commit()
            finally:
                await db.close()
            log.warning(
                "planned_maintenance_shutdown_armed",
                reason=clean_reason,
                expires_at=expires_at,
                allow_live=allow_live,
            )
            return values
        except Exception as e:
            log.error("planned_maintenance_shutdown_arm_failed", error=str(e))
            raise


async def consume_planned_maintenance_shutdown(*, alpaca_paper: bool) -> dict[str, str] | None:
    """Consume and clear a valid planned-maintenance shutdown marker, if present."""
    async with _DB_LOCK:
        try:
            await init_db()
            db = await _get_conn()
            try:
                cur = await db.execute(
                    """
                    SELECT key, value FROM runtime_config
                    WHERE key IN (
                        'planned_maintenance_shutdown',
                        'planned_maintenance_reason',
                        'planned_maintenance_expires_at',
                        'planned_maintenance_allow_live'
                    )
                    """
                )
                rows = await cur.fetchall()
                values = {str(row["key"]): str(row["value"]) for row in rows}
                marker_is_set = values.get("planned_maintenance_shutdown", "").lower() == "true"
                expires_raw = values.get("planned_maintenance_expires_at")
                allow_live = values.get("planned_maintenance_allow_live", "").lower() == "true"
                valid = marker_is_set and bool(expires_raw)
                if valid:
                    try:
                        expires_at = datetime.fromisoformat(str(expires_raw).replace("Z", "+00:00"))
                        valid = datetime.now(UTC) <= expires_at
                    except ValueError:
                        valid = False
                if valid and not alpaca_paper and not allow_live:
                    valid = False
                    log.error("planned_maintenance_live_marker_missing_allow_live")

                if rows:
                    await db.execute(
                        """
                        DELETE FROM runtime_config
                        WHERE key IN (
                            'planned_maintenance_shutdown',
                            'planned_maintenance_reason',
                            'planned_maintenance_expires_at',
                            'planned_maintenance_allow_live'
                        )
                        """
                    )
                    await db.commit()
                if not valid:
                    if marker_is_set:
                        log.warning(
                            "planned_maintenance_shutdown_marker_ignored",
                            expires_at=expires_raw,
                            allow_live=allow_live,
                            paper=alpaca_paper,
                        )
                    return None
                log.warning(
                    "planned_maintenance_shutdown_consumed",
                    reason=values.get("planned_maintenance_reason"),
                    allow_live=allow_live,
                    paper=alpaca_paper,
                )
                return values
            finally:
                await db.close()
        except Exception as e:
            log.error("planned_maintenance_shutdown_consume_failed", error=str(e))
            raise


async def reconcile_broker_orders(orders: list[dict[str, Any]]) -> int:
    """Upsert broker orders into SQLite. Returns number successfully persisted."""
    count = 0
    for order in orders:
        if await upsert_order_record(order):
            count += 1
    log.info("broker_orders_reconciled", count=count, attempted=len(orders))
    return count


async def upsert_pending_exit(symbol: str, order: dict[str, Any] | None = None, reason: str | None = None) -> bool:
    """Persist that an exit close is pending so restart cannot duplicate it."""
    clean_symbol = symbol.upper()
    if not clean_symbol:
        return False
    order = order or {}
    now = datetime.now(UTC).isoformat() + "Z"
    async with _DB_LOCK:
        try:
            await init_db()
            db = await _get_conn()
            try:
                await db.execute(
                    """
                    INSERT INTO pending_exits (
                        symbol, broker_order_id, client_order_id, reason, qty, status, created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, 'pending', ?, ?)
                    ON CONFLICT(symbol) DO UPDATE SET
                        broker_order_id=COALESCE(excluded.broker_order_id, pending_exits.broker_order_id),
                        client_order_id=COALESCE(excluded.client_order_id, pending_exits.client_order_id),
                        reason=COALESCE(excluded.reason, pending_exits.reason),
                        qty=COALESCE(excluded.qty, pending_exits.qty),
                        status='pending',
                        updated_at=excluded.updated_at
                    """,
                    (
                        clean_symbol,
                        order.get("broker_order_id") or order.get("id"),
                        order.get("client_order_id") or order.get("id"),
                        reason or order.get("rationale"),
                        order.get("qty"),
                        now,
                        now,
                    ),
                )
                await db.commit()
            finally:
                await db.close()
            log.info("pending_exit_upserted", symbol=clean_symbol, broker_order_id=order.get("broker_order_id") or order.get("id"))
            return True
        except Exception as e:
            log.error("pending_exit_upsert_failed", symbol=clean_symbol, error=str(e))
            return False


async def get_pending_exit_for_symbol(symbol: str) -> dict[str, Any] | None:
    """Return a persisted pending exit, if one exists for the symbol."""
    clean_symbol = symbol.upper()
    async with _DB_LOCK:
        try:
            await init_db()
            db = await _get_conn()
            try:
                cur = await db.execute(
                    """
                    SELECT symbol, broker_order_id, client_order_id, reason, qty, status, created_at, updated_at
                    FROM pending_exits
                    WHERE symbol = ? AND status = 'pending'
                    """,
                    (clean_symbol,),
                )
                row = await cur.fetchone()
                return dict(row) if row else None
            finally:
                await db.close()
        except Exception as e:
            log.error("pending_exit_lookup_failed", symbol=clean_symbol, error=str(e))
            raise


async def get_pending_exit_symbols() -> set[str]:
    """Return all symbols with an active persisted pending exit."""
    async with _DB_LOCK:
        try:
            await init_db()
            db = await _get_conn()
            try:
                cur = await db.execute("SELECT symbol FROM pending_exits WHERE status = 'pending'")
                rows = await cur.fetchall()
                return {str(row["symbol"]).upper() for row in rows}
            finally:
                await db.close()
        except Exception as e:
            log.error("pending_exit_symbols_failed", error=str(e))
            raise


async def get_pending_exits(limit: int = 10) -> list[dict[str, Any]]:
    """Return active pending exits for operator reports."""
    async with _DB_LOCK:
        try:
            await init_db()
            db = await _get_conn()
            try:
                cur = await db.execute(
                    """
                    SELECT symbol, broker_order_id, client_order_id, reason, qty, status, created_at, updated_at
                    FROM pending_exits
                    WHERE status = 'pending'
                    ORDER BY updated_at DESC, created_at DESC
                    LIMIT ?
                    """,
                    (limit,),
                )
                rows = await cur.fetchall()
                return [dict(row) for row in rows]
            finally:
                await db.close()
        except Exception as e:
            log.error("pending_exits_list_failed", error=str(e))
            raise


async def clear_pending_exit(symbol: str) -> bool:
    """Clear a pending exit after its matched broker close order is terminal."""
    clean_symbol = symbol.upper()
    async with _DB_LOCK:
        try:
            await init_db()
            db = await _get_conn()
            try:
                await db.execute("DELETE FROM pending_exits WHERE symbol = ?", (clean_symbol,))
                await db.commit()
            finally:
                await db.close()
            log.info("pending_exit_cleared", symbol=clean_symbol)
            return True
        except Exception as e:
            log.error("pending_exit_clear_failed", symbol=clean_symbol, error=str(e))
            return False


async def get_latest_order_records(limit: int = 5) -> list[dict[str, Any]]:
    """Return latest persisted order records for operator reports."""
    async with _DB_LOCK:
        try:
            await init_db()
            db = await _get_conn()
            try:
                cur = await db.execute(
                    """
                    SELECT
                        client_order_id,
                        broker_order_id,
                        symbol,
                        side,
                        qty,
                        order_type,
                        status,
                        filled_qty,
                        avg_fill_price,
                        submitted_at,
                        filled_at,
                        rationale
                    FROM orders
                    ORDER BY COALESCE(submitted_at, filled_at, '') DESC, rowid DESC
                    LIMIT ?
                    """,
                    (limit,),
                )
                rows = await cur.fetchall()
                return [dict(row) for row in rows]
            finally:
                await db.close()
        except Exception as e:
            log.error("latest_order_records_failed", error=str(e))
            raise


async def backfill_exit_reasons_from_journal(*, dry_run: bool = True, limit: int | None = None) -> dict[str, Any]:
    """Repair administrative exit reasons only when a journal order-id match proves the reason."""
    async with _DB_LOCK:
        try:
            await init_db()
            db = await _get_conn()
            try:
                limit_clause = "LIMIT ?" if limit is not None else ""
                params: tuple[Any, ...] = (int(limit),) if limit is not None else ()
                cur = await db.execute(
                    f"""
                    SELECT id, content, created_at
                    FROM journal_entries
                    WHERE content LIKE 'Auto-exit submitted for %'
                    ORDER BY id DESC
                    {limit_clause}
                    """,
                    params,
                )
                journal_rows = [dict(row) for row in await cur.fetchall()]
                updates: list[dict[str, Any]] = []
                ambiguous: list[dict[str, Any]] = []
                unmatched: list[dict[str, Any]] = []
                for journal in journal_rows:
                    match = AUTO_EXIT_JOURNAL_RE.search(str(journal.get("content") or ""))
                    if not match:
                        unmatched.append({"journal_id": journal.get("id"), "reason": "journal_parse_failed"})
                        continue
                    symbol = match.group("symbol").upper()
                    reason = match.group("reason").strip()
                    order_id = match.group("order_id").strip()
                    order_cur = await db.execute(
                        """
                        SELECT client_order_id, broker_order_id, symbol, side, status, rationale
                        FROM orders
                        WHERE upper(symbol) = ?
                          AND lower(side) IN ('sell', 'close', 'sell_short', 'buy_to_cover')
                          AND (broker_order_id = ? OR client_order_id = ?)
                          AND (
                                rationale IS NULL
                             OR rationale = ''
                             OR lower(rationale) IN ('broker_reconciliation', 'unknown')
                          )
                        """,
                        (symbol, order_id, order_id),
                    )
                    matches = [dict(row) for row in await order_cur.fetchall()]
                    if len(matches) == 1:
                        row = matches[0]
                        update = {
                            "journal_id": journal.get("id"),
                            "symbol": symbol,
                            "order_id": order_id,
                            "client_order_id": row.get("client_order_id"),
                            "broker_order_id": row.get("broker_order_id"),
                            "old_rationale": row.get("rationale"),
                            "new_rationale": reason,
                        }
                        updates.append(update)
                        if not dry_run:
                            await db.execute(
                                """
                                UPDATE orders
                                SET rationale = ?
                                WHERE client_order_id = ?
                                  AND (
                                        rationale IS NULL
                                     OR rationale = ''
                                     OR lower(rationale) IN ('broker_reconciliation', 'unknown')
                                  )
                                """,
                                (reason, row.get("client_order_id")),
                            )
                    elif len(matches) > 1:
                        ambiguous.append({"journal_id": journal.get("id"), "symbol": symbol, "order_id": order_id, "matches": len(matches)})
                    else:
                        unmatched.append({"journal_id": journal.get("id"), "symbol": symbol, "order_id": order_id})
                if not dry_run and updates:
                    now_date = datetime.now(UTC).date().isoformat()
                    await db.execute(
                        """
                        INSERT INTO journal_entries (date, kind, content)
                        VALUES (?, 'daily', ?)
                        """,
                        (
                            now_date,
                            (
                                "Exit reason backfill applied: "
                                f"{len(updates)} order row(s) repaired from exact auto-exit journal order-id matches."
                            ),
                        ),
                    )
                if not dry_run:
                    await db.commit()
                return {
                    "dry_run": dry_run,
                    "journal_entries_scanned": len(journal_rows),
                    "updates": updates,
                    "updated_count": len(updates) if not dry_run else 0,
                    "would_update_count": len(updates),
                    "ambiguous": ambiguous,
                    "unmatched": unmatched,
                }
            finally:
                await db.close()
        except Exception as e:
            log.error("exit_reason_backfill_failed", error=str(e))
            raise


async def get_latest_entry_order_for_symbol(symbol: str, *, before_utc_iso: str | None = None) -> dict[str, Any] | None:
    """Return the latest durable buy/long entry order for a symbol.

    If before_utc_iso is provided, ignore entries submitted/filled after that
    timestamp. This keeps exit outcome alerts from pairing a close with a later
    re-entry for the same symbol.
    """
    async with _DB_LOCK:
        try:
            await init_db()
            db = await _get_conn()
            try:
                before_clause = ""
                params: tuple[Any, ...]
                if before_utc_iso:
                    before_clause = "AND COALESCE(submitted_at, filled_at, '') <= ?"
                    params = (symbol.upper(), before_utc_iso)
                else:
                    params = (symbol.upper(),)
                cur = await db.execute(
                    f"""
                    SELECT
                        client_order_id,
                        broker_order_id,
                        symbol,
                        side,
                        qty,
                        order_type,
                        status,
                        filled_qty,
                        avg_fill_price,
                        submitted_at,
                        filled_at,
                        rationale
                    FROM orders
                    WHERE upper(symbol) = ?
                      AND lower(side) IN ('long', 'buy')
                      AND lower(status) NOT IN ('canceled', 'cancelled', 'rejected', 'expired')
                      {before_clause}
                    ORDER BY COALESCE(submitted_at, filled_at, '') DESC, rowid DESC
                    LIMIT 1
                    """,
                    params,
                )
                row = await cur.fetchone()
                return dict(row) if row else None
            finally:
                await db.close()
        except Exception as e:
            log.error("latest_entry_order_lookup_failed", symbol=symbol, error=str(e))
            raise


async def append_journal_entry(
    *,
    content: str,
    kind: str = "daily",
    date: str | None = None,
) -> int | None:
    """Append a lightweight operator journal entry."""
    clean_kind = kind.lower()
    if clean_kind not in {"daily", "weekly"}:
        raise ValueError(f"unsupported journal kind: {kind}")
    clean_date = date or datetime.now(UTC).date().isoformat()
    async with _DB_LOCK:
        try:
            await init_db()
            db = await _get_conn()
            try:
                cur = await db.execute(
                    """
                    INSERT INTO journal_entries (date, kind, content)
                    VALUES (?, ?, ?)
                    """,
                    (clean_date, clean_kind, content),
                )
                await db.commit()
                return int(cur.lastrowid) if cur.lastrowid is not None else None
            finally:
                await db.close()
        except Exception as e:
            log.error("journal_entry_append_failed", kind=clean_kind, date=clean_date, error=str(e))
            return None


async def get_latest_journal_entries(limit: int = 3) -> list[dict[str, Any]]:
    """Return latest journal entries for operator reports."""
    async with _DB_LOCK:
        try:
            await init_db()
            db = await _get_conn()
            try:
                cur = await db.execute(
                    """
                    SELECT id, date, kind, content, created_at
                    FROM journal_entries
                    ORDER BY created_at DESC, id DESC
                    LIMIT ?
                    """,
                    (limit,),
                )
                rows = await cur.fetchall()
                return [dict(row) for row in rows]
            finally:
                await db.close()
        except Exception as e:
            log.error("latest_journal_entries_failed", error=str(e))
            raise
