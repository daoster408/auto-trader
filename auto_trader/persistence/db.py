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
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import aiosqlite

from auto_trader.core.models import SystemState
from auto_trader.utils.logging import get_logger

log = get_logger("auto_trader.persistence.db")

_DB_PATH: Path = Path("auto_trader.db")
_DB_LOCK = asyncio.Lock()  # single writer guarantee (simple & cheap for v1)


def configure_db_path(path: str | Path) -> None:
    """Called at startup from settings so DB location is configurable."""
    global _DB_PATH
    _DB_PATH = Path(path).expanduser().resolve()


async def init_db() -> None:
    """Create tables from schema.sql if missing. Idempotent. Fast on ARM."""
    schema_path = Path(__file__).parent / "schema.sql"
    if not schema_path.exists():
        log.warning("schema.sql missing - state persistence degraded")
        return

    async with aiosqlite.connect(_DB_PATH) as db:
        schema = schema_path.read_text()
        await db.executescript(schema)
        await db.commit()
    log.info("db_initialized", path=str(_DB_PATH), size_kb=round(_DB_PATH.stat().st_size / 1024, 1) if _DB_PATH.exists() else 0)


async def _get_conn() -> aiosqlite.Connection:
    conn = await aiosqlite.connect(_DB_PATH)
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
                        approved, reason, symbol, side, proposed_qty, sized_qty,
                        equity_snapshot, metrics_json, model_tag, trace_id
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
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


async def upsert_order_record(order: dict[str, Any], risk_decision_id: int | None = None, rationale: str | None = None) -> bool:
    """Persist or refresh an order row from broker/manager normalized data."""
    client_order_id = str(order.get("client_order_id") or order.get("id") or order.get("broker_order_id") or "")
    broker_order_id = str(order.get("broker_order_id") or order.get("id") or client_order_id)
    if not client_order_id or not broker_order_id:
        log.warning("order_record_missing_id_skipped", order=order)
        return False

    async with _DB_LOCK:
        try:
            await init_db()
            db = await _get_conn()
            try:
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
                        rationale=COALESCE(excluded.rationale, orders.rationale)
                    """,
                    (
                        client_order_id,
                        broker_order_id,
                        str(order.get("symbol", "")).upper(),
                        str(order.get("side", "")),
                        float(order.get("qty") or 0.0),
                        str(order.get("order_type") or "market"),
                        str(order.get("status") or "unknown"),
                        float(order.get("filled_qty") or 0.0),
                        order.get("avg_fill_price"),
                        order.get("submitted_at"),
                        order.get("filled_at"),
                        risk_decision_id,
                        rationale or order.get("rationale"),
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
    """Clear a pending exit after a trusted position snapshot proves the symbol is gone."""
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


async def get_latest_entry_order_for_symbol(symbol: str) -> dict[str, Any] | None:
    """Return the latest durable buy/long entry order for a symbol."""
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
                    WHERE upper(symbol) = ?
                      AND lower(side) IN ('long', 'buy')
                      AND lower(status) NOT IN ('canceled', 'cancelled', 'rejected', 'expired')
                    ORDER BY COALESCE(submitted_at, filled_at, '') DESC, rowid DESC
                    LIMIT 1
                    """,
                    (symbol.upper(),),
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
