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
    await init_db()
    async with _DB_LOCK:
        try:
            async with await _get_conn() as db:
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
            async with await _get_conn() as db:
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
            log.info("state_persisted", state=state.value, reason=reason)
        except Exception as e:
            log.critical("state_persist_failed", state=state.value, error=str(e))
            # Never raise — in-memory HALTED is still effective for this process


# Future helpers (stubbed, no new logic)
async def log_risk_decision(**kwargs: Any) -> None:
    """Placeholder - will be wired by later Engineer pass without changing risk logic."""
    pass
