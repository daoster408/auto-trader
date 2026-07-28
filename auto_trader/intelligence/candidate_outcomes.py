"""Passive outcome resolution for persisted single-provider AI decisions."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

from auto_trader.persistence.db import (
    get_pending_ai_candidate_outcomes,
    update_ai_candidate_outcomes_batch,
)
from auto_trader.utils.logging import get_logger

log = get_logger("auto_trader.intelligence.candidate_outcomes")

OUTCOME_HORIZONS = (0, 1, 3, 5)
OUTCOME_RESOLUTION_BATCH_SIZE = 100
OUTCOME_RESOLUTION_MAX_ROWS = 1000


@dataclass(frozen=True)
class CandidateOutcomeResolution:
    pending_rows: int
    updated_rows: int
    resolved_rows: int
    partial_rows: int
    missing_symbols: int


def _parse_session_date(value: Any) -> date | None:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _bar_session_date(bar: dict[str, Any]) -> date | None:
    return _parse_session_date(bar.get("t") or bar.get("timestamp") or bar.get("date"))


def _bar_close(bar: dict[str, Any]) -> float | None:
    raw_value = bar.get("c") if bar.get("c") is not None else bar.get("close")
    if raw_value is None:
        return None
    try:
        value = float(raw_value)
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None


def _chunks(values: list[str], size: int) -> list[list[str]]:
    return [values[index : index + size] for index in range(0, len(values), size)]


def _existing_horizons(row: dict[str, Any]) -> dict[int, dict[str, Any]]:
    horizons: dict[int, dict[str, Any]] = {}
    for horizon in OUTCOME_HORIZONS:
        prefix = f"d{horizon}"
        if row.get(f"{prefix}_close") is None:
            continue
        horizons[horizon] = {
            "session_date": row.get(f"{prefix}_session_date"),
            "close": row.get(f"{prefix}_close"),
            "return_pct": row.get(f"{prefix}_return_pct"),
            "hypothetical_pnl": row.get(f"{prefix}_hypothetical_pnl"),
        }
    return horizons


def resolve_outcome_row(
    row: dict[str, Any],
    bars: list[dict[str, Any]],
    *,
    completed_through: date,
) -> dict[str, Any]:
    """Map completed trading-session bars to D0/D1/D3/D5 evidence."""
    decision_session = _parse_session_date(row.get("decision_session_date"))
    reference_price = float(row.get("reference_price") or 0.0)
    comparison_notional = float(row.get("comparison_notional") or 0.0)
    if decision_session is None or reference_price <= 0 or comparison_notional <= 0:
        return {
            "outcome_id": int(row["id"]),
            "horizons": _existing_horizons(row),
            "status": "invalid_reference",
            "last_error": "missing decision session, reference price, or comparison notional",
        }

    completed_bars: list[tuple[date, float]] = []
    for bar in bars:
        session_date = _bar_session_date(bar)
        close = _bar_close(bar)
        if (
            session_date is None
            or close is None
            or session_date < decision_session
            or session_date > completed_through
        ):
            continue
        completed_bars.append((session_date, close))
    completed_bars = sorted(dict(completed_bars).items())

    horizons = _existing_horizons(row)
    for horizon in OUTCOME_HORIZONS:
        if horizon in horizons or len(completed_bars) <= horizon:
            continue
        session_date, close = completed_bars[horizon]
        return_pct = ((close - reference_price) / reference_price) * 100.0
        horizons[horizon] = {
            "session_date": session_date.isoformat(),
            "close": close,
            "return_pct": return_pct,
            "hypothetical_pnl": comparison_notional * (return_pct / 100.0),
        }

    if all(horizon in horizons for horizon in OUTCOME_HORIZONS):
        status = "resolved"
        last_error = None
    elif horizons:
        status = "partial"
        last_error = None
    else:
        status = "pending"
        last_error = "no completed daily bar available"
    return {
        "outcome_id": int(row["id"]),
        "horizons": horizons,
        "status": status,
        "last_error": last_error,
    }


async def resolve_candidate_outcomes(
    adapter: Any,
    *,
    completed_through: date,
    max_rows: int = OUTCOME_RESOLUTION_MAX_ROWS,
) -> CandidateOutcomeResolution:
    """Resolve a bounded backlog using batched daily bars and no AI/order calls."""
    rows = await get_pending_ai_candidate_outcomes(limit=max_rows)
    if not rows:
        return CandidateOutcomeResolution(0, 0, 0, 0, 0)

    symbols = sorted({str(row.get("symbol") or "").upper() for row in rows if row.get("symbol")})
    earliest_session = min(
        parsed
        for parsed in (_parse_session_date(row.get("decision_session_date")) for row in rows)
        if parsed is not None
    )
    bars_by_symbol: dict[str, list[dict[str, Any]]] = {}
    for symbol_batch in _chunks(symbols, OUTCOME_RESOLUTION_BATCH_SIZE):
        batch_bars = await adapter.get_stock_daily_bars(
            symbol_batch,
            start=earliest_session,
            end=completed_through + timedelta(days=1),
        )
        for symbol, bars in batch_bars.items():
            bars_by_symbol[str(symbol).upper()] = list(bars)

    updates: list[dict[str, Any]] = []
    missing_symbols = 0
    for row in rows:
        symbol = str(row.get("symbol") or "").upper()
        bars = bars_by_symbol.get(symbol, [])
        if not bars:
            missing_symbols += 1
        updates.append(resolve_outcome_row(row, bars, completed_through=completed_through))
    await update_ai_candidate_outcomes_batch(updates)

    summary = CandidateOutcomeResolution(
        pending_rows=len(rows),
        updated_rows=len(updates),
        resolved_rows=sum(1 for update in updates if update["status"] == "resolved"),
        partial_rows=sum(1 for update in updates if update["status"] == "partial"),
        missing_symbols=missing_symbols,
    )
    log.info(
        "ai_candidate_outcomes_resolved",
        pending_rows=summary.pending_rows,
        updated_rows=summary.updated_rows,
        resolved_rows=summary.resolved_rows,
        partial_rows=summary.partial_rows,
        missing_symbols=summary.missing_symbols,
        completed_through=completed_through.isoformat(),
    )
    return summary
