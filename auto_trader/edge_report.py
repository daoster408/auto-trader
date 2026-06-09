"""Trade evidence report: prove edge, not just uptime."""
from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import aiosqlite

from auto_trader.config.settings import get_settings
from auto_trader.persistence.db import configure_db_path, get_configured_db_path, init_db
from auto_trader.utils.logging import setup_logging


ENTRY_SIDES = {"buy", "long"}
EXIT_SIDES = {"sell", "close", "sell_short", "buy_to_cover"}


@dataclass(frozen=True)
class ClosedTradeEvidence:
    symbol: str
    qty: float
    entry_price: float
    exit_price: float
    pnl: float
    pnl_pct: float
    entry_time: datetime | None
    exit_time: datetime | None
    exit_reason: str
    ai_verdict: str
    risk_profile: str
    signal_id: int | None


@dataclass(frozen=True)
class OpportunityEvidence:
    symbol: str
    outcome: str
    reason: str
    ai_verdict: str
    risk_profile: str
    signal_id: int
    created_at: datetime | None


@dataclass(frozen=True)
class EdgeReport:
    window_days: int
    closed_trades: list[ClosedTradeEvidence]
    opportunities: list[OpportunityEvidence]


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    text = str(value).strip()
    if not text:
        return None
    for candidate in (text, text.replace("Z", "+00:00")):
        try:
            parsed = datetime.fromisoformat(candidate)
            return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)
        except ValueError:
            pass
    try:
        parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        return parsed.replace(tzinfo=UTC)
    except ValueError:
        return None


def _event_time(row: dict[str, Any]) -> datetime | None:
    return _parse_dt(row.get("filled_at") or row.get("submitted_at"))


def _float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _json(value: Any) -> dict[str, Any]:
    try:
        parsed = json.loads(value or "{}")
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _risk_profile_from_signal(signal: dict[str, Any] | None, risk_decision: dict[str, Any] | None = None) -> str:
    metrics = _json((risk_decision or {}).get("metrics_json"))
    if metrics.get("risk_profile"):
        return str(metrics.get("risk_profile"))
    features = _json((signal or {}).get("features_json"))
    risk = features.get("risk") if isinstance(features.get("risk"), dict) else {}
    if risk.get("risk_profile"):
        return str(risk.get("risk_profile"))
    discovery = features.get("discovery") if isinstance(features.get("discovery"), dict) else {}
    if discovery.get("risk_profile"):
        return str(discovery.get("risk_profile"))
    return "unknown"


def _latest_ai_memo(memos: list[dict[str, Any]], *, signal_id: int | None) -> dict[str, Any] | None:
    matching = [memo for memo in memos if signal_id is not None and memo.get("signal_id") == signal_id]
    if not matching:
        return None
    matching.sort(
        key=lambda row: (
            1 if row.get("provider") == "multi" else 0,
            _parse_dt(row.get("created_at")) or datetime.min.replace(tzinfo=UTC),
            int(row.get("id") or 0),
        ),
        reverse=True,
    )
    return matching[0]


def _ai_verdict_for_signal(memos: list[dict[str, Any]], signal_id: int | None) -> str:
    memo = _latest_ai_memo(memos, signal_id=signal_id)
    if not memo:
        return "none"
    provider = str(memo.get("provider") or "unknown")
    verdict = str(memo.get("verdict") or "watch")
    return f"{provider}:{verdict}"


async def _fetch_rows(db_path: Path) -> dict[str, list[dict[str, Any]]]:
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        orders_cur = await db.execute(
            """
            SELECT client_order_id, broker_order_id, symbol, side, qty, order_type, status,
                   filled_qty, avg_fill_price, submitted_at, filled_at, risk_decision_id, rationale
            FROM orders
            WHERE lower(status) = 'filled'
            ORDER BY COALESCE(filled_at, submitted_at, ''), rowid
            """
        )
        signals_cur = await db.execute(
            """
            SELECT id, created_at, symbol, thesis, confidence, source, model_tag, features_json
            FROM signals
            ORDER BY created_at, id
            """
        )
        risks_cur = await db.execute(
            """
            SELECT id, created_at, signal_id, approved, reason, symbol, side, proposed_qty, sized_qty,
                   equity_snapshot, metrics_json, model_tag, trace_id
            FROM risk_decisions
            ORDER BY created_at, id
            """
        )
        memos_cur = await db.execute(
            """
            SELECT id, created_at, signal_id, symbol, provider, model_tag, prompt_version, input_hash,
                   verdict, confidence, used_only_provided_data, validation_passed, memo_json
            FROM ai_research_memos
            ORDER BY created_at, id
            """
        )
        orders = [dict(row) for row in await orders_cur.fetchall()]
        signals = [dict(row) for row in await signals_cur.fetchall()]
        risks = [dict(row) for row in await risks_cur.fetchall()]
        memos = [dict(row) for row in await memos_cur.fetchall()]

        return {
            "orders": orders,
            "signals": signals,
            "risk_decisions": risks,
            "ai_memos": memos,
        }


def _build_closed_trades(rows: dict[str, list[dict[str, Any]]], *, since: datetime) -> list[ClosedTradeEvidence]:
    orders = rows["orders"]
    risks_by_id = {int(row["id"]): row for row in rows["risk_decisions"]}
    signals_by_id = {int(row["id"]): row for row in rows["signals"]}
    memos = rows["ai_memos"]
    entries = [
        row
        for row in orders
        if str(row.get("side") or "").lower() in ENTRY_SIDES and _float(row.get("avg_fill_price")) > 0
    ]
    entries.sort(key=lambda row: _event_time(row) or datetime.max.replace(tzinfo=UTC))
    exits = [
        row
        for row in orders
        if str(row.get("side") or "").lower() in EXIT_SIDES and _float(row.get("avg_fill_price")) > 0
    ]
    exits.sort(key=lambda row: _event_time(row) or datetime.max.replace(tzinfo=UTC))
    closed: list[ClosedTradeEvidence] = []
    used_exits: set[str] = set()
    for entry in entries:
        entry_time = _event_time(entry)
        if entry_time is None:
            continue
        symbol = str(entry.get("symbol") or "").upper()
        candidates = []
        for exit_order in exits:
            exit_id = str(exit_order.get("client_order_id") or exit_order.get("broker_order_id") or "")
            if exit_id in used_exits or str(exit_order.get("symbol") or "").upper() != symbol:
                continue
            exit_time = _event_time(exit_order)
            if exit_time is None or exit_time < entry_time:
                continue
            candidates.append((exit_time, exit_order))
        if not candidates:
            continue
        candidates.sort(key=lambda item: item[0])
        exit_time, exit_order = candidates[0]
        exit_id = str(exit_order.get("client_order_id") or exit_order.get("broker_order_id") or "")
        used_exits.add(exit_id)
        qty = min(_float(entry.get("filled_qty") or entry.get("qty")), _float(exit_order.get("filled_qty") or exit_order.get("qty")))
        entry_price = _float(entry.get("avg_fill_price"))
        exit_price = _float(exit_order.get("avg_fill_price"))
        pnl = (exit_price - entry_price) * qty
        pnl_pct = (pnl / (entry_price * qty) * 100.0) if entry_price > 0 and qty > 0 else 0.0
        if exit_time < since:
            continue
        risk = risks_by_id.get(int(entry.get("risk_decision_id") or 0))
        signal_id = int(risk.get("signal_id")) if risk and risk.get("signal_id") is not None else None
        signal = signals_by_id.get(signal_id or -1)
        closed.append(
            ClosedTradeEvidence(
                symbol=symbol,
                qty=qty,
                entry_price=entry_price,
                exit_price=exit_price,
                pnl=pnl,
                pnl_pct=pnl_pct,
                entry_time=entry_time,
                exit_time=exit_time,
                exit_reason=str(exit_order.get("rationale") or "unknown"),
                ai_verdict=_ai_verdict_for_signal(memos, signal_id),
                risk_profile=_risk_profile_from_signal(signal, risk),
                signal_id=signal_id,
            )
        )
    return closed


def _build_opportunities(rows: dict[str, list[dict[str, Any]]], *, since: datetime) -> list[OpportunityEvidence]:
    signals = [
        signal
        for signal in rows["signals"]
        if (created_at := _parse_dt(signal.get("created_at"))) is not None and created_at >= since
    ]
    risks_by_signal: dict[int, list[dict[str, Any]]] = {}
    for risk in rows["risk_decisions"]:
        if risk.get("signal_id") is not None:
            risks_by_signal.setdefault(int(risk["signal_id"]), []).append(risk)
    orders_by_signal: dict[int, list[dict[str, Any]]] = {}
    risks_by_id = {int(row["id"]): row for row in rows["risk_decisions"]}
    for order in rows["orders"]:
        risk = risks_by_id.get(int(order.get("risk_decision_id") or 0))
        if risk and risk.get("signal_id") is not None:
            orders_by_signal.setdefault(int(risk["signal_id"]), []).append(order)
    memos_by_signal: dict[int, list[dict[str, Any]]] = {}
    for memo in rows["ai_memos"]:
        if memo.get("signal_id") is not None:
            memos_by_signal.setdefault(int(memo["signal_id"]), []).append(memo)

    opportunities: list[OpportunityEvidence] = []
    for signal in signals:
        signal_id = int(signal["id"])
        risks = risks_by_signal.get(signal_id, [])
        memos = memos_by_signal.get(signal_id, [])
        ai_verdict = _ai_verdict_for_signal(rows["ai_memos"], signal_id)
        risk = risks[-1] if risks else None
        risk_profile = _risk_profile_from_signal(signal, risk)
        if orders_by_signal.get(signal_id):
            outcome = "traded"
            reason = "order submitted"
        elif any(int(row.get("approved") or 0) == 0 for row in risks):
            blocked = next(row for row in risks if int(row.get("approved") or 0) == 0)
            outcome = "risk_blocked"
            reason = str(blocked.get("reason") or "risk rejected")
        elif any(str(row.get("provider")) == "prefilter" for row in memos):
            outcome = "prefilter_blocked"
            reason = "paid AI prefilter blocked"
        elif ai_verdict != "none":
            verdict = ai_verdict.split(":", 1)[-1]
            outcome = f"ai_{verdict}"
            reason = "AI committee verdict"
        else:
            outcome = "candidate_only"
            reason = "candidate logged without order"
        opportunities.append(
            OpportunityEvidence(
                symbol=str(signal.get("symbol") or "").upper(),
                outcome=outcome,
                reason=reason,
                ai_verdict=ai_verdict,
                risk_profile=risk_profile,
                signal_id=signal_id,
                created_at=_parse_dt(signal.get("created_at")),
            )
        )
    return opportunities


async def build_edge_report(*, db_path: Path | None = None, window_days: int = 7) -> EdgeReport:
    await init_db()
    since = datetime.now(UTC) - timedelta(days=max(1, int(window_days)))
    rows = await _fetch_rows(db_path or get_configured_db_path())
    return EdgeReport(
        window_days=window_days,
        closed_trades=_build_closed_trades(rows, since=since),
        opportunities=_build_opportunities(rows, since=since),
    )


def _money(value: float) -> str:
    sign = "-" if value < 0 else ""
    return f"{sign}${abs(value):,.2f}"


def _pct(value: float) -> str:
    sign = "+" if value > 0 else ""
    return f"{sign}{value:.2f}%"


def _bucket_counts(values: list[str]) -> list[tuple[str, int]]:
    counts: dict[str, int] = {}
    for value in values:
        counts[value] = counts.get(value, 0) + 1
    return sorted(counts.items(), key=lambda item: (-item[1], item[0]))


def _trade_stats(trades: list[ClosedTradeEvidence]) -> dict[str, float]:
    if not trades:
        return {
            "count": 0,
            "wins": 0,
            "losses": 0,
            "win_rate": 0.0,
            "realized_pnl": 0.0,
            "expectancy": 0.0,
            "avg_win": 0.0,
            "avg_loss": 0.0,
        }
    wins = [trade.pnl for trade in trades if trade.pnl > 0]
    losses = [trade.pnl for trade in trades if trade.pnl < 0]
    realized = sum(trade.pnl for trade in trades)
    return {
        "count": float(len(trades)),
        "wins": float(len(wins)),
        "losses": float(len(losses)),
        "win_rate": (len(wins) / len(trades) * 100.0),
        "realized_pnl": realized,
        "expectancy": realized / len(trades),
        "avg_win": (sum(wins) / len(wins)) if wins else 0.0,
        "avg_loss": (sum(losses) / len(losses)) if losses else 0.0,
    }


def _render_group_stats(title: str, groups: dict[str, list[ClosedTradeEvidence]]) -> list[str]:
    lines = [title]
    if not groups:
        return lines + ["- none"]
    for name, trades in sorted(groups.items(), key=lambda item: (-len(item[1]), item[0])):
        stats = _trade_stats(trades)
        lines.append(
            f"- {name}: n={int(stats['count'])}, P/L {_money(stats['realized_pnl'])}, "
            f"exp/trade {_money(stats['expectancy'])}, win {stats['win_rate']:.1f}%"
        )
    return lines


def render_edge_report(report: EdgeReport) -> str:
    trades = report.closed_trades
    opportunities = report.opportunities
    stats = _trade_stats(trades)
    lines = [
        "EDGE REPORT",
        f"Window: last {report.window_days} days",
        f"Closed trades: {int(stats['count'])}",
        f"Realized P/L: {_money(stats['realized_pnl'])}",
        f"Expectancy/trade: {_money(stats['expectancy'])}",
        f"Win rate: {stats['win_rate']:.1f}% ({int(stats['wins'])}W/{int(stats['losses'])}L)",
        f"Avg win/loss: {_money(stats['avg_win'])} / {_money(stats['avg_loss'])}",
    ]
    by_ai: dict[str, list[ClosedTradeEvidence]] = {}
    by_profile: dict[str, list[ClosedTradeEvidence]] = {}
    for trade in trades:
        by_ai.setdefault(trade.ai_verdict, []).append(trade)
        by_profile.setdefault(trade.risk_profile, []).append(trade)
    lines.extend(_render_group_stats("By AI verdict:", by_ai))
    lines.extend(_render_group_stats("By risk profile:", by_profile))
    lines.append("Opportunity outcomes:")
    if opportunities:
        for outcome, count in _bucket_counts([opportunity.outcome for opportunity in opportunities]):
            lines.append(f"- {outcome}: {count}")
    else:
        lines.append("- none")
    lines.append("Recent closed trades:")
    recent = sorted(trades, key=lambda trade: trade.exit_time or datetime.min.replace(tzinfo=UTC), reverse=True)[:5]
    if recent:
        for trade in recent:
            lines.append(
                f"- {trade.symbol}: {_money(trade.pnl)} ({_pct(trade.pnl_pct)}), "
                f"{trade.ai_verdict}, {trade.risk_profile}, exit={trade.exit_reason}"
            )
    else:
        lines.append("- none yet")
    lines.append("Next pressure:")
    if not trades:
        lines.append("- Need closed trades. Run aggressive paper until the report has outcomes, not vibes.")
    elif stats["expectancy"] <= 0:
        lines.append("- Negative/flat expectancy. Demote weak setup tags and force better candidates.")
    else:
        lines.append("- Positive expectancy. Keep sample growing and compare AI-approved vs non-approved paths.")
    return "\n".join(lines)


async def run_edge_report(*, window_days: int = 7) -> str:
    settings = get_settings()
    configure_db_path(getattr(settings, "db_path", "auto_trader.db"))
    report = await build_edge_report(window_days=window_days)
    return render_edge_report(report)


def main() -> None:
    parser = argparse.ArgumentParser(description="Read-only trade edge evidence report.")
    parser.add_argument("--days", type=int, default=7, help="Lookback window in days.")
    args = parser.parse_args()
    setup_logging("ERROR")
    print(asyncio.run(run_edge_report(window_days=args.days)))


if __name__ == "__main__":
    main()
