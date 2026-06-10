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
from auto_trader.intelligence.ai_paid_prefilter import INVERSE_OR_LEVERAGED_SYMBOLS
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
    setup_tags: tuple[str, ...] = ()
    provider_votes: tuple[str, ...] = ()


@dataclass(frozen=True)
class OpportunityEvidence:
    symbol: str
    outcome: str
    reason: str
    ai_verdict: str
    risk_profile: str
    signal_id: int
    created_at: datetime | None
    setup_tags: tuple[str, ...] = ()
    provider_votes: tuple[str, ...] = ()


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


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _num(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _pct_decimal(value: Any) -> float | None:
    number = _num(value)
    if number is None:
        return None
    if abs(number) > 1:
        return number / 100.0
    return number


def _merge_dicts(*parts: dict[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for part in parts:
        for key, value in part.items():
            if isinstance(value, dict) and isinstance(merged.get(key), dict):
                merged[key] = _merge_dicts(merged[key], value)
            elif value not in ({}, [], None):
                merged[key] = value
    return merged


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


def _memo_json(memo: dict[str, Any] | None) -> dict[str, Any]:
    if not memo:
        return {}
    return _json(memo.get("memo_json"))


def _research_context(signal: dict[str, Any] | None, memo: dict[str, Any] | None) -> dict[str, Any]:
    features = _json((signal or {}).get("features_json"))
    feature_context = _dict(features.get("research_context"))
    memo_packet = _dict(_memo_json(memo).get("input_packet"))
    memo_context = _dict(memo_packet.get("verified_research_context"))
    return _merge_dicts(feature_context, memo_context)


def _technical_value(features: dict[str, Any], context: dict[str, Any], name: str) -> Any:
    technical = _dict(context.get("technical"))
    discovery = _dict(features.get("discovery"))
    if name in technical:
        return technical.get(name)
    if name in discovery:
        return discovery.get(name)
    return features.get(name)


def _has_fundamental(context: dict[str, Any]) -> bool:
    fundamental = _dict(context.get("fundamental"))
    return any(value not in (None, "", 0, {}) for value in fundamental.values())


def _news_count(context: dict[str, Any]) -> int:
    return sum(
        1
        for item in _list(context.get("news"))
        if isinstance(item, dict) and str(item.get("headline") or "").strip()
    )


def _macro_tag(context: dict[str, Any]) -> str:
    macro = context.get("macro")
    if not isinstance(macro, dict):
        return "macro:missing"
    if macro.get("error"):
        return "macro:error"
    if macro.get("enabled") or macro.get("series") or macro.get("regime"):
        return "macro:ok"
    return "macro:missing"


def _setup_tags(signal: dict[str, Any] | None, memo: dict[str, Any] | None, risk_profile: str) -> tuple[str, ...]:
    symbol = str((signal or {}).get("symbol") or (memo or {}).get("symbol") or "").upper()
    features = _json((signal or {}).get("features_json"))
    context = _research_context(signal, memo)
    tags: list[str] = [f"profile:{risk_profile}"]

    rel_volume = _num(_technical_value(features, context, "rel_volume"))
    if rel_volume is None:
        tags.append("relvol:missing")
    elif rel_volume < 0.8:
        tags.append("relvol:low")
    elif rel_volume < 2.0:
        tags.append("relvol:normal")
    elif rel_volume < 4.0:
        tags.append("relvol:strong")
    else:
        tags.append("relvol:hot")

    change_pct = _pct_decimal(_technical_value(features, context, "change_pct"))
    if change_pct is None:
        tags.append("move:missing")
    elif change_pct < 0.005:
        tags.append("move:cold")
    elif change_pct < 0.08:
        tags.append("move:tradable")
    elif change_pct < 0.15:
        tags.append("move:extended")
    else:
        tags.append("move:parabolic")

    spread_pct = _pct_decimal(_technical_value(features, context, "spread_pct"))
    if spread_pct is None:
        tags.append("spread:missing")
    elif spread_pct <= 0.006:
        tags.append("spread:tight")
    elif spread_pct <= 0.01:
        tags.append("spread:workable")
    else:
        tags.append("spread:wide")

    distance_from_high_pct = _pct_decimal(_technical_value(features, context, "distance_from_high_pct"))
    if distance_from_high_pct is not None and distance_from_high_pct >= -0.02:
        tags.append("near_high")
    if _news_count(context) > 0:
        tags.append("news:present")
    else:
        tags.append("news:missing")
    tags.append("fundamental:present" if _has_fundamental(context) else "fundamental:missing")
    tags.append(_macro_tag(context))
    if symbol in INVERSE_OR_LEVERAGED_SYMBOLS:
        tags.append("inverse_or_leveraged")

    return tuple(dict.fromkeys(tags))


def _provider_votes_for_signal(memos: list[dict[str, Any]], signal_id: int | None) -> tuple[str, ...]:
    memo = _latest_ai_memo(memos, signal_id=signal_id)
    votes = _list(_memo_json(memo).get("provider_votes"))
    formatted: list[str] = []
    for vote in votes:
        if not isinstance(vote, dict):
            continue
        provider = str(vote.get("provider") or "unknown").strip().lower()
        verdict = str(vote.get("verdict") or "unknown").strip().lower()
        formatted.append(_provider_vote_label(provider, verdict, vote.get("validation_passed"), vote.get("confidence")))
    if not formatted and signal_id is not None:
        member_memos = [
            row
            for row in memos
            if row.get("signal_id") == signal_id and str(row.get("provider") or "") not in {"multi", "prefilter"}
        ]
        member_memos.sort(
            key=lambda row: (_parse_dt(row.get("created_at")) or datetime.min.replace(tzinfo=UTC), int(row.get("id") or 0))
        )
        for row in member_memos:
            provider = str(row.get("provider") or "unknown").strip().lower()
            verdict = str(row.get("verdict") or "unknown").strip().lower()
            formatted.append(_provider_vote_label(provider, verdict, row.get("validation_passed"), row.get("confidence")))
    return tuple(dict.fromkeys(formatted))


def _provider_vote_label(provider: str, verdict: str, validation_passed: Any, confidence: Any) -> str:
    validation = ":invalid" if validation_passed in (False, 0, "0", "false", "False") else ""
    band = _confidence_band(confidence)
    confidence_suffix = f":{band}" if band else ""
    return f"{provider}:{verdict}{validation}{confidence_suffix}"


def _confidence_band(value: Any) -> str:
    confidence = _num(value)
    if confidence is None:
        return ""
    if confidence >= 0.75:
        return "high_conf"
    if confidence >= 0.55:
        return "med_conf"
    return "low_conf"


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
        risk_profile = _risk_profile_from_signal(signal, risk)
        memo = _latest_ai_memo(memos, signal_id=signal_id)
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
                risk_profile=risk_profile,
                signal_id=signal_id,
                setup_tags=_setup_tags(signal, memo, risk_profile),
                provider_votes=_provider_votes_for_signal(memos, signal_id),
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
                setup_tags=_setup_tags(signal, _latest_ai_memo(rows["ai_memos"], signal_id=signal_id), risk_profile),
                provider_votes=_provider_votes_for_signal(rows["ai_memos"], signal_id),
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


def _render_group_stats(title: str, groups: dict[str, list[ClosedTradeEvidence]], *, limit: int = 8) -> list[str]:
    lines = [title]
    if not groups:
        return lines + ["- none"]
    for name, trades in sorted(groups.items(), key=lambda item: (-len(item[1]), item[0]))[:limit]:
        stats = _trade_stats(trades)
        sample = ", sample=thin" if int(stats["count"]) < 3 else ""
        lines.append(
            f"- {name}: n={int(stats['count'])}, P/L {_money(stats['realized_pnl'])}, "
            f"exp/trade {_money(stats['expectancy'])}, win {stats['win_rate']:.1f}%{sample}"
        )
    return lines


def _group_trades_by_values(
    trades: list[ClosedTradeEvidence], value_getter: Any
) -> dict[str, list[ClosedTradeEvidence]]:
    groups: dict[str, list[ClosedTradeEvidence]] = {}
    for trade in trades:
        for value in value_getter(trade):
            groups.setdefault(str(value), []).append(trade)
    return groups


def _short_reason(value: str, *, limit: int = 70) -> str:
    text = " ".join(str(value or "unknown").split())
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)].rstrip() + "..."


def _render_counts(title: str, values: list[str], *, limit: int = 8) -> list[str]:
    lines = [title]
    if not values:
        return lines + ["- none"]
    for value, count in _bucket_counts(values)[:limit]:
        lines.append(f"- {value}: {count}")
    return lines


def _trade_groups_by_values(
    trades: list[ClosedTradeEvidence], value_getter: Any
) -> list[tuple[str, list[ClosedTradeEvidence], dict[str, float]]]:
    grouped = _group_trades_by_values(trades, value_getter)
    rows = [(name, group, _trade_stats(group)) for name, group in grouped.items()]
    return sorted(rows, key=lambda row: (row[2]["realized_pnl"], row[2]["expectancy"], row[0]), reverse=True)


def _format_learning_group(name: str, stats: dict[str, float]) -> str:
    sample = ", thin" if int(stats["count"]) < 3 else ""
    return (
        f"- {name}: n={int(stats['count'])}{sample}, P/L {_money(stats['realized_pnl'])}, "
        f"exp {_money(stats['expectancy'])}, win {stats['win_rate']:.1f}%"
    )


def _learning_questions(
    *,
    trades: list[ClosedTradeEvidence],
    best_tag: tuple[str, list[ClosedTradeEvidence], dict[str, float]] | None,
    worst_tag: tuple[str, list[ClosedTradeEvidence], dict[str, float]] | None,
    worst_provider: tuple[str, list[ClosedTradeEvidence], dict[str, float]] | None,
    blocked_counts: list[tuple[str, int]],
) -> list[str]:
    questions: list[str] = []
    if len(trades) < 10:
        questions.append("- Sample is still thin; use this to aim experiments, not declare truth.")
    if best_tag and best_tag[2]["realized_pnl"] > 0:
        questions.append(f"- Review whether `{best_tag[0]}` setups deserve more candidate-triage attention after more samples.")
    if worst_tag and worst_tag[2]["realized_pnl"] < 0:
        questions.append(f"- Review whether `{worst_tag[0]}` setups need stronger catalysts or stricter prefilter evidence.")
    if worst_provider and worst_provider[2]["realized_pnl"] < 0:
        questions.append(f"- Audit observed outcomes for provider vote bucket `{worst_provider[0]}` as sample grows.")
    if blocked_counts:
        questions.append(f"- Review whether top blocked pressure `{blocked_counts[0][0]}` is useful filtering or excessive friction.")
    if not questions:
        questions.append("- Keep collecting closed trades; no obvious pressure point yet.")
    return questions


def render_learning_brief(report: EdgeReport) -> str:
    trades = report.closed_trades
    opportunities = report.opportunities
    stats = _trade_stats(trades)
    setup_groups = _trade_groups_by_values(trades, lambda trade: trade.setup_tags)
    provider_groups = _trade_groups_by_values(trades, lambda trade: trade.provider_votes)
    blocked = [opportunity for opportunity in opportunities if opportunity.outcome != "traded"]
    blocked_counts = _bucket_counts([f"{opportunity.outcome}: {_short_reason(opportunity.reason)}" for opportunity in blocked])
    setup_by_expectancy = sorted(
        setup_groups,
        key=lambda row: (row[2]["expectancy"], row[2]["realized_pnl"], row[0]),
        reverse=True,
    )
    best_tags = [row for row in setup_by_expectancy if row[2]["realized_pnl"] > 0][:5]
    worst_tags = sorted(setup_groups, key=lambda row: (row[2]["expectancy"], row[2]["realized_pnl"], row[0]))[:5]
    provider_sorted = sorted(provider_groups, key=lambda row: (row[2]["realized_pnl"], row[2]["expectancy"], row[0]), reverse=True)
    provider_worst = sorted(provider_groups, key=lambda row: (row[2]["realized_pnl"], row[2]["expectancy"], row[0]))

    lines = [
        "LEARNING BRIEF",
        f"Window: last {report.window_days} days",
        f"Evidence: {int(stats['count'])} closed trades, {len(opportunities)} opportunities",
        f"Realized P/L: {_money(stats['realized_pnl'])}; expectancy/trade {_money(stats['expectancy'])}; win {stats['win_rate']:.1f}%",
        f"Sample: {'thin' if int(stats['count']) < 10 else 'building'}",
        "Setup tags with positive observed P/L:",
    ]
    if best_tags:
        lines.extend(_format_learning_group(name, row_stats) for name, _group, row_stats in best_tags)
    else:
        lines.append("- none yet")
    lines.append("Setup tags with negative observed P/L:")
    weak_rendered = [row for row in worst_tags if row[2]["realized_pnl"] < 0]
    if weak_rendered:
        lines.extend(_format_learning_group(name, row_stats) for name, _group, row_stats in weak_rendered)
    else:
        lines.append("- none yet")
    lines.append("Observed outcomes by provider vote bucket:")
    if provider_sorted:
        for name, _group, row_stats in provider_sorted[:3]:
            lines.append(_format_learning_group(name, row_stats))
        for name, _group, row_stats in provider_worst[:2]:
            if row_stats["realized_pnl"] < 0:
                lines.append(_format_learning_group(name, row_stats))
    else:
        lines.append("- none yet")
    lines.append("Blocked pressure:")
    if blocked_counts:
        for value, count in blocked_counts[:5]:
            lines.append(f"- {value}: {count}")
    else:
        lines.append("- none")
    lines.append("Next review questions:")
    best_tag = best_tags[0] if best_tags else None
    worst_tag = weak_rendered[0] if weak_rendered else None
    worst_provider = provider_worst[0] if provider_worst else None
    lines.extend(
        _learning_questions(
            trades=trades,
            best_tag=best_tag,
            worst_tag=worst_tag,
            worst_provider=worst_provider,
            blocked_counts=blocked_counts,
        )
    )
    return "\n".join(lines)


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
    lines.extend(_render_group_stats("By setup tag:", _group_trades_by_values(trades, lambda trade: trade.setup_tags)))
    lines.extend(_render_group_stats("By provider vote:", _group_trades_by_values(trades, lambda trade: trade.provider_votes)))
    lines.extend(_render_counts("Opportunity outcomes:", [opportunity.outcome for opportunity in opportunities]))
    blocked = [opportunity for opportunity in opportunities if opportunity.outcome != "traded"]
    lines.extend(
        _render_counts(
            "Blocked pressure:",
            [f"{opportunity.outcome}: {_short_reason(opportunity.reason)}" for opportunity in blocked],
        )
    )
    lines.extend(
        _render_counts(
            "Opportunity setup tags:",
            [tag for opportunity in blocked for tag in opportunity.setup_tags],
        )
    )
    lines.append("Recent closed trades:")
    recent = sorted(trades, key=lambda trade: trade.exit_time or datetime.min.replace(tzinfo=UTC), reverse=True)[:5]
    if recent:
        for trade in recent:
            tags = ",".join(trade.setup_tags[:3]) if trade.setup_tags else "no-tags"
            lines.append(
                f"- {trade.symbol}: {_money(trade.pnl)} ({_pct(trade.pnl_pct)}), "
                f"{trade.ai_verdict}, {trade.risk_profile}, tags={tags}, exit={trade.exit_reason}"
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


async def run_learning_brief(*, window_days: int = 7) -> str:
    settings = get_settings()
    configure_db_path(getattr(settings, "db_path", "auto_trader.db"))
    report = await build_edge_report(window_days=window_days)
    return render_learning_brief(report)


def main() -> None:
    parser = argparse.ArgumentParser(description="Read-only trade edge evidence report.")
    parser.add_argument("--days", type=int, default=7, help="Lookback window in days.")
    parser.add_argument("--brief", action="store_true", help="Render the learning-loop brief instead of the edge report.")
    args = parser.parse_args()
    setup_logging("ERROR")
    if args.brief:
        print(asyncio.run(run_learning_brief(window_days=args.days)))
    else:
        print(asyncio.run(run_edge_report(window_days=args.days)))


if __name__ == "__main__":
    main()
