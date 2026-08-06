"""Trade evidence report: prove edge, not just uptime."""
from __future__ import annotations

import argparse
import asyncio
import json
import os
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import aiosqlite

from auto_trader.ai_cost_report import build_ai_cost_report
from auto_trader.config.settings import get_settings
from auto_trader.intelligence.ai_paid_prefilter import INVERSE_OR_LEVERAGED_SYMBOLS
from auto_trader.persistence.db import configure_db_path, get_configured_db_path, init_db
from auto_trader.utils.logging import setup_logging


ENTRY_SIDES = {"buy", "long"}
EXIT_SIDES = {"sell", "close", "sell_short", "buy_to_cover"}
HUMAN_SAMPLE_MIN_TRADES = 10
GROUP_SAMPLE_MIN_TRADES = 3
MISSING_TAG_PREFIXES = (
    "fundamental:missing",
    "macro:missing",
    "move:missing",
    "news:missing",
    "profile:unknown",
    "relvol:missing",
    "spread:missing",
)
SAFE_PROVIDER_NAMES = {"anthropic", "deepseek", "gemini", "multi", "openai", "prefilter", "shadow", "unknown", "xai"}
SAFE_VERDICTS = {"approve", "reject", "unknown", "watch"}
SAFE_PROVIDER_ANNOTATIONS = {"high_conf", "invalid", "low_conf", "med_conf"}
EXECUTION_MODE_FILTERS = {"paper", "live", "mixed", "unknown", "legacy"}
TRADING_DECISION_SOURCES = {
    "supervisor_entry",
    "legacy_supervisor_entry",
}


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
    execution_mode: str = "unknown"
    execution_mode_inherited: bool = False
    decision_source: str = "unknown"
    context_inferred: bool = False
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
    decision_source: str = "unknown"
    context_inferred: bool = False
    setup_tags: tuple[str, ...] = ()
    provider_votes: tuple[str, ...] = ()


@dataclass(frozen=True)
class CandidateOutcomeEvidence:
    symbol: str
    provider: str
    model_tag: str
    policy_tag: str
    verdict: str
    decision_at: datetime | None
    comparison_notional: float
    price_source: str
    status: str
    decision_source: str = "unknown"
    context_inferred: bool = False
    horizon_returns: dict[int, float] = field(default_factory=dict)
    horizon_pnl: dict[int, float] = field(default_factory=dict)


@dataclass(frozen=True)
class EdgeReport:
    window_days: int
    closed_trades: list[ClosedTradeEvidence]
    opportunities: list[OpportunityEvidence]
    candidate_outcomes: list[CandidateOutcomeEvidence] = field(default_factory=list)
    estimated_ai_cost: float | None = None
    ai_cost_unknown_calls: int = 0
    ai_cost_unknown_proxy: float | None = None
    ai_cost_unestimable_calls: int = 0
    ai_cost_unavailable_reason: str | None = None
    execution_mode_filter: str | None = None
    execution_mode_counts: dict[str, int] = field(default_factory=dict)
    scorecard_withheld: bool = False
    provenance_counts: dict[str, int] = field(default_factory=dict)


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


async def _fetch_rows(
    db_path: Path,
    *,
    since: datetime | None = None,
) -> dict[str, list[dict[str, Any]]]:
    cutoff = since.astimezone(UTC).isoformat().replace("+00:00", "Z") if since else None
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        orders_cur = await db.execute(
            """
            SELECT o.client_order_id, o.broker_order_id, o.symbol, o.side, o.qty,
                   o.order_type, o.status, o.filled_qty, o.avg_fill_price,
                   o.submitted_at, o.filled_at, o.risk_decision_id, o.rationale,
                   o.execution_mode, o.decision_context_id,
                   c.decision_source, c.inferred AS context_inferred
            FROM orders AS o
            LEFT JOIN decision_contexts AS c ON c.id = o.decision_context_id
            WHERE lower(o.status) = 'filled'
            ORDER BY COALESCE(o.filled_at, o.submitted_at, ''), o.rowid
            """
        )
        risks_cur = await db.execute(
            """
            SELECT r.id, r.created_at, r.signal_id, r.approved, r.reason, r.symbol,
                   r.side, r.proposed_qty, r.sized_qty, r.equity_snapshot,
                   r.metrics_json, r.model_tag, r.trace_id, r.decision_context_id,
                   c.decision_source, c.inferred AS context_inferred
            FROM risk_decisions AS r
            LEFT JOIN decision_contexts AS c ON c.id = r.decision_context_id
            ORDER BY r.created_at, r.id
            """
        )
        orders = [dict(row) for row in await orders_cur.fetchall()]
        risks = [dict(row) for row in await risks_cur.fetchall()]
        order_risk_ids = {
            int(row["risk_decision_id"])
            for row in orders
            if row.get("risk_decision_id") is not None
        }
        referenced_signal_ids = sorted(
            {
                int(row["signal_id"])
                for row in risks
                if row.get("signal_id") is not None and int(row["id"]) in order_risk_ids
            }
        )

        signal_where_clause = ""
        signal_params: list[Any] = []
        if cutoff is not None:
            signal_where_clause = "WHERE julianday(s.created_at) >= julianday(?)"
            signal_params.append(cutoff)
        if cutoff is not None and referenced_signal_ids:
            placeholders = ", ".join("?" for _ in referenced_signal_ids)
            signal_where_clause += f" OR s.id IN ({placeholders})"
            signal_params.extend(referenced_signal_ids)
        signals_cur = await db.execute(
            f"""
            SELECT s.id, s.created_at, s.symbol, s.thesis, s.confidence, s.source,
                   s.model_tag, s.features_json, s.decision_context_id,
                   c.decision_source, c.inferred AS context_inferred
            FROM signals AS s
            LEFT JOIN decision_contexts AS c ON c.id = s.decision_context_id
            {signal_where_clause}
            ORDER BY s.created_at, s.id
            """,
            tuple(signal_params),
        )

        memo_where_clause = ""
        memo_params: list[Any] = []
        if cutoff is not None:
            memo_where_clause = "WHERE julianday(m.created_at) >= julianday(?)"
            memo_params.append(cutoff)
        if cutoff is not None and referenced_signal_ids:
            placeholders = ", ".join("?" for _ in referenced_signal_ids)
            memo_where_clause += f" OR m.signal_id IN ({placeholders})"
            memo_params.extend(referenced_signal_ids)
        memos_cur = await db.execute(
            f"""
            SELECT m.id, m.created_at, m.signal_id, m.symbol, m.provider,
                   m.model_tag, m.prompt_version, m.input_hash, m.verdict,
                   m.confidence, m.used_only_provided_data, m.validation_passed,
                   m.memo_json, m.decision_context_id, c.decision_source,
                   c.inferred AS context_inferred
            FROM ai_research_memos AS m
            LEFT JOIN decision_contexts AS c ON c.id = m.decision_context_id
            {memo_where_clause}
            ORDER BY m.created_at, m.id
            """,
            tuple(memo_params),
        )
        outcomes_where_clause = "WHERE julianday(o.decision_at) >= julianday(?)" if cutoff is not None else ""
        outcomes_params = (cutoff,) if cutoff is not None else ()
        outcomes_cur = await db.execute(
            f"""
            SELECT o.id, o.created_at, o.updated_at, o.resolved_at, o.memo_id,
                   o.signal_id, o.symbol, o.provider, o.model_tag, o.policy_tag,
                   o.decision_at, o.decision_session_date, o.verdict, o.confidence,
                   o.reference_price, o.price_source, o.comparison_notional,
                   o.status, o.last_error, o.d0_session_date, o.d0_close,
                   o.d0_return_pct, o.d0_hypothetical_pnl, o.d1_session_date,
                   o.d1_close, o.d1_return_pct, o.d1_hypothetical_pnl,
                   o.d3_session_date, o.d3_close, o.d3_return_pct,
                   o.d3_hypothetical_pnl, o.d5_session_date, o.d5_close,
                   o.d5_return_pct, o.d5_hypothetical_pnl,
                   c.decision_source, c.inferred AS context_inferred
            FROM ai_candidate_outcomes AS o
            JOIN ai_research_memos AS m ON m.id = o.memo_id
            LEFT JOIN decision_contexts AS c ON c.id = m.decision_context_id
            {outcomes_where_clause}
            ORDER BY o.decision_at, o.id
            """,
            outcomes_params,
        )
        signals = [dict(row) for row in await signals_cur.fetchall()]
        memos = [dict(row) for row in await memos_cur.fetchall()]
        outcomes = [dict(row) for row in await outcomes_cur.fetchall()]

        return {
            "orders": orders,
            "signals": signals,
            "risk_decisions": risks,
            "ai_memos": memos,
            "candidate_outcomes": outcomes,
        }


def _trade_execution_mode(
    entry: dict[str, Any],
    exit_order: dict[str, Any],
) -> tuple[str, bool]:
    entry_mode = str(entry.get("execution_mode") or "unknown").lower()
    exit_mode = str(exit_order.get("execution_mode") or "unknown").lower()
    if entry_mode in {"paper", "live"}:
        if exit_mode in {"paper", "live"} and exit_mode != entry_mode:
            return "mixed", False
        return entry_mode, exit_mode == "unknown"
    return "unknown", False


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
        execution_mode, execution_mode_inherited = _trade_execution_mode(entry, exit_order)
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
                execution_mode=execution_mode,
                execution_mode_inherited=execution_mode_inherited,
                decision_source=str(entry.get("decision_source") or "unknown"),
                context_inferred=bool(entry.get("context_inferred")),
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
                decision_source=str(signal.get("decision_source") or "unknown"),
                context_inferred=bool(signal.get("context_inferred")),
                setup_tags=_setup_tags(signal, _latest_ai_memo(rows["ai_memos"], signal_id=signal_id), risk_profile),
                provider_votes=_provider_votes_for_signal(rows["ai_memos"], signal_id),
            )
        )
    return opportunities


def _build_candidate_outcomes(
    rows: dict[str, list[dict[str, Any]]],
    *,
    since: datetime,
) -> list[CandidateOutcomeEvidence]:
    evidence: list[CandidateOutcomeEvidence] = []
    for row in rows.get("candidate_outcomes", []):
        decision_at = _parse_dt(row.get("decision_at"))
        if decision_at is None or decision_at < since:
            continue
        horizon_returns: dict[int, float] = {}
        horizon_pnl: dict[int, float] = {}
        for horizon in (0, 1, 3, 5):
            return_value = _num(row.get(f"d{horizon}_return_pct"))
            pnl_value = _num(row.get(f"d{horizon}_hypothetical_pnl"))
            if return_value is not None:
                horizon_returns[horizon] = return_value
            if pnl_value is not None:
                horizon_pnl[horizon] = pnl_value
        evidence.append(
            CandidateOutcomeEvidence(
                symbol=str(row.get("symbol") or "").upper(),
                provider=str(row.get("provider") or "unknown").lower(),
                model_tag=str(row.get("model_tag") or "unknown"),
                policy_tag=str(row.get("policy_tag") or "unknown"),
                verdict=str(row.get("verdict") or "watch").lower(),
                decision_at=decision_at,
                comparison_notional=_float(row.get("comparison_notional"), 30.0),
                price_source=str(row.get("price_source") or "model_packet.price_unavailable"),
                status=str(row.get("status") or "pending"),
                decision_source=str(row.get("decision_source") or "unknown"),
                context_inferred=bool(row.get("context_inferred")),
                horizon_returns=horizon_returns,
                horizon_pnl=horizon_pnl,
            )
        )
    return evidence


async def build_edge_report(
    *,
    db_path: Path | None = None,
    window_days: int = 7,
    execution_mode: str | None = None,
) -> EdgeReport:
    mode_filter = str(execution_mode or "").strip().lower() or None
    if mode_filter is not None and mode_filter not in EXECUTION_MODE_FILTERS:
        raise ValueError(f"unsupported execution mode filter: {execution_mode}")
    await init_db()
    since = datetime.now(UTC) - timedelta(days=max(1, int(window_days)))
    rows = await _fetch_rows(db_path or get_configured_db_path(), since=since)
    all_trades = _build_closed_trades(rows, since=since)
    all_opportunities = _build_opportunities(rows, since=since)
    all_outcomes = _build_candidate_outcomes(rows, since=since)
    trading_trades = [
        trade for trade in all_trades if trade.decision_source in TRADING_DECISION_SOURCES
    ]
    trading_opportunities = [
        row for row in all_opportunities if row.decision_source in TRADING_DECISION_SOURCES
    ]
    trading_outcomes = [
        row for row in all_outcomes if row.decision_source in TRADING_DECISION_SOURCES
    ]
    legacy_only = mode_filter == "legacy"
    if legacy_only:
        selected_trades = [
            trade for trade in trading_trades if trade.decision_source == "legacy_supervisor_entry"
        ]
        opportunities = [
            row for row in trading_opportunities if row.decision_source == "legacy_supervisor_entry"
        ]
        outcomes = [
            row for row in trading_outcomes if row.decision_source == "legacy_supervisor_entry"
        ]
    else:
        selected_trades = trading_trades
        opportunities = trading_opportunities
        outcomes = trading_outcomes
    mode_counts = dict(_bucket_counts([trade.execution_mode for trade in selected_trades]))
    actual_mixed = (
        mode_counts.get("mixed", 0) > 0
        or (mode_counts.get("paper", 0) > 0 and mode_counts.get("live", 0) > 0)
    )
    scorecard_withheld = actual_mixed and mode_filter in {None, "mixed", "legacy"}
    if mode_filter in {"paper", "live", "unknown"}:
        trades = [trade for trade in selected_trades if trade.execution_mode == mode_filter]
    else:
        trades = selected_trades
    provenance_counts = {
        "captured_trades": sum(
            1 for trade in trading_trades if trade.decision_source == "supervisor_entry"
        ),
        "legacy_trades": sum(
            1 for trade in trading_trades if trade.decision_source == "legacy_supervisor_entry"
        ),
        "captured_decisions": sum(
            1 for row in trading_outcomes if row.decision_source == "supervisor_entry"
        ),
        "legacy_decisions": sum(
            1 for row in trading_outcomes if row.decision_source == "legacy_supervisor_entry"
        ),
        "offline_decisions_excluded": sum(
            1 for row in all_outcomes if row.decision_source not in TRADING_DECISION_SOURCES
        ),
    }
    return EdgeReport(
        window_days=window_days,
        closed_trades=trades,
        opportunities=opportunities,
        candidate_outcomes=outcomes,
        execution_mode_filter=mode_filter,
        execution_mode_counts=mode_counts,
        scorecard_withheld=scorecard_withheld,
        provenance_counts=provenance_counts,
    )


async def _attach_ai_cost(report: EdgeReport, *, settings: Any) -> EdgeReport:
    end_utc = datetime.now(UTC)
    start_utc = end_utc - timedelta(days=max(1, int(report.window_days)))
    try:
        cost = await build_ai_cost_report(
            settings=settings,
            start_utc=start_utc,
            end_utc=end_utc,
            decision_sources=(
                ("legacy_supervisor_entry",)
                if report.execution_mode_filter == "legacy"
                else ("supervisor_entry", "legacy_supervisor_entry")
            ),
        )
    except Exception as exc:
        return replace(report, ai_cost_unavailable_reason=f"cost estimate failed: {type(exc).__name__}")
    unknown_proxy = 0.0
    estimated_unknown_calls = 0
    unestimable_calls = 0
    for provider in cost.providers:
        if provider.usage_unknown <= 0:
            continue
        if provider.usage_known > 0:
            unknown_proxy += (
                provider.estimated_cost / provider.usage_known
            ) * provider.usage_unknown
            estimated_unknown_calls += provider.usage_unknown
        else:
            unestimable_calls += provider.usage_unknown
    return replace(
        report,
        estimated_ai_cost=None if cost.unavailable_reason else cost.total_estimated_cost,
        ai_cost_unknown_calls=cost.total_usage_unknown,
        ai_cost_unknown_proxy=unknown_proxy if estimated_unknown_calls else None,
        ai_cost_unestimable_calls=unestimable_calls,
        ai_cost_unavailable_reason=cost.unavailable_reason,
    )


def _money(value: float) -> str:
    sign = "-" if value < 0 else ""
    return f"{sign}${abs(value):,.2f}"


def _small_money(value: float) -> str:
    if 0 < abs(value) < 0.01:
        return "<$0.01" if value > 0 else ">-$0.01"
    return _money(value)


def _pct(value: float) -> str:
    sign = "+" if value > 0 else ""
    return f"{sign}{value:.2f}%"


def _bucket_counts(values: list[str]) -> list[tuple[str, int]]:
    counts: dict[str, int] = {}
    for value in values:
        counts[value] = counts.get(value, 0) + 1
    return sorted(counts.items(), key=lambda item: (-item[1], item[0]))


def _trade_stats(trades: list[ClosedTradeEvidence]) -> dict[str, Any]:
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
            "gross_profit": 0.0,
            "gross_loss": 0.0,
            "payoff_ratio": 0.0,
            "profit_factor": None,
            "max_realized_drawdown": 0.0,
            "breakeven_win_rate": 0.0,
        }
    wins = [trade.pnl for trade in trades if trade.pnl > 0]
    losses = [trade.pnl for trade in trades if trade.pnl < 0]
    realized = sum(trade.pnl for trade in trades)
    gross_profit = sum(wins)
    gross_loss = sum(losses)
    avg_win = (gross_profit / len(wins)) if wins else 0.0
    avg_loss = (gross_loss / len(losses)) if losses else 0.0
    loss_size = abs(avg_loss)
    payoff_ratio = (avg_win / loss_size) if avg_win > 0 and loss_size > 0 else 0.0
    profit_factor = (gross_profit / abs(gross_loss)) if gross_profit > 0 and gross_loss < 0 else None
    cumulative_pnl = 0.0
    peak_pnl = 0.0
    max_realized_drawdown = 0.0
    for trade in sorted(
        trades,
        key=lambda row: row.exit_time or datetime.min.replace(tzinfo=UTC),
    ):
        cumulative_pnl += trade.pnl
        peak_pnl = max(peak_pnl, cumulative_pnl)
        max_realized_drawdown = max(max_realized_drawdown, peak_pnl - cumulative_pnl)
    breakeven_win_rate = (loss_size / (avg_win + loss_size) * 100.0) if avg_win > 0 and loss_size > 0 else 0.0
    return {
        "count": float(len(trades)),
        "wins": float(len(wins)),
        "losses": float(len(losses)),
        "win_rate": (len(wins) / len(trades) * 100.0),
        "realized_pnl": realized,
        "expectancy": realized / len(trades),
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "gross_profit": gross_profit,
        "gross_loss": gross_loss,
        "payoff_ratio": payoff_ratio,
        "profit_factor": profit_factor,
        "max_realized_drawdown": max_realized_drawdown,
        "breakeven_win_rate": breakeven_win_rate,
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


def _is_missing_or_unknown_tag(tag: str) -> bool:
    return any(tag == prefix for prefix in MISSING_TAG_PREFIXES)


def _is_ai_approved(verdict: str) -> bool:
    return str(verdict or "").lower().endswith(":approve")


def _human_sample_label(count: int) -> str:
    return "thin" if count < HUMAN_SAMPLE_MIN_TRADES else "building"


def _human_group_stats(label: str, trades: list[ClosedTradeEvidence]) -> str:
    stats = _trade_stats(trades)
    sample = "thin" if int(stats["count"]) < GROUP_SAMPLE_MIN_TRADES else "building"
    return (
        f"- {label}: n={int(stats['count'])}, P/L {_money(stats['realized_pnl'])}, "
        f"exp/trade {_money(stats['expectancy'])}, win {stats['win_rate']:.1f}%, "
        f"avg W/L {_money(stats['avg_win'])}/{_money(stats['avg_loss'])}, "
        f"payoff {_payoff_ratio_text(stats)}, sample={sample}"
    )


def _payoff_ratio_text(stats: dict[str, float]) -> str:
    if stats["avg_win"] <= 0 and stats["avg_loss"] >= 0:
        return "n/a"
    if stats["avg_loss"] >= 0:
        return "no losses yet"
    if stats["avg_win"] <= 0:
        return "no wins yet"
    return f"{stats['payoff_ratio']:.2f}x"


def _profit_factor_text(stats: dict[str, Any]) -> str:
    value = stats.get("profit_factor")
    if value is None:
        return "n/a (no mix of profits and losses)"
    return f"{float(value):.2f}x"


def _win_rate_gap_text(stats: dict[str, float]) -> str:
    if stats["breakeven_win_rate"] <= 0:
        return "n/a"
    gap = stats["win_rate"] - stats["breakeven_win_rate"]
    sign = "+" if gap > 0 else ""
    return f"{sign}{gap:.1f} pts"


def _payoff_read(stats: dict[str, float]) -> str:
    if int(stats["count"]) == 0:
        return "Need closed trades before judging payoff shape."
    if stats["wins"] == 0:
        return "No winning exits yet; selection or exits are not paying."
    if stats["losses"] == 0:
        return "No losing exits yet; payoff shape is not stressed by losses in this window."
    if stats["expectancy"] > 0 and stats["win_rate"] >= stats["breakeven_win_rate"]:
        return "Win rate is clearing the breakeven bar for this payoff shape."
    if stats["expectancy"] > 0:
        return (
            "Realized expectancy is positive, but the headline win rate is below the calculated "
            "breakeven rate; flat trades and the small sample can create this mismatch."
        )
    if stats["win_rate"] >= stats["breakeven_win_rate"]:
        return "Win rate is near enough, but realized dollars are still flat/down; inspect fees, sizing, and outlier losses."
    loss_multiple = abs(stats["avg_loss"]) / stats["avg_win"] if stats["avg_win"] > 0 else 0.0
    return (
        f"Win rate is not enough at this payoff; average loss is {loss_multiple:.2f}x "
        "the average win."
    )


def _payoff_group_line(label: str, trades: list[ClosedTradeEvidence]) -> str:
    stats = _trade_stats(trades)
    sample = ", sample=thin" if int(stats["count"]) < GROUP_SAMPLE_MIN_TRADES else ""
    return (
        f"- {label}: n={int(stats['count'])}, P/L {_money(stats['realized_pnl'])}, "
        f"exp {_money(stats['expectancy'])}, avg W/L {_money(stats['avg_win'])}/{_money(stats['avg_loss'])}, "
        f"payoff {_payoff_ratio_text(stats)}{sample}"
    )


def _render_payoff_shape(trades: list[ClosedTradeEvidence], stats: dict[str, float]) -> list[str]:
    lines = [
        "Payoff shape:",
        f"- Gross profit/loss: {_money(stats['gross_profit'])} / {_money(stats['gross_loss'])}",
        f"- Avg win/loss: {_money(stats['avg_win'])} / {_money(stats['avg_loss'])}",
        f"- Win/loss payoff ratio: {_payoff_ratio_text(stats)}",
    ]
    if stats["breakeven_win_rate"] > 0:
        lines.append(
            f"- Breakeven win rate at this payoff: {stats['breakeven_win_rate']:.1f}% "
            f"(current {stats['win_rate']:.1f}%, gap {_win_rate_gap_text(stats)})"
        )
    else:
        lines.append("- Breakeven win rate at this payoff: n/a")
    if int(stats["count"]) < HUMAN_SAMPLE_MIN_TRADES:
        lines.append(
            f"- Sample: {_human_sample_label(int(stats['count']))}; payoff read is provisional until "
            f"{HUMAN_SAMPLE_MIN_TRADES}+ closed trades."
        )
    lines.append(f"- Read: {_payoff_read(stats)}")

    exit_groups = _group_trades_by_values(trades, lambda trade: [_exit_label(trade.exit_reason)])
    if exit_groups:
        lines.append("Payoff by exit reason (weakest first):")
        sorted_groups = sorted(
            exit_groups.items(),
            key=lambda item: (_trade_stats(item[1])["expectancy"], _trade_stats(item[1])["realized_pnl"], item[0]),
        )
        for label, group in sorted_groups[:5]:
            lines.append(_payoff_group_line(label, group))
    return lines


def _outcome_label(outcome: str) -> str:
    labels = {
        "traded": "Traded",
        "ai_approve": "AI approved but no order",
        "ai_watch": "AI said watch",
        "ai_reject": "AI rejected",
        "prefilter_blocked": "Paid prefilter blocked before AI spend",
        "risk_blocked": "RiskEngine blocked",
        "candidate_only": "Candidate logged only",
    }
    return labels.get(outcome, outcome.replace("_", " ").title())


def _blocker_label(outcome: str, reason: str) -> str:
    text = str(reason or "").lower()
    if outcome == "prefilter_blocked":
        return "Paid prefilter blocked weak setup"
    if outcome == "ai_watch":
        return "AI committee said watch"
    if outcome == "ai_approve":
        return "AI approved but no order followed"
    if outcome == "ai_reject":
        return "AI committee rejected"
    if outcome == "candidate_only":
        return "Candidate logged but did not reach order"
    if outcome == "risk_blocked":
        if "open position" in text or "already has" in text or "duplicate" in text:
            return "RiskEngine: already holding symbol"
        if "gross exposure" in text or "exposure" in text:
            return "RiskEngine: gross exposure cap"
        if "daily" in text or "max new" in text or "position limit" in text:
            return "RiskEngine: entry capacity limit"
        if "buying power" in text or "cash" in text:
            return "RiskEngine: cash/buying power"
        if "halt" in text or "paused" in text:
            return "RiskEngine: system state guard"
        return "RiskEngine: other block"
    return _short_reason(reason, limit=40)


def _exit_label(reason: str) -> str:
    text = str(reason or "").strip().lower()
    if text == "broker_reconciliation":
        return "broker matched filled exit (administrative)"
    if "take profit" in text:
        return "take profit"
    if "max loss" in text or "stop loss" in text:
        return "max loss / stop"
    if "trailing" in text:
        return "trailing stop"
    if "stagnation" in text:
        return "stagnation exit"
    if "max hold" in text:
        return "max hold"
    return _short_reason(reason or "unknown", limit=42)


def _tag_label(tag: str) -> str:
    labels = {
        "relvol:low": "low relative volume",
        "relvol:normal": "normal relative volume",
        "relvol:strong": "strong relative volume",
        "relvol:hot": "hot relative volume",
        "move:cold": "cold move",
        "move:tradable": "tradable move",
        "move:extended": "extended move",
        "move:parabolic": "parabolic move",
        "spread:tight": "tight spread",
        "spread:workable": "workable spread",
        "spread:wide": "wide spread",
        "news:present": "news present",
        "fundamental:present": "fundamentals present",
        "macro:ok": "macro context present",
        "macro:error": "macro context error",
        "near_high": "near high",
        "inverse_or_leveraged": "inverse/leveraged",
        "profile:aggressive": "aggressive profile",
        "profile:conservative": "conservative profile",
        "profile:risky": "risky profile",
    }
    return labels.get(tag, tag.replace(":", " ").replace("_", " "))


def _safe_provider_vote_label(label: str) -> str:
    parts = [part.strip().lower() for part in str(label or "").split(":") if part.strip()]
    if not parts:
        return "provider:unknown"
    provider = parts[0] if parts[0] in SAFE_PROVIDER_NAMES else "provider"
    verdict = parts[1] if len(parts) > 1 and parts[1] in SAFE_VERDICTS else "unknown"
    annotations = [part for part in parts[2:] if part in SAFE_PROVIDER_ANNOTATIONS]
    return ":".join([provider, verdict, *annotations])


def _render_candidate_funnel(opportunities: list[OpportunityEvidence]) -> list[str]:
    lines = ["Candidate funnel:"]
    counts = dict(_bucket_counts([opportunity.outcome for opportunity in opportunities]))
    traded = [opportunity for opportunity in opportunities if opportunity.outcome == "traded"]
    if traded:
        ai_approved = sum(1 for opportunity in traded if _is_ai_approved(opportunity.ai_verdict))
        lines.extend(
            [
                f"- Traded opportunities (all paths): {len(traded)}",
                f"- Of traded, valid AI approval recorded: {ai_approved}",
                f"- Of traded, no valid AI approval recorded: {len(traded) - ai_approved}",
            ]
        )
    ordered = ["ai_approve", "ai_watch", "ai_reject", "prefilter_blocked", "risk_blocked", "candidate_only"]
    for outcome in ordered:
        if outcome in counts:
            lines.append(f"- {_outcome_label(outcome)}: {counts[outcome]}")
    for outcome, count in sorted(counts.items(), key=lambda item: (-item[1], item[0])):
        if outcome not in ordered and outcome != "traded":
            lines.append(f"- {_outcome_label(outcome)}: {count}")
    if len(lines) == 1:
        lines.append("- none")
    return lines


def _blocked_pressure_counts(opportunities: list[OpportunityEvidence]) -> list[tuple[str, int]]:
    blocked = [opportunity for opportunity in opportunities if opportunity.outcome != "traded"]
    return _bucket_counts([_blocker_label(opportunity.outcome, opportunity.reason) for opportunity in blocked])


def _render_main_blockers(opportunities: list[OpportunityEvidence], *, limit: int = 5) -> list[str]:
    lines = ["Main blockers:"]
    blocked_counts = _blocked_pressure_counts(opportunities)
    if not blocked_counts:
        return lines + ["- none"]
    for label, count in blocked_counts[:limit]:
        lines.append(f"- {label}: {count}")
    return lines


def _render_signal_quality_notes(trades: list[ClosedTradeEvidence], opportunities: list[OpportunityEvidence]) -> list[str]:
    lines = ["Signal quality notes:"]
    trade_tags = [tag for trade in trades for tag in trade.setup_tags]
    opportunity_tags_all = [tag for opportunity in opportunities for tag in opportunity.setup_tags]
    missing_count = sum(1 for tag in [*trade_tags, *opportunity_tags_all] if _is_missing_or_unknown_tag(tag))
    setup_groups = [
        row
        for row in _trade_groups_by_values(trades, lambda trade: [tag for tag in trade.setup_tags if not _is_missing_or_unknown_tag(tag)])
        if int(row[2]["count"]) > 0
    ]
    positive = [row for row in setup_groups if row[2]["realized_pnl"] > 0]
    negative = [row for row in setup_groups if row[2]["realized_pnl"] < 0]
    positive.sort(key=lambda row: (row[2]["expectancy"], row[2]["realized_pnl"], row[0]), reverse=True)
    negative.sort(key=lambda row: (row[2]["expectancy"], row[2]["realized_pnl"], row[0]))
    if positive:
        rendered = ", ".join(f"{_tag_label(name)} ({int(stats['count'])})" for name, _group, stats in positive[:4])
        lines.append(f"- Helpful so far: {rendered}")
    else:
        lines.append("- Helpful so far: none yet")
    if negative:
        rendered = ", ".join(f"{_tag_label(name)} ({int(stats['count'])})" for name, _group, stats in negative[:3])
        lines.append(f"- Dragging so far: {rendered}")
    else:
        lines.append("- Dragging so far: none yet")
    if missing_count:
        lines.append(f"- Missing/unknown data tags hidden from detail: {missing_count}")
    opportunity_tags = [tag for tag in opportunity_tags_all if not _is_missing_or_unknown_tag(tag)]
    common = _bucket_counts(opportunity_tags)[:4]
    if common:
        lines.append("- Common candidate tags: " + ", ".join(f"{_tag_label(tag)} ({count})" for tag, count in common))
    return lines


def _candidate_horizon_stats(
    outcomes: list[CandidateOutcomeEvidence],
    horizon: int,
) -> dict[str, float]:
    resolved = [row for row in outcomes if horizon in row.horizon_returns]
    if not resolved:
        return {"count": 0.0, "avg_return_pct": 0.0, "hypothetical_pnl": 0.0}
    return {
        "count": float(len(resolved)),
        "avg_return_pct": sum(row.horizon_returns[horizon] for row in resolved) / len(resolved),
        "hypothetical_pnl": sum(row.horizon_pnl.get(horizon, 0.0) for row in resolved),
    }


def _candidate_outcome_groups(
    outcomes: list[CandidateOutcomeEvidence],
) -> dict[str, list[CandidateOutcomeEvidence]]:
    groups: dict[str, list[CandidateOutcomeEvidence]] = {}
    for row in outcomes:
        key = f"{row.policy_tag} ({row.model_tag})"
        groups.setdefault(key, []).append(row)
    return groups


def _candidate_selection_lift(
    outcomes: list[CandidateOutcomeEvidence],
    *,
    horizon: int = 5,
) -> dict[str, float] | None:
    approved = [row for row in outcomes if row.verdict == "approve"]
    not_approved = [row for row in outcomes if row.verdict in {"watch", "reject"}]
    approved_stats = _candidate_horizon_stats(approved, horizon)
    not_approved_stats = _candidate_horizon_stats(not_approved, horizon)
    if approved_stats["count"] <= 0 or not_approved_stats["count"] <= 0:
        return None
    spread = approved_stats["avg_return_pct"] - not_approved_stats["avg_return_pct"]
    comparison_notional = approved[0].comparison_notional if approved else 30.0
    estimated_added = comparison_notional * (spread / 100.0) * approved_stats["count"]
    return {
        "approved_count": approved_stats["count"],
        "not_approved_count": not_approved_stats["count"],
        "spread_pct": spread,
        "estimated_added": estimated_added,
    }


def _render_candidate_outcomes(outcomes: list[CandidateOutcomeEvidence]) -> list[str]:
    lines = [
        "AI decision outcomes (observed hypothetical; no rejected stock was purchased):",
        "- Basis: model-packet reference price and a fixed $30 hypothetical position.",
    ]
    if not outcomes:
        return lines + ["- No resolved provider-decision outcomes in this window yet."]
    pending = sum(1 for row in outcomes if row.status in {"pending", "partial"})
    invalid = sum(1 for row in outcomes if row.status == "invalid_reference")
    if pending or invalid:
        lines.append(f"- Data status: {pending} pending/partial, {invalid} invalid reference.")

    for label, group in sorted(
        _candidate_outcome_groups(outcomes).items(),
        key=lambda item: (-len(item[1]), item[0]),
    )[:4]:
        approved = [row for row in group if row.verdict == "approve"]
        not_approved = [row for row in group if row.verdict in {"watch", "reject"}]
        lines.append(
            f"- {label}: decisions={len(group)}, approved={len(approved)}, "
            f"watch/reject={len(not_approved)}"
        )
        for horizon in (0, 1, 3, 5):
            approved_stats = _candidate_horizon_stats(approved, horizon)
            not_approved_stats = _candidate_horizon_stats(not_approved, horizon)
            if approved_stats["count"] <= 0 and not_approved_stats["count"] <= 0:
                continue
            spread = approved_stats["avg_return_pct"] - not_approved_stats["avg_return_pct"]
            spread_text = (
                f", selection spread {_pct(spread)}"
                if approved_stats["count"] > 0 and not_approved_stats["count"] > 0
                else ""
            )
            lines.append(
                f"  D{horizon}: approved n={int(approved_stats['count'])}, "
                f"avg {_pct(approved_stats['avg_return_pct'])}, "
                f"$30 hypothetical {_money(approved_stats['hypothetical_pnl'])}; "
                f"watch/reject n={int(not_approved_stats['count'])}, "
                f"avg {_pct(not_approved_stats['avg_return_pct'])}, "
                f"$30 hypothetical {_money(not_approved_stats['hypothetical_pnl'])}"
                f"{spread_text}"
            )
    return lines


def _render_ai_edge_check(trades: list[ClosedTradeEvidence]) -> list[str]:
    approved = [trade for trade in trades if _is_ai_approved(trade.ai_verdict)]
    other = [trade for trade in trades if not _is_ai_approved(trade.ai_verdict)]
    lines = ["AI edge check:"]
    lines.append(_human_group_stats("AI-approved trades", approved))
    lines.append(_human_group_stats("Other/no-AI trades", other))
    approved_stats = _trade_stats(approved)
    other_stats = _trade_stats(other)
    if len(trades) < HUMAN_SAMPLE_MIN_TRADES or len(approved) < GROUP_SAMPLE_MIN_TRADES or len(other) < GROUP_SAMPLE_MIN_TRADES:
        lines.append("- Read: too thin to judge; keep collecting closed trades before declaring edge.")
    elif approved_stats["expectancy"] > other_stats["expectancy"]:
        lines.append("- Read: AI-approved trades are ahead so far.")
    elif approved_stats["expectancy"] < other_stats["expectancy"]:
        lines.append("- Read: AI-approved trades are lagging so far.")
    else:
        lines.append("- Read: AI-approved and other trades are roughly tied so far.")
    provider_groups = [
        row
        for row in _trade_groups_by_values(trades, lambda trade: trade.provider_votes)
        if int(row[2]["count"]) >= GROUP_SAMPLE_MIN_TRADES
    ]
    if provider_groups:
        provider_groups.sort(key=lambda row: (row[2]["expectancy"], row[2]["realized_pnl"], row[0]), reverse=True)
        lines.append(
            "- Provider vote buckets with enough sample: "
            + ", ".join(
                f"{_safe_provider_vote_label(name)} n={int(stats['count'])} "
                f"exp={_money(stats['expectancy'])} payoff={_payoff_ratio_text(stats)}"
                for name, _group, stats in provider_groups[:3]
            )
        )
    else:
        lines.append("- Provider vote detail: collapsed until each bucket has at least 3 closed trades.")
    return lines


def _summary_lines(
    trades: list[ClosedTradeEvidence],
    opportunities: list[OpportunityEvidence],
    stats: dict[str, Any],
    *,
    estimated_ai_cost: float | None,
) -> list[str]:
    approved = [trade for trade in trades if _is_ai_approved(trade.ai_verdict)]
    other = [trade for trade in trades if not _is_ai_approved(trade.ai_verdict)]
    approved_stats = _trade_stats(approved)
    other_stats = _trade_stats(other)
    sample = _human_sample_label(len(trades))
    lines = ["Read me first:"]
    if trades:
        lines.append(f"- Evidence sample is {sample}: {len(trades)} closed trades in this window.")
    else:
        lines.append("- No closed trades yet; this is candidate pressure only.")
    if len(trades) < HUMAN_SAMPLE_MIN_TRADES or len(approved) < GROUP_SAMPLE_MIN_TRADES or len(other) < GROUP_SAMPLE_MIN_TRADES:
        lines.append("- AI edge is not proven yet; approved vs non-approved samples are still thin.")
    elif approved_stats["expectancy"] > other_stats["expectancy"]:
        lines.append(
            f"- AI-approved trades are ahead so far: {_money(approved_stats['expectancy'])} vs "
            f"{_money(other_stats['expectancy'])} expectancy."
        )
    else:
        lines.append(
            f"- AI-approved trades are not ahead yet: {_money(approved_stats['expectancy'])} vs "
            f"{_money(other_stats['expectancy'])} expectancy."
        )
    blocked_counts = _blocked_pressure_counts(opportunities)
    if blocked_counts:
        lines.append(f"- Biggest pressure: {blocked_counts[0][0]} ({blocked_counts[0][1]}).")
    elif opportunities:
        lines.append("- Biggest pressure: none; all observed opportunities traded.")
    else:
        lines.append("- Biggest pressure: no opportunity evidence in this window.")
    net_pnl = stats["realized_pnl"] - estimated_ai_cost if estimated_ai_cost is not None else stats["realized_pnl"]
    if net_pnl > 0:
        suffix = " after estimated AI cost" if estimated_ai_cost is not None else " before unavailable AI cost"
        lines.append(f"- Dollar scorecard is positive{suffix}, but edge graduation needs more trades.")
    elif trades:
        lines.append("- Dollar scorecard is flat/negative; win rate cannot compensate for losing money.")
    return lines


def _render_recent_trades(trades: list[ClosedTradeEvidence]) -> list[str]:
    lines = ["Recent closed trades:"]
    recent = sorted(trades, key=lambda trade: trade.exit_time or datetime.min.replace(tzinfo=UTC), reverse=True)[:5]
    if not recent:
        return lines + ["- none yet"]
    for trade in recent:
        useful_tags = [_tag_label(tag) for tag in trade.setup_tags if not _is_missing_or_unknown_tag(tag)]
        tags = ", ".join(useful_tags[:3]) if useful_tags else "no strong tags"
        lines.append(
            f"- {trade.symbol}: {_money(trade.pnl)} ({_pct(trade.pnl_pct)}), "
            f"AI={trade.ai_verdict}, profile={trade.risk_profile}, tags={tags}, exit={_exit_label(trade.exit_reason)}"
        )
    return lines


def _render_next_read(trades: list[ClosedTradeEvidence], opportunities: list[OpportunityEvidence], stats: dict[str, float]) -> list[str]:
    lines = ["Next read:"]
    if not trades:
        lines.append("- Need closed trades before judging edge.")
        return lines
    if len(trades) < HUMAN_SAMPLE_MIN_TRADES:
        lines.append("- Keep the experiment running; sample is too thin for live-money conclusions.")
    elif stats["expectancy"] <= 0:
        lines.append("- Focus the next review on weak tags and whether AI blocks are filtering enough bad ideas.")
    else:
        lines.append("- Compare AI-approved trades against watch/reject/no-AI outcomes as the sample grows.")
    blocked_counts = _blocked_pressure_counts(opportunities)
    if blocked_counts:
        lines.append(f"- Watch whether `{blocked_counts[0][0]}` is useful filtering or excessive friction.")
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
        f"exp {_money(stats['expectancy'])}, win {stats['win_rate']:.1f}%, payoff {_payoff_ratio_text(stats)}"
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
        f"Realized P/L before AI cost: {_money(stats['realized_pnl'])}",
        (
            f"Net after estimated AI cost: {_money(stats['realized_pnl'] - report.estimated_ai_cost)}"
            if report.estimated_ai_cost is not None
            else "Net after estimated AI cost: unavailable"
        ),
        (
            f"Dollar expectancy/trade: {_money(stats['expectancy'])}; "
            f"profit factor {_profit_factor_text(stats)}; win rate (secondary) {stats['win_rate']:.1f}%"
        ),
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


def _memory_group(name: str, stats: dict[str, float]) -> dict[str, Any]:
    count = int(stats["count"])
    return {
        "key": name,
        "n": count,
        "sample": "thin" if count < 3 else "building",
        "realized_pnl": round(float(stats["realized_pnl"]), 4),
        "expectancy": round(float(stats["expectancy"]), 4),
        "win_rate": round(float(stats["win_rate"]), 2),
    }


def _memory_group_line(row: dict[str, Any]) -> str:
    return (
        f"{row['key']} n={row['n']} sample={row['sample']} "
        f"pnl={_money(float(row['realized_pnl']))} exp={_money(float(row['expectancy']))} "
        f"win={float(row['win_rate']):.1f}%"
    )


def render_scoreboard_memory_context(pack: dict[str, Any]) -> str:
    sample_label = str(pack.get("sample_label") or "thin")
    performance = _dict(pack.get("performance"))
    estimated_ai_cost = _num(performance.get("estimated_ai_cost"))
    net_after_ai_cost = _num(performance.get("net_after_estimated_ai_cost"))
    lines = [
        "SCOREBOARD MEMORY PACK",
        "Use as compact context only; this is observed evidence, not order authority.",
        (
            f"Window: {pack.get('window_days')}d; closed_trades={pack.get('closed_trade_count', 0)}; "
            f"opportunities={pack.get('opportunity_count', 0)}; sample={sample_label}"
        ),
        (
            f"Observed P/L before AI cost={_money(_float(performance.get('realized_pnl')))}; "
            f"estimated AI cost={_money(estimated_ai_cost) if estimated_ai_cost is not None else 'unavailable'}; "
            f"net={_money(net_after_ai_cost) if net_after_ai_cost is not None else 'unavailable'}; "
            f"dollar expectancy={_money(_float(performance.get('expectancy')))}"
        ),
        (
            f"Profit factor={performance.get('profit_factor') if performance.get('profit_factor') is not None else 'n/a'}; "
            f"max realized drawdown={_money(_float(performance.get('max_realized_drawdown')))}; "
            f"win rate (secondary)={_float(performance.get('win_rate')):.1f}%"
        ),
        f"Caveat: {_list(pack.get('notes'))[0] if _list(pack.get('notes')) else 'Use this to aim questions, not declare truth.'}",
        "Positive setup evidence:",
    ]
    positive = _list(pack.get("positive_observed_tags"))
    lines.extend([f"- {_memory_group_line(row)}" for row in positive[:5] if isinstance(row, dict)] or ["- none yet"])
    lines.append("Negative setup evidence:")
    negative = _list(pack.get("negative_observed_tags"))
    lines.extend([f"- {_memory_group_line(row)}" for row in negative[:5] if isinstance(row, dict)] or ["- none yet"])
    lines.append("Provider vote buckets:")
    providers = _list(pack.get("provider_vote_outcome_buckets"))
    lines.extend([f"- {_memory_group_line(row)}" for row in providers[:6] if isinstance(row, dict)] or ["- none yet"])
    lines.append("Blocked pressure:")
    blocked = _list(pack.get("blocked_pressure"))
    lines.extend([f"- {row.get('key')}: {row.get('count')}" for row in blocked[:5] if isinstance(row, dict)] or ["- none"])
    return "\n".join(lines)


def build_scoreboard_memory_pack(
    report: EdgeReport,
    *,
    generated_at: datetime | None = None,
    limit: int = 5,
) -> dict[str, Any]:
    trades = report.closed_trades
    opportunities = report.opportunities
    stats = _trade_stats(trades)
    setup_groups = _trade_groups_by_values(trades, lambda trade: trade.setup_tags)
    provider_groups = _trade_groups_by_values(trades, lambda trade: trade.provider_votes)
    blocked = [opportunity for opportunity in opportunities if opportunity.outcome != "traded"]
    blocked_counts = _bucket_counts([f"{opportunity.outcome}: {_short_reason(opportunity.reason)}" for opportunity in blocked])

    positive_tags = [
        _memory_group(name, row_stats)
        for name, _group, row_stats in sorted(
            setup_groups,
            key=lambda row: (row[2]["expectancy"], row[2]["realized_pnl"], row[0]),
            reverse=True,
        )
        if row_stats["realized_pnl"] > 0
    ][:limit]
    negative_tags = [
        _memory_group(name, row_stats)
        for name, _group, row_stats in sorted(
            setup_groups,
            key=lambda row: (row[2]["expectancy"], row[2]["realized_pnl"], row[0]),
        )
        if row_stats["realized_pnl"] < 0
    ][:limit]
    provider_sorted = sorted(
        provider_groups,
        key=lambda row: (row[2]["realized_pnl"], row[2]["expectancy"], row[0]),
        reverse=True,
    )
    provider_worst = [
        row
        for row in sorted(provider_groups, key=lambda row: (row[2]["realized_pnl"], row[2]["expectancy"], row[0]))
        if row[2]["realized_pnl"] < 0
    ]
    provider_rows: list[dict[str, Any]] = []
    seen_provider_keys: set[str] = set()
    for name, _group, row_stats in [*provider_sorted[:limit], *provider_worst[:limit]]:
        if name in seen_provider_keys:
            continue
        seen_provider_keys.add(name)
        provider_rows.append(_memory_group(name, row_stats))

    sample_quality = "thin" if int(stats["count"]) < 10 else "building"
    sample_caveat = (
        "Thin sample; use this to aim questions, not declare truth."
        if sample_quality == "thin"
        else "Building sample; still treat as observed evidence, not order authority."
    )
    generated = generated_at or datetime.now(UTC)
    pack: dict[str, Any] = {
        "kind": "scoreboard_memory_pack",
        "generated_at": generated.astimezone(UTC).isoformat().replace("+00:00", "Z"),
        "window_days": report.window_days,
        "closed_trade_count": int(stats["count"]),
        "opportunity_count": len(opportunities),
        "sample_label": sample_quality,
        "sample": {
            "closed_trades": int(stats["count"]),
            "opportunities": len(opportunities),
            "quality": sample_quality,
            "caveat": sample_caveat,
        },
        "notes": [
            sample_caveat,
            "Observed evidence only; do not treat this pack as order authority.",
            "RiskEngine remains the sizing and order-flow authority.",
        ],
        "performance": {
            "realized_pnl": round(float(stats["realized_pnl"]), 4),
            "estimated_ai_cost": (
                None if report.estimated_ai_cost is None else round(float(report.estimated_ai_cost), 4)
            ),
            "net_after_estimated_ai_cost": (
                None
                if report.estimated_ai_cost is None
                else round(float(stats["realized_pnl"]) - float(report.estimated_ai_cost), 4)
            ),
            "ai_cost_unknown_calls": int(report.ai_cost_unknown_calls),
            "ai_cost_unknown_proxy": (
                None
                if report.ai_cost_unknown_proxy is None
                else round(float(report.ai_cost_unknown_proxy), 4)
            ),
            "ai_cost_unestimable_calls": int(report.ai_cost_unestimable_calls),
            "expectancy": round(float(stats["expectancy"]), 4),
            "win_rate": round(float(stats["win_rate"]), 2),
            "wins": int(stats["wins"]),
            "losses": int(stats["losses"]),
            "avg_win": round(float(stats["avg_win"]), 4),
            "avg_loss": round(float(stats["avg_loss"]), 4),
            "gross_profit": round(float(stats["gross_profit"]), 4),
            "gross_loss": round(float(stats["gross_loss"]), 4),
            "payoff_ratio": round(float(stats["payoff_ratio"]), 4),
            "profit_factor": (
                None if stats["profit_factor"] is None else round(float(stats["profit_factor"]), 4)
            ),
            "max_realized_drawdown": round(float(stats["max_realized_drawdown"]), 4),
            "breakeven_win_rate": round(float(stats["breakeven_win_rate"]), 2),
        },
        "positive_observed_tags": positive_tags,
        "negative_observed_tags": negative_tags,
        "provider_vote_outcome_buckets": provider_rows[: limit * 2],
        "blocked_pressure": [{"key": key, "count": count} for key, count in blocked_counts[:limit]],
    }
    pack["prompt_context"] = render_scoreboard_memory_context(pack)
    return pack


def default_scoreboard_memory_pack_path(settings: Any) -> Path:
    override = getattr(settings, "scoreboard_memory_path", None) or os.getenv("AUTO_TRADER_SCOREBOARD_MEMORY_PATH")
    if override:
        return Path(override)
    db_path = Path(str(getattr(settings, "db_path", "auto_trader.db") or "auto_trader.db"))
    root = db_path.parent if str(db_path.parent) not in {"", "."} else Path(".")
    return root / "runtime" / "scoreboard_memory_pack.json"


def write_scoreboard_memory_pack(pack: dict[str, Any], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        tmp_path.write_text(json.dumps(pack, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        os.replace(tmp_path, path)
    except Exception:
        try:
            tmp_path.unlink(missing_ok=True)
        finally:
            raise
    return path


def _execution_mode_header(report: EdgeReport) -> list[str]:
    counts = {
        mode: int(report.execution_mode_counts.get(mode, 0))
        for mode in ("paper", "live", "unknown", "mixed")
    }
    if report.execution_mode_filter in {"paper", "live", "unknown"}:
        label = report.execution_mode_filter.upper()
    elif report.scorecard_withheld:
        label = "MIXED"
    else:
        present = [mode for mode, count in counts.items() if count > 0]
        label = present[0].upper() if len(present) == 1 else ("NONE" if not present else "MIXED")
    lines = [
        f"Execution mode: {label}",
        (
            "Trade mode counts: "
            f"paper={counts['paper']}, live={counts['live']}, "
            f"unknown={counts['unknown']}, conflicting={counts['mixed']}"
        ),
    ]
    if report.execution_mode_filter == "legacy":
        lines.append("Provenance filter: LEGACY INFERRED RECORDS ONLY")
    inherited = sum(1 for trade in report.closed_trades if trade.execution_mode_inherited)
    if inherited:
        lines.append(
            f"Mode note: {inherited} trade(s) inherit mode from a proven entry because the paired exit is unknown."
        )
    return lines


def _provenance_header(report: EdgeReport) -> list[str]:
    counts = report.provenance_counts
    return [
        (
            "Trade provenance: "
            f"captured={int(counts.get('captured_trades', 0))}, "
            f"legacy inferred={int(counts.get('legacy_trades', 0))}"
        ),
        (
            "AI decision provenance: "
            f"captured={int(counts.get('captured_decisions', 0))}, "
            f"legacy inferred={int(counts.get('legacy_decisions', 0))}, "
            f"offline excluded={int(counts.get('offline_decisions_excluded', 0))}"
        ),
        "Legacy note: inferred rows are included for continuity but never counted as proven provenance.",
    ]


def render_edge_report(report: EdgeReport) -> str:
    trades = report.closed_trades
    opportunities = report.opportunities
    candidate_outcomes = report.candidate_outcomes
    stats = _trade_stats(trades)
    outcome_groups = _candidate_outcome_groups(candidate_outcomes)
    primary_outcomes = max(outcome_groups.values(), key=len) if outcome_groups else []
    selection_lift = _candidate_selection_lift(primary_outcomes)
    lines = [
        "EDGE REPORT",
        f"Window: last {report.window_days} days",
    ]
    lines.extend(_execution_mode_header(report))
    lines.extend(_provenance_header(report))
    if report.scorecard_withheld:
        lines.extend(
            [
                "",
                "!!! MIXED PAPER/LIVE EVIDENCE !!!",
                "Dollar scorecard: WITHHELD to prevent blending simulated and real fills.",
                "Run /edge paper or /edge live. Use /edge mixed only for this diagnostic view.",
            ]
        )
        return "\n".join(lines)
    lines.append("")
    lines.extend(
        _summary_lines(
            trades,
            opportunities,
            stats,
            estimated_ai_cost=report.estimated_ai_cost,
        )
    )
    lines.extend(
        [
            "",
            "Dollar scorecard:",
            f"- Closed trades: {int(stats['count'])}",
            f"- Realized P/L before AI cost: {_money(stats['realized_pnl'])}",
            (
                f"- Estimated AI cost: {_money(report.estimated_ai_cost)}"
                if report.estimated_ai_cost is not None
                else "- Estimated AI cost: unavailable"
            ),
            (
                f"- Net after estimated AI cost: {_money(stats['realized_pnl'] - report.estimated_ai_cost)}"
                if report.estimated_ai_cost is not None
                else "- Net after estimated AI cost: unavailable"
            ),
            f"- Dollar expectancy/trade: {_money(stats['expectancy'])}",
            f"- Avg win/loss: {_money(stats['avg_win'])} / {_money(stats['avg_loss'])}",
            f"- Profit factor: {_profit_factor_text(stats)}",
            f"- Max realized drawdown: {_money(stats['max_realized_drawdown'])}",
            f"- Win rate (secondary): {stats['win_rate']:.1f}% ({int(stats['wins'])}W/{int(stats['losses'])}L)",
            (
                f"- Observed D5 AI selection lift: {_money(selection_lift['estimated_added'])} "
                f"across {int(selection_lift['approved_count'])} approved $30 hypotheticals "
                f"(approved minus watch/reject spread {_pct(selection_lift['spread_pct'])})."
                if selection_lift is not None
                else "- Observed D5 AI selection lift: pending approved and watch/reject outcomes."
            ),
        ]
    )
    if report.ai_cost_unknown_calls:
        if report.ai_cost_unknown_proxy is not None:
            lines.append(
                f"- AI cost coverage: {report.ai_cost_unknown_calls} billed call(s) lacked token usage; "
                f"same-provider observed averages suggest about {_small_money(report.ai_cost_unknown_proxy)} "
                "was omitted from the estimate above."
            )
        else:
            lines.append(
                f"- AI cost coverage: {report.ai_cost_unknown_calls} billed call(s) lacked token usage; "
                "no same-provider usage-known call exists in this window, so omitted cost is not estimable."
            )
        if report.ai_cost_unestimable_calls:
            lines.append(
                f"- AI cost coverage detail: {report.ai_cost_unestimable_calls} of those call(s) "
                "had no same-provider comparison."
            )
    if report.ai_cost_unavailable_reason:
        lines.append(f"- AI cost caveat: {report.ai_cost_unavailable_reason}")
    sections = [
        _render_payoff_shape(trades, stats),
        _render_ai_edge_check(trades),
        _render_candidate_outcomes(candidate_outcomes),
        _render_candidate_funnel(opportunities),
        _render_main_blockers(opportunities),
        _render_signal_quality_notes(trades, opportunities),
        _render_recent_trades(trades),
        _render_next_read(trades, opportunities, stats),
    ]
    for section in sections:
        lines.append("")
        lines.extend(section)
    return "\n".join(lines)


async def run_edge_report(*, window_days: int = 7, execution_mode: str | None = None) -> str:
    settings = get_settings()
    configure_db_path(getattr(settings, "db_path", "auto_trader.db"))
    report = await build_edge_report(window_days=window_days, execution_mode=execution_mode)
    report = await _attach_ai_cost(report, settings=settings)
    return render_edge_report(report)


async def run_learning_brief(*, window_days: int = 7) -> str:
    settings = get_settings()
    configure_db_path(getattr(settings, "db_path", "auto_trader.db"))
    report = await build_edge_report(window_days=window_days)
    report = await _attach_ai_cost(report, settings=settings)
    return render_learning_brief(report)


async def run_scoreboard_memory_pack(
    *,
    window_days: int = 7,
    cache_path: Path | None = None,
    write_cache: bool = False,
) -> str:
    settings = get_settings()
    configure_db_path(getattr(settings, "db_path", "auto_trader.db"))
    report = await build_edge_report(window_days=window_days)
    report = await _attach_ai_cost(report, settings=settings)
    pack = build_scoreboard_memory_pack(report)
    if write_cache:
        write_scoreboard_memory_pack(pack, cache_path or default_scoreboard_memory_pack_path(settings))
    return json.dumps(pack, indent=2, sort_keys=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Read-only trade edge evidence report.")
    parser.add_argument("--days", type=int, default=7, help="Lookback window in days.")
    parser.add_argument(
        "--execution-mode",
        choices=sorted(EXECUTION_MODE_FILTERS),
        default=None,
        help="Filter closed-trade evidence by paper, live, or unknown; mixed is diagnostic only.",
    )
    parser.add_argument("--brief", action="store_true", help="Render the learning-loop brief instead of the edge report.")
    parser.add_argument("--memory-pack", action="store_true", help="Render compact scoreboard memory JSON for AI context.")
    parser.add_argument("--write-cache", action="store_true", help="Write the memory pack to its cache path.")
    parser.add_argument("--cache-path", type=Path, default=None, help="Override memory-pack cache path.")
    args = parser.parse_args()
    setup_logging("ERROR")
    if args.memory_pack:
        print(
            asyncio.run(
                run_scoreboard_memory_pack(
                    window_days=args.days,
                    cache_path=args.cache_path,
                    write_cache=args.write_cache,
                )
            )
        )
    elif args.brief:
        print(asyncio.run(run_learning_brief(window_days=args.days)))
    else:
        print(asyncio.run(run_edge_report(window_days=args.days, execution_mode=args.execution_mode)))


if __name__ == "__main__":
    main()
