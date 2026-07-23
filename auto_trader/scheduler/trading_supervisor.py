"""Continuous Day-2 supervisor: reconcile, monitor, exits, optional entries."""
from __future__ import annotations

import asyncio
import hashlib
import inspect
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Awaitable, Callable
from zoneinfo import ZoneInfo

from auto_trader.broker.alpaca_adapter import AlpacaAdapter
from auto_trader.core.state_machine import StateMachine
from auto_trader.core.models import KillResult, RiskDecision, SystemState
from auto_trader.core.risk_profile import get_risk_profile
from auto_trader.execution.order_manager import OrderManager
from auto_trader.intelligence.ai_committee import (
    AGGREGATE_PROMPT_VERSION,
    PROMPT_VERSION,
    SIMPLIFIED_PROMPT_VERSION,
    ResearchMemo,
    ShadowResearchCommittee,
    UnavailableResearchCommittee,
    build_research_packet,
    create_research_committee,
    packet_hash,
    provider_failure_metadata,
    real_research_providers,
    research_committee_round,
)
from auto_trader.intelligence.ai_display import compact_ai_vote_lines_from_memo
from auto_trader.intelligence.ai_paid_prefilter import (
    PREFILTER_MODEL_TAG,
    PREFILTER_PROMPT_VERSION,
    PREFILTER_PROVIDER,
    evaluate_paid_ai_prefilter,
)
from auto_trader.intelligence.finnhub_client import FinnhubClient
from auto_trader.intelligence.fred_client import FredClient
from auto_trader.intelligence.research_context import build_risk_research_context, with_research_context
from auto_trader.intelligence.rules_fallback import get_simple_rules_signals
from auto_trader.persistence.db import (
    append_journal_entry,
    clear_pending_exit,
    count_ai_research_chargeable_attempts,
    count_entry_orders_since,
    count_ai_research_memos,
    get_latest_ai_research_memo_for_symbol,
    get_latest_entry_order_for_symbol,
    get_pending_exit_for_symbol,
    get_pending_exit_symbols,  # noqa: F401 - retained for existing test monkeypatch surface
    get_runtime_config_bool,
    get_runtime_config_int,  # noqa: F401 - retained for existing test monkeypatch surface
    get_runtime_config_value,
    log_ai_research_memo,
    log_signal,
    reconcile_broker_orders,
    set_runtime_config_value,
    update_account_risk_state,
    upsert_pending_exit,
    upsert_order_record,
)
from auto_trader.utils.logging import get_logger

NotifyFn = Callable[[str], Awaitable[None]]

log = get_logger("auto_trader.scheduler.trading_supervisor")

ENTRY_CANDIDATE_EVAL_LIMIT = 10
AI_ENTRY_GATE_CANDIDATE_BLOCK_VERDICTS = {"watch", "reject"}
AI_HIGH_EXPOSURE_UNANIMOUS_REASON = "projected gross exposure exceeds unanimous AI threshold"
STAGNATION_SNAPSHOT_MAX_AGE_MINUTES = 20


async def _runtime_risk_profile(settings: Any, *, paper: bool) -> str:
    try:
        runtime_value = await get_runtime_config_value("risk_profile")
    except Exception as e:
        log.warning("runtime_risk_profile_unavailable", error=str(e))
        runtime_value = None
    return get_risk_profile(runtime_value or getattr(settings, "risk_profile", "conservative"), paper=paper).name


async def _runtime_entry_cap(settings: Any, *, paper: bool, risk_profile: str) -> int:
    default = int(getattr(settings, "max_new_positions_per_day", 1) or 1)
    default = max(default, 1)
    try:
        raw_value = await get_runtime_config_value("max_new_positions_per_day")
    except Exception as e:
        log.warning("runtime_entry_cap_unavailable", error=str(e))
        return default
    if raw_value is None:
        return default
    try:
        configured = int(str(raw_value).strip())
    except (TypeError, ValueError):
        log.warning("runtime_entry_cap_invalid", value=raw_value, default=default)
        return default
    return max(configured, 1)


def _single_provider_prompt_version(committee: Any) -> str:
    return str(getattr(committee, "prompt_version", PROMPT_VERSION) or PROMPT_VERSION)


def _include_edge_memory(committee: Any) -> bool:
    return bool(getattr(committee, "include_edge_memory", True))


async def _get_profiled_rules_signals(
    adapter: AlpacaAdapter,
    *,
    settings: Any,
    max_signals: int,
    finnhub_client: FinnhubClient | None,
    fred_client: FredClient | None,
    risk_profile: str,
    paper: bool,
):
    parameters = inspect.signature(get_simple_rules_signals).parameters
    supports_kwargs = any(
        parameter.kind == inspect.Parameter.VAR_KEYWORD for parameter in parameters.values()
    )
    supports_profile = "risk_profile" in parameters or supports_kwargs
    if supports_profile:
        kwargs = {
            "max_signals": max_signals,
            "finnhub_client": finnhub_client,
            "fred_client": fred_client,
            "risk_profile": risk_profile,
            "paper": paper,
        }
    else:
        kwargs = {
            "max_signals": max_signals,
            "finnhub_client": finnhub_client,
            "fred_client": fred_client,
        }
    if "settings" in parameters or supports_kwargs:
        kwargs["settings"] = settings
    return await get_simple_rules_signals(adapter, **kwargs)


@dataclass(frozen=True)
class ExitDecision:
    symbol: str
    should_exit: bool
    reason: str
    metrics: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SupervisorTickResult:
    account: dict[str, Any] | None
    clock: dict[str, Any] | None
    positions: list[dict[str, Any]]
    reconciled_orders: int | None
    alerts: list[str]
    exit_decisions: list[ExitDecision]
    entry_result: dict[str, Any] | None
    errors: list[str]


@dataclass(frozen=True)
class AIResearchRunResult:
    symbol: str
    verdict: str
    validation_passed: bool
    reason: str
    memo: ResearchMemo | None = None
    called_provider: bool = False

    @property
    def approved_for_entry(self) -> bool:
        return self.validation_passed and self.verdict == "approve"


@dataclass(frozen=True)
class EntryCandidateEvaluation:
    signal: Any
    intent: Any
    risk_context: dict[str, Any]
    snapshot: Any
    capacity_decision: RiskDecision | None
    score: float


def _float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _mapping(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _position_stagnation_features(snapshot: dict[str, Any]) -> dict[str, float]:
    daily = _mapping(snapshot.get("dailyBar"))
    prev = _mapping(snapshot.get("prevDailyBar"))
    latest_trade = _mapping(snapshot.get("latestTrade"))
    latest_quote = _mapping(snapshot.get("latestQuote"))
    timestamps = [
        _parse_dt(latest_trade.get("t")),
        _parse_dt(latest_quote.get("t")),
        _parse_dt(daily.get("t")),
    ]
    fresh_after = datetime.now(UTC) - timedelta(minutes=STAGNATION_SNAPSHOT_MAX_AGE_MINUTES)
    if not any(ts is not None and ts >= fresh_after for ts in timestamps):
        return {}

    price = _float(latest_trade.get("p")) or _float(daily.get("c"))
    high = _float(daily.get("h"), default=None)
    low = _float(daily.get("l"), default=None)
    volume = _float(daily.get("v"), default=None)
    prev_volume = _float(prev.get("v"), default=None)

    features: dict[str, float] = {}
    if volume is not None and prev_volume is not None and prev_volume > 0:
        features["rel_volume"] = volume / prev_volume
    if price > 0 and high is not None and low is not None and high >= low:
        features["daily_range_pct"] = (high - low) / price * 100.0
    return features


def _stagnation_time_gate_open(settings: Any, clock: dict[str, Any] | None, local_now: datetime) -> bool:
    if not _regular_market_open(clock):
        return False
    sweep_hour = int(getattr(settings, "last_risk_sweep_hour", 12))
    sweep_minute = int(getattr(settings, "last_risk_sweep_minute", 55))
    stagnation_start = local_now.replace(hour=sweep_hour, minute=sweep_minute, second=0, microsecond=0)
    return local_now >= stagnation_start


def _intent_feature_number(intent: Any, name: str, *, default: float = 0.0) -> float:
    features = _mapping(getattr(intent, "features", None))
    discovery = _mapping(features.get("discovery"))
    if name in discovery:
        return _float(discovery.get(name), default)
    return _float(features.get(name), default)


def _entry_candidate_triage_score(intent: Any, capacity_decision: RiskDecision | None) -> float:
    """Cheap, deterministic slate ordering before paid AI sees a candidate."""
    discovery_score = _intent_feature_number(intent, "score")
    confidence = _float(getattr(intent, "confidence", None))
    rel_volume = _intent_feature_number(intent, "rel_volume", default=1.0)
    change_pct = _intent_feature_number(intent, "change_pct")
    spread_pct = _intent_feature_number(intent, "spread_pct", default=0.01)
    sized_qty = _float(getattr(capacity_decision, "sized_quantity", 0.0) if capacity_decision else 0.0)

    chase_penalty = max(0.0, change_pct - 0.12) * 20.0
    spread_penalty = max(0.0, spread_pct - 0.006) * 120.0
    return discovery_score + (confidence * 2.0) + (rel_volume * 0.35) + min(sized_qty, 10.0) * 0.01 - chase_penalty - spread_penalty


def _now_utc() -> str:
    return datetime.now(UTC).isoformat() + "Z"


def _account_status_is_active(value: Any) -> bool:
    normalized = str(value or "").strip().lower()
    if "." in normalized:
        normalized = normalized.rsplit(".", 1)[-1]
    return normalized == "active"


def _entry_snapshot(
    *,
    account: dict[str, Any],
    positions: list[dict[str, Any]],
    today_new_entries: int,
    max_new_positions_per_day: int,
    risk_profile: str,
):
    return type(
        "SupervisorSnapshot",
        (object,),
        {
            "equity": _float(account.get("equity")),
            "open_positions": positions,
            "today_new_entries": today_new_entries,
            "max_new_positions_per_day": max_new_positions_per_day,
            "risk_profile": risk_profile,
        },
    )()


def _preview_entry_capacity(order_manager: Any, intent: Any, snapshot: Any) -> RiskDecision | None:
    risk = getattr(order_manager, "risk", None)
    evaluate = getattr(risk, "evaluate", None)
    if not callable(evaluate):
        return None
    try:
        return evaluate(intent, snapshot, consume_daily_counter=False)
    except TypeError:
        log.warning("entry_capacity_preview_unsupported")
        return None


def _risk_context_with_entry_capacity(
    risk_context: dict[str, Any],
    decision: RiskDecision | None,
    *,
    settings: Any,
) -> dict[str, Any]:
    if decision is None:
        return risk_context
    metrics = decision.risk_metrics or {}
    projected_pct = _float(metrics.get("projected_gross_exposure_pct"))
    threshold = float(getattr(settings, "ai_high_exposure_unanimous_threshold_pct", 60.0) or 60.0)
    enriched = dict(risk_context)
    enriched["entry_capacity"] = {
        "precheck_approved": bool(decision.approved),
        "reason": decision.reason,
        "sized_quantity": decision.sized_quantity,
        "current_gross_exposure": metrics.get("current_gross_exposure", metrics.get("current")),
        "projected_gross_exposure": metrics.get("projected_gross_exposure", metrics.get("projected")),
        "projected_gross_exposure_pct": projected_pct,
        "max_gross_exposure_pct": metrics.get(
            "max_gross_exposure_pct",
            getattr(settings, "max_gross_exposure_pct", None),
        ),
    }
    enriched["ai_unanimous_threshold_pct"] = threshold
    enriched["ai_unanimous_required"] = bool(decision.approved and projected_pct > threshold)
    if enriched["ai_unanimous_required"]:
        enriched["ai_unanimous_reason"] = AI_HIGH_EXPOSURE_UNANIMOUS_REASON
    return enriched


def _alert_signature(message: str) -> str:
    return hashlib.sha256(message.encode("utf-8")).hexdigest()[:16]


def _alert_config_key(key: str) -> str:
    safe = "".join(ch if ch.isalnum() else "_" for ch in key.lower()).strip("_")
    return f"alert_{safe or 'supervisor'}"[:120]


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None


def _week_start_date(value: datetime) -> str:
    return value.date().fromordinal(value.date().toordinal() - value.weekday()).isoformat()


def _format_position_line(position: dict[str, Any]) -> str:
    symbol = str(position.get("symbol", "?")).upper()
    qty = _float(position.get("qty"))
    market_value = _float(position.get("market_value"))
    unrealized = _float(position.get("unrealized_pl"))
    cost_basis = _float(position.get("cost_basis"))
    pnl_pct = (unrealized / cost_basis * 100.0) if cost_basis else 0.0
    return f"{symbol}: qty {qty:.6f}, value ${market_value:,.2f}, P/L ${unrealized:,.2f} ({pnl_pct:.2f}%)"


def _short_order_id(order: dict[str, Any] | None) -> str:
    if not order:
        return "unknown"
    raw = order.get("broker_order_id") or order.get("id") or order.get("client_order_id") or ""
    return str(raw)[:8] or "unknown"


def _format_money(value: float | None) -> str:
    if value is None:
        return "unavailable"
    if value < 0:
        return f"-${abs(value):,.2f}"
    sign = "+" if value > 0 else ""
    return f"{sign}${value:,.2f}"


def _format_price(value: float | None) -> str:
    if value is None:
        return "unavailable"
    return f"${value:,.2f}"


def _format_pct(value: float | None) -> str:
    if value is None:
        return "unavailable"
    sign = "+" if value > 0 else ""
    return f"{sign}{value:.2f}%"


def _format_duration(start: datetime | None, end: datetime | None) -> str:
    if start is None or end is None:
        return "unavailable"
    seconds = max(0, int((end - start).total_seconds()))
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes = rem // 60
    if days:
        return f"{days}d {hours}h"
    if hours:
        return f"{hours}h {minutes}m"
    return f"{minutes}m"


def _paper_live_label(order: dict[str, Any] | None, *, adapter: AlpacaAdapter | None = None) -> str:
    if order and "paper" in order:
        return "PAPER" if bool(order.get("paper")) else "LIVE"
    if adapter is not None and hasattr(adapter, "paper"):
        return "PAPER" if bool(getattr(adapter, "paper")) else "LIVE"
    return "PAPER"


def _entry_order_action(order: dict[str, Any]) -> str:
    side = str(order.get("side") or "long").lower()
    if side == "long":
        return "market buy"
    return f"market {side}"


def _has_submitted_entry_order(result: dict[str, Any]) -> bool:
    order = result.get("order")
    if not isinstance(order, dict) or order.get("error"):
        return False
    return bool(order.get("id") or order.get("broker_order_id") or order.get("client_order_id"))


def _format_entry_notification(
    result: dict[str, Any],
    *,
    ai_result: "AIResearchRunResult | None" = None,
    adapter: AlpacaAdapter | None = None,
) -> str:
    order = result.get("order") or {}
    risk = result.get("risk_decision") or {}
    symbol = str(order.get("symbol") or (result.get("intent") or {}).get("symbol") or "?").upper()
    label = _paper_live_label(order, adapter=adapter)
    qty = _float(order.get("qty"))
    status = order.get("status") or "unknown"
    reason = risk.get("reason") or "risk decision unavailable"
    trace = risk.get("trace_id") or "unknown"
    decision_id = risk.get("risk_decision_id") or "unknown"
    risk_status = "approved" if bool(risk.get("approved")) else "not approved"
    lines = [
        f"{label} ENTRY SUBMITTED: {symbol}",
        f"Qty: {qty:.6f}",
        f"Order: {_entry_order_action(order)}",
        f"Status: {status}",
        f"Risk: {risk_status}, {reason}",
    ]
    if ai_result and ai_result.memo and isinstance(ai_result.memo.memo, dict):
        vote_lines = compact_ai_vote_lines_from_memo(ai_result.memo.memo)
        if vote_lines:
            lines.append("AI votes:")
            lines.extend(vote_lines[:5])
    lines.extend(
        [
            f"Trace: {trace}",
            f"Risk decision: {decision_id}",
            f"Order ID: {_short_order_id(order)}",
        ]
    )
    return "\n".join(lines)


def _short_journal_text(value: Any, *, limit: int = 260) -> str:
    text = str(value or "").strip()
    if not text:
        return "unavailable"
    text = " ".join(text.split())
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)].rstrip() + "..."


def _format_ai_vote_summary(ai_result: "AIResearchRunResult | None") -> str:
    if ai_result is None:
        return "not run"
    memo = ai_result.memo.memo if ai_result.memo and isinstance(ai_result.memo.memo, dict) else {}
    votes = memo.get("provider_votes") if isinstance(memo, dict) else None
    if not isinstance(votes, list) or not votes:
        confidence = f", confidence={ai_result.memo.confidence:.2f}" if ai_result.memo and ai_result.memo.confidence is not None else ""
        return f"{ai_result.verdict}{confidence}; reason={ai_result.reason}"
    parts = []
    for vote in votes:
        if not isinstance(vote, dict):
            continue
        provider = str(vote.get("provider") or "provider")
        verdict = str(vote.get("verdict") or "unknown")
        confidence = vote.get("confidence")
        if isinstance(confidence, int | float):
            parts.append(f"{provider} {verdict} {float(confidence):.2f}")
        else:
            parts.append(f"{provider} {verdict}")
    return ", ".join(parts) or f"{ai_result.verdict}; reason={ai_result.reason}"


def _format_entry_journal_entry(
    result: dict[str, Any],
    *,
    intent: Any,
    signal_id: int | None,
    ai_result: "AIResearchRunResult | None",
    risk_profile: str,
    adapter: AlpacaAdapter | None = None,
) -> str:
    order = result.get("order") or {}
    risk = result.get("risk_decision") or {}
    symbol = str(order.get("symbol") or getattr(intent, "symbol", "?")).upper()
    label = _paper_live_label(order, adapter=adapter)
    qty = _float(order.get("qty") or risk.get("sized_quantity") or risk.get("sized_qty"))
    price = _float(order.get("avg_fill_price") or getattr(intent, "entry_price", 0.0))
    notional = qty * price if qty and price else 0.0
    fill_text = _format_price(price) if price else "pending"
    notional_text = _format_price(notional) if notional else "pending"
    order_status = str(order.get("status") or "unknown")
    risk_reason = risk.get("reason") or "risk decision unavailable"
    risk_id = risk.get("risk_decision_id") or "unknown"
    trace_id = risk.get("trace_id") or "unknown"
    scanner_reason = _short_journal_text(getattr(intent, "rationale", ""))

    memo = ai_result.memo.memo if ai_result and ai_result.memo and isinstance(ai_result.memo.memo, dict) else {}
    committee = memo.get("committee") if isinstance(memo, dict) else {}
    bull_case = _short_journal_text(committee.get("bull_case") if isinstance(committee, dict) else None)
    bear_case = _short_journal_text(committee.get("bear_case") if isinstance(committee, dict) else None)
    ai_verdict = ai_result.verdict if ai_result else "not_run"
    ai_confidence = (
        f"{ai_result.memo.confidence:.2f}"
        if ai_result and ai_result.memo and ai_result.memo.confidence is not None
        else "n/a"
    )

    return "\n".join(
        [
            f"BUY: {symbol} ({label}, {risk_profile})",
            f"Order: qty {qty:.6f}, status {order_status}, fill {fill_text}, notional {notional_text}.",
            f"Scanner: {scanner_reason}",
            f"AI gate: {ai_verdict}, confidence {ai_confidence}; votes: {_format_ai_vote_summary(ai_result)}.",
            f"RiskEngine: {risk_reason}; decision {risk_id}; trace {trace_id}.",
            f"Why: {bull_case}",
            f"Watchouts: {bear_case}",
            f"Signal: {signal_id if signal_id is not None else 'unknown'}",
        ]
    )


def _format_exit_signal_notification(decision: ExitDecision) -> str:
    qty = _float(decision.metrics.get("qty"))
    pnl = _float(decision.metrics.get("unrealized_pl"))
    pnl_pct = _float(decision.metrics.get("pnl_pct"))
    return "\n".join(
        [
            f"EXIT SIGNAL (dry run): {decision.symbol}",
            f"Reason: {decision.reason}",
            f"Qty: {qty:.6f}",
            f"Open P/L: {_format_money(pnl)} ({_format_pct(pnl_pct)})",
        ]
    )


def _format_exit_submitted_notification(
    decision: ExitDecision,
    order: dict[str, Any],
    *,
    adapter: AlpacaAdapter | None = None,
) -> str:
    label = _paper_live_label(order, adapter=adapter)
    qty = _float(order.get("qty") or decision.metrics.get("qty"))
    status = order.get("status") or "unknown"
    return "\n".join(
        [
            f"{label} EXIT SUBMITTED: {decision.symbol}",
            f"Reason: {decision.reason}",
            f"Qty: {qty:.6f}",
            "Order: market close",
            f"Status: {status}",
            f"Order ID: {_short_order_id(order)}",
        ]
    )


def _format_exit_completed_notification(
    *,
    symbol: str,
    reason: str | None,
    close_order: dict[str, Any],
    entry_order: dict[str, Any] | None,
    adapter: AlpacaAdapter | None = None,
) -> str:
    label = _paper_live_label(close_order, adapter=adapter)
    qty = _float(close_order.get("filled_qty") or close_order.get("qty"))
    entry_price = _float((entry_order or {}).get("avg_fill_price"), default=None)
    exit_price = _float(close_order.get("avg_fill_price"), default=None)
    pnl: float | None = None
    pnl_pct: float | None = None
    if entry_price is not None and entry_price > 0 and exit_price is not None and qty > 0:
        close_side = str(close_order.get("side") or "sell").lower()
        if close_side in {"buy", "buy_to_cover"}:
            pnl = (entry_price - exit_price) * qty
        else:
            pnl = (exit_price - entry_price) * qty
        pnl_pct = (pnl / (entry_price * qty) * 100.0) if qty else None
    entry_time = _parse_dt((entry_order or {}).get("filled_at") or (entry_order or {}).get("submitted_at"))
    exit_time = _parse_dt(close_order.get("filled_at") or close_order.get("submitted_at"))
    return "\n".join(
        [
            f"{label} EXIT FILLED: {symbol}",
            f"Reason: {reason or 'unknown'}",
            f"Qty: {qty:.6f}",
            f"Entry: {_format_price(entry_price)}",
            f"Exit: {_format_price(exit_price)}",
            f"P/L: {_format_money(pnl)} ({_format_pct(pnl_pct)})",
            f"Held: {_format_duration(entry_time, exit_time)}",
            f"Order ID: {_short_order_id(close_order)}",
        ]
    )


def _is_terminal_order_status(status: Any) -> bool:
    return str(status or "").lower() in {"canceled", "cancelled", "filled", "rejected", "expired"}


def _is_failed_terminal_order_status(status: Any) -> bool:
    return str(status or "").lower() in {"canceled", "cancelled", "rejected", "expired"}


def _is_filled_order_status(status: Any) -> bool:
    return str(status or "").lower() == "filled"


def _is_close_order_for_position(order: dict[str, Any], *, symbol: str, position_qty: float) -> bool:
    if str(order.get("symbol", "")).upper() != symbol.upper():
        return False
    if _is_terminal_order_status(order.get("status")):
        return False
    side = str(order.get("side", "")).lower()
    if position_qty > 0:
        return side == "sell"
    if position_qty < 0:
        return side in {"buy", "buy_to_cover"}
    return False


def _regular_market_open(clock: dict[str, Any] | None) -> bool:
    return bool(clock and clock.get("is_open") is True)


def _is_open_entry_order(order: dict[str, Any]) -> bool:
    if _is_terminal_order_status(order.get("status")):
        return False
    side = str(order.get("side", "")).lower()
    return side in {"buy", "long"}


def _candidate_specific_ai_block(ai_result: AIResearchRunResult) -> bool:
    return ai_result.validation_passed and ai_result.verdict in AI_ENTRY_GATE_CANDIDATE_BLOCK_VERDICTS


def _cached_candidate_block_result(symbol: str, memo: dict[str, Any] | None) -> AIResearchRunResult | None:
    if memo is None or not bool(memo.get("validation_passed")):
        return None
    verdict = str(memo.get("verdict") or "watch").lower()
    if verdict not in AI_ENTRY_GATE_CANDIDATE_BLOCK_VERDICTS:
        return None
    prompt_version = str(memo.get("prompt_version") or "")
    reason = "ai_paid_prefilter_cached_watch" if prompt_version == PREFILTER_PROMPT_VERSION else f"ai_research_cached_{verdict}"
    return AIResearchRunResult(
        symbol=symbol.upper(),
        verdict=verdict,
        validation_passed=True,
        reason=reason,
        called_provider=False,
    )


def _order_matches_pending_exit(order: dict[str, Any], pending: dict[str, Any]) -> bool:
    order_ids = {
        str(order.get("broker_order_id") or ""),
        str(order.get("client_order_id") or ""),
        str(order.get("id") or ""),
    }
    pending_ids = {
        str(pending.get("broker_order_id") or ""),
        str(pending.get("client_order_id") or ""),
    }
    return bool((order_ids - {""}) & (pending_ids - {""}))


class TradingSupervisor:
    """Small supervised trading loop for paper burn-in.

    It never opens entries unless `auto_entry_enabled` is true, and never executes
    exits unless `auto_exit_enabled` is true. In default mode it reconciles,
    monitors, evaluates, logs, and alerts only.
    """

    def __init__(
        self,
        *,
        settings,
        state_machine: StateMachine,
        adapter: AlpacaAdapter,
        order_manager: OrderManager,
        notifier: NotifyFn | None = None,
    ) -> None:
        self.settings = settings
        self.sm = state_machine
        self.adapter = adapter
        self.order_manager = order_manager
        self.notifier = notifier
        self._last_reconcile_at: datetime | None = None
        self._seen_alert_keys: set[str] = set()
        self._position_high_values: dict[str, float] = {}
        self._pending_exit_symbols: set[str] = set()
        self._last_risk_sweep_dates: set[str] = set()
        self.finnhub_client = FinnhubClient(getattr(settings, "finnhub_api_key", None))
        self.fred_client = (
            None
            if bool(getattr(settings, "simplified_runtime_enabled", False))
            else FredClient(getattr(settings, "fred_api_key", None))
        )
        simplified_runtime = bool(getattr(settings, "simplified_runtime_enabled", False))
        self._ai_provider_setup_error: str | None = None
        if bool(getattr(settings, "ai_research_enabled", True)):
            try:
                self.research_committee = create_research_committee(settings)
            except Exception as exc:
                log.error("ai_research_provider_setup_failed", error=str(exc))
                self._ai_provider_setup_error = str(exc)
                self.research_committee = UnavailableResearchCommittee(
                    self._ai_provider_setup_error,
                    prompt_version=SIMPLIFIED_PROMPT_VERSION if simplified_runtime else PROMPT_VERSION,
                )
        else:
            self.research_committee = ShadowResearchCommittee(
                include_edge_memory=not simplified_runtime,
                prompt_version=SIMPLIFIED_PROMPT_VERSION if simplified_runtime else PROMPT_VERSION,
            )

    def _alert_local_date(self) -> str:
        timezone = ZoneInfo(getattr(self.settings, "report_timezone", "America/Los_Angeles"))
        return datetime.now(timezone).date().isoformat()

    async def _notify(self, message: str) -> bool:
        log.warning("supervisor_alert", message=message)
        if self.notifier is None:
            return True
        try:
            await self.notifier(message)
            return True
        except Exception as e:
            log.error("supervisor_notify_failed", error=str(e))
            return False

    async def _notify_once(self, key: str, message: str) -> bool:
        if key in self._seen_alert_keys:
            return False
        self._seen_alert_keys.add(key)
        return await self._notify(message)

    async def _notify_persisted_once(
        self,
        key: str,
        message: str,
        *,
        marker: str | None = None,
    ) -> bool:
        """Notify once per durable marker so planned restarts do not re-alert stale state."""
        marker_value = (marker or f"{self._alert_local_date()}:{_alert_signature(message)}").lower()
        config_key = _alert_config_key(f"{key}_{_alert_signature(marker_value)}")
        try:
            existing_marker = await get_runtime_config_value(config_key)
        except Exception as e:
            log.warning("supervisor_alert_marker_read_failed", key=config_key, error=str(e))
            return await self._notify_once(f"{key}:{marker_value}", message)
        if existing_marker == marker_value:
            log.info("supervisor_alert_deduped_persistent", key=config_key, marker=marker_value)
            return False

        sent = await self._notify_once(f"{key}:{marker_value}", message)
        if not sent:
            return False
        try:
            await set_runtime_config_value(config_key, marker_value)
        except Exception as e:
            log.warning("supervisor_alert_marker_write_failed", key=config_key, marker=marker_value, error=str(e))
        return True

    async def _halt_and_flatten(self, reason: str) -> KillResult:
        async def flatten() -> KillResult:
            cancelled = await self.adapter.cancel_all_orders()
            flattened = await self.adapter.flatten_all_positions()
            return KillResult(
                success=True,
                orders_cancelled=cancelled,
                positions_flattened=flattened,
                reason=reason,
                incident_report=f"SUPERVISOR ACCOUNT HALT: {reason}",
                timestamp=datetime.now(UTC),
            )

        return await self.sm.halt(reason, flatten_callback=flatten)

    async def _enforce_account_risk_halts(self, account: dict[str, Any]) -> dict[str, Any] | None:
        if self.sm.state == SystemState.HALTED:
            return None
        equity = _float(account.get("equity"))
        if equity <= 0:
            raise ValueError("account equity unavailable for account risk halt checks")
        timezone = ZoneInfo(getattr(self.settings, "report_timezone", "America/Los_Angeles"))
        local_now = datetime.now(timezone)
        metrics = await update_account_risk_state(
            equity=equity,
            day_date=local_now.date().isoformat(),
            week_start_date=_week_start_date(local_now),
        )
        breaches: list[str] = []
        if metrics["daily_loss_pct"] <= float(self.settings.daily_loss_halt_pct):
            breaches.append(
                f"daily loss {metrics['daily_loss_pct']:.2f}% <= {float(self.settings.daily_loss_halt_pct):.2f}%"
            )
        if metrics["weekly_loss_pct"] <= float(self.settings.weekly_loss_halt_pct):
            breaches.append(
                f"weekly loss {metrics['weekly_loss_pct']:.2f}% <= {float(self.settings.weekly_loss_halt_pct):.2f}%"
            )
        if metrics["peak_drawdown_pct"] <= float(self.settings.peak_drawdown_halt_pct):
            breaches.append(
                f"peak drawdown {metrics['peak_drawdown_pct']:.2f}% <= {float(self.settings.peak_drawdown_halt_pct):.2f}%"
            )
        if not breaches:
            return metrics

        reason = "account risk halt: " + "; ".join(breaches)
        await append_journal_entry(content=f"Account risk halt triggered. {reason}. Metrics {metrics}.")
        await self._notify_once("account-risk-halt", f"ACCOUNT RISK HALT: {reason}")
        await self._halt_and_flatten(reason)
        return metrics

    async def reconcile_once(self, *, days: int | None = None) -> int:
        lookback = int(days if days is not None else self.settings.reconcile_lookback_days)
        recent_orders = await self.adapter.get_recent_orders(days=lookback)
        reconciled = await reconcile_broker_orders(recent_orders)
        await self._clear_terminal_pending_exits_from_orders(recent_orders)
        self._last_reconcile_at = datetime.now(UTC)
        if reconciled != len(recent_orders):
            await self._notify_once(
                f"reconcile-incomplete-{self._last_reconcile_at.date()}",
                f"RECONCILIATION WARNING: persisted {reconciled}/{len(recent_orders)} broker orders at {_now_utc()}",
            )
        log.info("supervisor_reconciled", attempted=len(recent_orders), persisted=reconciled, lookback_days=lookback)
        return reconciled

    def evaluate_exit_rules(self, position: dict[str, Any]) -> ExitDecision:
        symbol = str(position.get("symbol", "")).upper()
        qty = abs(_float(position.get("qty")))
        if not symbol or qty <= 0:
            return ExitDecision(symbol=symbol or "UNKNOWN", should_exit=False, reason="no open quantity")

        unrealized = _float(position.get("unrealized_pl"))
        cost_basis = _float(position.get("cost_basis"))
        market_value = abs(_float(position.get("market_value")))
        if cost_basis <= 0:
            avg_entry = _float(position.get("avg_entry_price"))
            cost_basis = avg_entry * qty if avg_entry > 0 else market_value - unrealized
        pnl_pct = (unrealized / cost_basis * 100.0) if cost_basis else 0.0

        high_value = max(self._position_high_values.get(symbol, market_value), market_value)
        self._position_high_values[symbol] = high_value
        trailing_drawdown_pct = ((market_value - high_value) / high_value * 100.0) if high_value else 0.0

        metrics = {
            "qty": qty,
            "position_qty": _float(position.get("qty")),
            "market_value": market_value,
            "unrealized_pl": unrealized,
            "cost_basis": cost_basis,
            "pnl_pct": pnl_pct,
            "trailing_drawdown_pct": trailing_drawdown_pct,
            "entry_age_days": position.get("entry_age_days"),
            "stagnation_rel_volume": position.get("stagnation_rel_volume"),
            "stagnation_daily_range_pct": position.get("stagnation_daily_range_pct"),
        }

        max_loss_pct = float(self.settings.position_max_loss_pct)
        take_profit_pct = float(self.settings.position_take_profit_pct)
        trailing_stop_pct = float(self.settings.position_trailing_stop_pct)
        max_hold_days = int(self.settings.position_max_hold_days)
        if pnl_pct <= max_loss_pct:
            return ExitDecision(symbol=symbol, should_exit=True, reason="position max loss reached", metrics=metrics)
        if pnl_pct >= take_profit_pct:
            return ExitDecision(symbol=symbol, should_exit=True, reason="position take profit reached", metrics=metrics)
        if trailing_drawdown_pct <= -abs(trailing_stop_pct):
            return ExitDecision(symbol=symbol, should_exit=True, reason="position trailing stop reached", metrics=metrics)
        entry_age_days = position.get("entry_age_days")
        if entry_age_days is not None and float(entry_age_days) >= max_hold_days:
            return ExitDecision(symbol=symbol, should_exit=True, reason="position max hold reached", metrics=metrics)
        stagnation_enabled = bool(getattr(self.settings, "position_stagnation_exit_enabled", False))
        if stagnation_enabled and entry_age_days is not None:
            min_hold_days = float(getattr(self.settings, "position_stagnation_min_hold_days", 2.0))
            min_pnl_pct = float(getattr(self.settings, "position_stagnation_min_pnl_pct", -2.0))
            max_pnl_pct = float(getattr(self.settings, "position_stagnation_max_pnl_pct", 3.0))
            max_rel_volume = float(getattr(self.settings, "position_stagnation_max_rel_volume", 0.8))
            max_daily_range_pct = float(getattr(self.settings, "position_stagnation_max_daily_range_pct", 1.5))
            rel_volume = _float(position.get("stagnation_rel_volume"), default=None)
            daily_range_pct = _float(position.get("stagnation_daily_range_pct"), default=None)
            if (
                float(entry_age_days) >= min_hold_days
                and min_pnl_pct <= pnl_pct <= max_pnl_pct
                and rel_volume is not None
                and rel_volume <= max_rel_volume
                and daily_range_pct is not None
                and daily_range_pct <= max_daily_range_pct
            ):
                return ExitDecision(symbol=symbol, should_exit=True, reason="position stagnation exit", metrics=metrics)
        return ExitDecision(symbol=symbol, should_exit=False, reason="hold", metrics=metrics)

    async def _sync_persisted_pending_exits(self, open_symbols: set[str]) -> None:
        """Prune in-memory exits without inferring broker order status from positions."""
        self._pending_exit_symbols.intersection_update(open_symbols)

    async def _find_open_close_order(
        self,
        decision: ExitDecision,
        *,
        pending_exit: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        position_qty = _float(decision.metrics.get("position_qty"))
        open_orders = await self.adapter.get_open_orders()
        for order in open_orders:
            if not _is_close_order_for_position(order, symbol=decision.symbol, position_qty=position_qty):
                continue
            if pending_exit is not None and not _order_matches_pending_exit(order, pending_exit):
                continue
            return order
        return None

    async def _clear_terminal_pending_exits_from_orders(self, orders: list[dict[str, Any]]) -> None:
        """Clear pending exits whose broker close order is known terminal."""
        for order in orders:
            failed = _is_failed_terminal_order_status(order.get("status"))
            filled = _is_filled_order_status(order.get("status"))
            if not failed and not filled:
                continue
            symbol = str(order.get("symbol", "")).upper()
            if not symbol:
                continue
            pending = await get_pending_exit_for_symbol(symbol)
            if not pending or not _order_matches_pending_exit(order, pending):
                continue
            if await clear_pending_exit(symbol):
                self._pending_exit_symbols.discard(symbol)
                if filled:
                    try:
                        entry_lookup_before = order.get("submitted_at") or order.get("filled_at")
                        entry_order = await get_latest_entry_order_for_symbol(
                            symbol,
                            before_utc_iso=entry_lookup_before,
                        )
                    except Exception as e:
                        entry_order = None
                        log.warning("exit_entry_lookup_failed_for_notification", symbol=symbol, error=str(e))
                    reason = pending.get("reason")
                    await append_journal_entry(
                        content=(
                            f"Auto-exit completed for {symbol}: matched close order "
                            f"{order.get('broker_order_id') or order.get('id')} is filled; pending marker cleared."
                        )
                    )
                    await self._notify_once(
                        f"exit-completed-{symbol}-{order.get('broker_order_id') or order.get('id')}",
                        _format_exit_completed_notification(
                            symbol=symbol,
                            reason=reason,
                            close_order=order,
                            entry_order=entry_order,
                            adapter=self.adapter,
                        ),
                    )
                else:
                    await self._notify_once(
                        f"exit-pending-cleared-{symbol}-{order.get('status')}",
                        f"EXIT PENDING CLEARED: prior close for {symbol} is {order.get('status')}; supervisor may retry.",
                    )

    async def _execute_exit_if_enabled(
        self,
        decision: ExitDecision,
        *,
        clock: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        if not decision.should_exit:
            return None
        if self.sm.state.value == "HALTED":
            await self._notify_persisted_once(
                f"exit-suppressed-halted-{decision.symbol}",
                f"EXIT SUPPRESSED: system is HALTED; supervisor will not close {decision.symbol}.",
                marker=f"{self._alert_local_date()}:{decision.symbol.lower()}:{_alert_signature(decision.reason)}",
            )
            return None
        if not self.settings.auto_exit_enabled:
            await self._notify_once(
                f"exit-dry-run-{decision.symbol}-{decision.reason}",
                _format_exit_signal_notification(decision),
            )
            return None
        if not _regular_market_open(clock):
            log.info(
                "exit_suppressed_market_closed",
                symbol=decision.symbol,
                reason=decision.reason,
                market_open=None if clock is None else clock.get("is_open"),
                clock_source=None if clock is None else clock.get("source"),
                next_open=None if clock is None else clock.get("next_open"),
            )
            return None
        if decision.symbol in self._pending_exit_symbols:
            log.info(
                "exit_suppressed_pending_in_memory",
                symbol=decision.symbol,
                reason=decision.reason,
            )
            return None
        persisted_pending = await get_pending_exit_for_symbol(decision.symbol)
        if persisted_pending:
            open_close_order = await self._find_open_close_order(decision, pending_exit=persisted_pending)
            if open_close_order:
                self._pending_exit_symbols.add(decision.symbol)
                log.info(
                    "exit_suppressed_persisted_pending",
                    symbol=decision.symbol,
                    broker_order_id=persisted_pending.get("broker_order_id"),
                    reason=decision.reason,
                )
            else:
                await self._notify_once(
                    f"exit-pending-unresolved-{decision.symbol}",
                    (
                        f"EXIT NEEDS REVIEW: persisted pending close exists for {decision.symbol}, "
                        "but broker has no open close order. Pausing until operator review."
                    ),
                )
                if self.sm.can_trade():
                    self.sm.pause("persisted pending exit unresolved")
            return None
        try:
            open_close_order = await self._find_open_close_order(decision)
            if open_close_order:
                self._pending_exit_symbols.add(decision.symbol)
                persisted = await upsert_pending_exit(
                    decision.symbol,
                    open_close_order,
                    reason=decision.reason,
                )
                if not persisted:
                    await self._notify_once(
                        f"exit-pending-persistence-failed-{decision.symbol}",
                        (
                            f"EXIT WARNING: broker already has an open close order for {decision.symbol}, "
                            "but local pending-exit persistence failed."
                        ),
                    )
                    if self.sm.can_trade():
                        self.sm.pause("broker close order pending but local pending-exit persistence failed")
                log.info(
                    "exit_suppressed_broker_open_close_order",
                    symbol=decision.symbol,
                    broker_order_id=open_close_order.get("broker_order_id") or open_close_order.get("id"),
                    reason=decision.reason,
                )
                return None

            self._pending_exit_symbols.add(decision.symbol)
            order = await self.adapter.close_position(decision.symbol, reason=decision.reason)
            order_persisted = await upsert_order_record(order, rationale=decision.reason)
            pending_persisted = await upsert_pending_exit(decision.symbol, order, reason=decision.reason)
            if not order_persisted or not pending_persisted:
                await self._notify_once(
                    f"exit-persistence-failed-{decision.symbol}",
                    f"EXIT WARNING: broker close submitted for {decision.symbol}, but local persistence failed.",
                )
                if self.sm.can_trade():
                    self.sm.pause("exit order persistence failed")
            await append_journal_entry(
                content=(
                    f"Auto-exit submitted for {decision.symbol}: {decision.reason}. "
                    f"Order {order.get('broker_order_id') or order.get('id')}; "
                    f"qty {order.get('qty')}; status {order.get('status')}; "
                    f"metrics {decision.metrics}."
                )
            )
            await self._notify(_format_exit_submitted_notification(decision, order, adapter=self.adapter))
            return order
        except Exception as e:
            self._pending_exit_symbols.discard(decision.symbol)
            await self._notify_once(
                f"exit-failed-{decision.symbol}-{decision.reason}",
                f"EXIT FAILED: {decision.symbol} - {decision.reason} - {e}",
            )
            raise

    def _should_run_last_risk_sweep(self, clock: dict[str, Any] | None, local_now: datetime) -> bool:
        if not bool(getattr(self.settings, "last_risk_sweep_enabled", True)):
            return False
        if not _regular_market_open(clock):
            return False
        local_date = local_now.date().isoformat()
        if local_date in self._last_risk_sweep_dates:
            return False
        sweep_hour = int(getattr(self.settings, "last_risk_sweep_hour", 12))
        sweep_minute = int(getattr(self.settings, "last_risk_sweep_minute", 55))
        window_minutes = max(1, int(getattr(self.settings, "last_risk_sweep_window_minutes", 5)))
        start = local_now.replace(hour=sweep_hour, minute=sweep_minute, second=0, microsecond=0)
        return start <= local_now < start + timedelta(minutes=window_minutes)

    def _mark_last_risk_sweep_complete(self, local_now: datetime) -> None:
        self._last_risk_sweep_dates.add(local_now.date().isoformat())

    async def _maybe_submit_entry(
        self,
        *,
        account: dict[str, Any],
        clock: dict[str, Any],
        positions: list[dict[str, Any]],
        today_new_entries: int,
        max_new_positions_per_day: int,
        risk_profile: str = "conservative",
    ) -> dict[str, Any] | None:
        auto_entry_enabled = await get_runtime_config_bool(
            "auto_entry_enabled",
            default=bool(self.settings.auto_entry_enabled),
        )
        if not auto_entry_enabled:
            return None
        if not self.sm.can_trade():
            return None
        if not clock.get("is_open"):
            return None
        if (
            account.get("status") != "CONNECTED"
            or account.get("trading_blocked")
            or account.get("account_blocked")
            or not _account_status_is_active(account.get("account_status"))
        ):
            await self._notify_once("entry-account-not-tradable", "ENTRY BLOCKED: Alpaca account is not tradable.")
            return None
        open_position_count = sum(1 for p in positions if abs(_float(p.get("qty"))) > 0)
        if open_position_count >= max_new_positions_per_day:
            return None
        if today_new_entries >= max_new_positions_per_day:
            return None
        open_orders = await self.adapter.get_open_orders()
        open_entry_orders = [order for order in open_orders if _is_open_entry_order(order)]
        if open_entry_orders:
            symbols = sorted({str(order.get("symbol", "")).upper() for order in open_entry_orders if order.get("symbol")})
            log.info(
                "entry_suppressed_open_entry_order",
                count=len(open_entry_orders),
                symbols=symbols,
            )
            return None

        candidate_limit = max(1, ENTRY_CANDIDATE_EVAL_LIMIT)
        signals = await _get_profiled_rules_signals(
            self.adapter,
            settings=self.settings,
            max_signals=candidate_limit,
            finnhub_client=self.finnhub_client,
            fred_client=self.fred_client,
            risk_profile=risk_profile,
            paper=bool(getattr(self.adapter, "paper", False)),
        )
        if not signals:
            return None
        ai_research_enabled = bool(getattr(self.settings, "ai_research_enabled", True))
        ai_gate_enabled = await get_runtime_config_bool(
            "ai_entry_gate_enabled",
            default=bool(getattr(self.settings, "ai_entry_gate_enabled", False)),
        )
        simplified_runtime = bool(getattr(self.settings, "simplified_runtime_enabled", False))
        last_blocked_result: dict[str, Any] | None = None
        real_providers = real_research_providers(self.research_committee)
        evaluations: list[EntryCandidateEvaluation] = []
        for signal in signals:
            cached_candidate_block = await self._same_day_candidate_ai_block(
                signal.symbol,
                ai_gate_enabled=ai_gate_enabled,
                ai_research_enabled=ai_research_enabled,
                real_providers=real_providers,
            )
            if cached_candidate_block is not None:
                last_blocked_result = {
                    "blocked": True,
                    "reason": f"AI entry gate skipped cached blocked candidate {signal.symbol.upper()}: {cached_candidate_block.reason}",
                    "ai_gate": {
                        "verdict": cached_candidate_block.verdict,
                        "validation_passed": cached_candidate_block.validation_passed,
                        "reason": cached_candidate_block.reason,
                    },
                }
                log.info(
                    "ai_entry_gate_skipped_cached_candidate",
                    symbol=signal.symbol.upper(),
                    verdict=cached_candidate_block.verdict,
                    reason=cached_candidate_block.reason,
                )
                if not _candidate_specific_ai_block(cached_candidate_block):
                    await append_journal_entry(
                        content=(
                            f"AI entry gate blocked {signal.symbol.upper()}: {cached_candidate_block.reason}; "
                            f"verdict={cached_candidate_block.verdict}; "
                            f"validation_passed={cached_candidate_block.validation_passed}; signal_id=None"
                        )
                    )
                    return last_blocked_result
                continue
            risk_context = build_risk_research_context(
                account=account,
                clock=clock,
                positions=positions,
                today_new_entries=today_new_entries,
                max_new_positions_per_day=max_new_positions_per_day,
                risk_profile=risk_profile,
            )
            intent = with_research_context(signal, {"risk": risk_context})
            snapshot = _entry_snapshot(
                account=account,
                positions=positions,
                today_new_entries=today_new_entries,
                max_new_positions_per_day=max_new_positions_per_day,
                risk_profile=risk_profile,
            )
            capacity_decision = _preview_entry_capacity(self.order_manager, intent, snapshot)
            if capacity_decision is None and ai_gate_enabled and ai_research_enabled and real_providers:
                last_blocked_result = {
                    "blocked": True,
                    "reason": f"Entry blocked before paid AI for {intent.symbol.upper()}: capacity preview unavailable",
                    "risk_precheck": {
                        "approved": False,
                        "reason": "entry_capacity_preview_unavailable",
                    },
                }
                log.warning(
                    "entry_blocked_capacity_preview_unavailable",
                    symbol=intent.symbol.upper(),
                    providers=real_providers,
                )
                return last_blocked_result
            if capacity_decision is not None and not capacity_decision.approved:
                last_blocked_result = {
                    "blocked": True,
                    "reason": (
                        f"Entry skipped before paid AI for {intent.symbol.upper()}: "
                        f"{capacity_decision.reason}"
                    ),
                    "risk_precheck": {
                        "approved": False,
                        "reason": capacity_decision.reason,
                        "metrics": capacity_decision.risk_metrics,
                    },
                }
                log.info(
                    "entry_skipped_before_paid_ai",
                    symbol=intent.symbol.upper(),
                    reason=capacity_decision.reason,
                    metrics=capacity_decision.risk_metrics,
                )
                continue
            intent = with_research_context(
                signal,
                {
                    "risk": _risk_context_with_entry_capacity(
                        risk_context,
                        capacity_decision,
                        settings=self.settings,
                    )
                },
            )
            if ai_gate_enabled and ai_research_enabled and real_providers:
                prefilter = evaluate_paid_ai_prefilter(intent, settings=self.settings, risk_profile=risk_profile)
                if prefilter.blocked:
                    signal_id = await log_signal(
                        symbol=intent.symbol,
                        thesis=intent.rationale,
                        confidence=intent.confidence,
                        source="rules_fallback",
                        model_tag="rules_fallback/v0",
                        features=intent.features,
                    )
                    packet = build_research_packet(
                        intent,
                        signal_id=signal_id,
                        include_edge_memory=_include_edge_memory(self.research_committee),
                    )
                    await self._persist_paid_prefilter_block(
                        intent,
                        signal_id=signal_id,
                        packet=packet,
                        digest=packet_hash(packet),
                        prefilter=prefilter,
                        real_providers=real_providers,
                    )
                    await append_journal_entry(
                        content=(
                            f"AI entry gate blocked {intent.symbol.upper()}: "
                            f"ai_paid_prefilter_blocked:{','.join(prefilter.reasons)}; "
                            f"verdict=watch; validation_passed=True; signal_id={signal_id}"
                        )
                    )
                    last_blocked_result = {
                        "blocked": True,
                        "reason": (
                            f"AI entry gate prefilter skipped {intent.symbol.upper()}: "
                            + ",".join(prefilter.reasons)
                        ),
                        "ai_gate": {
                            "verdict": "watch",
                            "validation_passed": True,
                            "reason": "ai_paid_prefilter_blocked:" + ",".join(prefilter.reasons),
                        },
                    }
                    log.info(
                        "entry_slate_prefilter_skipped_candidate",
                        symbol=intent.symbol.upper(),
                        reasons=prefilter.reasons,
                        score=_entry_candidate_triage_score(intent, capacity_decision),
                    )
                    continue
            evaluations.append(
                EntryCandidateEvaluation(
                    signal=signal,
                    intent=intent,
                    risk_context=risk_context,
                    snapshot=snapshot,
                    capacity_decision=capacity_decision,
                    score=_entry_candidate_triage_score(intent, capacity_decision),
                )
            )

        if not evaluations:
            return last_blocked_result

        evaluations.sort(key=lambda candidate: candidate.score, reverse=True)
        if len(evaluations) > 1:
            log.info(
                "entry_slate_triaged",
                candidates=[
                    {
                        "symbol": candidate.intent.symbol.upper(),
                        "score": round(candidate.score, 4),
                    }
                    for candidate in evaluations
                ],
            )

        for evaluation in evaluations:
            intent = evaluation.intent
            snapshot = evaluation.snapshot
            signal_id = await log_signal(
                symbol=intent.symbol,
                thesis=intent.rationale,
                confidence=intent.confidence,
                source="rules_fallback",
                model_tag="rules_fallback/v0",
                features=intent.features,
            )
            ai_result: AIResearchRunResult | None = None
            paid_research_allowed = ai_gate_enabled or not real_providers
            if simplified_runtime and not ai_gate_enabled:
                log.info(
                    "simplified_ai_decision_bypassed_gate_disabled",
                    symbol=intent.symbol.upper(),
                )
            elif ai_research_enabled and self._ai_provider_setup_error:
                if ai_gate_enabled:
                    ai_result = AIResearchRunResult(
                        symbol=intent.symbol.upper(),
                        verdict="watch",
                        validation_passed=False,
                        reason="ai_research_provider_setup_failed",
                    )
                else:
                    log.warning(
                        "ai_research_skipped_provider_setup_failed_gate_disabled",
                        symbol=intent.symbol.upper(),
                    )
            elif ai_research_enabled and paid_research_allowed:
                ai_result = await self._run_ai_research(intent, signal_id=signal_id, risk_profile=risk_profile)
            elif ai_research_enabled and real_providers:
                log.info(
                    "paid_ai_research_skipped_gate_disabled",
                    symbol=intent.symbol.upper(),
                    providers=real_providers,
                )
            elif ai_gate_enabled:
                ai_result = AIResearchRunResult(
                    symbol=intent.symbol.upper(),
                    verdict="watch",
                    validation_passed=False,
                    reason="ai_research_disabled",
                )
            if ai_gate_enabled:
                if ai_result is None:
                    ai_result = AIResearchRunResult(
                        symbol=intent.symbol.upper(),
                        verdict="watch",
                        validation_passed=False,
                        reason="ai_research_not_run",
                    )
                real_providers = real_research_providers(self.research_committee)
                if ai_research_enabled and not real_providers:
                    ai_result = AIResearchRunResult(
                        symbol=intent.symbol.upper(),
                        verdict="watch",
                        validation_passed=False,
                        reason=(
                            "ai_research_provider_setup_failed"
                            if self._ai_provider_setup_error
                            else "real_ai_provider_required_for_entry_gate"
                        ),
                    )
                if not ai_result.approved_for_entry:
                    last_blocked_result = await self._block_entry_for_ai_gate(
                        intent,
                        signal_id=signal_id,
                        ai_result=ai_result,
                    )
                    if _candidate_specific_ai_block(ai_result):
                        log.info(
                            "ai_entry_gate_trying_next_candidate",
                            blocked_symbol=intent.symbol.upper(),
                            verdict=ai_result.verdict,
                            reason=ai_result.reason,
                        )
                        continue
                    return last_blocked_result
            result = await self.order_manager.submit_trade_intent(intent, snapshot, signal_id=signal_id)
            if _has_submitted_entry_order(result):
                try:
                    await append_journal_entry(
                        content=_format_entry_journal_entry(
                            result,
                            intent=intent,
                            signal_id=signal_id,
                            ai_result=ai_result,
                            risk_profile=risk_profile,
                            adapter=self.adapter,
                        )
                    )
                except Exception as e:
                    log.warning(
                        "entry_journal_failed",
                        symbol=intent.symbol.upper(),
                        signal_id=signal_id,
                        error=str(e),
                    )
                await self._notify(_format_entry_notification(result, ai_result=ai_result, adapter=self.adapter))
            return result
        return last_blocked_result

    async def _block_entry_for_ai_gate(
        self,
        intent,
        *,
        signal_id: int | None,
        ai_result: AIResearchRunResult,
    ) -> dict[str, Any]:
        reason = f"AI entry gate blocked {intent.symbol.upper()}: {ai_result.reason}"
        await append_journal_entry(
            content=(
                f"{reason}; verdict={ai_result.verdict}; "
                f"validation_passed={ai_result.validation_passed}; signal_id={signal_id}"
            )
        )
        log.info(
            "ai_entry_gate_blocked",
            symbol=intent.symbol.upper(),
            verdict=ai_result.verdict,
            validation_passed=ai_result.validation_passed,
            reason=ai_result.reason,
            signal_id=signal_id,
        )
        return {
            "blocked": True,
            "reason": reason,
            "ai_gate": {
                "verdict": ai_result.verdict,
                "validation_passed": ai_result.validation_passed,
                "reason": ai_result.reason,
            },
        }

    async def _same_day_candidate_ai_block(
        self,
        symbol: str,
        *,
        ai_gate_enabled: bool,
        ai_research_enabled: bool,
        real_providers: list[str],
    ) -> AIResearchRunResult | None:
        if not ai_gate_enabled or not ai_research_enabled or not real_providers:
            return None
        provider = str(getattr(self.research_committee, "provider", "shadow"))
        model_tag = str(getattr(self.research_committee, "model_tag", provider))
        cache_provider = real_providers[0] if len(real_providers) == 1 else provider
        cache_prompt_versions = (
            (_single_provider_prompt_version(self.research_committee), "ai_research_failure/v0")
            if len(real_providers) == 1
            else (AGGREGATE_PROMPT_VERSION, "ai_research_failure/v0")
        )
        lookups = (
            {
                "provider": cache_provider,
                "model_tag": model_tag,
                "prompt_versions": cache_prompt_versions,
            },
            {
                "provider": PREFILTER_PROVIDER,
                "model_tag": PREFILTER_MODEL_TAG,
                "prompt_versions": (PREFILTER_PROMPT_VERSION,),
            },
        )
        for lookup in lookups:
            try:
                cached = await get_latest_ai_research_memo_for_symbol(
                    provider=str(lookup["provider"]),
                    symbol=symbol,
                    model_tag=str(lookup["model_tag"]),
                    today_utc=True,
                    prompt_versions=lookup["prompt_versions"],
                )
            except Exception as e:
                log.warning(
                    "ai_entry_gate_cached_candidate_lookup_failed",
                    symbol=symbol.upper(),
                    provider=lookup["provider"],
                    error=str(e),
                )
                return AIResearchRunResult(
                    symbol=symbol.upper(),
                    verdict="watch",
                    validation_passed=False,
                    reason="ai_research_symbol_day_cache_failed",
                    called_provider=False,
                )
            result = _cached_candidate_block_result(symbol, cached)
            if result is not None:
                return result
        return None

    async def _persist_paid_prefilter_block(
        self,
        intent: Any,
        *,
        signal_id: int | None,
        packet: dict[str, Any],
        digest: str,
        prefilter: Any,
        real_providers: list[str],
    ) -> None:
        prefilter_already_logged = False
        try:
            prefilter_already_logged = await get_latest_ai_research_memo_for_symbol(
                provider=PREFILTER_PROVIDER,
                symbol=intent.symbol,
                today_utc=True,
                prompt_versions=(PREFILTER_PROMPT_VERSION,),
            ) is not None
        except Exception as e:
            log.warning(
                "ai_paid_prefilter_dedupe_lookup_failed",
                symbol=intent.symbol.upper(),
                error=str(e),
            )
        if not prefilter_already_logged:
            await log_ai_research_memo(
                signal_id=signal_id,
                symbol=intent.symbol,
                provider=PREFILTER_PROVIDER,
                model_tag=PREFILTER_MODEL_TAG,
                prompt_version=PREFILTER_PROMPT_VERSION,
                input_hash=digest,
                verdict="watch",
                confidence=None,
                used_only_provided_data=True,
                validation_passed=True,
                memo={
                    "input_packet": packet,
                    "committee": {
                        "symbol": intent.symbol.upper(),
                        "verdict": "watch",
                        "confidence": None,
                        "used_only_provided_data": True,
                        "validation_errors": [],
                        "judge_summary": (
                            "Paid AI research skipped by deterministic prefilter: "
                            + ", ".join(prefilter.reasons)
                        ),
                    },
                    "prefilter": {
                        "reasons": prefilter.reasons,
                        "evidence": prefilter.evidence,
                        "providers_saved": real_providers,
                    },
                },
            )
        else:
            log.info(
                "ai_paid_prefilter_skip_deduped",
                symbol=intent.symbol.upper(),
                reasons=prefilter.reasons,
            )

    async def _run_ai_research(
        self,
        intent,
        *,
        signal_id: int | None,
        risk_profile: str = "conservative",
    ) -> AIResearchRunResult:
        provider = str(getattr(self.research_committee, "provider", "shadow"))
        packet = build_research_packet(
            intent,
            signal_id=signal_id,
            include_edge_memory=_include_edge_memory(self.research_committee),
        )
        digest = packet_hash(packet)
        model_tag = str(getattr(self.research_committee, "model_tag", provider))
        real_providers = real_research_providers(self.research_committee)
        if real_providers:
            cache_provider = real_providers[0] if len(real_providers) == 1 else provider
            cache_prompt_versions = (
                (_single_provider_prompt_version(self.research_committee), "ai_research_failure/v0")
                if len(real_providers) == 1
                else (AGGREGATE_PROMPT_VERSION, "ai_research_failure/v0")
            )
            try:
                cached = await get_latest_ai_research_memo_for_symbol(
                    provider=cache_provider,
                    symbol=intent.symbol,
                    model_tag=model_tag,
                    today_utc=True,
                    prompt_versions=cache_prompt_versions,
                )
            except Exception as e:
                log.warning(
                    "ai_research_symbol_day_cache_failed",
                    symbol=intent.symbol.upper(),
                    provider=cache_provider,
                    error=str(e),
                )
                return AIResearchRunResult(
                    symbol=intent.symbol.upper(),
                    verdict="watch",
                    validation_passed=False,
                    reason="ai_research_symbol_day_cache_failed",
                    called_provider=False,
                )
            if cached is not None:
                cached_verdict = str(cached.get("verdict") or "watch")
                log.info(
                    "ai_research_symbol_day_cache_hit",
                    symbol=intent.symbol.upper(),
                    provider=cache_provider,
                    verdict=cached_verdict,
                    memo_id=cached.get("id"),
                )
                return AIResearchRunResult(
                    symbol=intent.symbol.upper(),
                    verdict=cached_verdict,
                    validation_passed=bool(cached.get("validation_passed")),
                    reason=f"ai_research_cached_{cached_verdict}",
                    called_provider=False,
                )
            prefilter = evaluate_paid_ai_prefilter(intent, settings=self.settings, risk_profile=risk_profile)
            if prefilter.blocked:
                await self._persist_paid_prefilter_block(
                    intent,
                    signal_id=signal_id,
                    packet=packet,
                    digest=digest,
                    prefilter=prefilter,
                    real_providers=real_providers,
                )
                log.info(
                    "ai_paid_prefilter_blocked",
                    symbol=intent.symbol.upper(),
                    reasons=prefilter.reasons,
                    evidence=prefilter.evidence,
                )
                return AIResearchRunResult(
                    symbol=intent.symbol.upper(),
                    verdict="watch",
                    validation_passed=True,
                    reason="ai_paid_prefilter_blocked:" + ",".join(prefilter.reasons),
                    called_provider=False,
                )
            for provider_name in real_providers:
                existing = await count_ai_research_memos(provider=provider_name, input_hash=digest)
                if existing is None:
                    log.warning("ai_research_skipped_dedupe_count_failed", symbol=intent.symbol, provider=provider_name)
                    return AIResearchRunResult(
                        symbol=intent.symbol.upper(),
                        verdict="watch",
                        validation_passed=False,
                        reason="ai_research_dedupe_count_failed",
                    )
                if existing > 0:
                    log.info(
                        "ai_research_deduped",
                        symbol=intent.symbol,
                        provider=provider_name,
                        input_hash=digest[:12],
                    )
                    return AIResearchRunResult(
                        symbol=intent.symbol.upper(),
                        verdict="watch",
                        validation_passed=False,
                        reason="ai_research_deduped_without_current_verdict",
                    )
            attempts_needed = len(real_providers)
            budget_provider = real_providers[0] if attempts_needed == 1 else None
            daily_max = int(getattr(self.settings, "ai_research_max_calls_per_day", 0) or 0)
            daily_count = await count_ai_research_chargeable_attempts(provider=budget_provider, today_utc=True)
            if daily_count is None:
                log.warning("ai_research_skipped_budget_count_failed", symbol=intent.symbol, provider=provider)
                return AIResearchRunResult(
                    symbol=intent.symbol.upper(),
                    verdict="watch",
                    validation_passed=False,
                    reason="ai_research_budget_count_failed",
                )
            remaining_calls = max(0, daily_max - daily_count)
            if daily_max <= 0 or remaining_calls < attempts_needed:
                budget_skip_already_logged = False
                try:
                    budget_skip_already_logged = await get_latest_ai_research_memo_for_symbol(
                        provider=provider,
                        symbol=intent.symbol,
                        model_tag=model_tag,
                        today_utc=True,
                        prompt_versions=("ai_research_budget/v0",),
                    ) is not None
                except Exception as e:
                    log.warning(
                        "ai_research_budget_skip_lookup_failed",
                        symbol=intent.symbol.upper(),
                        provider=provider,
                        error=str(e),
                    )
                if not budget_skip_already_logged:
                    await log_ai_research_memo(
                        signal_id=signal_id,
                        symbol=intent.symbol,
                        provider=provider,
                        model_tag=model_tag,
                        prompt_version="ai_research_budget/v0",
                        input_hash=digest,
                        verdict="watch",
                        confidence=None,
                        used_only_provided_data=True,
                        validation_passed=False,
                        memo={
                            "input_packet": packet,
                            "committee": {
                                "symbol": intent.symbol.upper(),
                                "verdict": "watch",
                                "confidence": None,
                                "used_only_provided_data": True,
                                "validation_errors": ["ai_research_budget_exhausted"],
                                "judge_summary": (
                                    "Real-provider research skipped because daily paid-call budget is exhausted "
                                    "or not explicitly enabled."
                                ),
                            },
                            "budget": {
                                "daily_max": daily_max,
                                "daily_count": daily_count,
                                "remaining_calls": remaining_calls,
                                "attempts_needed": attempts_needed,
                                "providers": real_providers,
                            },
                        },
                    )
                else:
                    log.info(
                        "ai_research_budget_skip_deduped",
                        symbol=intent.symbol.upper(),
                        provider=provider,
                    )
                return AIResearchRunResult(
                    symbol=intent.symbol.upper(),
                    verdict="watch",
                    validation_passed=False,
                    reason="ai_research_budget_exhausted",
                    called_provider=False,
                )
        try:
            research_round = await research_committee_round(self.research_committee, intent, signal_id=signal_id)
            member_memo_ids = []
            for memo in research_round.member_memos:
                memo_id = await log_ai_research_memo(
                    signal_id=signal_id,
                    symbol=memo.symbol,
                    provider=memo.provider,
                    model_tag=memo.model_tag,
                    prompt_version=memo.prompt_version,
                    input_hash=memo.input_hash,
                    verdict=memo.verdict,
                    confidence=memo.confidence,
                    used_only_provided_data=memo.used_only_provided_data,
                    validation_passed=memo.validation_passed,
                    memo=memo.memo,
                )
                member_memo_ids.append(
                    {
                        "provider": memo.provider,
                        "prompt_version": memo.prompt_version,
                        "input_hash": memo.input_hash,
                        "memo_id": memo_id,
                    }
                )
            if research_round.aggregate_memo not in research_round.member_memos:
                research_round.aggregate_memo.memo["provider_memo_ids"] = member_memo_ids
                memo = research_round.aggregate_memo
                await log_ai_research_memo(
                    signal_id=signal_id,
                    symbol=memo.symbol,
                    provider=memo.provider,
                    model_tag=memo.model_tag,
                    prompt_version=memo.prompt_version,
                    input_hash=memo.input_hash,
                    verdict=memo.verdict,
                    confidence=memo.confidence,
                    used_only_provided_data=memo.used_only_provided_data,
                    validation_passed=memo.validation_passed,
                    memo=memo.memo,
                )
                return AIResearchRunResult(
                    symbol=memo.symbol,
                    verdict=memo.verdict,
                    validation_passed=memo.validation_passed,
                    reason=f"ai_research_{memo.verdict}",
                    memo=memo,
                    called_provider=bool(real_providers),
                )
            memo = research_round.aggregate_memo
            return AIResearchRunResult(
                symbol=memo.symbol,
                verdict=memo.verdict,
                validation_passed=memo.validation_passed,
                reason=f"ai_research_{memo.verdict}",
                memo=memo,
                called_provider=bool(real_providers),
            )
        except Exception as e:
            failure = provider_failure_metadata(e, provider=provider, member=self.research_committee)
            validation_errors = ["ai_research_provider_failed", f"ai_research_provider_{failure['category']}"]
            await log_ai_research_memo(
                signal_id=signal_id,
                symbol=intent.symbol,
                provider=provider,
                model_tag=model_tag,
                prompt_version="ai_research_failure/v0",
                input_hash=digest,
                verdict="watch",
                confidence=None,
                used_only_provided_data=True,
                validation_passed=False,
                memo={
                    "input_packet": packet,
                    "committee": {
                        "symbol": intent.symbol.upper(),
                        "verdict": "watch",
                        "confidence": None,
                        "used_only_provided_data": True,
                        "validation_errors": validation_errors,
                        "judge_summary": "Real-provider research failed; deterministic RiskEngine remains authoritative.",
                    },
                    "error": failure["message"],
                    "provider_failure": failure,
                },
            )
            log.warning(
                "ai_research_failed",
                symbol=intent.symbol,
                provider=provider,
                error=failure["message"],
                error_category=failure["category"],
            )
            return AIResearchRunResult(
                symbol=intent.symbol.upper(),
                verdict="watch",
                validation_passed=False,
                reason="ai_research_provider_failed",
                called_provider=bool(real_providers),
            )

    async def tick_once(self) -> SupervisorTickResult:
        alerts: list[str] = []
        errors: list[str] = []
        exit_decisions: list[ExitDecision] = []
        entry_result: dict[str, Any] | None = None
        account: dict[str, Any] | None = None
        clock: dict[str, Any] | None = None
        positions: list[dict[str, Any]] = []
        positions_snapshot_ok = False
        reconciled: int | None = None
        today_new_entries = 0
        max_new_positions_per_day = int(self.settings.max_new_positions_per_day)
        risk_profile = get_risk_profile(
            getattr(self.settings, "risk_profile", "conservative"),
            paper=bool(getattr(self.adapter, "paper", False)),
        ).name
        timezone = ZoneInfo(getattr(self.settings, "report_timezone", "America/Los_Angeles"))
        local_now = datetime.now(timezone)
        last_risk_sweep_due = False

        try:
            account = await self.adapter.get_account_snapshot()
            if account.get("status") == "ERROR":
                errors.append(f"account unavailable: {account.get('error', 'ERROR')}")
        except Exception as e:
            errors.append(f"account unavailable: {e}")

        try:
            clock = await self.adapter.get_clock()
            if clock.get("source") == "error":
                errors.append(f"clock unavailable: {clock.get('error', 'ERROR')}")
            last_risk_sweep_due = self._should_run_last_risk_sweep(clock, local_now)
        except Exception as e:
            errors.append(f"clock unavailable: {e}")

        try:
            now = datetime.now(UTC)
            due = self._last_reconcile_at is None or (
                now - self._last_reconcile_at
            ).total_seconds() >= float(self.settings.reconcile_interval_seconds)
            due = due or last_risk_sweep_due
            if due:
                reconciled = await self.reconcile_once()
        except Exception as e:
            errors.append(f"reconciliation failed: {e}")

        try:
            positions = await self.adapter.get_positions_snapshot(strict=True)
            positions_snapshot_ok = True
        except Exception as e:
            errors.append(f"positions unavailable: {e}")

        try:
            local_day_start = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
            today_new_entries = await count_entry_orders_since(local_day_start.astimezone(UTC).isoformat())
        except Exception as e:
            errors.append(f"durable entry count unavailable: {e}")

        try:
            risk_profile = await _runtime_risk_profile(
                self.settings,
                paper=bool(getattr(self.adapter, "paper", False)),
            )
            max_new_positions_per_day = await _runtime_entry_cap(
                self.settings,
                paper=bool(getattr(self.adapter, "paper", False)),
                risk_profile=risk_profile,
            )
        except Exception as e:
            errors.append(f"runtime profile/entry cap unavailable: {e}")

        try:
            if account is not None and account.get("status") == "CONNECTED":
                await self._enforce_account_risk_halts(account)
        except Exception as e:
            errors.append(f"account risk halt check failed: {e}")

        if errors:
            alert = "SUPERVISOR WARNING: " + "; ".join(errors)
            alerts.append(alert)
            await self._notify_persisted_once("supervisor-errors", alert)
        alerted_error_count = len(errors)

        open_positions = [p for p in positions if abs(_float(p.get("qty"))) > 0]
        open_symbols = {str(p.get("symbol", "")).upper() for p in open_positions}
        if positions_snapshot_ok:
            try:
                await self._sync_persisted_pending_exits(open_symbols)
            except Exception as e:
                errors.append(f"pending exit sync failed: {e}")
        if open_positions:
            lines = ["POSITION MONITOR:"]
            lines.extend(_format_position_line(p) for p in open_positions)
            alert = "\n".join(lines)
            alerts.append(alert)
            log.info("positions_monitored", count=len(open_positions), summary=alert)

        if self.sm.state.value == "HALTED" and open_positions:
            symbols = ", ".join(sorted(open_symbols))
            alert = (
                "HALTED POSITION WARNING: broker still reports open positions after HALTED state"
                f": {symbols}."
            )
            alerts.append(alert)
            await self._notify_persisted_once(
                "halted-positions-open",
                alert,
                marker=f"{self._alert_local_date()}:{','.join(sorted(symbol.lower() for symbol in open_symbols))}",
            )

        stagnation_enabled = bool(getattr(self.settings, "position_stagnation_exit_enabled", False))
        if stagnation_enabled:
            stagnation_enabled = _stagnation_time_gate_open(self.settings, clock, local_now)
        stagnation_candidates: list[dict[str, Any]] = []
        for position in open_positions:
            try:
                entry = await get_latest_entry_order_for_symbol(str(position.get("symbol", "")))
                entry_time = _parse_dt((entry or {}).get("submitted_at") or (entry or {}).get("filled_at"))
                if entry_time:
                    position["entry_age_days"] = (datetime.now(UTC) - entry_time).total_seconds() / 86400.0
                    position["entry_submitted_at"] = entry_time.isoformat()
            except Exception as e:
                errors.append(f"entry age unavailable for {position.get('symbol')}: {e}")
            decision = self.evaluate_exit_rules(position)
            if stagnation_enabled and not decision.should_exit:
                stagnation_candidates.append(position)
                continue
            exit_decisions.append(decision)
            try:
                await self._execute_exit_if_enabled(decision, clock=clock)
            except Exception as e:
                errors.append(f"exit execution failed for {decision.symbol}: {e}")

        if stagnation_candidates:
            stagnation_symbols = sorted(str(p.get("symbol", "")).upper() for p in stagnation_candidates if p.get("symbol"))
            try:
                snapshots = await self.adapter.get_stock_snapshots(stagnation_symbols)
                for position in stagnation_candidates:
                    symbol = str(position.get("symbol", "")).upper()
                    features = _position_stagnation_features(_mapping(snapshots.get(symbol)))
                    if "rel_volume" in features:
                        position["stagnation_rel_volume"] = features["rel_volume"]
                    if "daily_range_pct" in features:
                        position["stagnation_daily_range_pct"] = features["daily_range_pct"]
            except Exception as e:
                log.warning("stagnation_features_unavailable", symbols=stagnation_symbols, error=str(e))

            for position in stagnation_candidates:
                decision = self.evaluate_exit_rules(position)
                exit_decisions.append(decision)
                try:
                    await self._execute_exit_if_enabled(decision, clock=clock)
                except Exception as e:
                    errors.append(f"exit execution failed for {decision.symbol}: {e}")

        if last_risk_sweep_due and positions_snapshot_ok and not errors:
            self._mark_last_risk_sweep_complete(local_now)
            log.warning(
                "last_risk_sweep_completed",
                local_date=local_now.date().isoformat(),
                positions=len(open_positions),
                exit_signals=sum(1 for decision in exit_decisions if decision.should_exit),
                reconciled_orders=reconciled,
            )

        if len(errors) > alerted_error_count:
            alert = "SUPERVISOR WARNING: " + "; ".join(errors)
            alerts.append(alert)
            await self._notify_persisted_once("supervisor-late-errors", alert)

        if account is not None and clock is not None and not errors:
            try:
                entry_result = await self._maybe_submit_entry(
                    account=account,
                    clock=clock,
                    positions=positions,
                    today_new_entries=today_new_entries,
                    max_new_positions_per_day=max_new_positions_per_day,
                    risk_profile=risk_profile,
                )
            except Exception as e:
                errors.append(f"entry loop failed: {e}")
                await self._notify_once("entry-loop-failed", f"ENTRY LOOP FAILED: {e}")

        return SupervisorTickResult(
            account=account,
            clock=clock,
            positions=positions,
            reconciled_orders=reconciled,
            alerts=alerts,
            exit_decisions=exit_decisions,
            entry_result=entry_result,
            errors=errors,
        )

    async def run(self, stop_event: asyncio.Event, *, startup_delay_seconds: float = 5.0) -> None:
        if startup_delay_seconds > 0:
            await asyncio.sleep(startup_delay_seconds)
        effective_auto_entry = await get_runtime_config_bool(
            "auto_entry_enabled",
            default=bool(self.settings.auto_entry_enabled),
        )
        await self._notify_persisted_once(
            "supervisor-started",
            (
                "TRADING SUPERVISOR STARTED: "
                f"auto_entry={effective_auto_entry}, "
                f"auto_exit={self.settings.auto_exit_enabled}, "
                f"monitor_interval={self.settings.position_monitor_interval_seconds}s"
            ),
            marker=(
                f"{self._alert_local_date()}:{bool(effective_auto_entry)}:"
                f"{bool(self.settings.auto_exit_enabled)}:"
                f"{self.settings.position_monitor_interval_seconds}:{self.sm.state.value.lower()}"
            ),
        )
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(self.tick_once(), timeout=float(self.settings.supervisor_tick_timeout_seconds))
            except asyncio.TimeoutError:
                await self._notify_persisted_once("supervisor-timeout", "SUPERVISOR WARNING: tick timed out.")
            except Exception as e:
                await self._notify_once("supervisor-failed", f"SUPERVISOR WARNING: tick failed: {e}")
                log.exception("supervisor_tick_failed", error=str(e))
            try:
                await asyncio.wait_for(
                    stop_event.wait(),
                    timeout=float(self.settings.position_monitor_interval_seconds),
                )
            except asyncio.TimeoutError:
                pass
