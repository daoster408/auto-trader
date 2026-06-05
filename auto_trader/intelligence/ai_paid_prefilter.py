"""Deterministic prefilter for paid AI entry-gate calls."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from auto_trader.core.models import TradeIntent
from auto_trader.intelligence.research_context import build_verified_research_context


PREFILTER_PROMPT_VERSION = "ai_paid_prefilter/v0"
PREFILTER_PROVIDER = "prefilter"
PREFILTER_MODEL_TAG = "deterministic_prefilter/v0"

INVERSE_OR_LEVERAGED_SYMBOLS = {
    "BITI",
    "DRIP",
    "GLL",
    "SDS",
    "SH",
    "SOXS",
    "SPXU",
    "SQQQ",
    "TECS",
    "TZA",
    "UVXY",
    "VIXY",
}


@dataclass(frozen=True)
class PaidAIPrefilterResult:
    symbol: str
    blocked: bool
    reasons: list[str]
    evidence: dict[str, Any]


def evaluate_paid_ai_prefilter(
    intent: TradeIntent,
    *,
    settings: Any,
) -> PaidAIPrefilterResult:
    """Return a deterministic paid-call prefilter decision.

    Missing context is not treated as a block. This filter only saves paid calls
    when verified packet data clearly shows a low-conviction setup.
    """
    symbol = intent.symbol.upper()
    if not bool(getattr(settings, "ai_paid_prefilter_enabled", True)):
        return PaidAIPrefilterResult(symbol=symbol, blocked=False, reasons=[], evidence={"enabled": False})

    context = build_verified_research_context(intent)
    technical = _dict(context.get("technical"))
    risk = _dict(context.get("risk"))
    fundamental = _dict(context.get("fundamental"))
    news = context.get("news") if isinstance(context.get("news"), list) else []

    rel_volume = _float(technical.get("rel_volume"))
    distance_from_high_pct = _float(technical.get("distance_from_high_pct"))
    spread_pct = _float(technical.get("spread_pct"))
    news_count = sum(1 for item in news if isinstance(item, dict) and str(item.get("headline") or "").strip())
    has_fundamental = any(
        fundamental.get(key) not in (None, "", 0)
        for key in ("name", "industry", "market_cap")
    )
    position_symbols = [
        str(symbol).upper()
        for symbol in _dict(risk.get("positions")).get("symbols", [])
        if str(symbol or "").strip()
    ]
    inverse_or_leveraged = symbol in INVERSE_OR_LEVERAGED_SYMBOLS
    inverse_overlap = sorted(set(position_symbols).intersection(INVERSE_OR_LEVERAGED_SYMBOLS))

    min_rel_volume = float(getattr(settings, "ai_paid_prefilter_min_rel_volume", 1.0) or 1.0)
    strong_rel_volume = float(getattr(settings, "ai_paid_prefilter_strong_rel_volume", 2.5) or 2.5)
    high_buffer = abs(float(getattr(settings, "ai_paid_prefilter_high_buffer_pct", 0.002) or 0.002))

    reasons: list[str] = []
    if rel_volume is not None and rel_volume < min_rel_volume:
        reasons.append("low_relative_volume")
    if (
        bool(getattr(settings, "ai_paid_prefilter_block_inverse_overlap", True))
        and inverse_or_leveraged
        and inverse_overlap
    ):
        reasons.append("inverse_or_leveraged_overlap")
    if inverse_or_leveraged and news_count == 0 and not has_fundamental:
        reasons.append("inverse_or_leveraged_missing_catalyst")
    if (
        distance_from_high_pct is not None
        and distance_from_high_pct >= -high_buffer
        and (rel_volume is None or rel_volume < strong_rel_volume)
        and news_count == 0
    ):
        reasons.append("near_high_without_strong_volume_or_news")

    evidence = {
        "enabled": True,
        "symbol": symbol,
        "rel_volume": rel_volume,
        "min_rel_volume": min_rel_volume,
        "strong_rel_volume": strong_rel_volume,
        "distance_from_high_pct": distance_from_high_pct,
        "high_buffer_pct": high_buffer,
        "spread_pct": spread_pct,
        "news_count": news_count,
        "has_fundamental": has_fundamental,
        "inverse_or_leveraged": inverse_or_leveraged,
        "inverse_overlap": inverse_overlap,
        "position_symbols": position_symbols,
    }
    return PaidAIPrefilterResult(symbol=symbol, blocked=bool(reasons), reasons=reasons, evidence=evidence)


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None
