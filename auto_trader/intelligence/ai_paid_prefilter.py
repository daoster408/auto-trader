"""Deterministic prefilter for paid AI entry-gate calls."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from auto_trader.core.models import TradeIntent
from auto_trader.core.risk_profile import get_risk_profile
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
    risk_profile: str | None = None,
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

    profile = get_risk_profile(
        risk_profile or getattr(settings, "risk_profile", "conservative"),
        paper=bool(getattr(settings, "alpaca_paper", True)),
    )
    profile_prefilter = profile.paid_ai_prefilter
    simplified_runtime = bool(getattr(settings, "simplified_runtime_enabled", False))
    if simplified_runtime:
        min_rel_volume = float(getattr(settings, "ai_paid_prefilter_min_rel_volume", 1.0))
        strong_rel_volume = float(getattr(settings, "ai_paid_prefilter_strong_rel_volume", 2.5))
        high_buffer = abs(float(getattr(settings, "ai_paid_prefilter_high_buffer_pct", 0.002)))
        block_inverse_overlap = bool(getattr(settings, "ai_paid_prefilter_block_inverse_overlap", True))
        control_mode = "explicit"
    else:
        conservative_prefilter = get_risk_profile("conservative", paper=True).paid_ai_prefilter
        min_rel_volume = _profile_or_env_float(
            settings,
            "ai_paid_prefilter_min_rel_volume",
            conservative_prefilter.min_rel_volume,
            profile_prefilter.min_rel_volume,
        )
        strong_rel_volume = _profile_or_env_float(
            settings,
            "ai_paid_prefilter_strong_rel_volume",
            conservative_prefilter.strong_rel_volume,
            profile_prefilter.strong_rel_volume,
        )
        high_buffer = abs(
            _profile_or_env_float(
                settings,
                "ai_paid_prefilter_high_buffer_pct",
                conservative_prefilter.high_buffer_pct,
                profile_prefilter.high_buffer_pct,
            )
        )
        block_inverse_overlap = bool(
            getattr(settings, "ai_paid_prefilter_block_inverse_overlap", profile_prefilter.block_inverse_overlap)
        ) and profile_prefilter.block_inverse_overlap
        control_mode = f"legacy_profile:{profile.name}"

    reasons: list[str] = []
    if rel_volume is not None and rel_volume < min_rel_volume:
        reasons.append("low_relative_volume")
    if (
        block_inverse_overlap
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
        "risk_profile": profile.name,
        "risk_control_mode": control_mode,
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


def _profile_or_env_float(settings: Any, attr: str, default_value: float, profile_value: float) -> float:
    setting_value = _float(getattr(settings, attr, default_value))
    if setting_value is None:
        return profile_value
    if _setting_explicitly_set(settings, attr) or setting_value != default_value:
        return setting_value
    return profile_value


def _setting_explicitly_set(settings: Any, attr: str) -> bool:
    alias = _settings_alias(settings, attr)
    if not alias:
        return False
    if alias in os.environ:
        return True
    env_file = Path(os.getenv("AUTO_TRADER_ENV_FILE", ".env"))
    try:
        lines = env_file.read_text(encoding="utf-8").splitlines()
    except OSError:
        return False
    prefix = f"{alias}="
    return any(line.strip().startswith(prefix) for line in lines if line.strip() and not line.strip().startswith("#"))


def _settings_alias(settings: Any, attr: str) -> str | None:
    model_fields = getattr(settings.__class__, "model_fields", {})
    field = model_fields.get(attr) if isinstance(model_fields, dict) else None
    alias = getattr(field, "alias", None)
    if alias:
        return str(alias)
    return {
        "ai_paid_prefilter_min_rel_volume": "AI_PAID_PREFILTER_MIN_REL_VOLUME",
        "ai_paid_prefilter_strong_rel_volume": "AI_PAID_PREFILTER_STRONG_REL_VOLUME",
        "ai_paid_prefilter_high_buffer_pct": "AI_PAID_PREFILTER_HIGH_BUFFER_PCT",
    }.get(attr)
