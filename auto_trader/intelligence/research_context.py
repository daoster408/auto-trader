"""Verified research context builders for AI advisory packets.

These helpers only reshape data the bot already fetched from configured
providers. They do not approve trades, size orders, or fetch paid model data.
"""
from __future__ import annotations

from dataclasses import replace
from typing import Any

from auto_trader.core.models import TradeIntent


def _num(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _bar(snapshot: dict[str, Any], key: str) -> dict[str, Any]:
    value = snapshot.get(key) or {}
    return _dict(value)


def _compact_quote(snapshot: dict[str, Any]) -> dict[str, Any]:
    quote = _dict(snapshot.get("latestQuote"))
    trade = _dict(snapshot.get("latestTrade"))
    return {
        "latest_trade_price": _num(trade.get("p")),
        "latest_trade_size": _num(trade.get("s")),
        "latest_trade_time": _text(trade.get("t")),
        "bid_price": _num(quote.get("bp")),
        "bid_size": _num(quote.get("bs")),
        "ask_price": _num(quote.get("ap")),
        "ask_size": _num(quote.get("as")),
        "quote_time": _text(quote.get("t")),
    }


def _compact_bar(bar: dict[str, Any]) -> dict[str, Any]:
    return {
        "open": _num(bar.get("o")),
        "high": _num(bar.get("h")),
        "low": _num(bar.get("l")),
        "close": _num(bar.get("c")),
        "volume": _num(bar.get("v")),
        "timestamp": _text(bar.get("t")),
    }


def build_alpaca_research_context(
    snapshot: dict[str, Any],
    *,
    price: float,
    change_pct: float,
    rel_volume: float,
    spread_pct: float | None,
    dollar_volume: float,
) -> dict[str, Any]:
    """Compact Alpaca snapshot evidence for model-readable research context."""
    daily = _compact_bar(_bar(snapshot, "dailyBar"))
    previous = _compact_bar(_bar(snapshot, "prevDailyBar"))
    minute = _compact_bar(_bar(snapshot, "minuteBar"))
    open_price = _num(daily.get("open"), 0.0) or 0.0
    prev_close = _num(previous.get("close"), 0.0) or 0.0
    high = _num(daily.get("high"), 0.0) or 0.0
    low = _num(daily.get("low"), 0.0) or 0.0
    gap_pct = ((open_price - prev_close) / prev_close) if prev_close > 0 else None
    intraday_pct = ((price - open_price) / open_price) if open_price > 0 else None
    distance_from_high_pct = ((price - high) / high) if high > 0 else None
    distance_from_low_pct = ((price - low) / low) if low > 0 else None
    return {
        "market": {
            "provider": "alpaca",
            "feed": "iex",
            "quote": _compact_quote(snapshot),
            "daily_bar": daily,
            "previous_daily_bar": previous,
            "minute_bar": minute,
        },
        "technical": {
            "price": price,
            "change_pct": change_pct,
            "intraday_pct": intraday_pct,
            "gap_pct": gap_pct,
            "rel_volume": rel_volume,
            "dollar_volume": dollar_volume,
            "spread_pct": spread_pct,
            "distance_from_high_pct": distance_from_high_pct,
            "distance_from_low_pct": distance_from_low_pct,
            "liquidity_pass": dollar_volume >= 2_000_000,
            "spread_pass": spread_pct is not None and spread_pct <= 0.006,
            "non_parabolic_pass": change_pct <= 0.12,
        },
    }


def normalize_finnhub_research_context(finnhub: dict[str, Any] | None) -> dict[str, Any]:
    """Map Finnhub enrichment into evidence lanes used by AI packets."""
    if not isinstance(finnhub, dict) or not finnhub:
        return {}
    context: dict[str, Any] = {
        "data_sources": {"finnhub": {"enabled": bool(finnhub.get("enabled"))}},
    }
    quote = finnhub.get("quote")
    if isinstance(quote, dict) and "error" not in quote:
        context["market"] = {
            "finnhub_quote": {
                "current": _num(quote.get("current")),
                "change": _num(quote.get("change")),
                "change_pct": _num(quote.get("change_pct")),
                "high": _num(quote.get("high")),
                "low": _num(quote.get("low")),
                "open": _num(quote.get("open")),
                "prev_close": _num(quote.get("prev_close")),
            }
        }
    profile = finnhub.get("profile")
    if isinstance(profile, dict) and "error" not in profile:
        context["fundamental"] = {
            "name": _text(profile.get("name")),
            "ticker": _text(profile.get("ticker")),
            "exchange": _text(profile.get("exchange")),
            "industry": _text(profile.get("industry")),
            "market_cap": _num(profile.get("market_cap")),
            "share_outstanding": _num(profile.get("share_outstanding")),
        }
    news = finnhub.get("news")
    if isinstance(news, list):
        context["news"] = [
            {
                "headline": _text(item.get("headline")),
                "source": _text(item.get("source")),
                "published_at": _text(item.get("published_at")),
                "url": _text(item.get("url")),
            }
            for item in news
            if isinstance(item, dict)
        ]
    return context


def build_risk_research_context(
    *,
    account: dict[str, Any] | None = None,
    clock: dict[str, Any] | None = None,
    positions: list[dict[str, Any]] | None = None,
    today_new_entries: int | None = None,
    max_new_positions_per_day: int | None = None,
) -> dict[str, Any]:
    positions = [_dict(position) for position in (positions or [])]
    open_positions = [p for p in positions if abs(_num(p.get("qty"), 0.0) or 0.0) > 0]
    return {
        "account": {
            "equity": _num((account or {}).get("equity")),
            "cash": _num((account or {}).get("cash")),
            "buying_power": _num((account or {}).get("buying_power")),
            "status": _text((account or {}).get("account_status")),
            "trading_blocked": bool((account or {}).get("trading_blocked")),
            "account_blocked": bool((account or {}).get("account_blocked")),
        },
        "market_clock": {
            "is_open": bool((clock or {}).get("is_open")),
            "next_open": _text((clock or {}).get("next_open")),
            "next_close": _text((clock or {}).get("next_close")),
        },
        "positions": {
            "open_count": len(open_positions),
            "symbols": sorted(_text(p.get("symbol")) for p in open_positions if _text(p.get("symbol"))),
            "gross_market_value": sum(abs(_num(p.get("market_value"), 0.0) or 0.0) for p in open_positions),
        },
        "entry_limits": {
            "today_new_entries": today_new_entries,
            "max_new_positions_per_day": max_new_positions_per_day,
        },
    }


def merge_research_context(*parts: dict[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for part in parts:
        for key, value in (part or {}).items():
            if isinstance(value, dict) and isinstance(merged.get(key), dict):
                merged[key] = {**merged[key], **value}
            elif value not in ({}, [], None):
                merged[key] = value
    return merged


def build_verified_research_context(intent: TradeIntent) -> dict[str, Any]:
    features = intent.features or {}
    existing = features.get("research_context")
    context = dict(existing) if isinstance(existing, dict) else {}
    context = merge_research_context(context, normalize_finnhub_research_context(features.get("finnhub")))

    missing = []
    for key in ("market", "technical", "fundamental", "news", "macro", "risk"):
        if not context.get(key):
            missing.append(key)
    context["data_quality"] = {
        "uses_only_verified_packet_data": True,
        "missing_sections": missing,
        "notes": "Missing sections are explicit evidence gaps, not permission to infer outside data.",
    }
    return context


def with_research_context(intent: TradeIntent, context: dict[str, Any]) -> TradeIntent:
    features = dict(intent.features or {})
    features["research_context"] = merge_research_context(
        features.get("research_context") if isinstance(features.get("research_context"), dict) else {},
        context,
    )
    return replace(intent, features=features)
