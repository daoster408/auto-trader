"""Dynamic rules-based stock discovery (v1 bootstrap).

No hardcoded watchlist. No LLM cost.

Discovery flow:
- Pull all active/tradable US equities from Alpaca.
- Require fractionable assets for small-account flexibility.
- Pull free Alpaca/IEX snapshots in batches.
- Filter for price, dollar volume, spread, non-parabolic movement.
- Rank candidates by relative volume + constructive momentum.
"""
from __future__ import annotations

import asyncio
import inspect
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from auto_trader.broker.alpaca_adapter import AlpacaAdapter
from auto_trader.core.models import TradeIntent
from auto_trader.core.risk_profile import DiscoveryProfile, get_risk_profile
from auto_trader.intelligence.finnhub_client import FinnhubClient
from auto_trader.intelligence.fred_client import FredClient
from auto_trader.intelligence.research_context import (
    build_alpaca_research_context,
    merge_research_context,
    normalize_finnhub_research_context,
    normalize_fred_research_context,
)
from auto_trader.utils.logging import get_logger

log = get_logger("auto_trader.intelligence.rules_fallback")


@dataclass(frozen=True)
class DiscoveryCandidate:
    symbol: str
    price: float
    score: float
    dollar_volume: float
    rel_volume: float
    change_pct: float
    spread_pct: float | None
    rationale: str
    research_context: dict[str, Any] | None = None


def _chunked(items: list[str], size: int) -> list[list[str]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _bar(snapshot: dict[str, Any], key: str) -> dict[str, Any]:
    value = snapshot.get(key) or {}
    return value if isinstance(value, dict) else {}


def _latest_price(snapshot: dict[str, Any]) -> float:
    latest_trade = snapshot.get("latestTrade") or {}
    daily = _bar(snapshot, "dailyBar")
    return _num(latest_trade.get("p")) or _num(daily.get("c"))


def _spread_pct(snapshot: dict[str, Any], price: float) -> float | None:
    quote = snapshot.get("latestQuote") or {}
    bid = _num(quote.get("bp"))
    ask = _num(quote.get("ap"))
    if price <= 0 or bid <= 0 or ask <= 0 or ask < bid:
        return None
    return (ask - bid) / price


def _parse_ts(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def _is_fresh(snapshot: dict[str, Any], max_age_minutes: int = 20) -> bool:
    latest_trade = snapshot.get("latestTrade") or {}
    latest_quote = snapshot.get("latestQuote") or {}
    timestamps = [
        _parse_ts(latest_trade.get("t")),
        _parse_ts(latest_quote.get("t")),
    ]
    fresh_after = datetime.now(UTC) - timedelta(minutes=max_age_minutes)
    return any(ts is not None and ts >= fresh_after for ts in timestamps)


def _candidate_from_snapshot(
    symbol: str,
    snapshot: dict[str, Any],
    *,
    discovery_profile: DiscoveryProfile,
) -> DiscoveryCandidate | None:
    price = _latest_price(snapshot)
    daily = _bar(snapshot, "dailyBar")
    prev = _bar(snapshot, "prevDailyBar")

    open_price = _num(daily.get("o"))
    volume = _num(daily.get("v"))
    prev_close = _num(prev.get("c"))
    prev_volume = _num(prev.get("v"))
    spread = _spread_pct(snapshot, price)

    if price <= 0 or prev_close <= 0 or volume <= 0:
        return None
    if not _is_fresh(snapshot):
        return None

    dollar_volume = price * volume
    change_pct = (price - prev_close) / prev_close
    intraday_pct = ((price - open_price) / open_price) if open_price > 0 else 0.0
    rel_volume = volume / prev_volume if prev_volume > 0 else 1.0

    # Profile filters widen the paper experiment funnel without creating order authority.
    if not discovery_profile.min_price <= price <= discovery_profile.max_price:
        return None
    if dollar_volume < discovery_profile.min_dollar_volume:
        return None
    if spread is None or spread > discovery_profile.max_spread_pct:
        return None
    if change_pct <= discovery_profile.min_change_pct or change_pct > discovery_profile.max_change_pct:
        return None
    if intraday_pct < discovery_profile.min_intraday_pct:
        return None

    # Score favors early attention + constructive move, while avoiding pure gap mania.
    gap_pct = ((_num(daily.get("o")) - prev_close) / prev_close) if prev_close > 0 else 0.0
    gap_penalty = max(abs(gap_pct) - 0.04, 0) * 20
    spread_penalty = (spread or 0) * 80
    score = (rel_volume * 1.8) + (change_pct * 35) + (intraday_pct * 15) - gap_penalty - spread_penalty

    rationale = (
        f"Dynamic discovery: price=${price:.2f}, change={change_pct:.2%}, "
        f"rel_vol={rel_volume:.2f}, dollar_vol=${dollar_volume:,.0f}"
    )
    return DiscoveryCandidate(
        symbol=symbol,
        price=price,
        score=score,
        dollar_volume=dollar_volume,
        rel_volume=rel_volume,
        change_pct=change_pct,
        spread_pct=spread,
        rationale=rationale,
        research_context=build_alpaca_research_context(
            snapshot,
            price=price,
            change_pct=change_pct,
            rel_volume=rel_volume,
            spread_pct=spread,
            dollar_volume=dollar_volume,
        ),
    )


def _alpaca_candidate_features(candidate: DiscoveryCandidate) -> dict[str, Any]:
    return {
        "provider": "alpaca",
        "price": candidate.price,
        "score": candidate.score,
        "dollar_volume": candidate.dollar_volume,
        "rel_volume": candidate.rel_volume,
        "change_pct": candidate.change_pct,
        "spread_pct": candidate.spread_pct,
    }


async def discover_dynamic_candidates(
    adapter: AlpacaAdapter,
    *,
    max_assets: int = 750,
    batch_size: int = 100,
    max_candidates: int = 10,
    risk_profile: str = "conservative",
    paper: bool = True,
) -> list[DiscoveryCandidate]:
    """Discover ranked candidates from the broad Alpaca tradable universe."""
    profile = get_risk_profile(risk_profile, paper=paper)
    discovery_profile = profile.discovery
    max_assets = max(max_assets, discovery_profile.max_assets)
    max_candidates = max(max_candidates, discovery_profile.max_candidates)
    assets = await adapter.get_tradable_assets()
    symbols = [a["symbol"] for a in assets if a.get("fractionable")]

    if max_assets:
        symbols = symbols[:max_assets]

    candidates: list[DiscoveryCandidate] = []
    successful_batches = 0
    for chunk in _chunked(symbols, batch_size):
        try:
            snapshots = await adapter.get_stock_snapshots(chunk)
            successful_batches += 1
            for symbol, snapshot in snapshots.items():
                candidate = _candidate_from_snapshot(
                    symbol.upper(),
                    snapshot,
                    discovery_profile=discovery_profile,
                )
                if candidate:
                    candidates.append(candidate)
        except Exception as e:
            log.warning("snapshot_batch_failed", batch_size=len(chunk), error=str(e))

        # Be gentle with free endpoints while still finishing quickly enough for a daily scan.
        await asyncio.sleep(0.25)

    if symbols and successful_batches == 0:
        raise RuntimeError("all snapshot batches failed; refusing to treat data outage as no candidates")

    ranked = sorted(candidates, key=lambda c: c.score, reverse=True)[:max_candidates]
    log.info(
        "dynamic_candidates_ranked",
        scanned=len(symbols),
        candidates=len(candidates),
        returned=len(ranked),
        risk_profile=profile.name,
    )
    return ranked


async def get_simple_rules_signals(
    adapter: AlpacaAdapter,
    max_signals: int = 2,
    finnhub_client: FinnhubClient | None = None,
    fred_client: FredClient | None = None,
    risk_profile: str = "conservative",
    paper: bool = True,
) -> list[TradeIntent]:
    """Return TradeIntents from dynamic market discovery (no watchlist)."""
    profile = get_risk_profile(risk_profile, paper=paper)
    discover_kwargs: dict[str, Any] = {"max_candidates": max(max_signals, 5)}
    parameters = inspect.signature(discover_dynamic_candidates).parameters
    supports_profile = "risk_profile" in parameters or any(
        parameter.kind == inspect.Parameter.VAR_KEYWORD for parameter in parameters.values()
    )
    if supports_profile:
        discover_kwargs["risk_profile"] = profile.name
        discover_kwargs["paper"] = paper
    candidates = await discover_dynamic_candidates(adapter, **discover_kwargs)
    signals: list[TradeIntent] = []
    for candidate in candidates[:max_signals]:
        features: dict[str, Any] = {
            "discovery": _alpaca_candidate_features(candidate),
            "risk_profile": profile.name,
        }
        research_context = candidate.research_context or {}
        if finnhub_client is not None and finnhub_client.enabled:
            features["finnhub"] = await finnhub_client.enrich_symbol(candidate.symbol)
            research_context = merge_research_context(
                research_context,
                normalize_finnhub_research_context(features["finnhub"]),
            )
        if fred_client is not None:
            features["fred"] = await fred_client.macro_context()
            research_context = merge_research_context(
                research_context,
                normalize_fred_research_context(features["fred"]),
            )
        if research_context:
            features["research_context"] = research_context
        signals.append(
            TradeIntent(
                symbol=candidate.symbol,
                side="long",
                entry_price=candidate.price,
                rationale=candidate.rationale,
                confidence=min(max(candidate.score / 8, 0.35), 0.9),
                features=features,
            )
        )
    return signals
