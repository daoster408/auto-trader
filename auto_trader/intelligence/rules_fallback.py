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
from zoneinfo import ZoneInfo

from auto_trader.broker.alpaca_adapter import AlpacaAdapter
from auto_trader.core.models import TradeIntent
from auto_trader.core.risk_profile import DiscoveryProfile, get_risk_profile, resolve_discovery_profile
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

NEW_YORK = ZoneInfo("America/New_York")
REGULAR_SESSION_MINUTES = 390
MAX_NORMALIZED_REL_VOLUME = 8.0
# Approximate cumulative US-equity volume through the regular session. The
# opening floor and operational cap prevent tiny early prints from exploding.
EXPECTED_CUMULATIVE_VOLUME_CURVE: tuple[tuple[int, float], ...] = (
    (0, 0.05),
    (15, 0.12),
    (30, 0.18),
    (60, 0.28),
    (120, 0.43),
    (180, 0.56),
    (240, 0.68),
    (300, 0.80),
    (360, 0.93),
    (REGULAR_SESSION_MINUTES, 1.0),
)


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
    raw_rel_volume: float | None = None
    expected_volume_fraction: float = 1.0
    volume_normalization_mode: str = "raw_fallback"
    volume_source_timestamp: str | None = None


@dataclass(frozen=True)
class RelativeVolume:
    raw: float
    normalized: float
    expected_fraction: float
    mode: str
    source_timestamp: str | None


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
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed
    except (TypeError, ValueError):
        return None


def _latest_market_data_timestamp(snapshot: dict[str, Any]) -> datetime | None:
    values = (
        _bar(snapshot, "latestTrade").get("t"),
        _bar(snapshot, "latestQuote").get("t"),
        _bar(snapshot, "minuteBar").get("t"),
        _bar(snapshot, "dailyBar").get("t"),
    )
    timestamps = [parsed for value in values if (parsed := _parse_ts(value)) is not None]
    return max(timestamps) if timestamps else None


def _expected_volume_fraction(session_minute: float) -> float:
    bounded_minute = min(max(session_minute, 0.0), float(REGULAR_SESSION_MINUTES))
    for (left_minute, left_fraction), (right_minute, right_fraction) in zip(
        EXPECTED_CUMULATIVE_VOLUME_CURVE[:-1],
        EXPECTED_CUMULATIVE_VOLUME_CURVE[1:],
        strict=True,
    ):
        if bounded_minute <= right_minute:
            width = right_minute - left_minute
            progress = (bounded_minute - left_minute) / width if width else 0.0
            return left_fraction + ((right_fraction - left_fraction) * progress)
    return 1.0


def _relative_volume(
    volume: float,
    previous_volume: float,
    *,
    source_timestamp: datetime | None,
) -> RelativeVolume:
    source = source_timestamp.isoformat() if source_timestamp is not None else None
    if previous_volume <= 0:
        return RelativeVolume(0.0, 0.0, 1.0, "missing_previous_volume", source)
    raw = volume / previous_volume
    if source_timestamp is None:
        return RelativeVolume(raw, raw, 1.0, "raw_fallback", source)

    local = source_timestamp.astimezone(NEW_YORK)
    session_open = local.replace(hour=9, minute=30, second=0, microsecond=0)
    session_close = local.replace(hour=16, minute=0, second=0, microsecond=0)
    if local < session_open or local > session_close:
        return RelativeVolume(raw, raw, 1.0, "outside_regular_session", source)

    session_minute = (local - session_open).total_seconds() / 60.0
    expected_fraction = _expected_volume_fraction(session_minute)
    normalized = min(raw / expected_fraction, MAX_NORMALIZED_REL_VOLUME)
    return RelativeVolume(
        raw,
        normalized,
        expected_fraction,
        "regular_session_curve_v1",
        source,
    )


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
    relative_volume = _relative_volume(
        volume,
        prev_volume,
        source_timestamp=_latest_market_data_timestamp(snapshot),
    )
    rel_volume = relative_volume.normalized

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
        f"rel_vol={rel_volume:.2f} (raw={relative_volume.raw:.2f}), "
        f"dollar_vol=${dollar_volume:,.0f}"
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
        raw_rel_volume=relative_volume.raw,
        expected_volume_fraction=relative_volume.expected_fraction,
        volume_normalization_mode=relative_volume.mode,
        volume_source_timestamp=relative_volume.source_timestamp,
        research_context=build_alpaca_research_context(
            snapshot,
            price=price,
            change_pct=change_pct,
            rel_volume=rel_volume,
            spread_pct=spread,
            dollar_volume=dollar_volume,
            raw_rel_volume=relative_volume.raw,
            expected_volume_fraction=relative_volume.expected_fraction,
            volume_normalization_mode=relative_volume.mode,
            volume_source_timestamp=relative_volume.source_timestamp,
            liquidity_threshold_dollars=discovery_profile.min_dollar_volume,
        ),
    )


def _alpaca_candidate_features(candidate: DiscoveryCandidate) -> dict[str, Any]:
    return {
        "provider": "alpaca",
        "price": candidate.price,
        "score": candidate.score,
        "dollar_volume": candidate.dollar_volume,
        "rel_volume": candidate.rel_volume,
        "raw_rel_volume": candidate.raw_rel_volume,
        "expected_volume_fraction": candidate.expected_volume_fraction,
        "volume_normalization_mode": candidate.volume_normalization_mode,
        "volume_source_timestamp": candidate.volume_source_timestamp,
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
    settings: Any | None = None,
) -> list[DiscoveryCandidate]:
    """Discover ranked candidates from the broad Alpaca tradable universe."""
    discovery_profile, control_mode = resolve_discovery_profile(
        settings,
        risk_profile=risk_profile,
        paper=paper,
    )
    if control_mode == "explicit":
        max_assets = discovery_profile.max_assets
        max_candidates = discovery_profile.max_candidates
    else:
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
        risk_profile=control_mode,
    )
    return ranked


async def get_simple_rules_signals(
    adapter: AlpacaAdapter,
    max_signals: int = 2,
    finnhub_client: FinnhubClient | None = None,
    fred_client: FredClient | None = None,
    risk_profile: str = "conservative",
    paper: bool = True,
    settings: Any | None = None,
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
    if "settings" in parameters or any(
        parameter.kind == inspect.Parameter.VAR_KEYWORD for parameter in parameters.values()
    ):
        discover_kwargs["settings"] = settings
    candidates = await discover_dynamic_candidates(adapter, **discover_kwargs)
    control_mode = "explicit" if settings is not None and bool(
        getattr(settings, "simplified_runtime_enabled", False)
    ) else profile.name
    signals: list[TradeIntent] = []
    for candidate in candidates[:max_signals]:
        features: dict[str, Any] = {
            "discovery": _alpaca_candidate_features(candidate),
            "risk_profile": control_mode,
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
