"""Optional FRED macro regime enrichment for AI research packets."""
from __future__ import annotations

import asyncio
import json
import re
from datetime import UTC, date, datetime
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from auto_trader.utils.api_budget import ApiBudget
from auto_trader.utils.logging import get_logger

log = get_logger("auto_trader.intelligence.fred_client")

CORE_RISK_SERIES: dict[str, str] = {
    "two_year_treasury_yield": "DGS2",
    "ten_year_treasury_yield": "DGS10",
    "ten_year_two_year_spread": "T10Y2Y",
    "high_yield_credit_spread": "BAMLH0A0HYM2",
    "consumer_price_index": "CPIAUCSL",
    "unemployment_rate": "UNRATE",
    "volatility_index": "VIXCLS",
    "fed_balance_sheet_assets": "WALCL",
}


def _num(value: Any) -> float | None:
    try:
        if value is None or value == ".":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_error(value: Exception) -> str:
    return re.sub(r"api_key=[^&\s)]+", "api_key=[REDACTED]", str(value))


class FredClient:
    """Small async FRED wrapper with one-process daily cache."""

    def __init__(
        self,
        api_key: str | None,
        *,
        api_budget: ApiBudget | None = None,
        timeout_seconds: float = 8.0,
    ) -> None:
        self.api_key = (api_key or "").strip()
        self.api_budget = api_budget or ApiBudget(
            name="fred",
            warning_threshold=30,
            critical_threshold=40,
        )
        self.timeout_seconds = timeout_seconds
        self._cache_day: date | None = None
        self._cache_value: dict[str, Any] | None = None

    @property
    def enabled(self) -> bool:
        return bool(self.api_key)

    async def macro_context(self) -> dict[str, Any]:
        """Return compact macro context for the current UTC day.

        Missing key and endpoint failures are returned as data so the supervisor
        can keep running and the AI packet can mark macro context explicitly.
        """
        if not self.enabled:
            return {
                "provider": "fred",
                "enabled": False,
                "error": "FRED_API_KEY is not configured",
                "series": {},
            }
        today = datetime.now(UTC).date()
        if self._cache_day == today and self._cache_value is not None:
            return self._cache_value

        rows = await asyncio.gather(
            *(self.latest_observation(series_id) for series_id in CORE_RISK_SERIES.values()),
            return_exceptions=True,
        )
        series: dict[str, Any] = {}
        for (name, series_id), value in zip(CORE_RISK_SERIES.items(), rows, strict=True):
            if isinstance(value, Exception):
                series[name] = {"series_id": series_id, "error": _safe_error(value)}
            else:
                series[name] = value
        context = {
            "provider": "fred",
            "enabled": True,
            "fetched_at": datetime.now(UTC).isoformat() + "Z",
            "series": series,
            "regime": self._macro_regime(series),
        }
        self._cache_day = today
        self._cache_value = context
        return context

    async def latest_observation(self, series_id: str) -> dict[str, Any]:
        data = await self._get_json(
            "/fred/series/observations",
            {
                "series_id": series_id,
                "sort_order": "desc",
                "limit": 1,
                "file_type": "json",
            },
            endpoint=f"series.{series_id}",
        )
        observations = data.get("observations") if isinstance(data, dict) else None
        latest = observations[0] if isinstance(observations, list) and observations else {}
        return {
            "series_id": series_id,
            "date": latest.get("date"),
            "value": _num(latest.get("value")),
        }

    async def _get_json(self, path: str, params: dict[str, Any], *, endpoint: str) -> Any:
        if not self.enabled:
            raise RuntimeError("FRED_API_KEY is not configured")
        if not self.api_budget.allow_nonessential(endpoint):
            raise RuntimeError(f"nonessential FRED API work deferred by API budget: {endpoint}")

        query = urlencode({**params, "api_key": self.api_key})
        url = f"https://api.stlouisfed.org{path}?{query}"
        self.api_budget.record(endpoint, essential=False)
        return await asyncio.to_thread(self._fetch_json, url)

    def _fetch_json(self, url: str) -> Any:
        request = Request(url, headers={"User-Agent": "auto-trader/0.1"})
        with urlopen(request, timeout=self.timeout_seconds) as response:
            body = response.read().decode("utf-8")
        return json.loads(body)

    def _macro_regime(self, series: dict[str, Any]) -> dict[str, Any]:
        vix = _series_value(series, "volatility_index")
        high_yield_spread = _series_value(series, "high_yield_credit_spread")
        curve = _series_value(series, "ten_year_two_year_spread")
        stress_flags = []
        if vix is not None and vix >= 25:
            stress_flags.append("elevated_volatility")
        if high_yield_spread is not None and high_yield_spread >= 5:
            stress_flags.append("elevated_credit_spread")
        if curve is not None and curve < 0:
            stress_flags.append("inverted_yield_curve")
        if len(stress_flags) >= 2:
            backdrop = "stressed"
        elif stress_flags:
            backdrop = "cautious"
        else:
            backdrop = "normal_or_unknown"
        return {
            "risk_backdrop": backdrop,
            "stress_flags": stress_flags,
            "advisory_only": True,
        }


def _series_value(series: dict[str, Any], key: str) -> float | None:
    row = series.get(key)
    if not isinstance(row, dict):
        return None
    return _num(row.get("value"))
