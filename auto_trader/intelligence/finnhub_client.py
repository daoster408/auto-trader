"""Optional Finnhub enrichment for candidate research context.

Finnhub data is non-authoritative in v1: it can enrich signal features and
operator visibility, but RiskEngine remains the only order gate.
"""
from __future__ import annotations

import asyncio
import json
import re
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from auto_trader.utils.api_budget import ApiBudget
from auto_trader.utils.logging import get_logger

log = get_logger("auto_trader.intelligence.finnhub_client")


def _num(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _str_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _safe_error(value: Exception) -> str:
    return re.sub(r"token=[^&\s)]+", "token=[REDACTED]", str(value))


class FinnhubClient:
    """Small async wrapper around Finnhub's REST API."""

    def __init__(
        self,
        api_key: str | None,
        *,
        api_budget: ApiBudget | None = None,
        timeout_seconds: float = 8.0,
    ) -> None:
        self.api_key = (api_key or "").strip()
        self.api_budget = api_budget or ApiBudget(
            name="finnhub",
            warning_threshold=45,
            critical_threshold=55,
        )
        self.timeout_seconds = timeout_seconds

    @property
    def enabled(self) -> bool:
        return bool(self.api_key)

    async def enrich_symbol(self, symbol: str, *, news_days: int = 7, max_news: int = 3) -> dict[str, Any]:
        """Return compact quote/profile/news enrichment for a symbol.

        Failures are returned as feature metadata instead of raising so the
        trading loop can continue with Alpaca-only rules.
        """
        clean_symbol = str(symbol or "").strip().upper()
        if not self.enabled:
            return {"provider": "finnhub", "enabled": False}
        if not clean_symbol:
            return {"provider": "finnhub", "enabled": True, "error": "missing_symbol"}

        quote, profile, news = await asyncio.gather(
            self.quote(clean_symbol),
            self.company_profile(clean_symbol),
            self.company_news(clean_symbol, days=news_days, limit=max_news),
            return_exceptions=True,
        )
        return {
            "provider": "finnhub",
            "enabled": True,
            "quote": self._value_or_error("quote", clean_symbol, quote),
            "profile": self._value_or_error("profile", clean_symbol, profile),
            "news": self._value_or_error("company-news", clean_symbol, news),
        }

    def _value_or_error(self, endpoint: str, symbol: str, value: Any) -> Any:
        if not isinstance(value, Exception):
            return value
        error = _safe_error(value)
        log.warning("finnhub_endpoint_failed", symbol=symbol, endpoint=endpoint, error=error)
        return {"error": error}

    async def quote(self, symbol: str) -> dict[str, Any]:
        data = await self._get_json("/quote", {"symbol": symbol}, endpoint="quote")
        return {
            "current": _num(data.get("c")),
            "change": _num(data.get("d")),
            "change_pct": _num(data.get("dp")),
            "high": _num(data.get("h")),
            "low": _num(data.get("l")),
            "open": _num(data.get("o")),
            "prev_close": _num(data.get("pc")),
        }

    async def company_profile(self, symbol: str) -> dict[str, Any]:
        data = await self._get_json("/stock/profile2", {"symbol": symbol}, endpoint="profile2")
        return {
            "name": _str_or_none(data.get("name")),
            "ticker": _str_or_none(data.get("ticker")),
            "exchange": _str_or_none(data.get("exchange")),
            "industry": _str_or_none(data.get("finnhubIndustry")),
            "market_cap": _num(data.get("marketCapitalization")),
            "share_outstanding": _num(data.get("shareOutstanding")),
        }

    async def company_news(self, symbol: str, *, days: int = 7, limit: int = 3) -> list[dict[str, Any]]:
        today = datetime.now(UTC).date()
        start = today - timedelta(days=max(days, 1))
        data = await self._get_json(
            "/company-news",
            {"symbol": symbol, "from": start.isoformat(), "to": today.isoformat()},
            endpoint="company-news",
        )
        if not isinstance(data, list):
            return []
        return [self._compact_news_item(item) for item in data[: max(limit, 0)] if isinstance(item, dict)]

    def _compact_news_item(self, item: dict[str, Any]) -> dict[str, Any]:
        published_at: str | None = None
        timestamp = item.get("datetime")
        if timestamp is not None:
            try:
                published_at = datetime.fromtimestamp(float(timestamp), tz=UTC).isoformat()
            except (TypeError, ValueError, OSError):
                published_at = None
        return {
            "headline": _str_or_none(item.get("headline")),
            "source": _str_or_none(item.get("source")),
            "published_at": published_at,
            "url": _str_or_none(item.get("url")),
        }

    async def _get_json(self, path: str, params: dict[str, Any], *, endpoint: str) -> Any:
        if not self.enabled:
            raise RuntimeError("FINNHUB_API_KEY is not configured")
        if not self.api_budget.allow_nonessential(endpoint):
            raise RuntimeError(f"nonessential Finnhub API work deferred by API budget: {endpoint}")

        query = urlencode({**params, "token": self.api_key})
        url = f"https://finnhub.io/api/v1{path}?{query}"
        self.api_budget.record(endpoint, essential=False)
        return await asyncio.to_thread(self._fetch_json, url)

    def _fetch_json(self, url: str) -> Any:
        request = Request(url, headers={"User-Agent": "auto-trader/0.1"})
        with urlopen(request, timeout=self.timeout_seconds) as response:
            body = response.read().decode("utf-8")
        return json.loads(body)
