"""Read-only AI provider cost report from persisted research memos."""
from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import aiosqlite

from auto_trader.config.settings import get_settings
from auto_trader.persistence.db import (
    CHARGEABLE_AI_RESEARCH_PROMPT_VERSIONS,
    configure_db_path,
    get_configured_db_path,
)
from auto_trader.utils.logging import setup_logging


@dataclass(frozen=True)
class ProviderPrice:
    input_price_per_mtok: float
    output_price_per_mtok: float
    source: str


@dataclass(frozen=True)
class ProviderCost:
    provider: str
    calls: int
    usage_known: int
    usage_unknown: int
    input_tokens: int
    output_tokens: int
    total_tokens: int
    estimated_cost: float
    input_price_per_mtok: float
    output_price_per_mtok: float
    pricing_source: str


@dataclass(frozen=True)
class AICostReport:
    start_utc: datetime
    end_utc: datetime
    timezone: str
    providers: list[ProviderCost]
    unavailable_reason: str | None = None

    @property
    def total_calls(self) -> int:
        return sum(row.calls for row in self.providers)

    @property
    def total_usage_known(self) -> int:
        return sum(row.usage_known for row in self.providers)

    @property
    def total_usage_unknown(self) -> int:
        return sum(row.usage_unknown for row in self.providers)

    @property
    def total_estimated_cost(self) -> float:
        return sum(row.estimated_cost for row in self.providers)


def _utc_window_for_local_days(*, days: int, timezone_name: str) -> tuple[datetime, datetime]:
    timezone = ZoneInfo(timezone_name)
    local_now = datetime.now(timezone)
    local_end = (local_now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    local_start = local_end - timedelta(days=days)
    return local_start.astimezone(UTC), local_end.astimezone(UTC)


def _sqlite_dt(value: datetime) -> str:
    return value.astimezone(UTC).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")


def _json_dict(value: Any) -> dict[str, Any]:
    try:
        parsed = json.loads(value or "{}")
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _usage(memo: dict[str, Any]) -> dict[str, int]:
    usage = memo.get("provider_usage")
    if not isinstance(usage, dict):
        return {}
    parsed: dict[str, int] = {}
    for key in ("input_tokens", "output_tokens", "total_tokens"):
        value = usage.get(key)
        if isinstance(value, int) and value >= 0:
            parsed[key] = value
    return parsed


def _provider_price(settings: Any, provider: str) -> ProviderPrice:
    input_override = getattr(settings, f"ai_research_{provider}_input_price_per_mtok", None)
    output_override = getattr(settings, f"ai_research_{provider}_output_price_per_mtok", None)
    input_price = (
        float(input_override)
        if input_override is not None
        else float(getattr(settings, "ai_research_input_price_per_mtok", 0.0) or 0.0)
    )
    output_price = (
        float(output_override)
        if output_override is not None
        else float(getattr(settings, "ai_research_output_price_per_mtok", 0.0) or 0.0)
    )
    source = "provider_override" if input_override is not None or output_override is not None else "global_estimate"
    return ProviderPrice(
        input_price_per_mtok=input_price,
        output_price_per_mtok=output_price,
        source=source,
    )


def _estimate_cost(*, input_tokens: int, output_tokens: int, price: ProviderPrice) -> float:
    input_cost = (input_tokens / 1_000_000.0) * price.input_price_per_mtok
    output_cost = (output_tokens / 1_000_000.0) * price.output_price_per_mtok
    return input_cost + output_cost


async def build_ai_cost_report(
    *,
    settings: Any,
    days: int = 1,
    start_utc: datetime | None = None,
    end_utc: datetime | None = None,
) -> AICostReport:
    timezone_name = str(getattr(settings, "report_timezone", "America/Los_Angeles") or "America/Los_Angeles")
    if start_utc is None or end_utc is None:
        start_utc, end_utc = _utc_window_for_local_days(days=days, timezone_name=timezone_name)
    else:
        start_utc = start_utc.astimezone(UTC)
        end_utc = end_utc.astimezone(UTC)
    configure_db_path(getattr(settings, "db_path", "auto_trader.db"))
    db_path = Path(get_configured_db_path())
    if not db_path.exists():
        return AICostReport(
            start_utc=start_utc,
            end_utc=end_utc,
            timezone=timezone_name,
            providers=[],
            unavailable_reason=f"database not found: {db_path}",
        )
    placeholders = ",".join("?" for _ in CHARGEABLE_AI_RESEARCH_PROMPT_VERSIONS)
    params: list[Any] = [
        "shadow",
        "multi",
        *CHARGEABLE_AI_RESEARCH_PROMPT_VERSIONS,
        _sqlite_dt(start_utc),
        _sqlite_dt(end_utc),
    ]
    db = await aiosqlite.connect(get_configured_db_path())
    db.row_factory = aiosqlite.Row
    try:
        cur = await db.execute(
            f"""
            SELECT provider, memo_json
            FROM ai_research_memos
            WHERE provider NOT IN (?, ?)
              AND prompt_version IN ({placeholders})
              AND created_at >= ?
              AND created_at < ?
            ORDER BY id ASC
            """,
            tuple(params),
        )
        rows = await cur.fetchall()
    except aiosqlite.OperationalError as exc:
        if "no such table" not in str(exc).lower():
            raise
        return AICostReport(
            start_utc=start_utc,
            end_utc=end_utc,
            timezone=timezone_name,
            providers=[],
            unavailable_reason="ai_research_memos table not found",
        )
    finally:
        await db.close()

    grouped: dict[str, dict[str, int]] = {}
    for row in rows:
        provider = str(row["provider"] or "unknown").strip().lower() or "unknown"
        bucket = grouped.setdefault(
            provider,
            {
                "calls": 0,
                "usage_known": 0,
                "usage_unknown": 0,
                "input_tokens": 0,
                "output_tokens": 0,
                "total_tokens": 0,
            },
        )
        bucket["calls"] += 1
        usage = _usage(_json_dict(row["memo_json"]))
        if usage:
            bucket["usage_known"] += 1
            bucket["input_tokens"] += int(usage.get("input_tokens", 0))
            bucket["output_tokens"] += int(usage.get("output_tokens", 0))
            bucket["total_tokens"] += int(
                usage.get("total_tokens")
                or usage.get("input_tokens", 0) + usage.get("output_tokens", 0)
            )
        else:
            bucket["usage_unknown"] += 1

    provider_costs: list[ProviderCost] = []
    for provider, values in sorted(grouped.items()):
        price = _provider_price(settings, provider)
        provider_costs.append(
            ProviderCost(
                provider=provider,
                calls=values["calls"],
                usage_known=values["usage_known"],
                usage_unknown=values["usage_unknown"],
                input_tokens=values["input_tokens"],
                output_tokens=values["output_tokens"],
                total_tokens=values["total_tokens"],
                estimated_cost=_estimate_cost(
                    input_tokens=values["input_tokens"],
                    output_tokens=values["output_tokens"],
                    price=price,
                ),
                input_price_per_mtok=price.input_price_per_mtok,
                output_price_per_mtok=price.output_price_per_mtok,
                pricing_source=price.source,
            )
        )

    return AICostReport(
        start_utc=start_utc,
        end_utc=end_utc,
        timezone=timezone_name,
        providers=provider_costs,
    )


def render_ai_cost_report(report: AICostReport) -> str:
    lines = [
        "AI COST REPORT",
        f"Window: {report.start_utc.isoformat()} to {report.end_utc.isoformat()} UTC",
        f"Timezone basis: {report.timezone}",
        f"Chargeable calls: {report.total_calls}",
        f"Usage known: {report.total_usage_known}",
        f"Unknown possible billed failures: {report.total_usage_unknown}",
        f"Estimated total: ${report.total_estimated_cost:.4f}",
    ]
    if report.unavailable_reason:
        lines.append(f"Unavailable: {report.unavailable_reason}")
    if not report.providers:
        lines.append("Providers: none")
        return "\n".join(lines)
    lines.append("By provider:")
    for row in report.providers:
        lines.append(
            f"- {row.provider}: calls={row.calls}, usage_known={row.usage_known}, "
            f"unknown={row.usage_unknown}, input={row.input_tokens}, output={row.output_tokens}, "
            f"total={row.total_tokens}, est=${row.estimated_cost:.4f}, "
            f"prices=${row.input_price_per_mtok:.2f}/${row.output_price_per_mtok:.2f} per MTok "
            f"({row.pricing_source})"
        )
    lines.append("Note: dollars are estimated from returned token usage; unknown rows may still have provider-side billing.")
    return "\n".join(lines)


async def run_ai_cost_report(*, days: int = 1, settings: Any | None = None) -> str:
    settings = settings or get_settings()
    report = await build_ai_cost_report(settings=settings, days=days)
    return render_ai_cost_report(report)


def main() -> None:
    parser = argparse.ArgumentParser(description="Read-only AI provider cost report.")
    parser.add_argument("--days", type=int, default=1, help="Local-day window size to include.")
    args = parser.parse_args()
    if args.days < 1 or args.days > 30:
        raise SystemExit("--days must be between 1 and 30")
    setup_logging("ERROR")
    print(asyncio.run(run_ai_cost_report(days=args.days)))


if __name__ == "__main__":
    main()
