"""One-off, no-order smoke test for real AI research providers."""
from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
from typing import Any

from auto_trader.ai_research_preflight import _cost_assumptions_from_settings
from auto_trader.config.settings import get_settings
from auto_trader.core.models import TradeIntent
from auto_trader.intelligence.ai_committee import create_research_committee
from auto_trader.persistence.db import (
    configure_db_path,
    count_ai_research_chargeable_attempts,
    init_db,
    log_ai_research_memo,
)
from auto_trader.utils.logging import setup_logging


@dataclass(frozen=True)
class AIResearchSmokeResult:
    ok: bool
    called_provider: bool
    provider: str
    model: str
    symbol: str
    prompt_version: str
    validation_passed: bool | None
    verdict: str
    confidence: float | None
    input_hash_prefix: str
    memo_id: int | None
    used_before: int | None
    used_after: int | None
    max_calls: int
    estimated_cost: float
    reason: str
    normalization_markers: list[str]

    @property
    def remaining_after(self) -> int | None:
        if self.used_after is None:
            return None
        return max(0, self.max_calls - self.used_after)


def render_ai_research_smoke(result: AIResearchSmokeResult) -> str:
    state = "PASS" if result.ok else "NOT_RUN"
    used_before = "unavailable" if result.used_before is None else str(result.used_before)
    used_after = "unavailable" if result.used_after is None else str(result.used_after)
    remaining_after = "unavailable" if result.remaining_after is None else str(result.remaining_after)
    markers = ", ".join(result.normalization_markers) if result.normalization_markers else "none"
    return "\n".join(
        [
            "AI RESEARCH SMOKE",
            f"State: {state}",
            f"Reason: {result.reason}",
            f"Provider: {result.provider}",
            f"Model: {result.model or 'n/a'}",
            f"Symbol: {result.symbol}",
            f"Prompt version: {result.prompt_version or 'n/a'}",
            f"Validation passed: {result.validation_passed}",
            f"Verdict: {result.verdict or 'n/a'}",
            f"Confidence: {result.confidence}",
            f"Input hash: {result.input_hash_prefix or 'n/a'}",
            f"Memo id: {result.memo_id}",
            f"Chargeable calls: before {used_before} / max {result.max_calls}; after {used_after}; remaining {remaining_after}",
            f"Estimated cost for this call: ${result.estimated_cost:.4f}",
            f"Normalization markers: {markers}",
        ]
    )


async def run_ai_research_smoke(
    *,
    settings: Any | None = None,
    symbol: str,
    price: float,
    confidence: float = 0.7,
    rationale: str = "one-off AI research smoke",
) -> AIResearchSmokeResult:
    settings = settings or get_settings()
    configure_db_path(getattr(settings, "db_path", "auto_trader.db"))
    await init_db()

    provider = str(getattr(settings, "ai_research_provider", "shadow") or "shadow").lower()
    model = str(getattr(settings, "ai_research_model", "") or "").strip()
    max_calls = int(getattr(settings, "ai_research_max_calls_per_day", 0) or 0)
    cost = _cost_assumptions_from_settings(settings).estimated_cost_per_memo
    symbol = symbol.upper()

    if provider == "shadow":
        return _not_run(provider, model, symbol, max_calls, cost, "real provider is required")
    if max_calls <= 0:
        return _not_run(provider, model, symbol, max_calls, cost, "AI_RESEARCH_MAX_CALLS_PER_DAY must be positive")

    used_before = await count_ai_research_chargeable_attempts(provider=provider, today_utc=True)
    if used_before is None:
        return _not_run(provider, model, symbol, max_calls, cost, "chargeable budget count unavailable")
    if used_before >= max_calls:
        return AIResearchSmokeResult(
            ok=False,
            called_provider=False,
            provider=provider,
            model=model,
            symbol=symbol,
            prompt_version="",
            validation_passed=None,
            verdict="",
            confidence=None,
            input_hash_prefix="",
            memo_id=None,
            used_before=used_before,
            used_after=used_before,
            max_calls=max_calls,
            estimated_cost=cost,
            reason="chargeable budget exhausted",
            normalization_markers=[],
        )

    committee = create_research_committee(settings)
    intent = TradeIntent(
        symbol=symbol,
        side="long",
        entry_price=price,
        rationale=rationale,
        confidence=confidence,
        features={
            "smoke": {
                "source": "ai_research_smoke",
                "advisory_only": True,
                "no_order_flow": True,
            }
        },
    )

    try:
        memo = await committee.research(intent, signal_id=None)
        memo_id = await log_ai_research_memo(
            signal_id=None,
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
        used_after = await count_ai_research_chargeable_attempts(provider=provider, today_utc=True)
        normalization = memo.memo.get("normalization") if isinstance(memo.memo, dict) else {}
        markers = normalization.get("markers", []) if isinstance(normalization, dict) else []
        return AIResearchSmokeResult(
            ok=True,
            called_provider=True,
            provider=memo.provider,
            model=memo.model_tag,
            symbol=memo.symbol,
            prompt_version=memo.prompt_version,
            validation_passed=memo.validation_passed,
            verdict=memo.verdict,
            confidence=memo.confidence,
            input_hash_prefix=memo.input_hash[:12],
            memo_id=memo_id,
            used_before=used_before,
            used_after=used_after if used_after is not None else used_before + 1,
            max_calls=max_calls,
            estimated_cost=cost,
            reason="provider memo completed",
            normalization_markers=[str(marker) for marker in markers],
        )
    except Exception as exc:
        failure_hash = "unavailable"
        memo_id = await log_ai_research_memo(
            signal_id=None,
            symbol=symbol,
            provider=provider,
            model_tag=f"{provider}/{model}" if model else provider,
            prompt_version="ai_research_failure/v0",
            input_hash=failure_hash,
            verdict="watch",
            confidence=None,
            used_only_provided_data=True,
            validation_passed=False,
            memo={
                "committee": {
                    "symbol": symbol,
                    "verdict": "watch",
                    "confidence": None,
                    "used_only_provided_data": True,
                    "validation_errors": ["ai_research_provider_failed"],
                    "judge_summary": "One-off real-provider smoke failed before producing a valid advisory memo.",
                },
                "error": str(exc),
            },
        )
        used_after = await count_ai_research_chargeable_attempts(provider=provider, today_utc=True)
        return AIResearchSmokeResult(
            ok=False,
            called_provider=True,
            provider=provider,
            model=f"{provider}/{model}" if model else provider,
            symbol=symbol,
            prompt_version="ai_research_failure/v0",
            validation_passed=False,
            verdict="watch",
            confidence=None,
            input_hash_prefix=failure_hash,
            memo_id=memo_id,
            used_before=used_before,
            used_after=used_after if used_after is not None else used_before + 1,
            max_calls=max_calls,
            estimated_cost=cost,
            reason=f"provider failed: {exc}",
            normalization_markers=[],
        )


def _not_run(
    provider: str,
    model: str,
    symbol: str,
    max_calls: int,
    estimated_cost: float,
    reason: str,
) -> AIResearchSmokeResult:
    return AIResearchSmokeResult(
        ok=False,
        called_provider=False,
        provider=provider,
        model=model,
        symbol=symbol,
        prompt_version="",
        validation_passed=None,
        verdict="",
        confidence=None,
        input_hash_prefix="",
        memo_id=None,
        used_before=None,
        used_after=None,
        max_calls=max_calls,
        estimated_cost=estimated_cost,
        reason=reason,
        normalization_markers=[],
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Run one no-order real-provider AI research smoke.")
    parser.add_argument("symbol", help="Ticker symbol for the synthetic advisory memo.")
    parser.add_argument("--price", type=float, default=1.0, help="Synthetic entry price for the packet.")
    parser.add_argument("--confidence", type=float, default=0.7, help="Synthetic candidate confidence.")
    parser.add_argument("--rationale", default="one-off AI research smoke", help="Synthetic rationale.")
    args = parser.parse_args()

    setup_logging("ERROR")
    result = asyncio.run(
        run_ai_research_smoke(
            symbol=args.symbol,
            price=args.price,
            confidence=args.confidence,
            rationale=args.rationale,
        )
    )
    print(render_ai_research_smoke(result))
    raise SystemExit(0 if result.ok else 2)


if __name__ == "__main__":
    main()
