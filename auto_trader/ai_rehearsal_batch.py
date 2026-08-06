"""No-order batch rehearsal for candidate -> prefilter -> AI gate flow."""
from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from zoneinfo import ZoneInfo

from auto_trader.ai_entry_gate_rehearsal import _missing_sections, _model_label, _persist_round
from auto_trader.ai_research_preflight import _cost_assumptions_from_settings
from auto_trader.broker.alpaca_adapter import AlpacaAdapter
from auto_trader.config.settings import get_settings
from auto_trader.core.models import TradeIntent
from auto_trader.core.risk_profile import get_risk_profile
from auto_trader.intelligence.ai_committee import (
    ResearchCommittee,
    ShadowResearchCommittee,
    create_research_committee,
    real_research_providers,
    research_committee_round,
    selected_research_providers,
)
from auto_trader.intelligence.ai_paid_prefilter import evaluate_paid_ai_prefilter
from auto_trader.intelligence.finnhub_client import FinnhubClient
from auto_trader.intelligence.fred_client import FredClient
from auto_trader.intelligence.research_context import build_risk_research_context, with_research_context
from auto_trader.intelligence.rules_fallback import get_simple_rules_signals
from auto_trader.persistence.db import (
    configure_db_path,
    count_ai_research_chargeable_attempts,
    count_entry_orders_since,
    get_runtime_config_value,
    init_db,
    log_ai_research_memo,
)
from auto_trader.utils.logging import setup_logging


@dataclass(frozen=True)
class BatchCandidateResult:
    symbol: str
    price: float | None
    confidence: float | None
    prefilter: str
    verdict: str
    validation_passed: bool | None
    would_continue_to_risk_engine: bool
    called_provider: bool
    provider: str
    confidence_after_research: float | None
    reason: str
    missing_sections: list[str]


@dataclass(frozen=True)
class AIRehearsalBatchResult:
    ok: bool
    paid_mode: bool
    requested_limit: int
    generated: int
    blocked_by_prefilter: int
    reviewed: int
    approved_for_risk_engine: int
    used_before: int | None
    used_after: int | None
    max_calls: int
    estimated_cost_per_candidate: float
    provider: str
    model: str
    risk_profile: str
    reason: str
    candidates: list[BatchCandidateResult]


def render_ai_rehearsal_batch(result: AIRehearsalBatchResult) -> str:
    used_before = "unavailable" if result.used_before is None else str(result.used_before)
    used_after = "unavailable" if result.used_after is None else str(result.used_after)
    mode = "PAID" if result.paid_mode else "SHADOW"
    lines = [
        "AI REHEARSAL BATCH",
        "No orders submitted. RiskEngine and execution stack are not invoked.",
        f"Mode: {mode}",
        f"State: {'OK' if result.ok else 'NOT_RUN'}",
        f"Reason: {result.reason}",
        f"Risk profile: {result.risk_profile}",
        f"Provider: {result.provider}",
        f"Model: {result.model or 'n/a'}",
        f"Candidates requested: {result.requested_limit}",
        f"Candidates generated: {result.generated}",
        f"Prefilter blocked: {result.blocked_by_prefilter}",
        f"AI reviewed: {result.reviewed}",
        f"Would continue to RiskEngine: {result.approved_for_risk_engine}",
        f"Chargeable calls: before {used_before} / max {result.max_calls}; after {used_after}",
        f"Estimated cost per candidate: ${result.estimated_cost_per_candidate:.4f}",
        "",
        "Candidates:",
    ]
    if not result.candidates:
        lines.append("- none")
    for row in result.candidates:
        missing = ",".join(row.missing_sections) if row.missing_sections else "none"
        lines.append(
            f"- {row.symbol}: prefilter={row.prefilter}, verdict={row.verdict}, "
            f"validation={row.validation_passed}, continue={row.would_continue_to_risk_engine}, "
            f"provider_called={row.called_provider}, reason={row.reason}, missing={missing}"
        )
    return "\n".join(lines)


async def run_ai_rehearsal_batch(
    *,
    limit: int = 10,
    paid: bool = False,
    settings: Any | None = None,
    adapter: Any | None = None,
    committee: ResearchCommittee | None = None,
) -> AIRehearsalBatchResult:
    settings = settings or get_settings()
    configure_db_path(getattr(settings, "db_path", "auto_trader.db"))
    await init_db()

    paper = bool(getattr(settings, "alpaca_paper", True))
    risk_profile = await _runtime_risk_profile(settings, paper=paper)
    limit = max(1, min(int(limit), 25))
    configured_providers = selected_research_providers(settings)
    provider = configured_providers[0] if len(configured_providers) == 1 else "multi"
    model = _model_label(settings, configured_providers)
    max_calls = int(getattr(settings, "ai_research_max_calls_per_day", 0) or 0)
    attempts_per_candidate = max(1, len([name for name in configured_providers if name != "shadow"]))
    estimated_cost = _cost_assumptions_from_settings(settings).estimated_cost_per_memo * attempts_per_candidate
    budget_provider = None if len(configured_providers) > 1 else provider
    used_before = await _chargeable_count(provider=budget_provider)
    if paid and used_before is None:
        return _empty_result(
            paid=paid,
            limit=limit,
            used_before=None,
            used_after=None,
            max_calls=max_calls,
            estimated_cost=estimated_cost,
            provider=provider,
            model=model,
            risk_profile=risk_profile,
            reason="chargeable budget count unavailable",
        )

    if paid and max_calls <= 0:
        return _empty_result(
            paid=paid,
            limit=limit,
            used_before=used_before,
            used_after=used_before,
            max_calls=max_calls,
            estimated_cost=estimated_cost,
            provider=provider,
            model=model,
            risk_profile=risk_profile,
            reason="AI_RESEARCH_MAX_CALLS_PER_DAY must be positive for paid mode",
        )
    if paid and used_before is not None and max_calls - used_before < attempts_per_candidate:
        return _empty_result(
            paid=paid,
            limit=limit,
            used_before=used_before,
            used_after=used_before,
            max_calls=max_calls,
            estimated_cost=estimated_cost,
            provider=provider,
            model=model,
            risk_profile=risk_profile,
            reason="chargeable budget exhausted",
        )

    adapter = adapter or AlpacaAdapter(
        api_key=getattr(settings, "alpaca_api_key", ""),
        api_secret=getattr(settings, "alpaca_api_secret", ""),
        paper=paper,
    )
    account = await adapter.get_account_snapshot()
    clock = await adapter.get_clock()
    positions = await adapter.get_positions_snapshot(strict=True)
    today_new_entries = await _today_new_entries(settings)
    max_new_positions = int(getattr(settings, "max_new_positions_per_day", 1) or 1)
    finnhub_client = FinnhubClient(getattr(settings, "finnhub_api_key", None))
    fred_client = FredClient(getattr(settings, "fred_api_key", None))
    signals = await get_simple_rules_signals(
        adapter,
        max_signals=limit,
        finnhub_client=finnhub_client,
        fred_client=fred_client,
        risk_profile=risk_profile,
        paper=paper,
    )
    if not signals:
        return _empty_result(
            paid=paid,
            limit=limit,
            used_before=used_before,
            used_after=used_before,
            max_calls=max_calls,
            estimated_cost=estimated_cost,
            provider=provider,
            model=model,
            risk_profile=risk_profile,
            reason="no candidates generated",
        )

    active_committee = committee or (create_research_committee(settings) if paid else ShadowResearchCommittee())
    if paid and not real_research_providers(active_committee):
        return _empty_result(
            paid=paid,
            limit=limit,
            used_before=used_before,
            used_after=used_before,
            max_calls=max_calls,
            estimated_cost=estimated_cost,
            provider=provider,
            model=model,
            risk_profile=risk_profile,
            reason="real provider is required for paid mode",
        )

    rows: list[BatchCandidateResult] = []
    for signal in signals[:limit]:
        intent = _with_batch_context(
            signal,
            account=account,
            clock=clock,
            positions=positions,
            today_new_entries=today_new_entries,
            max_new_positions_per_day=max_new_positions,
        )
        context = ((intent.features or {}).get("research_context") or {}) if isinstance(intent.features, dict) else {}
        missing_sections = _missing_sections(context)
        prefilter = evaluate_paid_ai_prefilter(intent, settings=settings, risk_profile=risk_profile)
        if prefilter.blocked:
            rows.append(
                BatchCandidateResult(
                    symbol=intent.symbol.upper(),
                    price=intent.entry_price,
                    confidence=intent.confidence,
                    prefilter="block:" + ",".join(prefilter.reasons),
                    verdict="watch",
                    validation_passed=True,
                    would_continue_to_risk_engine=False,
                    called_provider=False,
                    provider="prefilter",
                    confidence_after_research=None,
                    reason="prefilter blocked before AI review",
                    missing_sections=missing_sections,
                )
            )
            continue
        if paid:
            used_now = await _chargeable_count(provider=budget_provider)
            if used_now is None:
                rows.append(
                    BatchCandidateResult(
                        symbol=intent.symbol.upper(),
                        price=intent.entry_price,
                        confidence=intent.confidence,
                        prefilter="pass",
                        verdict="watch",
                        validation_passed=False,
                        would_continue_to_risk_engine=False,
                        called_provider=False,
                        provider=provider,
                        confidence_after_research=None,
                        reason="paid budget count unavailable before candidate review",
                        missing_sections=missing_sections,
                    )
                )
                break
            if max_calls - used_now < attempts_per_candidate:
                rows.append(
                    BatchCandidateResult(
                        symbol=intent.symbol.upper(),
                        price=intent.entry_price,
                        confidence=intent.confidence,
                        prefilter="pass",
                        verdict="watch",
                        validation_passed=False,
                        would_continue_to_risk_engine=False,
                        called_provider=False,
                        provider=provider,
                        confidence_after_research=None,
                        reason="paid budget exhausted before candidate review",
                        missing_sections=missing_sections,
                    )
                )
                continue
        try:
            research_round = await research_committee_round(active_committee, intent, signal_id=None)
            if paid:
                await _persist_round(
                    research_round.member_memos,
                    research_round.aggregate_memo,
                    decision_source="ai_rehearsal_batch",
                )
            memo = research_round.aggregate_memo
            would_continue = memo.validation_passed and memo.verdict == "approve"
            rows.append(
                BatchCandidateResult(
                    symbol=memo.symbol,
                    price=intent.entry_price,
                    confidence=intent.confidence,
                    prefilter="pass",
                    verdict=memo.verdict,
                    validation_passed=memo.validation_passed,
                    would_continue_to_risk_engine=would_continue,
                    called_provider=paid,
                    provider=memo.provider,
                    confidence_after_research=memo.confidence,
                    reason="AI gate would continue to RiskEngine" if would_continue else f"AI gate would block: {memo.verdict}",
                    missing_sections=missing_sections,
                )
            )
        except Exception as exc:
            if paid:
                await log_ai_research_memo(
                    signal_id=None,
                    symbol=intent.symbol.upper(),
                    provider=provider,
                    model_tag=model or provider,
                    prompt_version="ai_research_failure/v0",
                    input_hash="unavailable",
                    verdict="watch",
                    confidence=None,
                    used_only_provided_data=True,
                    validation_passed=False,
                    decision_source="ai_rehearsal_batch",
                    memo={
                        "source": "ai_rehearsal_batch",
                        "error": str(exc),
                        "candidate": {"symbol": intent.symbol.upper(), "entry_price": intent.entry_price},
                    },
                )
            rows.append(
                BatchCandidateResult(
                    symbol=intent.symbol.upper(),
                    price=intent.entry_price,
                    confidence=intent.confidence,
                    prefilter="pass",
                    verdict="watch",
                    validation_passed=False,
                    would_continue_to_risk_engine=False,
                    called_provider=paid,
                    provider=provider,
                    confidence_after_research=None,
                    reason=f"AI review failed: {exc}",
                    missing_sections=missing_sections,
                )
            )

    used_after = await _chargeable_count(provider=budget_provider)
    reviewed = sum(1 for row in rows if row.prefilter == "pass")
    blocked = sum(1 for row in rows if row.prefilter.startswith("block:"))
    approved = sum(1 for row in rows if row.would_continue_to_risk_engine)
    return AIRehearsalBatchResult(
        ok=True,
        paid_mode=paid,
        requested_limit=limit,
        generated=len(signals),
        blocked_by_prefilter=blocked,
        reviewed=reviewed,
        approved_for_risk_engine=approved,
        used_before=used_before,
        used_after=used_after,
        max_calls=max_calls,
        estimated_cost_per_candidate=estimated_cost if paid else 0.0,
        provider=provider if paid else "shadow",
        model=model if paid else "shadow_ai_committee/v0",
        risk_profile=risk_profile,
        reason="batch rehearsal completed",
        candidates=rows,
    )


def _empty_result(
    *,
    paid: bool,
    limit: int,
    used_before: int | None,
    used_after: int | None,
    max_calls: int,
    estimated_cost: float,
    provider: str,
    model: str,
    risk_profile: str,
    reason: str,
) -> AIRehearsalBatchResult:
    return AIRehearsalBatchResult(
        ok=False,
        paid_mode=paid,
        requested_limit=limit,
        generated=0,
        blocked_by_prefilter=0,
        reviewed=0,
        approved_for_risk_engine=0,
        used_before=used_before,
        used_after=used_after,
        max_calls=max_calls,
        estimated_cost_per_candidate=estimated_cost if paid else 0.0,
        provider=provider if paid else "shadow",
        model=model if paid else "shadow_ai_committee/v0",
        risk_profile=risk_profile,
        reason=reason,
        candidates=[],
    )


async def _runtime_risk_profile(settings: Any, *, paper: bool) -> str:
    try:
        runtime_value = await get_runtime_config_value("risk_profile")
    except Exception:
        runtime_value = None
    return get_risk_profile(runtime_value or getattr(settings, "risk_profile", "conservative"), paper=paper).name


async def _chargeable_count(*, provider: str | None) -> int | None:
    try:
        return await count_ai_research_chargeable_attempts(provider=provider, today_utc=True)
    except Exception:
        return None


def _with_batch_context(
    intent: TradeIntent,
    *,
    account: dict[str, Any],
    clock: dict[str, Any],
    positions: list[dict[str, Any]],
    today_new_entries: int,
    max_new_positions_per_day: int,
) -> TradeIntent:
    return with_research_context(
        intent,
        {
            "risk": build_risk_research_context(
                account=account,
                clock=clock,
                positions=positions,
                today_new_entries=today_new_entries,
                max_new_positions_per_day=max_new_positions_per_day,
            )
        },
    )


async def _today_new_entries(settings: Any) -> int:
    local_now = datetime.now(ZoneInfo(str(getattr(settings, "report_timezone", "America/Los_Angeles"))))
    local_day_start = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
    return await count_entry_orders_since(local_day_start.astimezone(UTC).isoformat())


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a no-order AI gate rehearsal batch.")
    parser.add_argument("--limit", type=int, default=10, help="Number of candidates to rehearse, max 25.")
    parser.add_argument("--paid", action="store_true", help="Allow real paid provider calls within configured budget.")
    args = parser.parse_args()
    setup_logging("ERROR")
    result = asyncio.run(run_ai_rehearsal_batch(limit=args.limit, paid=args.paid))
    print(render_ai_rehearsal_batch(result))
    raise SystemExit(0 if result.ok else 2)


if __name__ == "__main__":
    main()
