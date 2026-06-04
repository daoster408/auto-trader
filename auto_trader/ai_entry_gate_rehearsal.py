"""No-order rehearsal for the real AI entry gate path."""
from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from zoneinfo import ZoneInfo

from auto_trader.ai_research_preflight import _cost_assumptions_from_settings
from auto_trader.broker.alpaca_adapter import AlpacaAdapter
from auto_trader.config.settings import get_settings
from auto_trader.core.models import TradeIntent
from auto_trader.intelligence.ai_committee import (
    ResearchCommittee,
    ResearchMemo,
    create_research_committee,
    real_research_providers,
    research_committee_round,
    selected_research_providers,
)
from auto_trader.intelligence.finnhub_client import FinnhubClient
from auto_trader.intelligence.fred_client import FredClient
from auto_trader.intelligence.research_context import build_risk_research_context, with_research_context
from auto_trader.intelligence.rules_fallback import get_simple_rules_signals
from auto_trader.persistence.db import (
    configure_db_path,
    count_ai_research_chargeable_attempts,
    count_entry_orders_since,
    init_db,
    log_ai_research_memo,
)
from auto_trader.utils.logging import setup_logging


@dataclass(frozen=True)
class AIEntryGateRehearsalResult:
    ok: bool
    called_provider: bool
    would_continue_to_risk_engine: bool
    provider: str
    model: str
    symbol: str
    entry_price: float | None
    prompt_version: str
    validation_passed: bool | None
    verdict: str
    confidence: float | None
    input_hash_prefix: str
    memo_id: int | None
    used_before: int | None
    used_after: int | None
    max_calls: int
    attempts_needed: int
    estimated_cost_per_round: float
    reason: str
    missing_sections: list[str]
    macro_status: str
    provider_results: list[dict[str, Any]]

    @property
    def remaining_after(self) -> int | None:
        if self.used_after is None:
            return None
        return max(0, self.max_calls - self.used_after)


def render_ai_entry_gate_rehearsal(result: AIEntryGateRehearsalResult) -> str:
    if result.would_continue_to_risk_engine:
        state = "WOULD_CONTINUE_TO_RISKENGINE"
    elif result.ok:
        state = "WOULD_BLOCK_BEFORE_RISKENGINE"
    else:
        state = "NOT_RUN"
    used_before = "unavailable" if result.used_before is None else str(result.used_before)
    used_after = "unavailable" if result.used_after is None else str(result.used_after)
    remaining_after = "unavailable" if result.remaining_after is None else str(result.remaining_after)
    missing = ", ".join(result.missing_sections) if result.missing_sections else "none"
    lines = [
        "AI ENTRY GATE REHEARSAL",
        "No orders submitted. The execution stack is not invoked.",
        f"State: {state}",
        f"Reason: {result.reason}",
        f"Provider: {result.provider}",
        f"Model: {result.model or 'n/a'}",
        f"Symbol: {result.symbol or 'n/a'}",
        f"Entry price: {result.entry_price}",
        f"Prompt version: {result.prompt_version or 'n/a'}",
        f"Validation passed: {result.validation_passed}",
        f"Verdict: {result.verdict or 'n/a'}",
        f"Confidence: {result.confidence}",
        f"Input hash: {result.input_hash_prefix or 'n/a'}",
        f"Memo id: {result.memo_id}",
        f"Chargeable calls: before {used_before} / max {result.max_calls}; after {used_after}; remaining {remaining_after}",
        f"Chargeable calls needed: {result.attempts_needed}",
        f"Estimated cost per round: ${result.estimated_cost_per_round:.4f}",
        f"Missing research sections: {missing}",
        f"Macro context: {result.macro_status}",
    ]
    if result.provider_results:
        lines.append("Provider results:")
        lines.extend(
            (
                f"- {row.get('provider')}: validation={row.get('validation_passed')}, "
                f"verdict={row.get('verdict')}, confidence={row.get('confidence')}, "
                f"memo_id={row.get('memo_id')}"
            )
            for row in result.provider_results
        )
    return "\n".join(lines)


async def run_ai_entry_gate_rehearsal(
    *,
    settings: Any | None = None,
    adapter: Any | None = None,
    committee: ResearchCommittee | None = None,
) -> AIEntryGateRehearsalResult:
    settings = settings or get_settings()
    configure_db_path(getattr(settings, "db_path", "auto_trader.db"))
    await init_db()

    configured_providers = selected_research_providers(settings)
    provider = configured_providers[0] if len(configured_providers) == 1 else "multi"
    model = _model_label(settings, configured_providers)
    max_calls = int(getattr(settings, "ai_research_max_calls_per_day", 0) or 0)
    attempts_needed = len([name for name in configured_providers if name != "shadow"])
    cost_per_round = _cost_assumptions_from_settings(settings).estimated_cost_per_memo * max(1, attempts_needed)

    if configured_providers == ["shadow"]:
        return _not_run(provider, model, max_calls, attempts_needed, cost_per_round, "real provider is required")
    if not bool(getattr(settings, "ai_research_enabled", True)):
        return _not_run(provider, model, max_calls, attempts_needed, cost_per_round, "AI_RESEARCH_ENABLED must be true")
    if max_calls <= 0:
        return _not_run(
            provider,
            model,
            max_calls,
            attempts_needed,
            cost_per_round,
            "AI_RESEARCH_MAX_CALLS_PER_DAY must be positive",
        )

    budget_provider = configured_providers[0] if len(configured_providers) == 1 else None
    used_before = await count_ai_research_chargeable_attempts(provider=budget_provider, today_utc=True)
    if used_before is None:
        return _not_run(provider, model, max_calls, attempts_needed, cost_per_round, "chargeable budget count unavailable")
    if max(0, max_calls - used_before) < max(1, attempts_needed):
        return AIEntryGateRehearsalResult(
            ok=False,
            called_provider=False,
            would_continue_to_risk_engine=False,
            provider=provider,
            model=model,
            symbol="",
            entry_price=None,
            prompt_version="",
            validation_passed=None,
            verdict="",
            confidence=None,
            input_hash_prefix="",
            memo_id=None,
            used_before=used_before,
            used_after=used_before,
            max_calls=max_calls,
            attempts_needed=attempts_needed,
            estimated_cost_per_round=cost_per_round,
            reason="chargeable budget exhausted",
            missing_sections=[],
            macro_status="not evaluated",
            provider_results=[],
        )

    adapter = adapter or AlpacaAdapter(
        api_key=getattr(settings, "alpaca_api_key", ""),
        api_secret=getattr(settings, "alpaca_api_secret", ""),
        paper=bool(getattr(settings, "alpaca_paper", True)),
    )
    account = await adapter.get_account_snapshot()
    clock = await adapter.get_clock()
    positions = await adapter.get_positions_snapshot(strict=True)
    today_new_entries = await _today_new_entries(settings)
    finnhub_client = FinnhubClient(getattr(settings, "finnhub_api_key", None))
    fred_client = FredClient(getattr(settings, "fred_api_key", None))

    signals = await get_simple_rules_signals(
        adapter,
        max_signals=1,
        finnhub_client=finnhub_client,
        fred_client=fred_client,
    )
    if not signals:
        return _not_run(provider, model, max_calls, attempts_needed, cost_per_round, "no candidate generated")

    intent = _with_rehearsal_context(
        signals[0],
        account=account,
        clock=clock,
        positions=positions,
        today_new_entries=today_new_entries,
        max_new_positions_per_day=int(getattr(settings, "max_new_positions_per_day", 1) or 1),
    )
    context = ((intent.features or {}).get("research_context") or {}) if isinstance(intent.features, dict) else {}
    missing_sections = _missing_sections(context)
    macro_status = _macro_status(context)
    committee = committee or create_research_committee(settings)
    real_providers = real_research_providers(committee)
    if not real_providers:
        return _not_run(provider, model, max_calls, attempts_needed, cost_per_round, "real provider is required")

    try:
        research_round = await research_committee_round(committee, intent, signal_id=None)
        memo_ids, provider_results = await _persist_round(research_round.member_memos, research_round.aggregate_memo)
        memo = research_round.aggregate_memo
        memo_id = memo_ids.get((memo.provider, memo.prompt_version))
        used_after = await count_ai_research_chargeable_attempts(provider=budget_provider, today_utc=True)
        would_continue = memo.validation_passed and memo.verdict == "approve"
        return AIEntryGateRehearsalResult(
            ok=True,
            called_provider=True,
            would_continue_to_risk_engine=would_continue,
            provider=memo.provider,
            model=memo.model_tag,
            symbol=memo.symbol,
            entry_price=intent.entry_price,
            prompt_version=memo.prompt_version,
            validation_passed=memo.validation_passed,
            verdict=memo.verdict,
            confidence=memo.confidence,
            input_hash_prefix=memo.input_hash[:12],
            memo_id=memo_id,
            used_before=used_before,
            used_after=used_after if used_after is not None else used_before + max(1, attempts_needed),
            max_calls=max_calls,
            attempts_needed=max(1, attempts_needed),
            estimated_cost_per_round=cost_per_round,
            reason="valid approve would continue to deterministic risk checks"
            if would_continue
            else f"AI gate would block: {memo.verdict}",
            missing_sections=missing_sections,
            macro_status=macro_status,
            provider_results=provider_results,
        )
    except Exception as exc:
        memo_id = await log_ai_research_memo(
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
            memo={
                "source": "ai_entry_gate_rehearsal",
                "error": str(exc),
                "candidate": {"symbol": intent.symbol.upper(), "entry_price": intent.entry_price},
            },
        )
        used_after = await count_ai_research_chargeable_attempts(provider=budget_provider, today_utc=True)
        return AIEntryGateRehearsalResult(
            ok=True,
            called_provider=True,
            would_continue_to_risk_engine=False,
            provider=provider,
            model=model,
            symbol=intent.symbol.upper(),
            entry_price=intent.entry_price,
            prompt_version="ai_research_failure/v0",
            validation_passed=False,
            verdict="watch",
            confidence=None,
            input_hash_prefix="unavailable",
            memo_id=memo_id,
            used_before=used_before,
            used_after=used_after if used_after is not None else used_before + 1,
            max_calls=max_calls,
            attempts_needed=max(1, attempts_needed),
            estimated_cost_per_round=cost_per_round,
            reason=f"provider failed: {exc}",
            missing_sections=missing_sections,
            macro_status=macro_status,
            provider_results=[],
        )


def _with_rehearsal_context(
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


async def _persist_round(
    member_memos: list[ResearchMemo],
    aggregate_memo: ResearchMemo,
) -> tuple[dict[tuple[str, str], int], list[dict[str, Any]]]:
    memo_ids: dict[tuple[str, str], int] = {}
    provider_memo_ids = []
    for memo in member_memos:
        memo_id = await _log_memo(memo)
        memo_ids[(memo.provider, memo.prompt_version)] = memo_id
        provider_memo_ids.append(
            {
                "provider": memo.provider,
                "prompt_version": memo.prompt_version,
                "input_hash": memo.input_hash,
                "memo_id": memo_id,
            }
        )
    if aggregate_memo not in member_memos:
        aggregate_memo.memo["provider_memo_ids"] = provider_memo_ids
        memo_ids[(aggregate_memo.provider, aggregate_memo.prompt_version)] = await _log_memo(aggregate_memo)
    provider_results = [
        {
            "provider": memo.provider,
            "validation_passed": memo.validation_passed,
            "verdict": memo.verdict,
            "confidence": memo.confidence,
            "memo_id": memo_ids.get((memo.provider, memo.prompt_version)),
        }
        for memo in member_memos
    ]
    return memo_ids, provider_results


async def _log_memo(memo: ResearchMemo) -> int:
    return await log_ai_research_memo(
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


def _missing_sections(context: dict[str, Any]) -> list[str]:
    data_quality = context.get("data_quality") if isinstance(context, dict) else {}
    missing = data_quality.get("missing_sections") if isinstance(data_quality, dict) else []
    if not isinstance(missing, list):
        return []
    return [str(item) for item in missing]


def _macro_status(context: dict[str, Any]) -> str:
    macro = context.get("macro") if isinstance(context, dict) else {}
    if not isinstance(macro, dict) or not macro:
        return "missing"
    if macro.get("error"):
        return f"error: {macro.get('error')}"
    if macro.get("enabled") is False:
        return "disabled"
    series = macro.get("series")
    count = len(series) if isinstance(series, dict) else 0
    return f"enabled; series={count}"


def _model_label(settings: Any, providers: list[str]) -> str:
    if len(providers) == 1:
        return str(getattr(settings, "ai_research_model", "") or "").strip()
    return ",".join(f"{provider}:{_provider_model(settings, provider)}" for provider in providers)


def _provider_model(settings: Any, provider: str) -> str:
    attr = f"ai_research_{provider}_model"
    return str(getattr(settings, attr, "") or getattr(settings, "ai_research_model", "") or "").strip() or "n/a"


def _not_run(
    provider: str,
    model: str,
    max_calls: int,
    attempts_needed: int,
    cost_per_round: float,
    reason: str,
) -> AIEntryGateRehearsalResult:
    return AIEntryGateRehearsalResult(
        ok=False,
        called_provider=False,
        would_continue_to_risk_engine=False,
        provider=provider,
        model=model,
        symbol="",
        entry_price=None,
        prompt_version="",
        validation_passed=None,
        verdict="",
        confidence=None,
        input_hash_prefix="",
        memo_id=None,
        used_before=None,
        used_after=None,
        max_calls=max_calls,
        attempts_needed=max(1, attempts_needed),
        estimated_cost_per_round=cost_per_round,
        reason=reason,
        missing_sections=[],
        macro_status="not evaluated",
        provider_results=[],
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Run one no-order rehearsal of the AI entry gate.")
    parser.parse_args()
    setup_logging("ERROR")
    result = asyncio.run(run_ai_entry_gate_rehearsal())
    print(render_ai_entry_gate_rehearsal(result))
    raise SystemExit(0 if result.ok else 2)


if __name__ == "__main__":
    main()
