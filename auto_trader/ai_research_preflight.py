"""Read-only preflight for real AI research activation."""
from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import dataclass
from typing import Any
from urllib.request import Request, urlopen

from auto_trader.account_risk_validate import ValidationGate, validation_exit_code
from auto_trader.config.settings import get_settings
from auto_trader.intelligence.ai_committee import (
    model_for_provider,
    research_provider_timeout_seconds,
    selected_research_providers,
)
from auto_trader.persistence.db import (
    configure_db_path,
    count_ai_research_chargeable_attempts,
    get_runtime_config_bool,
    init_db,
)
from auto_trader.utils.logging import setup_logging


_PROVIDER_KEY_ATTRS = {
    "openai": "openai_api_key",
    "xai": "xai_api_key",
    "anthropic": "anthropic_api_key",
    "gemini": "gemini_api_key",
}


@dataclass(frozen=True)
class CostAssumptions:
    input_tokens_per_call: int
    output_tokens_per_call: int
    input_price_per_mtok: float
    output_price_per_mtok: float

    @property
    def estimated_cost_per_memo(self) -> float:
        input_cost = (self.input_tokens_per_call / 1_000_000.0) * self.input_price_per_mtok
        output_cost = (self.output_tokens_per_call / 1_000_000.0) * self.output_price_per_mtok
        return input_cost + output_cost


@dataclass(frozen=True)
class ProviderReadiness:
    provider: str
    model: str
    key_present: bool
    real_provider: bool
    model_availability: str = "not_checked"


@dataclass(frozen=True)
class AIResearchPreflightReport:
    ready: bool
    provider: str
    model: str
    key_present: bool
    ai_entry_gate_enabled: bool
    providers: list[ProviderReadiness]
    max_calls: int
    used_calls: int | None
    remaining_calls: int | None
    attempts_per_round: int
    timeout_seconds: float
    provider_timeout_seconds: dict[str, float]
    provider_timeout_budget_seconds: float
    sequential_timeout_budget_seconds: float
    supervisor_tick_timeout_seconds: float
    cost: CostAssumptions
    gates: list[ValidationGate]

    @property
    def estimated_daily_cost(self) -> float:
        return self.cost.estimated_cost_per_memo * max(0, self.max_calls)

    @property
    def estimated_cost_per_round(self) -> float:
        return self.cost.estimated_cost_per_memo * max(1, self.attempts_per_round)

    @property
    def remaining_rounds(self) -> int | None:
        if self.remaining_calls is None:
            return None
        return self.remaining_calls // max(1, self.attempts_per_round)


def _provider_key_present(settings: Any, provider: str) -> bool:
    attr = _PROVIDER_KEY_ATTRS.get(provider)
    if not attr:
        return False
    return bool(str(getattr(settings, attr, "") or "").strip())


def _gate(name: str, ok: bool, detail: str) -> ValidationGate:
    return ValidationGate(name=name, status="PASS" if ok else "FAIL", detail=detail)


def _warn_gate(name: str, ok: bool, detail: str) -> ValidationGate:
    return ValidationGate(name=name, status="PASS" if ok else "WARN", detail=detail)


def _cost_assumptions_from_settings(settings: Any) -> CostAssumptions:
    return CostAssumptions(
        input_tokens_per_call=int(getattr(settings, "ai_research_est_input_tokens", 15000) or 0),
        output_tokens_per_call=int(getattr(settings, "ai_research_est_output_tokens", 2000) or 0),
        input_price_per_mtok=float(getattr(settings, "ai_research_input_price_per_mtok", 5.0) or 0.0),
        output_price_per_mtok=float(getattr(settings, "ai_research_output_price_per_mtok", 25.0) or 0.0),
    )


def _xai_available_models(api_key: str, *, timeout_seconds: float) -> set[str]:
    request = Request(
        "https://api.x.ai/v1/models",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "User-Agent": "auto-trader/0.1",
        },
        method="GET",
    )
    with urlopen(request, timeout=timeout_seconds) as response:
        payload = json.loads(response.read().decode("utf-8"))
    return {str(row.get("id", "")).strip() for row in payload.get("data", []) if row.get("id")}


def _provider_model_availability(settings: Any, providers: list[str], timeout_seconds: float) -> dict[str, str]:
    """Read-only provider model checks that are cheap enough for operator preflight."""
    availability: dict[str, str] = {}
    if "xai" in providers:
        model = model_for_provider(settings, "xai")
        api_key = str(getattr(settings, "xai_api_key", "") or "").strip()
        if not model or not api_key:
            availability["xai"] = "not_checked"
        else:
            try:
                models = _xai_available_models(api_key, timeout_seconds=min(max(timeout_seconds, 1.0), 15.0))
            except Exception:
                availability["xai"] = "error"
            else:
                availability["xai"] = "available" if model in models else "unavailable"
    return availability


def build_ai_research_preflight_report(
    *,
    settings: Any,
    used_calls: int | None,
    cost: CostAssumptions | None = None,
    ai_entry_gate_enabled: bool | None = None,
    model_availability: dict[str, str] | None = None,
) -> AIResearchPreflightReport:
    """Build a zero-network readiness report for paid AI research."""
    providers = selected_research_providers(settings)
    provider = providers[0] if len(providers) == 1 else "multi"
    enabled = bool(getattr(settings, "ai_research_enabled", True))
    effective_ai_entry_gate_enabled = (
        bool(getattr(settings, "ai_entry_gate_enabled", False))
        if ai_entry_gate_enabled is None
        else bool(ai_entry_gate_enabled)
    )
    model_availability = model_availability or {}
    provider_reports = [
        ProviderReadiness(
            provider=name,
            model=model_for_provider(settings, name),
            key_present=False if name == "shadow" else _provider_key_present(settings, name),
            real_provider=name in _PROVIDER_KEY_ATTRS,
            model_availability=model_availability.get(name, "not_checked"),
        )
        for name in providers
    ]
    model = (
        provider_reports[0].model
        if len(provider_reports) == 1
        else ",".join(f"{row.provider}:{row.model or 'n/a'}" for row in provider_reports)
    )
    max_calls = int(getattr(settings, "ai_research_max_calls_per_day", 0) or 0)
    timeout_seconds = float(getattr(settings, "ai_research_timeout_seconds", 8.0) or 8.0)
    real_provider = all(row.real_provider for row in provider_reports)
    key_present = all(row.key_present for row in provider_reports)
    all_models_present = all(row.model for row in provider_reports)
    model_availability_ok = all(row.model_availability not in {"unavailable", "error"} for row in provider_reports)
    model_availability_detail = ", ".join(
        f"{row.provider}:{row.model or 'n/a'}={row.model_availability}" for row in provider_reports
    )
    attempts_per_round = len([row for row in provider_reports if row.real_provider])
    provider_timeout_seconds = {
        row.provider: research_provider_timeout_seconds(settings, row.provider)
        for row in provider_reports
        if row.real_provider
    }
    provider_timeout_budget_seconds = max(provider_timeout_seconds.values(), default=0.0)
    sequential_timeout_budget_seconds = sum(provider_timeout_seconds.values())
    supervisor_tick_timeout_seconds = float(getattr(settings, "supervisor_tick_timeout_seconds", 20) or 20)
    remaining_calls = max(0, max_calls - used_calls) if used_calls is not None else None
    cost = cost or _cost_assumptions_from_settings(settings)

    gates = [
        _gate("AI enabled", enabled, f"AI_RESEARCH_ENABLED={str(enabled).lower()}"),
        _gate("Real provider selected", real_provider, f"AI_RESEARCH_PROVIDER={provider}"),
        _gate("Explicit model", all_models_present, "all provider models are set" if all_models_present else model),
        _gate("Provider model available", model_availability_ok, model_availability_detail),
        _gate("Provider key present", key_present, "key_present=true" if key_present else "key_present=false"),
        _gate("Daily call budget", max_calls > 0, f"AI_RESEARCH_MAX_CALLS_PER_DAY={max_calls}"),
        _gate(
            "Budget count available",
            used_calls is not None,
            f"used_calls={used_calls}" if used_calls is not None else "budget count unavailable",
        ),
        _gate(
            "Calls remaining",
            remaining_calls is not None and remaining_calls >= max(1, attempts_per_round),
            f"remaining_calls={remaining_calls}" if remaining_calls is not None else "budget count unavailable",
        ),
        _gate("Timeout bounded", 1.0 <= timeout_seconds <= 15.0, f"timeout_seconds={timeout_seconds:g}"),
        _warn_gate(
            "Provider timeout budget",
            provider_timeout_budget_seconds <= supervisor_tick_timeout_seconds,
            (
                f"provider_timeouts={provider_timeout_seconds}, "
                f"parallel_budget={provider_timeout_budget_seconds:g}s, "
                f"sequential_sum={sequential_timeout_budget_seconds:g}s, "
                f"supervisor_tick_timeout={supervisor_tick_timeout_seconds:g}s"
            ),
        ),
    ]
    ready = not any(gate.status == "FAIL" for gate in gates)
    return AIResearchPreflightReport(
        ready=ready,
        provider=provider,
        model=model,
        key_present=key_present,
        ai_entry_gate_enabled=effective_ai_entry_gate_enabled,
        providers=provider_reports,
        max_calls=max_calls,
        used_calls=used_calls,
        remaining_calls=remaining_calls,
        attempts_per_round=attempts_per_round,
        timeout_seconds=timeout_seconds,
        provider_timeout_seconds=provider_timeout_seconds,
        provider_timeout_budget_seconds=provider_timeout_budget_seconds,
        sequential_timeout_budget_seconds=sequential_timeout_budget_seconds,
        supervisor_tick_timeout_seconds=supervisor_tick_timeout_seconds,
        cost=cost,
        gates=gates,
    )


def render_ai_research_preflight(report: AIResearchPreflightReport) -> str:
    state = "READY" if report.ready else "NOT_READY"
    used = "unavailable" if report.used_calls is None else str(report.used_calls)
    remaining = "unavailable" if report.remaining_calls is None else str(report.remaining_calls)
    rounds = "unavailable" if report.remaining_rounds is None else str(report.remaining_rounds)
    model = report.model or "n/a"
    lines = [
        "AI RESEARCH PREFLIGHT",
        f"State: {state}",
        f"Provider: {report.provider}",
        f"Model: {model}",
        f"Key present: {str(report.key_present).lower()}",
        f"AI entry gate enabled: {str(report.ai_entry_gate_enabled).lower()}",
        f"Chargeable daily calls: used {used} / max {report.max_calls}; remaining {remaining}",
        f"Chargeable calls per round: {report.attempts_per_round}",
        f"Full rounds remaining: {rounds}",
        f"Timeout: {report.timeout_seconds:g}s",
        (
            "Provider timeout budget: "
            f"parallel max {report.provider_timeout_budget_seconds:g}s, "
            f"sequential sum {report.sequential_timeout_budget_seconds:g}s vs "
            f"supervisor tick {report.supervisor_tick_timeout_seconds:g}s"
        ),
        (
            "Cost assumptions: "
            f"input_tokens={report.cost.input_tokens_per_call}, "
            f"output_tokens={report.cost.output_tokens_per_call}, "
            f"input_price_per_mtok=${report.cost.input_price_per_mtok:.2f}, "
            f"output_price_per_mtok=${report.cost.output_price_per_mtok:.2f}"
        ),
        f"Estimated cost per memo: ${report.cost.estimated_cost_per_memo:.4f}",
        f"Estimated cost per round: ${report.estimated_cost_per_round:.4f}",
        f"Estimated worst-case daily cost: ${report.estimated_daily_cost:.4f}",
    ]
    if len(report.providers) > 1:
        lines.append("Providers:")
        lines.extend(
            (
                f"- {row.provider}: model={row.model or 'n/a'}, "
                f"key_present={str(row.key_present).lower()}, "
                f"model_availability={row.model_availability}"
            )
            for row in report.providers
        )
    lines.append("Gates:")
    lines.extend(f"- [{gate.status}] {gate.name}: {gate.detail}" for gate in report.gates)
    return "\n".join(lines)


async def run_ai_research_preflight(settings: Any | None = None) -> tuple[str, list[ValidationGate]]:
    settings = settings or get_settings()
    configure_db_path(getattr(settings, "db_path", "auto_trader.db"))
    await init_db()
    providers = selected_research_providers(settings)
    budget_provider = providers[0] if len(providers) == 1 and providers[0] != "shadow" else None
    used_calls = await count_ai_research_chargeable_attempts(provider=budget_provider, today_utc=True)
    ai_entry_gate_enabled = await get_runtime_config_bool(
        "ai_entry_gate_enabled",
        default=bool(getattr(settings, "ai_entry_gate_enabled", False)),
    )
    model_availability = _provider_model_availability(
        settings,
        providers,
        float(getattr(settings, "ai_research_timeout_seconds", 8.0) or 8.0),
    )
    report = build_ai_research_preflight_report(
        settings=settings,
        used_calls=used_calls,
        ai_entry_gate_enabled=ai_entry_gate_enabled,
        model_availability=model_availability,
    )
    return render_ai_research_preflight(report), report.gates


def main() -> None:
    parser = argparse.ArgumentParser(description="Read-only preflight for real AI research activation.")
    parser.parse_args()
    setup_logging("ERROR")
    report, gates = asyncio.run(run_ai_research_preflight())
    print(report)
    raise SystemExit(validation_exit_code(gates))


if __name__ == "__main__":
    main()
