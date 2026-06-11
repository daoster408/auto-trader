"""Read-only Week 2 launchpad report for operator readiness."""
from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from auto_trader.ai_postmortem_review import (
    default_ai_postmortem_path,
    postmortem_model_for_provider,
    selected_postmortem_providers,
)
from auto_trader.broker.alpaca_adapter import AlpacaAdapter
from auto_trader.brain_review import default_brain_guidance_path
from auto_trader.config.settings import get_settings
from auto_trader.core.models import SystemState
from auto_trader.core.risk_profile import get_risk_profile
from auto_trader.edge_report import default_scoreboard_memory_pack_path
from auto_trader.intelligence.ai_committee import selected_research_providers
from auto_trader.intelligence.brain_guidance import load_brain_guidance_context
from auto_trader.intelligence.scoreboard_memory import load_scoreboard_memory_context
from auto_trader.persistence.db import (
    configure_db_path,
    count_ai_postmortem_chargeable_attempts,
    count_ai_postmortem_escalation_chargeable_attempts,
    count_ai_research_chargeable_attempts,
    count_entry_orders_since,
    get_pending_exits,
    get_runtime_config_values,
    init_db,
    load_system_state,
)
from auto_trader.utils.logging import setup_logging


OPEN_ORDER_STATUSES = {
    "accepted",
    "new",
    "pending",
    "pending_new",
    "submitted",
    "held",
    "partially_filled",
}
MAX_POSTMORTEM_READINESS_BYTES = 32_000


@dataclass(frozen=True)
class LaunchpadGate:
    name: str
    status: str
    detail: str


@dataclass(frozen=True)
class CacheReadiness:
    label: str
    status: str
    path: Path
    generated_at: str | None = None
    detail: str | None = None


@dataclass(frozen=True)
class IntelligenceReadiness:
    fred_key_present: bool
    postmortem_providers: list[str]
    postmortem_models: dict[str, str]
    postmortem_budget_used: int | None
    postmortem_budget_max: int
    escalation_enabled: bool
    escalation_provider: str
    escalation_model: str
    escalation_budget_used: int | None
    escalation_budget_max: int
    caches: list[CacheReadiness]
    error: str | None = None


def _float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _status(value: Any) -> str:
    return str(value or "").lower()


def _symbol(value: Any) -> str:
    return str(value or "").upper()


def _account_status_is_active(value: Any) -> bool:
    normalized = str(value or "").strip().lower()
    if "." in normalized:
        normalized = normalized.rsplit(".", 1)[-1]
    return normalized == "active"


def _open_positions(positions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [position for position in positions if abs(_float(position.get("qty"))) > 0]


def _open_orders(orders: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [order for order in orders if _status(order.get("status")) in OPEN_ORDER_STATUSES]


def _format_position(position: dict[str, Any]) -> str:
    symbol = _symbol(position.get("symbol")) or "UNKNOWN"
    qty = _float(position.get("qty"))
    value = _float(position.get("market_value"))
    pnl = _float(position.get("unrealized_pl"))
    return f"- {symbol}: qty {qty:.6f}, value ${value:.2f}, unrealized P/L ${pnl:.2f}"


def _format_order(order: dict[str, Any]) -> str:
    symbol = _symbol(order.get("symbol")) or "UNKNOWN"
    side = _status(order.get("side")) or "unknown"
    qty = _float(order.get("qty"))
    status = order.get("status") or "unknown"
    raw_id = order.get("broker_order_id") or order.get("client_order_id") or order.get("id") or ""
    short_id = str(raw_id)[:8] or "unknown"
    return f"- {symbol}: {side} {qty:.6f}, {status}, id {short_id}"


def _today_start_utc(settings: Any) -> str:
    timezone = ZoneInfo(str(getattr(settings, "report_timezone", "America/Los_Angeles")))
    local_now = datetime.now(timezone)
    local_day_start = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
    return local_day_start.astimezone(UTC).isoformat()


def _cache_from_context(label: str, path: Path, context: dict[str, Any]) -> CacheReadiness:
    status = str(context.get("status") or "missing")
    rendered_status = "ready" if status == "loaded" else status
    detail = str(context.get("error") or "") or None
    return CacheReadiness(
        label=label,
        status=rendered_status,
        path=path,
        generated_at=str(context.get("generated_at") or "") or None,
        detail=detail,
    )


def _readiness_cache(
    path: Path,
    *,
    label: str,
    expected_kind: str,
    max_bytes: int = MAX_POSTMORTEM_READINESS_BYTES,
) -> CacheReadiness:
    try:
        if not path.exists():
            return CacheReadiness(label=label, status="missing", path=path, detail="cache file not found")
        size = path.stat().st_size
        if size > max_bytes:
            return CacheReadiness(
                label=label,
                status="oversized",
                path=path,
                detail=f"cache size {size} exceeds max {max_bytes}",
            )
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return CacheReadiness(label=label, status="malformed", path=path, detail="cache JSON is malformed")
    except Exception:
        return CacheReadiness(label=label, status="error", path=path, detail="cache read failed")
    if not isinstance(payload, dict):
        return CacheReadiness(label=label, status="invalid", path=path, detail="cache root is not an object")
    if payload.get("kind") != expected_kind:
        return CacheReadiness(
            label=label,
            status="invalid",
            path=path,
            generated_at=str(payload.get("generated_at") or "") or None,
            detail="unexpected cache kind",
        )
    detail = None
    if label == "AI postmortem":
        status = str(payload.get("status") or "unknown")
        paid_called = payload.get("paid_called")
        detail = f"status={status}, paid_called={paid_called}"
    return CacheReadiness(
        label=label,
        status="ready",
        path=path,
        generated_at=str(payload.get("generated_at") or "") or None,
        detail=detail,
    )


def build_intelligence_readiness(
    settings: Any,
    *,
    postmortem_budget_used: int | None = None,
    escalation_budget_used: int | None = None,
) -> IntelligenceReadiness:
    try:
        providers = selected_postmortem_providers(settings)
        models = {provider: postmortem_model_for_provider(settings, provider) for provider in providers}
        scoreboard_path = default_scoreboard_memory_pack_path(settings)
        guidance_path = default_brain_guidance_path(settings)
        caches = [
            _cache_from_context(
                "Scoreboard memory",
                scoreboard_path,
                load_scoreboard_memory_context(path=scoreboard_path),
            ),
            _cache_from_context(
                "Brain guidance",
                guidance_path,
                load_brain_guidance_context(path=guidance_path),
            ),
            _readiness_cache(
                default_ai_postmortem_path(settings),
                label="AI postmortem",
                expected_kind="ai_postmortem_pack",
            ),
        ]
        return IntelligenceReadiness(
            fred_key_present=bool(getattr(settings, "fred_api_key", None)),
            postmortem_providers=providers,
            postmortem_models=models,
            postmortem_budget_used=postmortem_budget_used,
            postmortem_budget_max=int(getattr(settings, "ai_postmortem_max_calls_per_day", 0) or 0),
            escalation_enabled=bool(getattr(settings, "ai_postmortem_escalation_enabled", False)),
            escalation_provider=str(getattr(settings, "ai_postmortem_escalation_provider", "") or ""),
            escalation_model=str(getattr(settings, "ai_postmortem_escalation_model", "") or ""),
            escalation_budget_used=escalation_budget_used,
            escalation_budget_max=int(getattr(settings, "ai_postmortem_escalation_max_calls_per_day", 0) or 0),
            caches=caches,
        )
    except Exception:
        return IntelligenceReadiness(
            fred_key_present=bool(getattr(settings, "fred_api_key", None)),
            postmortem_providers=[],
            postmortem_models={},
            postmortem_budget_used=postmortem_budget_used,
            postmortem_budget_max=int(getattr(settings, "ai_postmortem_max_calls_per_day", 0) or 0),
            escalation_enabled=bool(getattr(settings, "ai_postmortem_escalation_enabled", False)),
            escalation_provider=str(getattr(settings, "ai_postmortem_escalation_provider", "") or ""),
            escalation_model=str(getattr(settings, "ai_postmortem_escalation_model", "") or ""),
            escalation_budget_used=escalation_budget_used,
            escalation_budget_max=int(getattr(settings, "ai_postmortem_escalation_max_calls_per_day", 0) or 0),
            caches=[],
            error="readiness build failed",
        )


def _budget_text(used: int | None, maximum: int) -> str:
    return f"{'unavailable' if used is None else used} / {maximum}"


def _render_intelligence_readiness(readiness: IntelligenceReadiness) -> list[str]:
    providers = ", ".join(readiness.postmortem_providers) if readiness.postmortem_providers else "none"
    model_parts = [
        f"{provider}={readiness.postmortem_models.get(provider) or 'unset'}"
        for provider in readiness.postmortem_providers
    ]
    lines = [
        "Intelligence readiness:",
        f"- FRED macro key: {'present' if readiness.fred_key_present else 'missing'}",
        f"- Postmortem providers: {providers}",
    ]
    if model_parts:
        lines.append(f"- Postmortem models: {', '.join(model_parts)}")
    lines.append(f"- Postmortem budget: {_budget_text(readiness.postmortem_budget_used, readiness.postmortem_budget_max)}")
    escalation_state = "armed" if readiness.escalation_enabled else "off"
    escalation_label = readiness.escalation_provider or "unset"
    if readiness.escalation_model:
        escalation_label = f"{escalation_label}/{readiness.escalation_model}"
    lines.append(
        f"- Fable/escalation: {escalation_state}; {escalation_label}; "
        f"budget {_budget_text(readiness.escalation_budget_used, readiness.escalation_budget_max)}"
    )
    for cache in readiness.caches:
        generated = f", generated {cache.generated_at}" if cache.generated_at else ""
        detail = f", {cache.detail}" if cache.detail else ""
        lines.append(f"- {cache.label}: {cache.status}{generated}{detail}; path {cache.path}")
    if readiness.error:
        lines.append(f"- readiness error: {readiness.error}")
    return lines


def _runtime_bool(runtime_config: dict[str, str], key: str, default: bool) -> bool:
    raw = runtime_config.get(key)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _runtime_int(runtime_config: dict[str, str], key: str, default: int, *, minimum: int = 1, maximum: int) -> int:
    raw = runtime_config.get(key)
    if raw is None:
        return min(max(default, minimum), maximum)
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return min(max(default, minimum), maximum)
    return min(max(value, minimum), maximum)


def _what_happens_next(
    *,
    system_state: SystemState,
    account_tradable: bool,
    market_open: bool | None,
    open_positions: list[dict[str, Any]],
    open_orders: list[dict[str, Any]],
    runtime_auto_entry: bool,
    ai_gate_enabled: bool,
    today_new_entries: int | None,
    max_entries: int,
    errors: list[str],
) -> str:
    if errors:
        return "repair snapshot/API availability before trusting entry flow"
    if system_state == SystemState.HALTED:
        if open_positions:
            return "stay HALTED; monitor open positions/orders until recovery is intentionally handled"
        if open_orders:
            return "stay HALTED; wait for open orders to clear before resume"
        if market_open is not True:
            return "stay HALTED; market is closed, so resume waits for an intentional market-hours check"
        return "eligible for intentional resume after operator review"
    if not account_tradable:
        return "block trading; broker account is not tradable"
    if market_open is not True:
        return "monitor/reconcile only; regular market is closed"
    if not runtime_auto_entry:
        return "monitor exits only; runtime auto-entry is disabled"
    if today_new_entries is not None and today_new_entries >= max_entries:
        return "monitor exits only; daily entry cap is already reached"
    if open_positions:
        return "monitor open positions and evaluate exits; new entries depend on position limits"
    if ai_gate_enabled:
        return "discover candidates, run paid prefilter/AI gate, then RiskEngine if approved"
    return "discover candidates, then deterministic RiskEngine decides any paper entry"


def build_week2_launchpad_report(
    *,
    settings: Any,
    system_state: SystemState,
    system_meta: dict[str, Any],
    account: dict[str, Any],
    clock: dict[str, Any],
    positions: list[dict[str, Any]],
    open_orders: list[dict[str, Any]],
    pending_exits: list[dict[str, Any]],
    runtime_config: dict[str, str],
    today_new_entries: int | None,
    ai_calls_used: int | None,
    intelligence_readiness: IntelligenceReadiness | None = None,
    errors: list[str] | None = None,
) -> tuple[str, list[LaunchpadGate]]:
    snapshot_errors = errors or []
    paper = bool(getattr(settings, "alpaca_paper", True))
    profile = get_risk_profile(runtime_config.get("risk_profile") or getattr(settings, "risk_profile", "conservative"), paper=paper)
    max_entries = _runtime_int(
        runtime_config,
        "max_new_positions_per_day",
        int(getattr(settings, "max_new_positions_per_day", 1) or 1),
        maximum=profile.max_runtime_entries_paper if paper else int(getattr(settings, "max_new_positions_per_day", 1) or 1),
    )
    runtime_auto_entry = _runtime_bool(runtime_config, "auto_entry_enabled", bool(getattr(settings, "auto_entry_enabled", False)))
    ai_gate_enabled = _runtime_bool(runtime_config, "ai_entry_gate_enabled", bool(getattr(settings, "ai_entry_gate_enabled", False)))
    max_ai_calls = int(getattr(settings, "ai_research_max_calls_per_day", 0) or 0)
    providers = selected_research_providers(settings)
    open_positions = _open_positions(positions)
    active_open_orders = _open_orders(open_orders)
    market_open = clock.get("is_open")
    account_tradable = (
        account.get("status") == "CONNECTED"
        and _account_status_is_active(account.get("account_status"))
        and not account.get("trading_blocked")
        and not account.get("account_blocked")
    )
    resume_allowed = (
        system_state == SystemState.HALTED
        and paper
        and account_tradable
        and market_open is True
        and not open_positions
        and not active_open_orders
        and not pending_exits
        and not snapshot_errors
    )
    next_action = _what_happens_next(
        system_state=system_state,
        account_tradable=account_tradable,
        market_open=market_open,
        open_positions=open_positions,
        open_orders=active_open_orders,
        runtime_auto_entry=runtime_auto_entry,
        ai_gate_enabled=ai_gate_enabled,
        today_new_entries=today_new_entries,
        max_entries=max_entries,
        errors=snapshot_errors,
    )

    gates = [
        LaunchpadGate("oracle/broker snapshots", "PASS" if not snapshot_errors else "FAIL", "; ".join(snapshot_errors) if snapshot_errors else "all read-only snapshots loaded"),
        LaunchpadGate("paper mode", "PASS" if paper else "FAIL", f"ALPACA_PAPER={paper}"),
        LaunchpadGate("broker tradable", "PASS" if account_tradable else "FAIL", f"status={account.get('status')}, account_status={account.get('account_status')}"),
        LaunchpadGate("market clock", "PASS" if market_open is True else "WARN", "market is open" if market_open is True else "regular market is closed"),
        LaunchpadGate("resume allowed", "PASS" if resume_allowed else "WARN", "YES" if resume_allowed else "NO"),
    ]
    overall = "FAIL" if any(gate.status == "FAIL" for gate in gates) else "WARN" if any(gate.status == "WARN" for gate in gates) else "PASS"
    ai_used_text = "unavailable" if ai_calls_used is None else str(ai_calls_used)
    lines = [
        "WEEK 2 LAUNCHPAD",
        "Read-only: no orders submitted, canceled, reconciled, or resumed.",
        f"Overall: {overall}",
        f"Bot state: {system_state.value}",
        f"Halt reason: {system_meta.get('halt_reason') or 'n/a'}",
        f"Paper: {paper}",
        f"Market open: {market_open}",
        f"Account: {account.get('status')}, {account.get('account_status')}",
        f"Equity: ${_float(account.get('equity')):.2f}",
        f"Cash: ${_float(account.get('cash')):.2f}",
        f"Risk profile: {profile.name}",
        f"Runtime auto-entry: {runtime_auto_entry}",
        f"Runtime AI entry gate: {ai_gate_enabled}",
        f"AI providers: {', '.join(providers) if providers else 'none'}",
        f"AI paid budget: {ai_used_text} / {max_ai_calls}",
        f"Today new entries: {today_new_entries if today_new_entries is not None else 'unavailable'} / {max_entries}",
        f"Resume allowed: {'YES' if resume_allowed else 'NO'}",
        f"What happens next: {next_action}",
        "",
        "Gates:",
    ]
    lines.extend(f"- [{gate.status}] {gate.name}: {gate.detail}" for gate in gates)
    lines.append("")
    lines.append("Positions:")
    lines.extend(_format_position(position) for position in open_positions) if open_positions else lines.append("- none")
    lines.append("Open orders:")
    lines.extend(_format_order(order) for order in active_open_orders) if active_open_orders else lines.append("- none")
    lines.append("Pending exits:")
    if pending_exits:
        lines.extend(f"- {_symbol(item.get('symbol'))}: {item.get('status') or 'pending'}, reason {item.get('reason') or 'n/a'}" for item in pending_exits)
    else:
        lines.append("- none")
    if intelligence_readiness is not None:
        lines.append("")
        lines.extend(_render_intelligence_readiness(intelligence_readiness))
    return "\n".join(lines), gates


async def run_week2_launchpad() -> tuple[str, list[LaunchpadGate]]:
    settings = get_settings()
    setup_logging(settings.log_level)
    configure_db_path(settings.db_path)
    await init_db()

    errors: list[str] = []
    system_state, system_meta = await load_system_state()
    adapter = AlpacaAdapter(
        api_key=settings.alpaca_api_key,
        api_secret=settings.alpaca_api_secret,
        paper=settings.alpaca_paper,
    )

    try:
        account = await adapter.get_account_snapshot()
        if account.get("status") == "ERROR":
            errors.append(f"account unavailable: {account.get('error', 'account snapshot returned ERROR')}")
    except Exception as exc:
        account = {"status": "ERROR", "error": str(exc)}
        errors.append(f"account unavailable: {exc}")
    try:
        clock = await adapter.get_clock()
        if clock.get("source") == "error":
            errors.append(f"clock unavailable: {clock.get('error', 'market clock returned ERROR')}")
    except Exception as exc:
        clock = {"is_open": None, "source": "error", "error": str(exc)}
        errors.append(f"clock unavailable: {exc}")
    try:
        positions = await adapter.get_positions_snapshot(strict=True)
    except Exception as exc:
        positions = []
        errors.append(f"positions unavailable: {exc}")
    try:
        open_orders = await adapter.get_open_orders()
    except Exception as exc:
        open_orders = []
        errors.append(f"open orders unavailable: {exc}")
    try:
        pending_exits = await get_pending_exits(limit=20)
    except Exception as exc:
        pending_exits = []
        errors.append(f"pending exits unavailable: {exc}")
    try:
        runtime_config = await get_runtime_config_values()
    except Exception as exc:
        runtime_config = {}
        errors.append(f"runtime config unavailable: {exc}")
    try:
        today_new_entries = await count_entry_orders_since(_today_start_utc(settings))
    except Exception as exc:
        today_new_entries = None
        errors.append(f"entry count unavailable: {exc}")
    try:
        ai_calls_used = await count_ai_research_chargeable_attempts(provider=None, today_utc=True)
    except Exception as exc:
        ai_calls_used = None
        errors.append(f"AI budget count unavailable: {exc}")
    try:
        postmortem_calls_used = await count_ai_postmortem_chargeable_attempts(provider=None, today_utc=True)
    except Exception:
        postmortem_calls_used = None
    try:
        escalation_calls_used = await count_ai_postmortem_escalation_chargeable_attempts(provider=None, today_utc=True)
    except Exception:
        escalation_calls_used = None
    intelligence_readiness = build_intelligence_readiness(
        settings,
        postmortem_budget_used=postmortem_calls_used,
        escalation_budget_used=escalation_calls_used,
    )

    return build_week2_launchpad_report(
        settings=settings,
        system_state=system_state,
        system_meta=system_meta,
        account=account,
        clock=clock,
        positions=positions,
        open_orders=open_orders,
        pending_exits=pending_exits,
        runtime_config=runtime_config,
        today_new_entries=today_new_entries,
        ai_calls_used=ai_calls_used,
        intelligence_readiness=intelligence_readiness,
        errors=errors,
    )


def launchpad_exit_code(gates: list[LaunchpadGate]) -> int:
    if any(gate.status == "FAIL" for gate in gates):
        return 2
    if any(gate.status == "WARN" for gate in gates):
        return 1
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the read-only Week 2 launchpad report.")
    parser.parse_args()
    report, gates = asyncio.run(run_week2_launchpad())
    print(report)
    raise SystemExit(launchpad_exit_code(gates))


if __name__ == "__main__":
    main()
