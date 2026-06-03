"""Live cutover go/no-go preflight and broker-safe halt drill."""
from __future__ import annotations

import argparse
import asyncio
import io
from contextlib import redirect_stdout
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
import subprocess
import tempfile
from typing import Any

from auto_trader.account_risk_validate import (
    ValidationGate,
    rehearse_supervisor_account_halt,
    validation_exit_code as account_risk_validation_exit_code,
)
from auto_trader.broker.alpaca_adapter import AlpacaAdapter
from auto_trader.config.settings import get_settings
from auto_trader.core.models import KillResult, SystemState
from auto_trader.core.state_machine import StateMachine
from auto_trader.persistence.db import (
    configure_db_path,
    get_pending_exits,
    get_runtime_config_values,
    init_db,
    load_system_state,
    save_system_state,
)
from auto_trader.utils.logging import setup_logging


@dataclass(frozen=True)
class HaltDrillReport:
    report: str
    gates: list[ValidationGate]


class _HaltDrillAdapter:
    paper = True

    def __init__(self) -> None:
        self.cancel_calls = 0
        self.flatten_calls = 0

    async def cancel_all_orders(self) -> int:
        self.cancel_calls += 1
        return 2

    async def flatten_all_positions(self) -> int:
        self.flatten_calls += 1
        return 1


def _gate(name: str, status: str, detail: str) -> ValidationGate:
    return ValidationGate(name=name, status=status, detail=detail)


def _float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _runtime_bool(values: dict[str, str], key: str) -> bool | None:
    raw = values.get(key)
    if raw is None:
        return None
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on", "enabled"}:
        return True
    if normalized in {"0", "false", "no", "off", "disabled"}:
        return False
    return None


def _runtime_int(values: dict[str, str], key: str) -> int | None:
    raw = values.get(key)
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _account_status_is_active(value: Any) -> bool:
    normalized = str(value or "").strip().lower()
    if "." in normalized:
        normalized = normalized.rsplit(".", 1)[-1]
    return normalized == "active"


async def rehearse_halt_drill() -> HaltDrillReport:
    """Prove HALTED persistence plus cancel/flatten wiring without touching Alpaca."""
    gates: list[ValidationGate] = []
    with tempfile.TemporaryDirectory() as tmp:
        configure_db_path(Path(tmp) / "live_preflight_halt_drill.db")
        await init_db()

        state_machine = StateMachine(initial_state=SystemState.ACTIVE, persist_hook=save_system_state)
        adapter = _HaltDrillAdapter()

        async def flatten() -> KillResult:
            cancelled = await adapter.cancel_all_orders()
            flattened = await adapter.flatten_all_positions()
            return KillResult(
                success=True,
                orders_cancelled=cancelled,
                positions_flattened=flattened,
                reason="live preflight halt drill",
                incident_report="BROKER-SAFE HALT DRILL",
                timestamp=datetime.now(UTC),
            )

        result = await state_machine.halt("live preflight halt drill", flatten_callback=flatten)
        persisted_state, persisted_meta = await load_system_state()
        checks = [
            (
                "drill state halted",
                state_machine.state == SystemState.HALTED and persisted_state == SystemState.HALTED,
                f"memory={state_machine.state.value}, persisted={persisted_state.value}",
            ),
            (
                "drill halt reason persisted",
                "live preflight halt drill" in str(persisted_meta.get("halt_reason", "")),
                f"reason={persisted_meta.get('halt_reason')}",
            ),
            (
                "drill cancel path called",
                adapter.cancel_calls == 1 and result.orders_cancelled == 2,
                f"cancel_calls={adapter.cancel_calls}, orders_cancelled={result.orders_cancelled}",
            ),
            (
                "drill flatten path called",
                adapter.flatten_calls == 1 and result.positions_flattened == 1,
                f"flatten_calls={adapter.flatten_calls}, positions_flattened={result.positions_flattened}",
            ),
            (
                "drill broker isolated",
                getattr(adapter, "paper", False) is True,
                "fake adapter only; no Alpaca client constructed",
            ),
        ]
        lines = [
            "LIVE PREFLIGHT HALT DRILL",
            "",
            "Gates:",
        ]
        for name, ok, detail in checks:
            status = "PASS" if ok else "FAIL"
            gates.append(_gate(name, status, detail))
            lines.append(f"- [{status}] {name}: {detail}")
        overall = "FAIL" if any(gate.status == "FAIL" for gate in gates) else "PASS"
        lines.insert(1, f"Overall: {overall}")
        return HaltDrillReport(report="\n".join(lines), gates=gates)


def _effective_runtime_int(
    runtime_config: dict[str, str],
    key: str,
    *,
    default: int,
) -> tuple[int | None, str]:
    runtime_value = _runtime_int(runtime_config, key)
    if runtime_value is not None:
        return runtime_value, f"runtime {key}={runtime_value}"
    if key in runtime_config:
        return None, f"runtime {key} invalid: {runtime_config[key]}"
    return int(default), f"env default {key}={default}"


def _active_service_pid(service_name: str) -> tuple[int | None, str]:
    try:
        proc = subprocess.run(
            ["systemctl", "show", "-p", "MainPID", "--value", service_name],
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except Exception as exc:
        return None, f"systemctl unavailable: {exc}"
    output = proc.stdout.strip()
    if proc.returncode != 0:
        detail = proc.stderr.strip() or output or f"systemctl exit {proc.returncode}"
        return None, detail
    try:
        pid = int(output)
    except (TypeError, ValueError):
        return None, f"invalid MainPID={output or 'empty'}"
    if pid <= 0:
        return None, f"inactive MainPID={pid}"
    return pid, f"systemd MainPID={pid}"


def build_live_preflight_report(
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
    account_risk_gates: list[ValidationGate],
    halt_drill_gates: list[ValidationGate],
    active_service_pid: int | None,
    service_pid_detail: str,
    max_equity: float,
    max_new_positions: int,
    allow_current_live: bool = False,
    allow_open_positions: bool = False,
    errors: list[str] | None = None,
) -> tuple[str, list[ValidationGate]]:
    snapshot_errors = errors or []
    gates: list[ValidationGate] = []
    alpaca_paper = bool(getattr(settings, "alpaca_paper", False))
    live_mode_ok = alpaca_paper or allow_current_live
    account_tradable = (
        account.get("status") == "CONNECTED"
        and _account_status_is_active(account.get("account_status"))
        and not account.get("trading_blocked")
        and not account.get("account_blocked")
    )
    equity = _float(account.get("equity"))
    runtime_auto_entry = _runtime_bool(runtime_config, "auto_entry_enabled")
    runtime_auto_exit = _runtime_bool(runtime_config, "auto_exit_enabled")
    runtime_max_entries, max_entries_detail = _effective_runtime_int(
        runtime_config,
        "max_new_positions_per_day",
        default=int(getattr(settings, "max_new_positions_per_day", 0)),
    )
    planned_capability = runtime_config.get("runtime_capability_planned_maintenance_shutdown") == "true"
    planned_pid = runtime_config.get("runtime_capability_planned_maintenance_pid")
    try:
        planned_pid_int = int(planned_pid) if planned_pid else None
    except (TypeError, ValueError):
        planned_pid_int = None
    planned_pid_matches = (
        planned_capability
        and planned_pid_int is not None
        and active_service_pid is not None
        and planned_pid_int == active_service_pid
    )

    gates.extend(
        [
            _gate(
                "current mode safe for preflight",
                "PASS" if live_mode_ok else "FAIL",
                (
                    f"ALPACA_PAPER={alpaca_paper}"
                    if alpaca_paper
                    else "already live; rerun only with --allow-current-live after reviewed cutover"
                ),
            ),
            _gate(
                "shutdown flatten enabled",
                "PASS" if getattr(settings, "shutdown_flatten_on_exit", False) else "FAIL",
                f"SHUTDOWN_FLATTEN_ON_EXIT={getattr(settings, 'shutdown_flatten_on_exit', None)}",
            ),
            _gate(
                "system state active",
                "PASS" if system_state == SystemState.ACTIVE else "FAIL",
                f"state={system_state.value}, halt_reason={system_meta.get('halt_reason')}",
            ),
            _gate(
                "broker account tradable",
                "PASS" if account_tradable else "FAIL",
                f"status={account.get('status')}, account_status={account.get('account_status')}, "
                f"trading_blocked={account.get('trading_blocked')}, account_blocked={account.get('account_blocked')}",
            ),
            _gate(
                "launch equity cap",
                "PASS" if 0 < equity <= max_equity else "FAIL",
                f"equity=${equity:.2f}, max=${max_equity:.2f}",
            ),
            _gate(
                "open positions clear",
                "PASS" if not positions or allow_open_positions else "FAIL",
                f"open_positions={len(positions)}, allow_open_positions={allow_open_positions}",
            ),
            _gate(
                "open orders clear",
                "PASS" if not open_orders else "FAIL",
                f"open_orders={len(open_orders)}",
            ),
            _gate(
                "pending exits clear",
                "PASS" if not pending_exits else "FAIL",
                f"pending_exits={len(pending_exits)}",
            ),
            _gate(
                "runtime max entries capped",
                "PASS" if runtime_max_entries is not None and 0 < runtime_max_entries <= max_new_positions else "FAIL",
                f"{max_entries_detail}, max_allowed={max_new_positions}",
            ),
            _gate(
                "auto-exit enabled",
                "PASS" if bool(runtime_auto_exit if runtime_auto_exit is not None else getattr(settings, "auto_exit_enabled", False)) else "FAIL",
                f"runtime={runtime_auto_exit}, env={getattr(settings, 'auto_exit_enabled', None)}",
            ),
            _gate(
                "auto-entry runtime intent set",
                "PASS" if runtime_auto_entry is not None else "FAIL",
                f"runtime={runtime_auto_entry}, env={getattr(settings, 'auto_entry_enabled', None)}",
            ),
            _gate(
                "planned deploy capability active",
                "PASS" if planned_pid_matches else "FAIL",
                (
                    f"planned_maintenance={planned_capability}, "
                    f"marker_pid={planned_pid or 'missing'}, "
                    f"active_pid={active_service_pid or 'unknown'}, "
                    f"{service_pid_detail}"
                ),
            ),
            _gate(
                "market clock reachable",
                "PASS" if clock.get("source") != "error" else "FAIL",
                f"is_open={clock.get('is_open')}, source={clock.get('source')}",
            ),
            _gate(
                "snapshot data available",
                "PASS" if not snapshot_errors else "FAIL",
                "; ".join(snapshot_errors) if snapshot_errors else "all required snapshots loaded",
            ),
            _gate(
                "account-risk rehearsal passed",
                "PASS" if account_risk_validation_exit_code(account_risk_gates) == 0 else "FAIL",
                f"gates={len(account_risk_gates)}",
            ),
            _gate(
                "halt drill passed",
                "PASS" if account_risk_validation_exit_code(halt_drill_gates) == 0 else "FAIL",
                f"gates={len(halt_drill_gates)}",
            ),
        ]
    )

    overall = "FAIL" if any(gate.status == "FAIL" for gate in gates) else "PASS"
    lines = [
        "LIVE CUTOVER PREFLIGHT",
        f"Overall: {overall}",
        f"Mode: {'paper' if alpaca_paper else 'live'}",
        f"Market open: {clock.get('is_open')}",
        "",
        "Gates:",
    ]
    lines.extend(f"- [{gate.status}] {gate.name}: {gate.detail}" for gate in gates)
    return "\n".join(lines), gates


async def run_live_preflight(
    *,
    max_equity: float,
    max_new_positions: int,
    allow_current_live: bool = False,
    allow_open_positions: bool = False,
    base_equity: float = 400.0,
    service_name: str = "auto-trader",
) -> tuple[str, list[ValidationGate]]:
    settings = get_settings()
    setup_logging("ERROR")
    configure_db_path(settings.db_path)
    await init_db()

    adapter = AlpacaAdapter(
        api_key=settings.alpaca_api_key,
        api_secret=settings.alpaca_api_secret,
        paper=settings.alpaca_paper,
    )
    errors: list[str] = []
    system_state, system_meta = await load_system_state()
    runtime_config = await get_runtime_config_values()
    active_service_pid, service_pid_detail = _active_service_pid(service_name)

    try:
        account = await adapter.get_account_snapshot()
    except Exception as exc:
        account = {"status": "ERROR", "error": str(exc), "equity": 0.0}
        errors.append(f"account unavailable: {exc}")

    try:
        clock = await adapter.get_clock()
    except Exception as exc:
        clock = {"source": "error", "is_open": None, "error": str(exc)}
        errors.append(f"market clock unavailable: {exc}")

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
        pending_exits = await get_pending_exits(limit=50)
    except Exception as exc:
        pending_exits = []
        errors.append(f"pending exits unavailable: {exc}")

    with redirect_stdout(io.StringIO()):
        account_risk_report, account_risk_gates = await rehearse_supervisor_account_halt(
            settings=settings,
            base_equity=base_equity,
        )
        halt_drill = await rehearse_halt_drill()
    report, gates = build_live_preflight_report(
        settings=settings,
        system_state=system_state,
        system_meta=system_meta,
        account=account,
        clock=clock,
        positions=positions,
        open_orders=open_orders,
        pending_exits=pending_exits,
        runtime_config=runtime_config,
        account_risk_gates=account_risk_gates,
        halt_drill_gates=halt_drill.gates,
        active_service_pid=active_service_pid,
        service_pid_detail=service_pid_detail,
        max_equity=max_equity,
        max_new_positions=max_new_positions,
        allow_current_live=allow_current_live,
        allow_open_positions=allow_open_positions,
        errors=errors,
    )
    sections = [
        report,
        "",
        "Embedded Account-Risk Rehearsal:",
        account_risk_report,
        "",
        "Embedded Halt Drill:",
        halt_drill.report,
    ]
    return "\n".join(sections), gates


def main() -> None:
    parser = argparse.ArgumentParser(description="Run live cutover go/no-go preflight.")
    parser.add_argument("--max-equity", type=float, default=500.0, help="Maximum account equity allowed for launch.")
    parser.add_argument(
        "--max-new-positions",
        type=int,
        default=3,
        help="Maximum effective new-position cap allowed for launch.",
    )
    parser.add_argument(
        "--base-equity",
        type=float,
        default=400.0,
        help="Synthetic base equity for embedded account-risk rehearsal.",
    )
    parser.add_argument(
        "--service-name",
        default="auto-trader",
        help="systemd service name whose active MainPID must match the runtime capability marker.",
    )
    parser.add_argument(
        "--allow-current-live",
        action="store_true",
        help="Allow running the preflight after a reviewed switch to ALPACA_PAPER=false.",
    )
    parser.add_argument(
        "--allow-open-positions",
        action="store_true",
        help="Allow open positions during a reviewed in-position live check.",
    )
    args = parser.parse_args()
    report, gates = asyncio.run(
        run_live_preflight(
            max_equity=args.max_equity,
            max_new_positions=args.max_new_positions,
            allow_current_live=args.allow_current_live,
            allow_open_positions=args.allow_open_positions,
            base_equity=args.base_equity,
            service_name=args.service_name,
        )
    )
    print(report)
    raise SystemExit(account_risk_validation_exit_code(gates))


if __name__ == "__main__":
    main()
