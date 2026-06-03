"""Dry-run validation for account-level risk halt thresholds."""
from __future__ import annotations

import argparse
import asyncio
import io
import sqlite3
from contextlib import redirect_stdout
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import tempfile
from typing import Any
from zoneinfo import ZoneInfo

from auto_trader.config.settings import get_settings
from auto_trader.core.models import SystemState
from auto_trader.core.state_machine import StateMachine
from auto_trader.persistence.db import (
    configure_db_path,
    get_latest_journal_entries,
    init_db,
    load_system_state,
    save_system_state,
    update_account_risk_state,
)
from auto_trader.scheduler.trading_supervisor import TradingSupervisor, _week_start_date
from auto_trader.utils.logging import setup_logging


@dataclass(frozen=True)
class AccountRiskScenario:
    name: str
    equity: float
    day_start_equity: float
    week_start_equity: float
    peak_equity: float
    expected_halt: bool


@dataclass(frozen=True)
class AccountRiskDecision:
    scenario: AccountRiskScenario
    daily_loss_pct: float
    weekly_loss_pct: float
    peak_drawdown_pct: float
    breaches: list[str]

    @property
    def should_halt(self) -> bool:
        return bool(self.breaches)


@dataclass(frozen=True)
class ValidationGate:
    name: str
    status: str
    detail: str


def _pct(current: float, baseline: float) -> float:
    return ((current - baseline) / baseline * 100.0) if baseline else 0.0


def evaluate_account_risk_scenario(
    scenario: AccountRiskScenario,
    *,
    daily_loss_halt_pct: float,
    weekly_loss_halt_pct: float,
    peak_drawdown_halt_pct: float,
) -> AccountRiskDecision:
    """Evaluate halt thresholds without touching broker state."""
    daily_loss_pct = _pct(scenario.equity, scenario.day_start_equity)
    weekly_loss_pct = _pct(scenario.equity, scenario.week_start_equity)
    peak_drawdown_pct = _pct(scenario.equity, scenario.peak_equity)
    breaches: list[str] = []
    if daily_loss_pct <= daily_loss_halt_pct:
        breaches.append(f"daily loss {daily_loss_pct:.2f}% <= {daily_loss_halt_pct:.2f}%")
    if weekly_loss_pct <= weekly_loss_halt_pct:
        breaches.append(f"weekly loss {weekly_loss_pct:.2f}% <= {weekly_loss_halt_pct:.2f}%")
    if peak_drawdown_pct <= peak_drawdown_halt_pct:
        breaches.append(f"peak drawdown {peak_drawdown_pct:.2f}% <= {peak_drawdown_halt_pct:.2f}%")
    return AccountRiskDecision(
        scenario=scenario,
        daily_loss_pct=daily_loss_pct,
        weekly_loss_pct=weekly_loss_pct,
        peak_drawdown_pct=peak_drawdown_pct,
        breaches=breaches,
    )


def _default_scenarios(*, base_equity: float) -> list[AccountRiskScenario]:
    return [
        AccountRiskScenario(
            name="healthy",
            equity=base_equity,
            day_start_equity=base_equity,
            week_start_equity=base_equity,
            peak_equity=base_equity,
            expected_halt=False,
        ),
        AccountRiskScenario(
            name="daily-loss-breach",
            equity=base_equity * 0.98,
            day_start_equity=base_equity,
            week_start_equity=base_equity,
            peak_equity=base_equity,
            expected_halt=True,
        ),
        AccountRiskScenario(
            name="weekly-loss-breach",
            equity=base_equity * 0.95,
            day_start_equity=base_equity * 0.95,
            week_start_equity=base_equity,
            peak_equity=base_equity,
            expected_halt=True,
        ),
        AccountRiskScenario(
            name="peak-drawdown-breach",
            equity=base_equity,
            day_start_equity=base_equity,
            week_start_equity=base_equity,
            peak_equity=base_equity * 1.07,
            expected_halt=True,
        ),
    ]


def build_account_risk_validation_report(
    *,
    settings: Any,
    base_equity: float,
    scenarios: list[AccountRiskScenario] | None = None,
) -> tuple[str, list[ValidationGate]]:
    daily_threshold = float(settings.daily_loss_halt_pct)
    weekly_threshold = float(settings.weekly_loss_halt_pct)
    peak_threshold = float(settings.peak_drawdown_halt_pct)
    scenario_list = scenarios or _default_scenarios(base_equity=base_equity)
    gates: list[ValidationGate] = []
    lines = [
        "ACCOUNT RISK HALT VALIDATION",
        f"Base equity: ${base_equity:,.2f}",
        (
            "Thresholds: "
            f"daily {daily_threshold:.2f}%, "
            f"weekly {weekly_threshold:.2f}%, "
            f"peak drawdown {peak_threshold:.2f}%"
        ),
        "",
        "Scenarios:",
    ]
    for scenario in scenario_list:
        decision = evaluate_account_risk_scenario(
            scenario,
            daily_loss_halt_pct=daily_threshold,
            weekly_loss_halt_pct=weekly_threshold,
            peak_drawdown_halt_pct=peak_threshold,
        )
        status = "PASS" if decision.should_halt == scenario.expected_halt else "FAIL"
        expected = "HALT" if scenario.expected_halt else "ALLOW"
        actual = "HALT" if decision.should_halt else "ALLOW"
        detail = (
            f"expected={expected}, actual={actual}, "
            f"daily={decision.daily_loss_pct:.2f}%, "
            f"weekly={decision.weekly_loss_pct:.2f}%, "
            f"peak_drawdown={decision.peak_drawdown_pct:.2f}%"
        )
        if decision.breaches:
            detail += "; breaches: " + "; ".join(decision.breaches)
        gates.append(ValidationGate(name=scenario.name, status=status, detail=detail))
        lines.append(f"- [{status}] {scenario.name}: {detail}")
    overall = "FAIL" if any(gate.status == "FAIL" for gate in gates) else "PASS"
    lines.insert(2, f"Overall: {overall}")
    return "\n".join(lines), gates


def validation_exit_code(gates: list[ValidationGate]) -> int:
    return 2 if any(gate.status == "FAIL" for gate in gates) else 0


class _RehearsalAdapter:
    paper = True

    def __init__(self) -> None:
        self.cancel_calls = 0
        self.flatten_calls = 0

    async def cancel_all_orders(self) -> int:
        self.cancel_calls += 1
        return 1

    async def flatten_all_positions(self) -> int:
        self.flatten_calls += 1
        return 1


class _RehearsalOrderManager:
    pass


@dataclass(frozen=True)
class _SupervisorHaltRehearsalScenario:
    name: str
    equity: float
    day_start_equity: float
    week_start_equity: float
    peak_equity: float
    expected_breach: str


def _supervisor_rehearsal_scenarios(
    *,
    base_equity: float,
    daily_shock_pct: float,
) -> list[_SupervisorHaltRehearsalScenario]:
    daily_equity = base_equity * (1.0 + daily_shock_pct / 100.0)
    weekly_equity = base_equity * 0.95
    return [
        _SupervisorHaltRehearsalScenario(
            name="daily-loss",
            equity=daily_equity,
            day_start_equity=base_equity,
            week_start_equity=base_equity,
            peak_equity=base_equity,
            expected_breach="daily loss",
        ),
        _SupervisorHaltRehearsalScenario(
            name="weekly-loss",
            equity=weekly_equity,
            day_start_equity=weekly_equity,
            week_start_equity=base_equity,
            peak_equity=base_equity,
            expected_breach="weekly loss",
        ),
        _SupervisorHaltRehearsalScenario(
            name="peak-drawdown",
            equity=base_equity,
            day_start_equity=base_equity,
            week_start_equity=base_equity,
            peak_equity=base_equity * 1.07,
            expected_breach="peak drawdown",
        ),
    ]


def _override_rehearsal_baselines(
    *,
    db_path: Path,
    day_start_equity: float,
    week_start_equity: float,
    peak_equity: float,
) -> None:
    with sqlite3.connect(db_path) as db:
        db.execute(
            """
            UPDATE account_risk_state
            SET day_start_equity = ?, week_start_equity = ?, peak_equity = ?
            WHERE id = 1
            """,
            (day_start_equity, week_start_equity, peak_equity),
        )
        db.commit()


async def _run_supervisor_rehearsal_scenario(
    *,
    settings: Any,
    scenario: _SupervisorHaltRehearsalScenario,
) -> tuple[list[str], list[ValidationGate]]:
    gates: list[ValidationGate] = []
    lines = [f"{scenario.name}:"]

    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / f"{scenario.name}_account_risk_rehearsal.db"
        configure_db_path(db_path)
        await init_db()

        async def persist_state(state: SystemState, reason: str | None) -> None:
            await save_system_state(state, reason)

        timezone = ZoneInfo(getattr(settings, "report_timezone", "America/Los_Angeles"))
        local_now = datetime.now(timezone)
        day_date = local_now.date().isoformat()
        week_start = _week_start_date(local_now)
        await update_account_risk_state(
            equity=scenario.equity,
            day_date=day_date,
            week_start_date=week_start,
        )
        _override_rehearsal_baselines(
            db_path=db_path,
            day_start_equity=scenario.day_start_equity,
            week_start_equity=scenario.week_start_equity,
            peak_equity=scenario.peak_equity,
        )

        notifications: list[str] = []

        async def notify(message: str) -> None:
            notifications.append(message)

        state_machine = StateMachine(initial_state=SystemState.ACTIVE, persist_hook=persist_state)
        adapter = _RehearsalAdapter()
        supervisor = TradingSupervisor(
            settings=settings,
            state_machine=state_machine,
            adapter=adapter,  # type: ignore[arg-type]
            order_manager=_RehearsalOrderManager(),  # type: ignore[arg-type]
            notifier=notify,
        )
        metrics = await supervisor._enforce_account_risk_halts({"equity": scenario.equity})
        persisted_state, persisted_meta = await load_system_state()
        journal_entries = await get_latest_journal_entries(limit=5)
        halt_reason = str(persisted_meta.get("halt_reason", ""))

        lines.append(
            "  Metrics: "
            f"daily={metrics['daily_loss_pct']:.2f}%, "
            f"weekly={metrics['weekly_loss_pct']:.2f}%, "
            f"peak_drawdown={metrics['peak_drawdown_pct']:.2f}%"
        )
        checks = [
            (
                f"{scenario.name} expected breach",
                scenario.expected_breach in halt_reason,
                f"reason={persisted_meta.get('halt_reason')}",
            ),
            (
                f"{scenario.name} state halted",
                state_machine.state == SystemState.HALTED and persisted_state == SystemState.HALTED,
                f"memory={state_machine.state.value}, persisted={persisted_state.value}",
            ),
            (
                f"{scenario.name} halt reason persisted",
                "account risk halt" in halt_reason,
                f"reason={persisted_meta.get('halt_reason')}",
            ),
            (
                f"{scenario.name} cancel orders called",
                adapter.cancel_calls == 1,
                f"cancel_calls={adapter.cancel_calls}",
            ),
            (
                f"{scenario.name} flatten positions called",
                adapter.flatten_calls == 1,
                f"flatten_calls={adapter.flatten_calls}",
            ),
            (
                f"{scenario.name} notification emitted",
                any("ACCOUNT RISK HALT" in message for message in notifications),
                f"notifications={len(notifications)}",
            ),
            (
                f"{scenario.name} journal entry written",
                any("Account risk halt triggered" in str(entry.get("content", "")) for entry in journal_entries),
                f"journal_entries={len(journal_entries)}",
            ),
        ]
        for name, ok, detail in checks:
            status = "PASS" if ok else "FAIL"
            gates.append(ValidationGate(name=name, status=status, detail=detail))
            lines.append(f"  - [{status}] {name}: {detail}")

    return lines, gates


async def rehearse_supervisor_account_halt(
    *,
    settings: Any,
    base_equity: float,
    shock_pct: float = -2.0,
) -> tuple[str, list[ValidationGate]]:
    """Run the real supervisor account-halt path in a temp DB with a fake broker."""
    if shock_pct >= 0:
        raise ValueError("shock_pct must be negative")

    gates: list[ValidationGate] = []
    lines = [
        "SUPERVISOR ACCOUNT HALT REHEARSAL",
        f"Base equity: ${base_equity:,.2f}",
        f"Daily shock: {shock_pct:.2f}%",
        "",
        "Scenarios:",
    ]
    for scenario in _supervisor_rehearsal_scenarios(base_equity=base_equity, daily_shock_pct=shock_pct):
        scenario_lines, scenario_gates = await _run_supervisor_rehearsal_scenario(
            settings=settings,
            scenario=scenario,
        )
        lines.extend(scenario_lines)
        gates.extend(scenario_gates)

    overall = "FAIL" if any(gate.status == "FAIL" for gate in gates) else "PASS"
    lines.insert(1, f"Overall: {overall}")
    return "\n".join(lines), gates


def main() -> None:
    parser = argparse.ArgumentParser(description="Dry-run validate account risk halt thresholds.")
    parser.add_argument("--base-equity", type=float, default=400.0, help="Synthetic base equity for scenarios.")
    parser.add_argument(
        "--rehearse-supervisor-halt",
        action="store_true",
        help="Run a temp-DB rehearsal of the real supervisor halt/cancel/flatten path.",
    )
    parser.add_argument(
        "--shock-pct",
        type=float,
        default=-2.0,
        help="Synthetic daily-loss equity shock percentage for --rehearse-supervisor-halt.",
    )
    args = parser.parse_args()

    settings = get_settings()
    setup_logging("ERROR")
    if args.rehearse_supervisor_halt:
        with redirect_stdout(io.StringIO()):
            report, gates = asyncio.run(
                rehearse_supervisor_account_halt(
                    settings=settings,
                    base_equity=args.base_equity,
                    shock_pct=args.shock_pct,
                )
            )
    else:
        report, gates = build_account_risk_validation_report(settings=settings, base_equity=args.base_equity)
    print(report)
    raise SystemExit(validation_exit_code(gates))


if __name__ == "__main__":
    main()
