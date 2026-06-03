"""Dry-run validation for account-level risk halt thresholds."""
from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Any

from auto_trader.config.settings import get_settings
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


def main() -> None:
    parser = argparse.ArgumentParser(description="Dry-run validate account risk halt thresholds.")
    parser.add_argument("--base-equity", type=float, default=400.0, help="Synthetic base equity for scenarios.")
    args = parser.parse_args()

    settings = get_settings()
    setup_logging(settings.log_level)
    report, gates = build_account_risk_validation_report(settings=settings, base_equity=args.base_equity)
    print(report)
    raise SystemExit(validation_exit_code(gates))


if __name__ == "__main__":
    main()
