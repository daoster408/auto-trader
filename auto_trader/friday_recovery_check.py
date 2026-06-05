"""Read-only Friday recovery check after HALTED queued-flatten state."""
from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
from typing import Any

from auto_trader.broker.alpaca_adapter import AlpacaAdapter
from auto_trader.config.settings import get_settings
from auto_trader.core.models import SystemState
from auto_trader.persistence.db import configure_db_path, get_pending_exits, load_system_state
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


@dataclass(frozen=True)
class RecoveryGate:
    name: str
    status: str
    detail: str


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


def _order_id(order: dict[str, Any]) -> str:
    return str(order.get("broker_order_id") or order.get("client_order_id") or order.get("id") or "")


def _open_positions(positions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [position for position in positions if abs(_float(position.get("qty"))) > 0]


def _is_queued_close_for_position(order: dict[str, Any], position: dict[str, Any]) -> bool:
    if _symbol(order.get("symbol")) != _symbol(position.get("symbol")):
        return False
    if _status(order.get("status")) not in OPEN_ORDER_STATUSES:
        return False
    side = _status(order.get("side"))
    qty = _float(position.get("qty"))
    if qty > 0:
        return side == "sell"
    if qty < 0:
        return side in {"buy", "buy_to_cover"}
    return False


def _queued_close_orders(
    open_orders: list[dict[str, Any]],
    position: dict[str, Any],
) -> list[dict[str, Any]]:
    return [order for order in open_orders if _is_queued_close_for_position(order, position)]


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
    short_id = _order_id(order)[:8] or "unknown"
    return f"- {symbol}: {side} {qty:.6f}, {status}, id {short_id}"


def build_friday_recovery_report(
    *,
    settings: Any,
    system_state: SystemState,
    system_meta: dict[str, Any],
    account: dict[str, Any],
    clock: dict[str, Any],
    positions: list[dict[str, Any]],
    open_orders: list[dict[str, Any]],
    pending_exits: list[dict[str, Any]],
    errors: list[str] | None = None,
) -> tuple[str, list[RecoveryGate]]:
    """Build a deterministic operator report from read-only snapshots."""
    snapshot_errors = errors or []
    open_positions = _open_positions(positions)
    missing_close_symbols = [
        _symbol(position.get("symbol")) or "UNKNOWN"
        for position in open_positions
        if not _queued_close_orders(open_orders, position)
    ]
    queued_close_symbols = [
        _symbol(position.get("symbol")) or "UNKNOWN"
        for position in open_positions
        if _queued_close_orders(open_orders, position)
    ]

    account_tradable = (
        account.get("status") == "CONNECTED"
        and _account_status_is_active(account.get("account_status"))
        and not account.get("trading_blocked")
        and not account.get("account_blocked")
    )
    market_open = clock.get("is_open")
    stale_pending_exits = not open_positions and not open_orders and bool(pending_exits)

    gates: list[RecoveryGate] = [
        RecoveryGate(
            "paper mode",
            "PASS" if getattr(settings, "alpaca_paper", False) else "FAIL",
            f"ALPACA_PAPER={getattr(settings, 'alpaca_paper', None)}",
        ),
        RecoveryGate(
            "system halted until reviewed",
            "PASS" if system_state == SystemState.HALTED else "FAIL",
            f"state={system_state.value}, halt_reason={system_meta.get('halt_reason')}",
        ),
        RecoveryGate(
            "broker account tradable",
            "PASS" if account_tradable else "FAIL",
            f"status={account.get('status')}, account_status={account.get('account_status')}, "
            f"trading_blocked={account.get('trading_blocked')}, account_blocked={account.get('account_blocked')}",
        ),
        RecoveryGate(
            "market clock",
            "PASS" if market_open is True else "WARN",
            "market is open" if market_open is True else "market is closed; queued market sells may wait",
        ),
        RecoveryGate(
            "data availability",
            "PASS" if not snapshot_errors else "FAIL",
            "; ".join(snapshot_errors) if snapshot_errors else "all required snapshots loaded",
        ),
    ]

    if missing_close_symbols:
        gates.append(
            RecoveryGate(
                "queued flatten coverage",
                "FAIL",
                "open position(s) without queued close order: " + ", ".join(sorted(missing_close_symbols)),
            )
        )
    elif open_positions:
        gates.append(
            RecoveryGate(
                "queued flatten coverage",
                "PASS",
                "queued close order(s) visible for: " + ", ".join(sorted(queued_close_symbols)),
            )
        )
    else:
        gates.append(RecoveryGate("queued flatten coverage", "PASS", "no open positions require queued closes"))

    if stale_pending_exits:
        gates.append(
            RecoveryGate(
                "pending exits clear",
                "FAIL",
                f"{len(pending_exits)} stale pending exit marker(s) remain after flat/no open orders",
            )
        )
    elif pending_exits:
        gates.append(
            RecoveryGate(
                "pending exits clear",
                "WARN",
                f"{len(pending_exits)} pending exit marker(s) still visible while recovery is pending",
            )
        )
    else:
        gates.append(RecoveryGate("pending exits clear", "PASS", "no pending exit markers"))

    hard_failure = any(gate.status == "FAIL" for gate in gates)
    if hard_failure:
        recovery_state = "FAIL"
        resume_allowed = False
    elif open_positions:
        recovery_state = "WAITING_QUEUED_FLATTEN"
        resume_allowed = False
    elif open_orders:
        recovery_state = "WAITING_OPEN_ORDERS_CLEAR"
        resume_allowed = False
    elif market_open is not True:
        recovery_state = "WAITING_MARKET_OPEN"
        resume_allowed = False
    else:
        recovery_state = "READY_TO_RESUME"
        resume_allowed = True

    overall = "FAIL" if hard_failure else "WARN" if not resume_allowed else "PASS"
    lines = [
        "FRIDAY RECOVERY CHECK",
        "Read-only: no orders submitted, canceled, reconciled, or resumed.",
        f"Overall: {overall}",
        f"Recovery state: {recovery_state}",
        f"Resume allowed: {'YES' if resume_allowed else 'NO'}",
        f"Market open: {market_open}",
        f"Open positions: {len(open_positions)}",
        f"Open orders: {len(open_orders)}",
        f"Pending exits: {len(pending_exits)}",
        "",
        "Gates:",
    ]
    lines.extend(f"- [{gate.status}] {gate.name}: {gate.detail}" for gate in gates)
    lines.append("")
    lines.append("Positions:")
    if open_positions:
        lines.extend(_format_position(position) for position in open_positions)
    else:
        lines.append("- none")
    lines.append("Open orders:")
    if open_orders:
        lines.extend(_format_order(order) for order in open_orders)
    else:
        lines.append("- none")
    return "\n".join(lines), gates


def recovery_exit_code(gates: list[RecoveryGate], report: str) -> int:
    if any(gate.status == "FAIL" for gate in gates):
        return 2
    if "Resume allowed: YES" in report:
        return 0
    return 1


async def run_friday_recovery_check() -> tuple[str, list[RecoveryGate]]:
    settings = get_settings()
    setup_logging(settings.log_level)
    configure_db_path(settings.db_path)

    errors: list[str] = []
    system_state, system_meta = await load_system_state()
    adapter = AlpacaAdapter(
        api_key=settings.alpaca_api_key,
        api_secret=settings.alpaca_api_secret,
        paper=settings.alpaca_paper,
    )

    try:
        account = await adapter.get_account_snapshot()
    except Exception as exc:
        account = {"status": "ERROR", "error": str(exc)}
        errors.append(f"account unavailable: {exc}")

    try:
        clock = await adapter.get_clock()
    except Exception as exc:
        clock = {"is_open": None, "source": "error", "error": str(exc)}
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
        pending_exits = await get_pending_exits(limit=20)
    except Exception as exc:
        pending_exits = []
        errors.append(f"pending exits unavailable: {exc}")

    return build_friday_recovery_report(
        settings=settings,
        system_state=system_state,
        system_meta=system_meta,
        account=account,
        clock=clock,
        positions=positions,
        open_orders=open_orders,
        pending_exits=pending_exits,
        errors=errors,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Read-only Friday HALTED recovery check.")
    parser.parse_args()
    report, gates = asyncio.run(run_friday_recovery_check())
    print(report)
    raise SystemExit(recovery_exit_code(gates, report))


if __name__ == "__main__":
    main()
