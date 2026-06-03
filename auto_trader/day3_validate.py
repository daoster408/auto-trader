"""Read-only Day 3 market-open validation for the AMPX close lifecycle."""
from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
from typing import Any

from auto_trader.broker.alpaca_adapter import AlpacaAdapter
from auto_trader.config.settings import get_settings
from auto_trader.persistence.db import (
    configure_db_path,
    get_latest_order_records,
    get_pending_exits,
    init_db,
    reconcile_broker_orders,
)
from auto_trader.utils.logging import setup_logging


@dataclass(frozen=True)
class ValidationGate:
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


def _symbol(value: Any) -> str:
    return str(value or "").upper()


def _status(value: Any) -> str:
    return str(value or "").lower()


def _order_id(order: dict[str, Any]) -> str:
    return str(order.get("broker_order_id") or order.get("client_order_id") or order.get("id") or "")


def _is_failed_terminal_order(order: dict[str, Any]) -> bool:
    return _status(order.get("status")) in {"canceled", "cancelled", "rejected", "expired"}


def _is_close_order_for_symbol(order: dict[str, Any], symbol: str) -> bool:
    if _symbol(order.get("symbol")) != symbol:
        return False
    if _is_failed_terminal_order(order):
        return False
    return _status(order.get("side")) in {"sell", "buy_to_cover"}


def _dedupe_orders(orders: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[str, dict[str, Any]] = {}
    fallback_index = 0
    for order in orders:
        key = _order_id(order)
        if not key:
            fallback_index += 1
            key = f"unknown-{fallback_index}"
        deduped[key] = order
    return list(deduped.values())


def _matching_position(positions: list[dict[str, Any]], symbol: str) -> dict[str, Any] | None:
    for position in positions:
        if _symbol(position.get("symbol")) == symbol and abs(_float(position.get("qty"))) > 0:
            return position
    return None


def _gate(name: str, status: str, detail: str) -> ValidationGate:
    return ValidationGate(name=name, status=status, detail=detail)


def build_day3_validation_report(
    *,
    symbol: str,
    settings: Any,
    account: dict[str, Any],
    clock: dict[str, Any],
    positions: list[dict[str, Any]],
    broker_orders: list[dict[str, Any]],
    local_orders: list[dict[str, Any]],
    pending_exits: list[dict[str, Any]],
    reconciled_orders: int | None,
    errors: list[str] | None = None,
) -> tuple[str, list[ValidationGate]]:
    """Build a deterministic text report from broker/DB snapshots."""
    clean_symbol = symbol.upper()
    snapshot_errors = errors or []
    position = _matching_position(positions, clean_symbol)
    symbol_pending = [p for p in pending_exits if _symbol(p.get("symbol")) == clean_symbol]
    close_orders = [
        order for order in _dedupe_orders(broker_orders + local_orders)
        if _is_close_order_for_symbol(order, clean_symbol)
    ]
    close_statuses = ", ".join(
        f"{_order_id(order)[:8] or 'unknown'}:{order.get('status', 'unknown')}" for order in close_orders
    ) or "none"

    account_tradable = (
        account.get("status") == "CONNECTED"
        and "active" in str(account.get("account_status", "")).lower()
        and not account.get("trading_blocked")
        and not account.get("account_blocked")
    )
    market_open = clock.get("is_open")

    gates: list[ValidationGate] = [
        _gate(
            "paper mode",
            "PASS" if getattr(settings, "alpaca_paper", False) else "FAIL",
            f"ALPACA_PAPER={getattr(settings, 'alpaca_paper', None)}",
        ),
        _gate(
            "auto-entry stays off",
            "PASS" if not getattr(settings, "auto_entry_enabled", False) else "FAIL",
            f"AUTO_ENTRY_ENABLED={getattr(settings, 'auto_entry_enabled', None)}",
        ),
        _gate(
            "auto-exit enabled",
            "PASS" if getattr(settings, "auto_exit_enabled", False) else "FAIL",
            f"AUTO_EXIT_ENABLED={getattr(settings, 'auto_exit_enabled', None)}",
        ),
        _gate(
            "broker account tradable",
            "PASS" if account_tradable else "FAIL",
            f"status={account.get('status')}, account_status={account.get('account_status')}, "
            f"trading_blocked={account.get('trading_blocked')}, account_blocked={account.get('account_blocked')}",
        ),
        _gate(
            "market open",
            "PASS" if market_open is True else "WARN",
            "market is open" if market_open is True else "market is closed; accepted close may remain queued",
        ),
        _gate(
            "broker reconciliation",
            "PASS" if reconciled_orders is not None else "FAIL",
            f"orders reconciled={reconciled_orders if reconciled_orders is not None else 'unknown'}",
        ),
        _gate(
            "data availability",
            "PASS" if not snapshot_errors else "FAIL",
            "; ".join(snapshot_errors) if snapshot_errors else "all required snapshots loaded",
        ),
    ]

    if position:
        qty = _float(position.get("qty"))
        value = _float(position.get("market_value"))
        gates.append(_gate("position status", "WARN", f"{clean_symbol} still open: qty={qty:.6f}, value=${value:.2f}"))
    else:
        gates.append(_gate("position status", "PASS", f"{clean_symbol} position is gone"))

    if close_orders:
        gates.append(_gate("close order visible", "PASS", f"{len(close_orders)} close order(s): {close_statuses}"))
    else:
        gates.append(_gate("close order visible", "FAIL", f"no {clean_symbol} close order found"))

    gates.append(
        _gate(
            "duplicate close count",
            "PASS" if len(close_orders) <= 1 else "FAIL",
            f"{len(close_orders)} non-failed close order(s) found for {clean_symbol}",
        )
    )

    if position and symbol_pending:
        pending_id = _order_id(symbol_pending[0])[:8] or "unknown"
        gates.append(_gate("pending-exit marker", "PASS", f"pending exit is present while position is open: {pending_id}"))
    elif position and not symbol_pending:
        gates.append(_gate("pending-exit marker", "FAIL", "position is open but pending exit is missing"))
    elif not position and symbol_pending:
        pending_id = _order_id(symbol_pending[0])[:8] or "unknown"
        gates.append(_gate("pending-exit marker", "FAIL", f"position is gone but pending exit remains: {pending_id}"))
    else:
        gates.append(_gate("pending-exit marker", "PASS", "position is gone and pending exit is clear"))

    if position:
        lifecycle = "WAITING: close lifecycle is still pending"
    elif any(_status(order.get("status")) == "filled" for order in close_orders):
        lifecycle = "PASSED: close filled, position gone, pending marker clear"
    else:
        lifecycle = "CHECK: position gone without a filled close order in recent/local order snapshots"

    overall = "FAIL" if any(g.status == "FAIL" for g in gates) else "WARN" if any(g.status == "WARN" for g in gates) else "PASS"
    lines = [
        "DAY 3 MARKET-OPEN VALIDATION",
        f"Symbol: {clean_symbol}",
        f"Overall: {overall}",
        f"Lifecycle: {lifecycle}",
        "",
        "Gates:",
    ]
    lines.extend(f"- [{gate.status}] {gate.name}: {gate.detail}" for gate in gates)
    return "\n".join(lines), gates


def validation_exit_code(gates: list[ValidationGate]) -> int:
    return 2 if any(gate.status == "FAIL" for gate in gates) else 0


async def run_validation(symbol: str) -> tuple[str, list[ValidationGate]]:
    settings = get_settings()
    setup_logging(settings.log_level)
    configure_db_path(settings.db_path)
    await init_db()

    adapter = AlpacaAdapter(
        api_key=settings.alpaca_api_key,
        api_secret=settings.alpaca_api_secret,
        paper=settings.alpaca_paper,
    )

    errors: list[str] = []

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
        broker_orders = await adapter.get_recent_orders(days=7)
    except Exception as exc:
        broker_orders = []
        errors.append(f"broker orders unavailable: {exc}")

    try:
        reconciled = await reconcile_broker_orders(broker_orders)
    except Exception as exc:
        reconciled = None
        errors.append(f"broker reconciliation unavailable: {exc}")

    try:
        positions = await adapter.get_positions_snapshot(strict=True)
    except Exception as exc:
        positions = []
        errors.append(f"positions unavailable: {exc}")

    try:
        local_orders = await get_latest_order_records(limit=10)
    except Exception as exc:
        local_orders = []
        errors.append(f"local order records unavailable: {exc}")

    try:
        pending_exits = await get_pending_exits(limit=10)
    except Exception as exc:
        pending_exits = []
        errors.append(f"pending exits unavailable: {exc}")

    return build_day3_validation_report(
        symbol=symbol,
        settings=settings,
        account=account,
        clock=clock,
        positions=positions,
        broker_orders=broker_orders,
        local_orders=local_orders,
        pending_exits=pending_exits,
        reconciled_orders=reconciled,
        errors=errors,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate Day 3 AMPX close lifecycle readiness.")
    parser.add_argument("--symbol", default="AMPX", help="Symbol to validate. Default: AMPX")
    args = parser.parse_args()

    report, gates = asyncio.run(run_validation(args.symbol))
    print(report)
    raise SystemExit(validation_exit_code(gates))


if __name__ == "__main__":
    main()
