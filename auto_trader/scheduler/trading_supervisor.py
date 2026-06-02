"""Continuous Day-2 supervisor: reconcile, monitor, exits, optional entries."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Awaitable, Callable
from zoneinfo import ZoneInfo

from auto_trader.broker.alpaca_adapter import AlpacaAdapter
from auto_trader.core.state_machine import StateMachine
from auto_trader.execution.order_manager import OrderManager
from auto_trader.intelligence.rules_fallback import get_simple_rules_signals
from auto_trader.persistence.db import (
    append_journal_entry,
    clear_pending_exit,
    count_entry_orders_since,
    get_latest_entry_order_for_symbol,
    get_pending_exit_for_symbol,
    get_pending_exit_symbols,
    reconcile_broker_orders,
    upsert_pending_exit,
    upsert_order_record,
)
from auto_trader.utils.logging import get_logger

NotifyFn = Callable[[str], Awaitable[None]]

log = get_logger("auto_trader.scheduler.trading_supervisor")


@dataclass(frozen=True)
class ExitDecision:
    symbol: str
    should_exit: bool
    reason: str
    metrics: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SupervisorTickResult:
    account: dict[str, Any] | None
    clock: dict[str, Any] | None
    positions: list[dict[str, Any]]
    reconciled_orders: int | None
    alerts: list[str]
    exit_decisions: list[ExitDecision]
    entry_result: dict[str, Any] | None
    errors: list[str]


def _float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _now_utc() -> str:
    return datetime.now(UTC).isoformat() + "Z"


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None


def _format_position_line(position: dict[str, Any]) -> str:
    symbol = str(position.get("symbol", "?")).upper()
    qty = _float(position.get("qty"))
    market_value = _float(position.get("market_value"))
    unrealized = _float(position.get("unrealized_pl"))
    cost_basis = _float(position.get("cost_basis"))
    pnl_pct = (unrealized / cost_basis * 100.0) if cost_basis else 0.0
    return f"{symbol}: qty {qty:.6f}, value ${market_value:,.2f}, P/L ${unrealized:,.2f} ({pnl_pct:.2f}%)"


def _is_terminal_order_status(status: Any) -> bool:
    return str(status or "").lower() in {"canceled", "cancelled", "filled", "rejected", "expired"}


def _is_failed_terminal_order_status(status: Any) -> bool:
    return str(status or "").lower() in {"canceled", "cancelled", "rejected", "expired"}


def _is_close_order_for_position(order: dict[str, Any], *, symbol: str, position_qty: float) -> bool:
    if str(order.get("symbol", "")).upper() != symbol.upper():
        return False
    if _is_terminal_order_status(order.get("status")):
        return False
    side = str(order.get("side", "")).lower()
    if position_qty > 0:
        return side == "sell"
    if position_qty < 0:
        return side in {"buy", "buy_to_cover"}
    return False


def _order_matches_pending_exit(order: dict[str, Any], pending: dict[str, Any]) -> bool:
    order_ids = {
        str(order.get("broker_order_id") or ""),
        str(order.get("client_order_id") or ""),
        str(order.get("id") or ""),
    }
    pending_ids = {
        str(pending.get("broker_order_id") or ""),
        str(pending.get("client_order_id") or ""),
    }
    return bool((order_ids - {""}) & (pending_ids - {""}))


class TradingSupervisor:
    """Small supervised trading loop for paper burn-in.

    It never opens entries unless `auto_entry_enabled` is true, and never executes
    exits unless `auto_exit_enabled` is true. In default mode it reconciles,
    monitors, evaluates, logs, and alerts only.
    """

    def __init__(
        self,
        *,
        settings,
        state_machine: StateMachine,
        adapter: AlpacaAdapter,
        order_manager: OrderManager,
        notifier: NotifyFn | None = None,
    ) -> None:
        self.settings = settings
        self.sm = state_machine
        self.adapter = adapter
        self.order_manager = order_manager
        self.notifier = notifier
        self._last_reconcile_at: datetime | None = None
        self._seen_alert_keys: set[str] = set()
        self._position_high_values: dict[str, float] = {}
        self._pending_exit_symbols: set[str] = set()

    async def _notify(self, message: str) -> None:
        log.warning("supervisor_alert", message=message)
        if self.notifier is None:
            return
        try:
            await self.notifier(message)
        except Exception as e:
            log.error("supervisor_notify_failed", error=str(e))

    async def _notify_once(self, key: str, message: str) -> None:
        if key in self._seen_alert_keys:
            return
        self._seen_alert_keys.add(key)
        await self._notify(message)

    async def reconcile_once(self, *, days: int | None = None) -> int:
        lookback = int(days if days is not None else self.settings.reconcile_lookback_days)
        recent_orders = await self.adapter.get_recent_orders(days=lookback)
        reconciled = await reconcile_broker_orders(recent_orders)
        await self._clear_failed_pending_exits_from_orders(recent_orders)
        self._last_reconcile_at = datetime.now(UTC)
        if reconciled != len(recent_orders):
            await self._notify_once(
                f"reconcile-incomplete-{self._last_reconcile_at.date()}",
                f"RECONCILIATION WARNING: persisted {reconciled}/{len(recent_orders)} broker orders at {_now_utc()}",
            )
        log.info("supervisor_reconciled", attempted=len(recent_orders), persisted=reconciled, lookback_days=lookback)
        return reconciled

    def evaluate_exit_rules(self, position: dict[str, Any]) -> ExitDecision:
        symbol = str(position.get("symbol", "")).upper()
        qty = abs(_float(position.get("qty")))
        if not symbol or qty <= 0:
            return ExitDecision(symbol=symbol or "UNKNOWN", should_exit=False, reason="no open quantity")

        unrealized = _float(position.get("unrealized_pl"))
        cost_basis = _float(position.get("cost_basis"))
        market_value = abs(_float(position.get("market_value")))
        if cost_basis <= 0:
            avg_entry = _float(position.get("avg_entry_price"))
            cost_basis = avg_entry * qty if avg_entry > 0 else market_value - unrealized
        pnl_pct = (unrealized / cost_basis * 100.0) if cost_basis else 0.0

        high_value = max(self._position_high_values.get(symbol, market_value), market_value)
        self._position_high_values[symbol] = high_value
        trailing_drawdown_pct = ((market_value - high_value) / high_value * 100.0) if high_value else 0.0

        metrics = {
            "qty": qty,
            "position_qty": _float(position.get("qty")),
            "market_value": market_value,
            "unrealized_pl": unrealized,
            "cost_basis": cost_basis,
            "pnl_pct": pnl_pct,
            "trailing_drawdown_pct": trailing_drawdown_pct,
            "entry_age_days": position.get("entry_age_days"),
        }

        max_loss_pct = float(self.settings.position_max_loss_pct)
        take_profit_pct = float(self.settings.position_take_profit_pct)
        trailing_stop_pct = float(self.settings.position_trailing_stop_pct)
        max_hold_days = int(self.settings.position_max_hold_days)
        if pnl_pct <= max_loss_pct:
            return ExitDecision(symbol=symbol, should_exit=True, reason="position max loss reached", metrics=metrics)
        if pnl_pct >= take_profit_pct:
            return ExitDecision(symbol=symbol, should_exit=True, reason="position take profit reached", metrics=metrics)
        if trailing_drawdown_pct <= -abs(trailing_stop_pct):
            return ExitDecision(symbol=symbol, should_exit=True, reason="position trailing stop reached", metrics=metrics)
        entry_age_days = position.get("entry_age_days")
        if entry_age_days is not None and float(entry_age_days) >= max_hold_days:
            return ExitDecision(symbol=symbol, should_exit=True, reason="position max hold reached", metrics=metrics)
        return ExitDecision(symbol=symbol, should_exit=False, reason="hold", metrics=metrics)

    async def _sync_persisted_pending_exits(self, open_symbols: set[str]) -> None:
        """Keep memory pending exits aligned with durable state after trusted position reads."""
        persisted = await get_pending_exit_symbols()
        for symbol in persisted - open_symbols:
            await clear_pending_exit(symbol)
        self._pending_exit_symbols.intersection_update(open_symbols)

    async def _find_open_close_order(
        self,
        decision: ExitDecision,
        *,
        pending_exit: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        position_qty = _float(decision.metrics.get("position_qty"))
        open_orders = await self.adapter.get_open_orders()
        for order in open_orders:
            if not _is_close_order_for_position(order, symbol=decision.symbol, position_qty=position_qty):
                continue
            if pending_exit is not None and not _order_matches_pending_exit(order, pending_exit):
                continue
            return order
        return None

    async def _clear_failed_pending_exits_from_orders(self, orders: list[dict[str, Any]]) -> None:
        """Clear pending exits whose broker close order is known failed/canceled."""
        for order in orders:
            if not _is_failed_terminal_order_status(order.get("status")):
                continue
            symbol = str(order.get("symbol", "")).upper()
            if not symbol:
                continue
            pending = await get_pending_exit_for_symbol(symbol)
            if not pending or not _order_matches_pending_exit(order, pending):
                continue
            if await clear_pending_exit(symbol):
                self._pending_exit_symbols.discard(symbol)
                await self._notify_once(
                    f"exit-pending-cleared-{symbol}-{order.get('status')}",
                    f"EXIT PENDING CLEARED: prior close for {symbol} is {order.get('status')}; supervisor may retry.",
                )

    async def _execute_exit_if_enabled(self, decision: ExitDecision) -> dict[str, Any] | None:
        if not decision.should_exit:
            return None
        if self.sm.state.value == "HALTED":
            await self._notify_once(
                f"exit-suppressed-halted-{decision.symbol}",
                f"EXIT SUPPRESSED: system is HALTED; supervisor will not close {decision.symbol}.",
            )
            return None
        if not self.settings.auto_exit_enabled:
            await self._notify_once(
                f"exit-dry-run-{decision.symbol}-{decision.reason}",
                f"EXIT SIGNAL (dry run): {decision.symbol} - {decision.reason} - {decision.metrics}",
            )
            return None
        if decision.symbol in self._pending_exit_symbols:
            await self._notify_once(
                f"exit-pending-{decision.symbol}",
                f"EXIT SUPPRESSED: close order already submitted for {decision.symbol}.",
            )
            return None
        persisted_pending = await get_pending_exit_for_symbol(decision.symbol)
        if persisted_pending:
            open_close_order = await self._find_open_close_order(decision, pending_exit=persisted_pending)
            if open_close_order:
                self._pending_exit_symbols.add(decision.symbol)
                await self._notify_once(
                    f"exit-pending-persisted-{decision.symbol}",
                    f"EXIT SUPPRESSED: persisted pending close exists for {decision.symbol}.",
                )
            else:
                await self._notify_once(
                    f"exit-pending-unresolved-{decision.symbol}",
                    (
                        f"EXIT NEEDS REVIEW: persisted pending close exists for {decision.symbol}, "
                        "but broker has no open close order. Pausing until operator review."
                    ),
                )
                if self.sm.can_trade():
                    self.sm.pause("persisted pending exit unresolved")
            return None
        try:
            open_close_order = await self._find_open_close_order(decision)
            if open_close_order:
                self._pending_exit_symbols.add(decision.symbol)
                persisted = await upsert_pending_exit(
                    decision.symbol,
                    open_close_order,
                    reason=decision.reason,
                )
                if not persisted:
                    await self._notify_once(
                        f"exit-pending-persistence-failed-{decision.symbol}",
                        (
                            f"EXIT WARNING: broker already has an open close order for {decision.symbol}, "
                            "but local pending-exit persistence failed."
                        ),
                    )
                    if self.sm.can_trade():
                        self.sm.pause("broker close order pending but local pending-exit persistence failed")
                await self._notify_once(
                    f"exit-pending-broker-{decision.symbol}",
                    f"EXIT SUPPRESSED: broker already has an open close order for {decision.symbol}.",
                )
                return None

            self._pending_exit_symbols.add(decision.symbol)
            order = await self.adapter.close_position(decision.symbol, reason=decision.reason)
            order_persisted = await upsert_order_record(order, rationale=decision.reason)
            pending_persisted = await upsert_pending_exit(decision.symbol, order, reason=decision.reason)
            if not order_persisted or not pending_persisted:
                await self._notify_once(
                    f"exit-persistence-failed-{decision.symbol}",
                    f"EXIT WARNING: broker close submitted for {decision.symbol}, but local persistence failed.",
                )
                if self.sm.can_trade():
                    self.sm.pause("exit order persistence failed")
            await append_journal_entry(
                content=(
                    f"Auto-exit submitted for {decision.symbol}: {decision.reason}. "
                    f"Order {order.get('broker_order_id') or order.get('id')}; "
                    f"qty {order.get('qty')}; status {order.get('status')}; "
                    f"metrics {decision.metrics}."
                )
            )
            await self._notify(f"EXIT SUBMITTED: {decision.symbol} - {decision.reason} - order {order.get('id')}")
            return order
        except Exception as e:
            self._pending_exit_symbols.discard(decision.symbol)
            await self._notify_once(
                f"exit-failed-{decision.symbol}-{decision.reason}",
                f"EXIT FAILED: {decision.symbol} - {decision.reason} - {e}",
            )
            raise

    async def _maybe_submit_entry(
        self,
        *,
        account: dict[str, Any],
        clock: dict[str, Any],
        positions: list[dict[str, Any]],
        today_new_entries: int,
    ) -> dict[str, Any] | None:
        if not self.settings.auto_entry_enabled:
            return None
        if not self.sm.can_trade():
            return None
        if not clock.get("is_open"):
            return None
        account_status = str(account.get("account_status", "")).lower()
        if (
            account.get("status") != "CONNECTED"
            or account.get("trading_blocked")
            or account.get("account_blocked")
            or "active" not in account_status
        ):
            await self._notify_once("entry-account-not-tradable", "ENTRY BLOCKED: Alpaca account is not tradable.")
            return None
        open_position_count = sum(1 for p in positions if abs(_float(p.get("qty"))) > 0)
        if open_position_count >= int(self.settings.max_new_positions_per_day):
            return None
        if today_new_entries >= int(self.settings.max_new_positions_per_day):
            return None

        signals = await get_simple_rules_signals(self.adapter, max_signals=1)
        if not signals:
            return None
        intent = signals[0]
        snapshot = type(
            "SupervisorSnapshot",
            (object,),
            {
                "equity": _float(account.get("equity")),
                "open_positions": positions,
                "today_new_entries": today_new_entries,
            },
        )()
        result = await self.order_manager.submit_trade_intent(intent, snapshot)
        if result.get("order"):
            await self._notify(f"ENTRY RESULT: {intent.symbol} - {result.get('risk_decision')} - {result.get('order')}")
        return result

    async def tick_once(self) -> SupervisorTickResult:
        alerts: list[str] = []
        errors: list[str] = []
        exit_decisions: list[ExitDecision] = []
        entry_result: dict[str, Any] | None = None
        account: dict[str, Any] | None = None
        clock: dict[str, Any] | None = None
        positions: list[dict[str, Any]] = []
        positions_snapshot_ok = False
        reconciled: int | None = None
        today_new_entries = 0

        try:
            account = await self.adapter.get_account_snapshot()
            if account.get("status") == "ERROR":
                errors.append(f"account unavailable: {account.get('error', 'ERROR')}")
        except Exception as e:
            errors.append(f"account unavailable: {e}")

        try:
            clock = await self.adapter.get_clock()
            if clock.get("source") == "error":
                errors.append(f"clock unavailable: {clock.get('error', 'ERROR')}")
        except Exception as e:
            errors.append(f"clock unavailable: {e}")

        try:
            now = datetime.now(UTC)
            due = self._last_reconcile_at is None or (
                now - self._last_reconcile_at
            ).total_seconds() >= float(self.settings.reconcile_interval_seconds)
            if due:
                reconciled = await self.reconcile_once()
        except Exception as e:
            errors.append(f"reconciliation failed: {e}")

        try:
            positions = await self.adapter.get_positions_snapshot(strict=True)
            positions_snapshot_ok = True
        except Exception as e:
            errors.append(f"positions unavailable: {e}")

        try:
            timezone = ZoneInfo(getattr(self.settings, "report_timezone", "America/Los_Angeles"))
            local_now = datetime.now(timezone)
            local_day_start = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
            today_new_entries = await count_entry_orders_since(local_day_start.astimezone(UTC).isoformat())
        except Exception as e:
            errors.append(f"durable entry count unavailable: {e}")

        if errors:
            alert = "SUPERVISOR WARNING: " + "; ".join(errors)
            alerts.append(alert)
            await self._notify_once(f"supervisor-errors-{datetime.now(UTC).date()}", alert)
        alerted_error_count = len(errors)

        open_positions = [p for p in positions if abs(_float(p.get("qty"))) > 0]
        open_symbols = {str(p.get("symbol", "")).upper() for p in open_positions}
        if positions_snapshot_ok:
            try:
                await self._sync_persisted_pending_exits(open_symbols)
            except Exception as e:
                errors.append(f"pending exit sync failed: {e}")
        if open_positions:
            lines = ["POSITION MONITOR:"]
            lines.extend(_format_position_line(p) for p in open_positions)
            alert = "\n".join(lines)
            alerts.append(alert)
            log.info("positions_monitored", count=len(open_positions), summary=alert)

        if self.sm.state.value == "HALTED" and open_positions:
            alert = "HALTED POSITION WARNING: broker still reports open positions after HALTED state."
            alerts.append(alert)
            await self._notify_once("halted-positions-open", alert)

        for position in open_positions:
            try:
                entry = await get_latest_entry_order_for_symbol(str(position.get("symbol", "")))
                entry_time = _parse_dt((entry or {}).get("submitted_at") or (entry or {}).get("filled_at"))
                if entry_time:
                    position["entry_age_days"] = (datetime.now(UTC) - entry_time).total_seconds() / 86400.0
                    position["entry_submitted_at"] = entry_time.isoformat()
            except Exception as e:
                errors.append(f"entry age unavailable for {position.get('symbol')}: {e}")
            decision = self.evaluate_exit_rules(position)
            exit_decisions.append(decision)
            try:
                await self._execute_exit_if_enabled(decision)
            except Exception as e:
                errors.append(f"exit execution failed for {decision.symbol}: {e}")

        if len(errors) > alerted_error_count:
            alert = "SUPERVISOR WARNING: " + "; ".join(errors)
            alerts.append(alert)
            await self._notify_once(f"supervisor-late-errors-{datetime.now(UTC).date()}", alert)

        if account is not None and clock is not None and not errors:
            try:
                entry_result = await self._maybe_submit_entry(
                    account=account,
                    clock=clock,
                    positions=positions,
                    today_new_entries=today_new_entries,
                )
            except Exception as e:
                errors.append(f"entry loop failed: {e}")
                await self._notify_once("entry-loop-failed", f"ENTRY LOOP FAILED: {e}")

        return SupervisorTickResult(
            account=account,
            clock=clock,
            positions=positions,
            reconciled_orders=reconciled,
            alerts=alerts,
            exit_decisions=exit_decisions,
            entry_result=entry_result,
            errors=errors,
        )

    async def run(self, stop_event: asyncio.Event, *, startup_delay_seconds: float = 5.0) -> None:
        if startup_delay_seconds > 0:
            await asyncio.sleep(startup_delay_seconds)
        await self._notify_once(
            "supervisor-started",
            (
                "TRADING SUPERVISOR STARTED: "
                f"auto_entry={self.settings.auto_entry_enabled}, "
                f"auto_exit={self.settings.auto_exit_enabled}, "
                f"monitor_interval={self.settings.position_monitor_interval_seconds}s"
            ),
        )
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(self.tick_once(), timeout=float(self.settings.supervisor_tick_timeout_seconds))
            except asyncio.TimeoutError:
                await self._notify_once("supervisor-timeout", "SUPERVISOR WARNING: tick timed out.")
            except Exception as e:
                await self._notify_once("supervisor-failed", f"SUPERVISOR WARNING: tick failed: {e}")
                log.exception("supervisor_tick_failed", error=str(e))
            try:
                await asyncio.wait_for(
                    stop_event.wait(),
                    timeout=float(self.settings.position_monitor_interval_seconds),
                )
            except asyncio.TimeoutError:
                pass
