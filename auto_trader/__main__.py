"""AUTO-TRADER main entrypoint (Optimizer production hardening pass).

Role: Optimizer
Model: xai/grok-build-0.1
Date: 2026-06-01

Hardened:
- /kill is fully async + retried + highest priority (no deadlocks)
- HALTED state persisted via DB on every transition (survives restart)
- Real alpaca-py SDK on kill/health paths + tenacity
- Structured logging (UTC + model_tag)
- Graceful shutdown + signal handling integrated end-to-end
- Basic observability (health file + startup metrics)
- Fast ARM startup (lazy client, minimal top-level work)
- All risk/kill gates untouched and reinforced
"""
import asyncio
from contextlib import suppress
import fcntl
import hashlib
import os
import signal
import sys
from datetime import UTC, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from auto_trader.broker.alpaca_adapter import AlpacaAdapter
from auto_trader.comms.telegram_bot import TelegramBot
from auto_trader.config.settings import get_settings
from auto_trader.core.models import KillResult, SystemState
from auto_trader.core.risk_engine import RiskEngine
from auto_trader.core.state_machine import StateMachine
from auto_trader.execution.order_manager import OrderManager
from auto_trader.persistence.db import (
    consume_planned_maintenance_shutdown,
    init_db,
    load_system_state,
    save_system_state,
)
from auto_trader.scheduler.trading_supervisor import TradingSupervisor
from auto_trader.utils.logging import setup_logging, get_logger

log = get_logger("auto_trader.main")


def _acquire_single_instance_lock(db_path: str) -> tuple[Path, object]:
    """Prevent duplicate bot processes for the same persisted trading state."""
    resolved_db = Path(db_path).expanduser().resolve()
    digest = hashlib.sha256(str(resolved_db).encode("utf-8")).hexdigest()[:16]
    lock_path = Path("/tmp") / f"auto_trader_{digest}.lock"
    handle = lock_path.open("a+", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as e:
        handle.seek(0)
        existing = handle.read().strip()
        handle.close()
        detail = f" Existing holder: {existing}" if existing else ""
        raise RuntimeError(f"another auto-trader instance is already running for {resolved_db}.{detail}") from e
    handle.seek(0)
    handle.truncate()
    handle.write(f"pid={os.getpid()}\ndb_path={resolved_db}\nstarted_at={datetime.now(UTC).isoformat()}Z\n")
    handle.flush()
    return lock_path, handle


async def _persist_hook(state, reason: str | None) -> None:
    """Async persist for StateMachine - makes HALTED bulletproof across restarts."""
    await save_system_state(state, reason)


async def _touch_health_file(ok: bool = True) -> None:
    """Basic observability hook for external watchdog (cheap on ARM)."""
    path = Path("/tmp/auto_trader_healthy")
    try:
        if ok:
            path.write_text(datetime.now(UTC).isoformat())
        else:
            path.unlink(missing_ok=True)
    except Exception:
        pass  # never fail main on observability


async def _emergency_halt(
    state_machine: StateMachine,
    adapter: AlpacaAdapter,
    reason: str,
) -> None:
    """
    Dual-path kill entry point (called from both /kill command and OS signals).
    Best-effort: persist HALTED, cancel orders, flatten positions, even on partial failure.
    Uses shield + timeout so shutdown doesn't hang forever.
    """
    log.critical("emergency_halt_triggered", reason=reason)

    async def _do_kill() -> None:
        try:
            async def _flatten_both() -> KillResult:
                cancelled = await adapter.cancel_all_orders()
                flattened = await adapter.flatten_all_positions()
                return KillResult(
                    success=True,
                    orders_cancelled=cancelled,
                    positions_flattened=flattened,
                    reason=reason,
                    incident_report="EMERGENCY FLATTEN (signal/shutdown path)",
                    timestamp=datetime.now(UTC),
                )

            # This will set state=HALTED, persist first, then call flatten
            await state_machine.halt(reason, flatten_callback=_flatten_both)
        except Exception as e:
            log.exception("emergency_halt_inner_failed", error=str(e))

    try:
        # Protect with shield + 15s timeout so we don't block shutdown indefinitely
        await asyncio.wait_for(asyncio.shield(_do_kill()), timeout=15.0)
    except asyncio.TimeoutError:
        log.error("emergency_halt_timed_out")
    except Exception as e:
        log.exception("emergency_halt_outer_failed", error=str(e))


def _should_emergency_halt_on_shutdown(
    settings,
    state_machine: StateMachine,
    *,
    planned_maintenance: bool = False,
) -> bool:
    """Return whether process shutdown should force HALTED + flatten."""
    if planned_maintenance:
        log.warning(
            "shutdown_emergency_halt_skipped_for_planned_maintenance",
            state=state_machine.state.value,
            paper=getattr(settings, "alpaca_paper", None),
        )
        return False
    if not settings.shutdown_flatten_on_exit:
        log.warning(
            "shutdown_emergency_halt_skipped_by_config",
            state=state_machine.state.value,
        )
        return False
    return state_machine.state in {SystemState.ACTIVE, SystemState.PAUSED}


async def _handle_signal_shutdown(
    *,
    sig: int,
    settings,
    state_machine: StateMachine,
    adapter: AlpacaAdapter,
    stop_event: asyncio.Event,
    shutdown_context: dict[str, bool] | None = None,
) -> None:
    """Handle OS process-exit signals without affecting Telegram /kill semantics."""
    log.critical("signal_kill_initiated", sig=sig)
    planned_maintenance = False
    try:
        marker = await consume_planned_maintenance_shutdown(
            alpaca_paper=bool(getattr(settings, "alpaca_paper", False))
        )
        planned_maintenance = marker is not None
    except Exception as e:
        log.error("planned_maintenance_shutdown_check_failed", error=str(e))
    if shutdown_context is not None and planned_maintenance:
        shutdown_context["planned_maintenance"] = True
    if _should_emergency_halt_on_shutdown(
        settings,
        state_machine,
        planned_maintenance=planned_maintenance,
    ):
        await _emergency_halt(state_machine, adapter, f"signal_{sig}")
    else:
        log.warning(
            "signal_emergency_halt_skipped_by_config",
            sig=sig,
            state=state_machine.state.value,
            planned_maintenance=planned_maintenance,
        )
    stop_event.set()


async def main() -> None:
    settings = get_settings()
    setup_logging(
        level=settings.log_level,
        model_tag="optimizer/harden-2026-06-01",
        json_logs=os.getenv("LOG_JSON", "false").lower() == "true",
    )
    lock_path, instance_lock = _acquire_single_instance_lock(settings.db_path)
    log.info("single_instance_lock_acquired", path=str(lock_path))

    log.critical("=== AUTO-TRADER OPTIMIZER HARDENED START ===")
    log.info(
        "config_loaded",
        paper=settings.alpaca_paper,
        risk_per_trade_pct=settings.risk_per_trade_pct,
        hosting_target="oracle-arm-lowmem",
    )

    # Init DB + restore HALTED state if present (critical for persistence blocker)
    from auto_trader.persistence.db import configure_db_path
    configure_db_path(settings.db_path)
    await init_db()
    restored_state, meta = await load_system_state()
    log.info("state_restored", state=restored_state.value, meta=meta)

    # Core - wire real persist hook
    state_machine = StateMachine(
        initial_state=restored_state,
        persist_hook=_persist_hook,
    )
    risk_engine = RiskEngine(state_machine, settings)

    # Broker (real SDK now wired for kill paths)
    adapter = AlpacaAdapter(
        api_key=settings.alpaca_api_key,
        api_secret=settings.alpaca_api_secret,
        paper=settings.alpaca_paper,
    )
    order_manager = OrderManager(risk_engine, adapter)

    # Health + observability on startup
    health = await adapter.health_check()
    log.info("alpaca_startup_health", **{k: v for k, v in health.items() if k != "error"})
    await _touch_health_file(health.get("ok", False))
    if not health.get("ok"):
        log.error("alpaca_degraded_mode")

    # Telegram (kill now highest-priority async + retried)
    bot = TelegramBot(
        token=settings.telegram_bot_token,
        state_machine=state_machine,
        risk_engine=risk_engine,
        adapter=adapter,
        resume_token=settings.resume_token,
        allowed_ids=settings.telegram_allowed_ids,
    )
    bot.build()

    supervisor = TradingSupervisor(
        settings=settings,
        state_machine=state_machine,
        adapter=adapter,
        order_manager=order_manager,
        notifier=bot.send_alert,
    )

    # Graceful shutdown + dual-path kill (OS signals now also trigger real halt+flatten)
    stop_event = asyncio.Event()
    shutdown_context = {"planned_maintenance": False}

    def _signal_handler(sig: int) -> None:
        log.warning("signal_received", sig=sig, action="process_shutdown_requested")
        # Schedule the real kill path without blocking the signal handler
        asyncio.create_task(
            _handle_signal_shutdown(
                sig=sig,
                settings=settings,
                state_machine=state_machine,
                adapter=adapter,
                stop_event=stop_event,
                shutdown_context=shutdown_context,
            )
        )

    # Modern signal handling (works on ARM, avoids deprecated loop methods in some Pythons)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda s=sig: _signal_handler(s))

    # Periodic health touch (lightweight observability)
    async def _health_watcher():
        while not stop_event.is_set():
            await asyncio.sleep(30)
            await _touch_health_file(True)

    health_task = asyncio.create_task(_health_watcher())
    supervisor_task = asyncio.create_task(supervisor.run(stop_event))

    try:
        log.info(
            "starting_telegram_control_surface",
            commands="status,pause,resume,kill,report",
            kill_priority="absolute",
        )
        print("\n✅ AUTO-TRADER (OPTIMIZED) running on Oracle ARM target.")
        print("   /status | /pause | /resume <token> | /kill | /report")
        print("   /kill is bulletproof async + real SDK + retries + HALTED persists.\n")

        await bot.run(stop_event=stop_event)
    except Exception:
        log.exception("fatal_error_main_loop")
    finally:
        health_task.cancel()
        with suppress(asyncio.CancelledError):
            await health_task
        stop_event.set()
        try:
            await asyncio.wait_for(supervisor_task, timeout=5.0)
        except asyncio.TimeoutError:
            supervisor_task.cancel()
            with suppress(asyncio.CancelledError):
                await supervisor_task
        # Belt + suspenders: production exits force HALTED best-effort unless explicitly disabled for supervised local runs.
        if _should_emergency_halt_on_shutdown(
            settings,
            state_machine,
            planned_maintenance=shutdown_context["planned_maintenance"],
        ):
            await _emergency_halt(state_machine, adapter, "shutdown_without_explicit_kill")
        await bot.shutdown()
        await _touch_health_file(False)
        log.info("shutdown_complete", uptime_s="n/a")
        print("\nShutdown complete. State persisted.")


# === Convenience helper for first paper trade testing (once .env is set) ===
async def run_first_paper_trade_test():
    """
    One-shot function to test the full paper order path:
    rules_fallback → RiskEngine → OrderManager → real Alpaca paper order.

    Run manually after you have .env with paper keys:
        python -c "
        import asyncio
        from auto_trader.__main__ import run_first_paper_trade_test
        asyncio.run(run_first_paper_trade_test())
        "
    """
    from auto_trader.config.settings import get_settings
    from auto_trader.core.risk_engine import RiskEngine
    from auto_trader.core.state_machine import StateMachine
    from auto_trader.execution.order_manager import OrderManager
    from auto_trader.intelligence.rules_fallback import get_simple_rules_signals
    from auto_trader.persistence.db import (
        configure_db_path,
        count_entry_orders_since,
        init_db,
        load_system_state,
        reconcile_broker_orders,
        save_system_state,
    )

    settings = get_settings()
    setup_logging(settings.log_level)

    configure_db_path(settings.db_path)
    await init_db()
    restored_state, _ = await load_system_state()

    async def _test_persist_hook(state, reason=None):
        await save_system_state(state, reason)

    sm = StateMachine(initial_state=restored_state, persist_hook=_test_persist_hook)
    risk = RiskEngine(sm, settings)
    adapter = AlpacaAdapter(
        api_key=settings.alpaca_api_key,
        api_secret=settings.alpaca_api_secret,
        paper=settings.alpaca_paper,
    )

    order_mgr = OrderManager(risk, adapter)

    if not sm.can_trade():
        print(
            f"System state is {sm.state.value}; refusing one-shot order. "
            "Start the bot and use /resume <token> before testing paper orders."
        )
        return {"error": "system_not_active", "state": sm.state.value}

    clock = await adapter.get_clock()
    if not clock.get("is_open"):
        print("Market is not open; refusing one-shot paper order test.")
        return {"error": "market_closed", "clock": clock}

    account = await adapter.get_account_snapshot()
    account_status = str(account.get("account_status", "")).lower()
    if (
        account.get("status") != "CONNECTED"
        or account.get("trading_blocked")
        or account.get("account_blocked")
        or "active" not in account_status
    ):
        print("Alpaca account is not tradable; refusing paper order test.")
        return {"error": "account_not_tradable", "account": account}

    try:
        recent_orders = await adapter.get_recent_orders(days=2)
        reconciled_count = await reconcile_broker_orders(recent_orders)
        if reconciled_count != len(recent_orders):
            print("Broker reconciliation incomplete; refusing paper order test.")
            return {
                "error": "broker_reconciliation_incomplete",
                "attempted": len(recent_orders),
                "persisted": reconciled_count,
            }
        local_now = datetime.now(ZoneInfo(settings.report_timezone))
        local_day_start = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
        today_start_utc = local_day_start.astimezone(UTC).isoformat()
        today_new_entries = await count_entry_orders_since(today_start_utc)
    except Exception as e:
        print(f"Broker reconciliation/count failed; refusing paper order test: {e}")
        return {"error": "broker_reconciliation_or_count_failed", "detail": str(e)}

    try:
        positions = await adapter.get_positions_snapshot(strict=True)
    except Exception as e:
        print(f"Position snapshot failed; refusing paper order test: {e}")
        return {"error": "positions_snapshot_failed", "detail": str(e)}

    open_position_count = sum(1 for p in positions if abs(float(p.get("qty", 0) or 0)) > 0)
    if open_position_count >= settings.max_new_positions_per_day:
        print("Open position limit already reached; refusing paper order test.")
        return {
            "error": "open_position_limit_reached",
            "open_positions": positions,
            "today_new_entries": today_new_entries,
        }
    if today_new_entries >= settings.max_new_positions_per_day:
        print("Durable daily entry limit already reached; refusing paper order test.")
        return {
            "error": "daily_entry_limit_reached",
            "open_positions": positions,
            "today_new_entries": today_new_entries,
        }

    # Get simple rule-based signals
    try:
        signals = await get_simple_rules_signals(adapter, max_signals=1)
    except Exception as e:
        print(f"Market data discovery failed; refusing paper order test: {e}")
        return {"error": "market_data_discovery_failed", "detail": str(e)}
    if not signals:
        print("No signals generated.")
        return

    intent = signals[0]

    snapshot = type("obj", (object,), {
        "equity": float(account.get("equity", 0.0)),
        "open_positions": positions,
        "today_new_entries": today_new_entries,
    })()

    if snapshot.equity <= 0:
        print("No valid account equity from Alpaca; refusing paper order test.")
        return {"error": "missing_equity"}

    print(f"Attempting first paper trade on {intent.symbol} via risk gate...")
    result = await order_mgr.submit_trade_intent(intent, snapshot)
    print("Result:", result)
    return result


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nShutdown via KeyboardInterrupt.")
        sys.exit(0)
    except Exception as e:
        print(f"Fatal: {e}")
        sys.exit(1)
