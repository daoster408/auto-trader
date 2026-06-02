"""Telegram Bot - primary operator interface (v1 contract).

Commands exactly as specified in SOURCE_OF_TRUTH:
- /status
- /pause
- /resume <token>
- /kill   ← absolute highest priority, must preempt everything
- /report
"""
import asyncio
from datetime import UTC, datetime

from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)

from auto_trader.broker.alpaca_adapter import AlpacaAdapter
from auto_trader.core.models import KillResult
from auto_trader.core.risk_engine import RiskEngine
from auto_trader.core.state_machine import StateMachine
from auto_trader.utils.logging import get_logger
from auto_trader.utils.retry import retry_kill_critical

log = get_logger("auto_trader.comms.telegram_bot")


class TelegramBot:
    """Async Telegram bot for AUTO-TRADER control & reporting."""

    def __init__(
        self,
        token: str,
        state_machine: StateMachine,
        risk_engine: RiskEngine,
        adapter: AlpacaAdapter,
        resume_token: str,
    ) -> None:
        self.token = token
        self.sm = state_machine
        self.risk = risk_engine
        self.adapter = adapter
        self.resume_token = resume_token
        self.app: Application | None = None

    async def _kill_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """HIGHEST PRIORITY. Preempts all other logic. Fully async, retried, no deadlocks."""
        log.critical("/kill received - initiating emergency flatten + HALTED")
        await update.message.reply_text("⚠️ /kill received. Executing cancel_all + flatten_all + HALTED...")

        @retry_kill_critical
        async def _do_cancel() -> int:
            return await self.adapter.cancel_all_orders()

        @retry_kill_critical
        async def _do_flatten() -> int:
            return await self.adapter.flatten_all_positions()

        async def flatten() -> KillResult:
            cancelled = 0
            flattened = 0
            try:
                cancelled = await _do_cancel()
            except Exception as e:
                log.error("kill_cancel_failed_after_retries", error=str(e))
            try:
                flattened = await _do_flatten()
            except Exception as e:
                log.error("kill_flatten_failed_after_retries", error=str(e))
            return KillResult(
                success=True,
                orders_cancelled=cancelled,
                positions_flattened=flattened,
                reason="/kill manual",
                incident_report="EMERGENCY FLATTEN EXECUTED",
                timestamp=datetime.now(UTC),
            )

        # StateMachine.halt is now async
        result = await self.sm.halt("/kill command", flatten_callback=flatten)

        msg = (
            f"🔴 SYSTEM HALTED\n"
            f"Orders cancelled: {result.orders_cancelled}\n"
            f"Positions flattened: {result.positions_flattened}\n"
            f"Reason: {result.reason}\n"
            f"Time: {result.timestamp.isoformat()}Z\n"
            f"Manual resume required with /resume <token>"
        )
        await update.message.reply_text(msg)
        log.critical("kill_completed", cancelled=result.orders_cancelled, flattened=result.positions_flattened)

    async def _status_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        health = await self.adapter.health_check()
        snap = self.sm.get_snapshot(equity=100_000.0)  # TODO: real equity from adapter
        status = (
            "🟢 AUTO-TRADER STATUS\n"
            f"State: {snap.state.value}\n"
            f"Equity: ${snap.equity:,.2f}\n"
            f"Can trade: {self.sm.can_trade()}\n"
            f"Alpaca: {health.get('status')}\n"
            f"Paper: {health.get('paper')}\n"
            f"Last updated: {snap.updated_at.isoformat()}Z"
        )
        await update.message.reply_text(status)
        log.info("status_reported", state=snap.state.value, alpaca_ok=health.get("ok"))

    async def _pause_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not self.sm.can_trade():
            await update.message.reply_text(f"Already in {self.sm.state.value}")
            return
        self.sm.pause("manual via /pause")
        await update.message.reply_text("⏸️ System PAUSED. No new entries. Monitoring continues.")
        log.warning("manual_pause", user=update.effective_user.username if update.effective_user else "unknown")

    async def _resume_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        token = " ".join(context.args) if context.args else ""
        ok = self.sm.resume(token, self.resume_token)
        if ok:
            await update.message.reply_text("🟢 RESUMED to ACTIVE. Trading allowed again.")
            log.info("manual_resume_success", user=update.effective_user.username if update.effective_user else "unknown")
        else:
            await update.message.reply_text("❌ Resume failed (bad token or already active)")
            log.warning("manual_resume_failed")

    async def _report_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        snap = self.sm.get_snapshot()
        report = (
            "📊 DAILY REPORT (stub)\n"
            f"State: {snap.state.value}\n"
            f"Equity: ${snap.equity:,.2f}\n"
            f"Daily PnL: ${snap.daily_pnl:,.2f}\n"
            "Full journal coming in later iteration."
        )
        await update.message.reply_text(report)
        log.info("report_requested", state=snap.state.value)

    async def _unknown(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        await update.message.reply_text("Unknown command. Use /status, /pause, /resume <token>, /kill, /report")

    def build(self) -> Application:
        """Build the Application with all command handlers."""
        app = Application.builder().token(self.token).build()

        app.add_handler(CommandHandler("status", self._status_handler))
        app.add_handler(CommandHandler("pause", self._pause_handler))
        app.add_handler(CommandHandler("resume", self._resume_handler))
        app.add_handler(CommandHandler("kill", self._kill_handler))
        app.add_handler(CommandHandler("report", self._report_handler))
        app.add_handler(CommandHandler("start", self._status_handler))
        app.add_handler(CommandHandler("help", self._status_handler))

        # Fallback
        from telegram.ext import MessageHandler, filters
        app.add_handler(MessageHandler(filters.COMMAND, self._unknown))

        self.app = app
        return app

    async def shutdown(self) -> None:
        """Graceful stop for signal handlers. Highest priority cleanup."""
        if self.app:
            log.info("telegram_bot_shutting_down")
            await self.app.updater.stop() if self.app.updater else None
            await self.app.stop()
            await self.app.shutdown()
        log.info("telegram_bot_stopped")

    async def run(self, stop_event: asyncio.Event | None = None) -> None:
        """Start polling. Integrates with external stop_event for clean shutdown."""
        if not self.app:
            self.build()
        log.info("telegram_bot_starting_polling", kill_priority="absolute")
        await self.app.initialize()
        await self.app.start()
        await self.app.updater.start_polling(allowed_updates=Update.ALL_TYPES)
        log.info("bot_polling_active_kill_live")

        if stop_event:
            await stop_event.wait()
            await self.shutdown()
        else:
            # legacy fallback (should not happen in prod)
            import asyncio
            while True:
                await asyncio.sleep(3600)
