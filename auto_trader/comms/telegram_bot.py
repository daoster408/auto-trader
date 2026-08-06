"""Telegram Bot - primary operator interface (v1 contract).

Commands exactly as specified in SOURCE_OF_TRUTH:
- /status
- /pause
- /resume <token>
- /kill   ← absolute highest priority, must preempt everything
- /report
- /edge [days]
- /ai [symbol]
"""
import asyncio
from datetime import UTC, datetime
from typing import Any
from zoneinfo import ZoneInfo

from telegram import Update
from telegram.error import NetworkError, TimedOut
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)

from auto_trader.broker.alpaca_adapter import AlpacaAdapter
from auto_trader.core.models import KillResult, SystemState
from auto_trader.core.risk_engine import RiskEngine
from auto_trader.core.risk_profile import VALID_RISK_PROFILES, get_risk_profile
from auto_trader.core.state_machine import StateMachine
from auto_trader.edge_report import run_edge_report
from auto_trader.intelligence.ai_display import compact_ai_decision_line
from auto_trader.persistence.db import (
    append_journal_entry,
    count_entry_orders_since,
    get_latest_journal_entries,
    get_latest_order_records,
    get_pending_exits,
    get_runtime_config_bool,
    get_runtime_config_int,
    get_runtime_config_values,
    reconcile_broker_orders,
    read_latest_ai_research_memos,
    set_runtime_config_value,
)
from auto_trader.utils.logging import get_logger
from auto_trader.utils.retry import retry_kill_critical

log = get_logger("auto_trader.comms.telegram_bot")

MAX_EDGE_REPORT_DAYS = 90
EDGE_REPORT_TIMEOUT_SECONDS = 30.0
MAX_AI_DECISION_ROWS = 12
AI_DECISION_LOOKBACK_ROWS = 80
AI_DECISION_EXCLUDED_PROVIDERS = ("multi", "shadow", "prefilter")
TELEGRAM_POLLING_RETRY_INITIAL_SECONDS = 2.0
TELEGRAM_POLLING_RETRY_MAX_SECONDS = 60.0


def _money(value: Any) -> str:
    try:
        return f"${float(value):,.2f}"
    except (TypeError, ValueError):
        return "n/a"


def _number(value: Any, digits: int = 6) -> str:
    try:
        return f"{float(value):,.{digits}f}".rstrip("0").rstrip(".")
    except (TypeError, ValueError):
        return "n/a"


def _format_positions(positions: list[dict[str, Any]]) -> str:
    if not positions:
        return "Open positions: none"
    lines = ["Open positions:"]
    for pos in positions:
        symbol = str(pos.get("symbol", "?")).upper()
        qty = _number(pos.get("qty"))
        market_value = _money(pos.get("market_value"))
        unrealized = _money(pos.get("unrealized_pl"))
        lines.append(f"- {symbol}: qty {qty}, value {market_value}, unrealized P/L {unrealized}")
    return "\n".join(lines)


def _format_orders(orders: list[dict[str, Any]], *, title: str = "Latest orders") -> str:
    if not orders:
        return f"{title}: none"
    lines = [f"{title}:"]
    for order in orders:
        symbol = str(order.get("symbol", "?")).upper()
        status = str(order.get("status", "unknown"))
        qty = _number(order.get("filled_qty") or order.get("qty"))
        avg = _money(order.get("avg_fill_price"))
        broker_id = str(order.get("broker_order_id") or order.get("client_order_id") or "n/a")
        short_id = broker_id[:8] if broker_id != "n/a" else broker_id
        lines.append(f"- {symbol}: {status}, qty {qty}, avg {avg}, id {short_id}")
    return "\n".join(lines)


def _order_lookup_by_id(orders: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    lookup: dict[str, dict[str, Any]] = {}
    for order in orders:
        for key in ("broker_order_id", "client_order_id", "id"):
            value = str(order.get(key) or "")
            if value:
                lookup[value] = order
    return lookup


def _format_pending_exits(pending_exits: list[dict[str, Any]], orders: list[dict[str, Any]]) -> str:
    if not pending_exits:
        return "Pending exits: none"
    order_lookup = _order_lookup_by_id(orders)
    lines = ["Pending exits:"]
    for pending in pending_exits:
        symbol = str(pending.get("symbol", "?")).upper()
        broker_id = str(pending.get("broker_order_id") or pending.get("client_order_id") or "n/a")
        matching_order = order_lookup.get(broker_id) if broker_id != "n/a" else None
        status = str((matching_order or {}).get("status") or pending.get("status") or "pending")
        qty = _number(pending.get("qty"))
        reason = str(pending.get("reason") or "unspecified")
        short_id = broker_id[:8] if broker_id != "n/a" else broker_id
        lines.append(f"- {symbol}: {status}, qty {qty}, id {short_id}, reason {reason}; duplicate exits suppressed")
    return "\n".join(lines)


def _queued_close_order_warnings(
    *,
    state: str,
    positions: list[dict[str, Any]],
    orders: list[dict[str, Any]],
) -> list[str]:
    if state != "HALTED" or not positions:
        return []
    position_qty: dict[str, float] = {}
    for pos in positions:
        symbol = str(pos.get("symbol", "")).upper()
        try:
            qty = float(pos.get("qty") or 0.0)
        except (TypeError, ValueError):
            qty = 0.0
        if symbol and abs(qty) > 0:
            position_qty[symbol] = qty
    close_symbols: list[str] = []
    for order in orders:
        symbol = str(order.get("symbol", "")).upper()
        side = str(order.get("side", "")).lower()
        status = str(order.get("status", "")).lower()
        qty = position_qty.get(symbol)
        if qty is None or status not in {"accepted", "new", "pending_new", "pending"}:
            continue
        if (qty > 0 and side == "sell") or (qty < 0 and side in {"buy", "buy_to_cover"}):
            close_symbols.append(symbol)
    if not close_symbols:
        return ["HALTED with open positions and no queued close order detected"]
    symbols = ", ".join(sorted(set(close_symbols)))
    return [f"HALTED with queued close orders pending for: {symbols}"]


def _format_journal_entries(entries: list[dict[str, Any]]) -> str:
    if not entries:
        return "Journal: no entries yet"
    lines = ["Journal:"]
    for entry in entries:
        created = str(entry.get("created_at") or entry.get("date") or "unknown")
        content = " ".join(str(entry.get("content") or "").split())
        if len(content) > 180:
            content = content[:177] + "..."
        lines.append(f"- {created}: {content}")
    return "\n".join(lines)


def _format_ai_decision_rows(rows: list[dict[str, Any]], *, symbol: str | None = None) -> str:
    clean_symbol = str(symbol or "").strip().upper()
    filtered = [
        row
        for row in rows
        if str(row.get("provider") or "").lower() not in set(AI_DECISION_EXCLUDED_PROVIDERS)
        and (not clean_symbol or str(row.get("symbol") or "").upper() == clean_symbol)
    ][:MAX_AI_DECISION_ROWS]
    title = f"AI DECISIONS: {clean_symbol}" if clean_symbol else f"AI DECISIONS: latest {MAX_AI_DECISION_ROWS}"
    if not filtered:
        return f"{title}\nNo recent provider decisions found."
    lines = [title, "Read-only persisted memo view; newest first."]
    for row in filtered:
        line = compact_ai_decision_line(
            provider=row.get("provider"),
            verdict=row.get("verdict"),
            validation_passed=row.get("validation_passed"),
            prompt_version=row.get("prompt_version"),
            confidence=row.get("confidence"),
            symbol=row.get("symbol"),
            memo=row.get("memo") if isinstance(row.get("memo"), dict) else None,
            include_confidence=True,
        )
        age = _format_ai_decision_age(row.get("created_at"))
        lines.append(f"- {line}; {age}")
    text = "\n".join(lines)
    if len(text) > 3900:
        return text[:3850].rstrip() + "\n...truncated; use /ai SYMBOL."
    return text


def _format_ai_decision_age(created_at: Any) -> str:
    timestamp = _parse_sqlite_timestamp(created_at)
    if timestamp is None:
        return "age unknown"
    seconds = max(0, int((datetime.now(UTC) - timestamp).total_seconds()))
    if seconds < 120:
        return "just now"
    minutes = seconds // 60
    if minutes < 120:
        return f"{minutes}m ago"
    hours = minutes // 60
    if hours < 48:
        return f"{hours}h ago"
    return f"{hours // 24}d ago"


def _parse_sqlite_timestamp(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    for candidate in (text, text.replace(" ", "T")):
        try:
            parsed = datetime.fromisoformat(candidate.replace("Z", "+00:00"))
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
        except ValueError:
            continue
    return None


def _entry_status(
    state_can_trade: bool,
    auto_entry_enabled: bool,
    positions: list[dict[str, Any]],
    max_positions: int,
    today_new_entries: int | None,
    errors: list[Any],
    account_tradable: bool,
) -> str:
    if not auto_entry_enabled:
        return "disabled by runtime config"
    if not state_can_trade:
        return "blocked by system state"
    if errors:
        return "blocked by unavailable risk data"
    if not account_tradable:
        return "blocked by broker account status"
    open_position_count = sum(1 for p in positions if abs(float(p.get("qty") or 0.0)) > 0)
    if open_position_count >= max_positions:
        return "blocked by open-position limit"
    if today_new_entries is None:
        return "blocked by unavailable durable entry count"
    if today_new_entries >= max_positions:
        return "blocked by daily-entry limit"
    return "allowed"


def _runtime_int_from_values(values: dict[str, Any], key: str, *, default: int) -> int:
    value = values.get(key)
    if value is None:
        return int(default)
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


class TelegramBot:
    """Async Telegram bot for AUTO-TRADER control & reporting."""

    def __init__(
        self,
        token: str,
        state_machine: StateMachine,
        risk_engine: RiskEngine,
        adapter: AlpacaAdapter,
        resume_token: str,
        allowed_ids: str | list[int] | None = None,
    ) -> None:
        self.token = token
        self.sm = state_machine
        self.risk = risk_engine
        self.adapter = adapter
        self.resume_token = resume_token
        self.allowed_ids = self._parse_allowed_ids(allowed_ids)
        self.app: Application | None = None
        self._shutdown_lock = asyncio.Lock()
        self._shutdown_complete = False

    @staticmethod
    def _is_expected_lifecycle_runtime_error(error: RuntimeError) -> bool:
        message = str(error).lower()
        return "not running" in message or "not initialized" in message

    @staticmethod
    def _is_retryable_polling_error(error: BaseException) -> bool:
        return isinstance(error, (NetworkError, TimedOut, OSError, ConnectionError, TimeoutError))

    @staticmethod
    def _parse_allowed_ids(raw: str | list[int] | None) -> set[int]:
        if isinstance(raw, list):
            return {int(value) for value in raw}
        if not raw:
            return set()
        allowed: set[int] = set()
        for item in str(raw).split(","):
            item = item.strip()
            if not item:
                continue
            allowed.add(int(item))
        return allowed

    def _is_authorized(self, update: Update) -> bool:
        chat_id = update.effective_chat.id if update.effective_chat else None
        user_id = update.effective_user.id if update.effective_user else None
        if chat_id is not None and chat_id in self.allowed_ids:
            return True
        return chat_id is not None and user_id is not None and chat_id == user_id and user_id in self.allowed_ids

    async def _reject_unauthorized(self, update: Update, command: str) -> None:
        chat_id = update.effective_chat.id if update.effective_chat else None
        user_id = update.effective_user.id if update.effective_user else None
        log.critical("telegram_unauthorized_command", command=command, chat_id=chat_id, user_id=user_id)
        if update.message:
            await update.message.reply_text("Unauthorized.")

    async def _require_authorized(self, update: Update, command: str) -> bool:
        if self._is_authorized(update):
            return True
        await self._reject_unauthorized(update, command)
        return False

    async def send_alert(self, message: str) -> None:
        """Send an operator alert to configured allowlisted IDs."""
        if not self.allowed_ids:
            log.warning("telegram_alert_skipped_no_allowed_ids")
            return
        if not self.app:
            self.build()
        assert self.app is not None
        for chat_id in sorted(self.allowed_ids):
            try:
                await self.app.bot.send_message(chat_id=chat_id, text=message)
            except Exception as e:
                log.error("telegram_alert_send_failed", chat_id=chat_id, error=str(e))

    async def _reconcile_and_snapshot(self) -> dict[str, Any]:
        """Read-only operator snapshot. Never submits orders."""
        result: dict[str, Any] = {
            "health": {"paper": self.adapter.paper},
            "account": None,
            "positions": [],
            "orders": [],
            "broker_orders": [],
            "pending_exits": [],
            "journal_entries": [],
            "runtime_config": {},
            "reconciled": None,
            "today_new_entries": None,
            "errors": [],
        }

        try:
            account = await self.adapter.get_account_snapshot()
            result["account"] = account
            if account.get("status") == "ERROR":
                error = account.get("error", "account snapshot returned ERROR")
                result["errors"].append(f"account unavailable: {error}")
        except Exception as e:
            log.warning("telegram_account_failed", error=str(e))
            result["account"] = {"status": "ERROR"}
            result["errors"].append(f"account unavailable: {e}")

        try:
            clock = await self.adapter.get_clock()
            if clock.get("source") == "error":
                error = clock.get("error", "market clock returned ERROR")
                result["errors"].append(f"market clock unavailable: {error}")
            result["health"].update(
                {
                    "status": result["account"].get("status"),
                    "paper": self.adapter.paper,
                    "market_open": None if clock.get("source") == "error" else clock.get("is_open"),
                }
            )
        except Exception as e:
            log.warning("telegram_clock_failed", error=str(e))
            result["errors"].append(f"market clock unavailable: {e}")

        try:
            recent_orders = await self.adapter.get_recent_orders(days=7)
            result["broker_orders"] = recent_orders
            result["reconciled"] = await reconcile_broker_orders(recent_orders)
        except Exception as e:
            log.warning("telegram_reconciliation_failed", error=str(e))
            result["errors"].append(f"reconciliation failed: {e}")

        try:
            timezone = ZoneInfo(getattr(self.risk.settings, "report_timezone", "America/Los_Angeles"))
            local_now = datetime.now(timezone)
            local_day_start = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
            result["today_new_entries"] = await count_entry_orders_since(local_day_start.astimezone(UTC).isoformat())
        except Exception as e:
            log.warning("telegram_entry_count_failed", error=str(e))
            result["errors"].append(f"durable entry count unavailable: {e}")

        try:
            result["positions"] = await self.adapter.get_positions_snapshot(strict=True)
        except Exception as e:
            log.warning("telegram_positions_failed", error=str(e))
            result["errors"].append(f"positions unavailable: {e}")

        try:
            result["orders"] = await get_latest_order_records(limit=3)
        except Exception as e:
            log.warning("telegram_orders_failed", error=str(e))
            result["errors"].append(f"orders unavailable: {e}")

        try:
            result["pending_exits"] = await get_pending_exits(limit=5)
        except Exception as e:
            log.warning("telegram_pending_exits_failed", error=str(e))
            result["errors"].append(f"pending exits unavailable: {e}")

        try:
            result["journal_entries"] = await get_latest_journal_entries(limit=3)
        except Exception as e:
            log.warning("telegram_journal_failed", error=str(e))
            result["errors"].append(f"journal unavailable: {e}")

        try:
            result["runtime_config"] = await get_runtime_config_values()
        except Exception as e:
            log.warning("telegram_runtime_config_failed", error=str(e))
            result["errors"].append(f"runtime config unavailable: {e}")

        return result

    async def _bounded_snapshot(self) -> dict[str, Any]:
        try:
            return await asyncio.wait_for(self._reconcile_and_snapshot(), timeout=8.0)
        except Exception as e:
            log.warning("telegram_snapshot_timed_out_or_failed", error=str(e))
            return {
                "health": {"paper": self.adapter.paper},
                "account": {"status": "ERROR"},
                "positions": [],
                "orders": [],
                "broker_orders": [],
                "pending_exits": [],
                "journal_entries": [],
                "runtime_config": {},
                "reconciled": None,
                "today_new_entries": None,
                "errors": [f"snapshot unavailable: {e}"],
            }

    def _build_status_message(self, snapshot: dict[str, Any]) -> str:
        account = snapshot.get("account") or {}
        health = snapshot.get("health") or {}
        positions = snapshot.get("positions") or []
        orders = snapshot.get("orders") or []
        broker_orders = snapshot.get("broker_orders") or []
        pending_exits = snapshot.get("pending_exits") or []
        errors = snapshot.get("errors") or []
        today_new_entries = snapshot.get("today_new_entries")
        runtime_config = snapshot.get("runtime_config") or {}
        auto_entry_enabled = str(
            runtime_config.get("auto_entry_enabled", str(getattr(self.risk.settings, "auto_entry_enabled", False)))
        ).lower() in {"1", "true", "yes", "on", "enabled"}
        ai_entry_gate_enabled = str(
            runtime_config.get(
                "ai_entry_gate_enabled",
                str(getattr(self.risk.settings, "ai_entry_gate_enabled", False)),
            )
        ).lower() in {"1", "true", "yes", "on", "enabled"}
        simplified_runtime = bool(getattr(self.risk.settings, "simplified_runtime_enabled", False))
        ai_provider = str(getattr(self.risk.settings, "ai_research_provider", "shadow") or "shadow")
        risk_profile = self._risk_profile_from_values(runtime_config)
        equity = float(account.get("equity") or health.get("equity") or 0.0)
        snap = self.sm.get_snapshot(
            equity=equity,
            cash=float(account.get("cash") or 0.0),
            open_positions=positions,
        )
        state_warnings = _queued_close_order_warnings(
            state=snap.state.value,
            positions=positions,
            orders=broker_orders + orders,
        )
        max_positions = _runtime_int_from_values(
            runtime_config,
            "max_new_positions_per_day",
            default=int(getattr(self.risk.settings, "max_new_positions_per_day", 1) or 1),
        )
        max_positions = max(max_positions, 1)
        account_tradable = (
            account.get("status") == "CONNECTED"
            and "active" in str(account.get("account_status", "")).lower()
            and not account.get("trading_blocked")
            and not account.get("account_blocked")
        )
        lines = [
            "AUTO-TRADER STATUS",
            f"State: {snap.state.value}",
            f"State allows trading: {self.sm.can_trade()}",
            f"Runtime auto-entry: {auto_entry_enabled}",
            f"Runtime AI entry gate: {ai_entry_gate_enabled}",
            (
                "AI decision path: required before RiskEngine"
                if ai_entry_gate_enabled
                else "AI decision path: BYPASSED (AI gate disabled; deterministic RiskEngine path only)"
            ),
            f"Runtime mode: {'simplified' if simplified_runtime else 'legacy'}",
            f"AI provider: {ai_provider if simplified_runtime else 'legacy configured provider set'}",
            (
                f"Risk controls: explicit (legacy profile metadata: {risk_profile})"
                if simplified_runtime
                else f"Risk profile: {risk_profile}"
            ),
            f"New entries: {_entry_status(self.sm.can_trade(), auto_entry_enabled, positions, max_positions, today_new_entries, errors, account_tradable)}",
            f"Today new entries: {today_new_entries if today_new_entries is not None else 'unknown'} / {max_positions}",
            f"Alpaca: {account.get('status') or health.get('status')}",
            f"Paper: {health.get('paper')}",
            f"Market open: {health.get('market_open')}",
            f"Account status: {account.get('account_status', 'unknown')}",
            f"Trading blocked: {account.get('trading_blocked', 'unknown')}",
            f"Account blocked: {account.get('account_blocked', 'unknown')}",
            f"Equity: {_money(snap.equity)}",
            f"Cash: {_money(account.get('cash'))}",
            f"Buying power: {_money(account.get('buying_power'))}",
            _format_positions(positions),
            _format_orders(orders, title="Latest order"),
            _format_pending_exits(pending_exits, broker_orders + orders),
        ]
        if snapshot.get("reconciled") is not None:
            lines.append(f"Orders reconciled: {snapshot['reconciled']}")
        warnings = [str(e) for e in errors] + state_warnings
        if warnings:
            lines.append("Warnings: " + "; ".join(warnings))
        lines.append(f"Last updated: {snap.updated_at.isoformat()}Z")
        return "\n".join(lines)

    async def _build_config_message(self) -> str:
        values = await get_runtime_config_values()
        risk_profile = self._risk_profile_from_values(values)
        auto_entry = await get_runtime_config_bool(
            "auto_entry_enabled",
            default=bool(getattr(self.risk.settings, "auto_entry_enabled", False)),
        )
        max_new_positions = await get_runtime_config_int(
            "max_new_positions_per_day",
            default=int(getattr(self.risk.settings, "max_new_positions_per_day", 1) or 1),
            minimum=1,
        )
        auto_entry_source = "runtime" if "auto_entry_enabled" in values else "env default"
        max_positions_source = "runtime" if "max_new_positions_per_day" in values else "env default"
        ai_entry_gate = await get_runtime_config_bool(
            "ai_entry_gate_enabled",
            default=bool(getattr(self.risk.settings, "ai_entry_gate_enabled", False)),
        )
        ai_entry_gate_source = "runtime" if "ai_entry_gate_enabled" in values else "env default"
        risk_profile_source = "runtime" if "risk_profile" in values else "env default"
        simplified_runtime = bool(getattr(self.risk.settings, "simplified_runtime_enabled", False))
        ai_provider = str(getattr(self.risk.settings, "ai_research_provider", "shadow") or "shadow")
        risk_line = (
            f"risk_profile: {risk_profile} (legacy metadata; ignored by simplified controls)"
            if simplified_runtime
            else f"risk_profile: {risk_profile} ({risk_profile_source})"
        )
        usage_lines = [
            "Use: /config auto_entry on | /config auto_entry off",
            "     /config ai_gate on | /config ai_gate off",
            "     /config max_entries <positive integer>",
        ]
        if not simplified_runtime:
            usage_lines.insert(2, "     /config risk_profile conservative | aggressive | risky")
        return "\n".join(
            [
                "RUNTIME CONFIG",
                f"simplified_runtime_enabled: {simplified_runtime} (env)",
                f"ai_research_provider: {ai_provider} (env)",
                f"auto_entry_enabled: {auto_entry} ({auto_entry_source})",
                f"ai_entry_gate_enabled: {ai_entry_gate} ({ai_entry_gate_source})",
                (
                    "ai_decision_path: required before RiskEngine"
                    if ai_entry_gate
                    else "ai_decision_path: BYPASSED; deterministic RiskEngine path only"
                ),
                risk_line,
                f"auto_exit_enabled: {bool(getattr(self.risk.settings, 'auto_exit_enabled', False))} (env)",
                f"max_new_positions_per_day: {max_new_positions} ({max_positions_source})",
                *usage_lines,
            ]
        )

    def _risk_profile_from_values(self, values: dict[str, str]) -> str:
        return get_risk_profile(
            values.get("risk_profile") or getattr(self.risk.settings, "risk_profile", "conservative"),
            paper=bool(getattr(self.adapter, "paper", False)),
        ).name

    async def _runtime_values_or_empty(self) -> dict[str, str]:
        try:
            return await get_runtime_config_values()
        except Exception as e:
            log.warning("runtime_config_values_unavailable", error=str(e))
            return {}

    def _build_report_message(self, snapshot: dict[str, Any]) -> str:
        account = snapshot.get("account") or {}
        positions = snapshot.get("positions") or []
        orders = snapshot.get("orders") or []
        broker_orders = snapshot.get("broker_orders") or []
        pending_exits = snapshot.get("pending_exits") or []
        journal_entries = snapshot.get("journal_entries") or []
        errors = snapshot.get("errors") or []
        unrealized = sum(float(p.get("unrealized_pl") or 0.0) for p in positions)
        exposure = sum(abs(float(p.get("market_value") or 0.0)) for p in positions)
        state_warnings = _queued_close_order_warnings(
            state=self.sm.state.value,
            positions=positions,
            orders=broker_orders + orders,
        )
        lines = [
            "DAILY REPORT",
            f"State: {self.sm.state.value}",
            f"Equity: {_money(account.get('equity'))}",
            f"Cash: {_money(account.get('cash'))}",
            f"Account status: {account.get('account_status', 'unknown')}",
            f"Trading blocked: {account.get('trading_blocked', 'unknown')}",
            f"Account blocked: {account.get('account_blocked', 'unknown')}",
            f"Open exposure: {_money(exposure)}",
            f"Open unrealized P/L: {_money(unrealized)}",
            _format_positions(positions),
            _format_orders(orders),
            _format_pending_exits(pending_exits, broker_orders + orders),
            f"Orders reconciled: {snapshot.get('reconciled') if snapshot.get('reconciled') is not None else 'unknown'}",
            f"Generated at: {datetime.now(UTC).isoformat()}Z",
            _format_journal_entries(journal_entries),
        ]
        warnings = [str(e) for e in errors] + state_warnings
        if warnings:
            lines.append("Warnings: " + "; ".join(warnings))
        return "\n".join(lines)

    async def _kill_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """HIGHEST PRIORITY. Preempts all other logic. Fully async, retried, no deadlocks."""
        if not await self._require_authorized(update, "/kill"):
            return
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
        if not await self._require_authorized(update, "/status"):
            return
        snapshot = await self._bounded_snapshot()
        await update.message.reply_text(self._build_status_message(snapshot))
        log.info(
            "status_reported",
            state=self.sm.state.value,
            warnings=len(snapshot.get("errors") or []),
        )

    async def _pause_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._require_authorized(update, "/pause"):
            return
        if not self.sm.can_trade():
            await update.message.reply_text(f"Already in {self.sm.state.value}")
            return
        self.sm.pause("manual via /pause")
        await update.message.reply_text("⏸️ System PAUSED. No new entries. Monitoring continues.")
        log.warning("manual_pause", user=update.effective_user.username if update.effective_user else "unknown")

    async def _resume_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._require_authorized(update, "/resume"):
            return
        token = " ".join(context.args) if context.args else ""
        ok = self.sm.resume(token, self.resume_token)
        if ok:
            await update.message.reply_text("🟢 RESUMED to ACTIVE. Trading allowed again.")
            log.info("manual_resume_success", user=update.effective_user.username if update.effective_user else "unknown")
        else:
            await update.message.reply_text("❌ Resume failed (bad token or already active)")
            log.warning("manual_resume_failed")

    async def _report_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._require_authorized(update, "/report"):
            return
        snapshot = await self._bounded_snapshot()
        await update.message.reply_text(self._build_report_message(snapshot))
        log.info(
            "report_requested",
            state=self.sm.state.value,
            warnings=len(snapshot.get("errors") or []),
        )

    async def _edge_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._require_authorized(update, "/edge"):
            return
        args = [str(arg).strip() for arg in (context.args or []) if str(arg).strip()]
        if len(args) > 2:
            await update.message.reply_text(
                f"Use: /edge [days] [paper|live|mixed|unknown], where days is 1-{MAX_EDGE_REPORT_DAYS}."
            )
            return
        mode = next((arg.lower() for arg in args if arg.lower() in {"paper", "live", "mixed", "unknown"}), None)
        day_args = [arg for arg in args if arg.lower() not in {"paper", "live", "mixed", "unknown"}]
        if len(day_args) > 1 or (len(args) == 2 and mode is None):
            await update.message.reply_text(
                f"Use: /edge [days] [paper|live|mixed|unknown], where days is 1-{MAX_EDGE_REPORT_DAYS}."
            )
            return
        try:
            days = int(day_args[0]) if day_args else 14
        except ValueError:
            await update.message.reply_text(
                f"Use: /edge [days] [paper|live|mixed|unknown], where days is 1-{MAX_EDGE_REPORT_DAYS}."
            )
            return
        if days < 1 or days > MAX_EDGE_REPORT_DAYS:
            await update.message.reply_text(
                f"Use: /edge [days] [paper|live|mixed|unknown], where days is 1-{MAX_EDGE_REPORT_DAYS}."
            )
            return
        try:
            report = await asyncio.wait_for(
                run_edge_report(window_days=days, execution_mode=mode),
                timeout=EDGE_REPORT_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            log.warning(
                "edge_report_failed",
                error="timeout",
                exception_type="TimeoutError",
                days=days,
                timeout_seconds=EDGE_REPORT_TIMEOUT_SECONDS,
            )
            await update.message.reply_text(
                "EDGE REPORT timed out while reading the evidence ledger. Please retry in a moment."
            )
            return
        except Exception as e:
            log.warning(
                "edge_report_failed",
                error=str(e),
                exception_type=type(e).__name__,
                days=days,
            )
            await update.message.reply_text(
                "EDGE REPORT unavailable due to an internal report error. Please retry in a moment."
            )
            return
        if len(report) > 3900:
            report = report[:3850].rstrip() + "\n...truncated; use /edge with fewer days."
        await update.message.reply_text(report)
        log.info("edge_report_requested", days=days, execution_mode=mode)

    async def _ai_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._require_authorized(update, "/ai"):
            return
        args = [str(arg).strip().upper() for arg in (context.args or []) if str(arg).strip()]
        if len(args) > 1:
            await update.message.reply_text("Use: /ai [symbol]")
            return
        symbol = args[0] if args else None
        if symbol and (len(symbol) > 12 or not all(ch.isalnum() or ch in {".", "-"} for ch in symbol)):
            await update.message.reply_text("Use: /ai [symbol]")
            return
        try:
            rows = await asyncio.wait_for(
                read_latest_ai_research_memos(
                    limit=AI_DECISION_LOOKBACK_ROWS,
                    symbol=symbol,
                    exclude_providers=AI_DECISION_EXCLUDED_PROVIDERS,
                ),
                timeout=4.0,
            )
        except Exception as e:
            log.warning("ai_decision_report_failed", error=str(e), symbol=symbol)
            await update.message.reply_text("AI DECISIONS unavailable.")
            return
        report = _format_ai_decision_rows(rows, symbol=symbol)
        if self.sm.state != SystemState.ACTIVE:
            report = (
                f"AI PIPELINE {self.sm.state.value}: new entry research is not running.\n"
                "The decisions below are historical.\n\n"
                f"{report}"
            )
        await update.message.reply_text(report)
        log.info("ai_decision_report_requested", symbol=symbol or "latest")

    async def _config_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._require_authorized(update, "/config"):
            return
        args = [str(arg).strip().lower() for arg in (context.args or []) if str(arg).strip()]
        if not args:
            await update.message.reply_text(await self._build_config_message())
            return
        if len(args) != 2:
            values = await self._runtime_values_or_empty()
            await update.message.reply_text(
                "Use: /config auto_entry on | /config auto_entry off; "
                "/config ai_gate on | /config ai_gate off; "
                "/config risk_profile conservative|aggressive|risky; "
                "/config max_entries <positive integer>"
            )
            return
        key = args[0]
        raw_value = args[1]
        if key in {"auto_entry", "auto_entry_enabled", "entry"}:
            if raw_value not in {"on", "off", "true", "false", "1", "0", "enabled", "disabled"}:
                await update.message.reply_text("Use: /config auto_entry on | /config auto_entry off")
                return
            enabled = raw_value in {"on", "true", "1", "enabled"}
            persisted = await set_runtime_config_value("auto_entry_enabled", "true" if enabled else "false")
            if not persisted:
                await update.message.reply_text("Runtime config update failed. Auto-entry unchanged.")
                return
            await append_journal_entry(content=f"Runtime config updated: auto_entry_enabled={enabled}.")
            state_note = "" if self.sm.can_trade() else f" State is {self.sm.state.value}, so entries remain blocked."
            await update.message.reply_text(f"Runtime auto-entry set to {enabled}.{state_note}")
            log.warning(
                "runtime_auto_entry_updated",
                enabled=enabled,
                user=update.effective_user.username if update.effective_user else "unknown",
            )
            return
        if key in {"ai_gate", "ai_entry_gate", "ai_entry_gate_enabled", "entry_gate"}:
            if raw_value not in {"on", "off", "true", "false", "1", "0", "enabled", "disabled"}:
                await update.message.reply_text("Use: /config ai_gate on | /config ai_gate off")
                return
            enabled = raw_value in {"on", "true", "1", "enabled"}
            persisted = await set_runtime_config_value("ai_entry_gate_enabled", "true" if enabled else "false")
            if not persisted:
                await update.message.reply_text("Runtime config update failed. AI entry gate unchanged.")
                return
            await append_journal_entry(content=f"Runtime config updated: ai_entry_gate_enabled={enabled}.")
            note = " Gate is fail-closed; only valid real-provider approve can continue to RiskEngine." if enabled else ""
            await update.message.reply_text(f"Runtime AI entry gate set to {enabled}.{note}")
            log.warning(
                "runtime_ai_entry_gate_updated",
                enabled=enabled,
                user=update.effective_user.username if update.effective_user else "unknown",
            )
            return
        if key in {"risk_profile", "profile", "mode"}:
            if bool(getattr(self.risk.settings, "simplified_runtime_enabled", False)):
                await update.message.reply_text(
                    "Risk profiles are parked in simplified mode. Sizing, discovery, and prefilter controls "
                    "come from explicit reviewed environment settings."
                )
                return
            if raw_value not in set(VALID_RISK_PROFILES):
                await update.message.reply_text("Use: /config risk_profile conservative | aggressive | risky")
                return
            profile = get_risk_profile(raw_value, paper=bool(getattr(self.adapter, "paper", False)))
            if raw_value != profile.name:
                await update.message.reply_text("Experiment risk profiles are paper-only. Live mode stays conservative.")
                return
            persisted = await set_runtime_config_value("risk_profile", profile.name)
            if not persisted:
                await update.message.reply_text("Runtime config update failed. Risk profile unchanged.")
                return
            values = await self._runtime_values_or_empty()
            existing_max = _runtime_int_from_values(
                values,
                "max_new_positions_per_day",
                default=int(getattr(self.risk.settings, "max_new_positions_per_day", 1) or 1),
            )
            await append_journal_entry(content=f"Runtime config updated: risk_profile={profile.name}.")
            note = " Risky is paper-only and cannot bypass hard brakes." if profile.name == "risky" else ""
            entry_note = f" Max entries remain independently set to {existing_max}."
            await update.message.reply_text(
                f"Runtime risk profile set to {profile.name}. "
                f"Max entries can be set independently with /config max_entries <positive integer>.{entry_note}{note}"
            )
            log.warning(
                "runtime_risk_profile_updated",
                risk_profile=profile.name,
                user=update.effective_user.username if update.effective_user else "unknown",
            )
            return
        if key in {"max_entries", "max_new_positions_per_day", "daily_entries"}:
            values = await self._runtime_values_or_empty()
            risk_profile = self._risk_profile_from_values(values)
            old_entries = _runtime_int_from_values(
                values,
                "max_new_positions_per_day",
                default=int(getattr(self.risk.settings, "max_new_positions_per_day", 1) or 1),
            )
            try:
                max_entries = int(raw_value)
            except ValueError:
                await update.message.reply_text("Use: /config max_entries <positive integer>")
                return
            if max_entries < 1:
                await update.message.reply_text("Use: /config max_entries <positive integer>")
                return
            persisted = await set_runtime_config_value("max_new_positions_per_day", str(max_entries))
            if not persisted:
                await update.message.reply_text("Runtime config update failed. Entry cap unchanged.")
                return
            mode = "paper" if bool(getattr(self.adapter, "paper", False)) else "live"
            await append_journal_entry(
                content=(
                    "Runtime config updated: "
                    f"max_new_positions_per_day {old_entries}->{max_entries}; "
                    f"mode={mode}; risk_profile={risk_profile}."
                )
            )
            await update.message.reply_text(f"Runtime max entries per day set to {max_entries}.")
            log.warning(
                "runtime_max_entries_updated",
                max_entries=max_entries,
                user=update.effective_user.username if update.effective_user else "unknown",
            )
            return
        await update.message.reply_text(
            "Use: /config auto_entry on | /config auto_entry off; "
            "/config ai_gate on | /config ai_gate off; "
            "/config risk_profile conservative|aggressive|risky; "
            "/config max_entries <positive integer>"
        )

    async def _unknown(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._require_authorized(update, "unknown"):
            return
        await update.message.reply_text(
            "Unknown command. Use /status, /pause, /resume <token>, /kill, /report, /edge, /ai, /config"
        )

    def build(self) -> Application:
        """Build the Application with all command handlers."""
        app = Application.builder().token(self.token).concurrent_updates(True).build()

        app.add_handler(CommandHandler("status", self._status_handler))
        app.add_handler(CommandHandler("pause", self._pause_handler))
        app.add_handler(CommandHandler("resume", self._resume_handler))
        app.add_handler(CommandHandler("kill", self._kill_handler))
        app.add_handler(CommandHandler("report", self._report_handler))
        app.add_handler(CommandHandler("edge", self._edge_handler))
        app.add_handler(CommandHandler("ai", self._ai_handler))
        app.add_handler(CommandHandler("config", self._config_handler))
        app.add_handler(CommandHandler("start", self._status_handler))
        app.add_handler(CommandHandler("help", self._status_handler))

        # Fallback
        from telegram.ext import MessageHandler, filters
        app.add_handler(MessageHandler(filters.COMMAND, self._unknown))

        self.app = app
        return app

    def _polling_error_callback(self, error: Exception) -> None:
        log.warning(
            "telegram_polling_transport_error",
            error_type=type(error).__name__,
            error=str(error),
        )

    async def _teardown_app_for_reconnect(self) -> None:
        """Best-effort per-attempt cleanup before rebuilding a fresh Application."""
        app = self.app
        if not app:
            return
        log.warning("telegram_polling_reconnect_teardown_started")
        if app.updater:
            try:
                await app.updater.stop()
            except RuntimeError as e:
                if not self._is_expected_lifecycle_runtime_error(e):
                    log.warning("telegram_reconnect_updater_stop_failed", error=str(e))
                else:
                    log.info("telegram_reconnect_updater_already_stopped", error=str(e))
            except Exception as e:
                log.warning("telegram_reconnect_updater_stop_failed", error=str(e))
        try:
            await app.stop()
        except RuntimeError as e:
            if not self._is_expected_lifecycle_runtime_error(e):
                log.warning("telegram_reconnect_app_stop_failed", error=str(e))
            else:
                log.info("telegram_reconnect_app_already_stopped", error=str(e))
        except Exception as e:
            log.warning("telegram_reconnect_app_stop_failed", error=str(e))
        try:
            await app.shutdown()
        except RuntimeError as e:
            if not self._is_expected_lifecycle_runtime_error(e):
                log.warning("telegram_reconnect_app_shutdown_failed", error=str(e))
            else:
                log.info("telegram_reconnect_app_already_shutdown", error=str(e))
        except Exception as e:
            log.warning("telegram_reconnect_app_shutdown_failed", error=str(e))
        self.app = None
        self._shutdown_complete = False
        log.warning("telegram_polling_reconnect_teardown_complete")

    async def _sleep_until_retry_or_stop(self, stop_event: asyncio.Event | None, delay_seconds: float) -> bool:
        if not stop_event:
            await asyncio.sleep(delay_seconds)
            return False
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=delay_seconds)
            return True
        except TimeoutError:
            return False

    async def shutdown(self) -> None:
        """Graceful stop for signal handlers. Highest priority cleanup."""
        async with self._shutdown_lock:
            if self._shutdown_complete:
                log.info("telegram_bot_shutdown_skipped_already_stopped")
                return
            if self.app:
                log.info("telegram_bot_shutting_down")
                if self.app.updater:
                    try:
                        await self.app.updater.stop()
                    except RuntimeError as e:
                        if not self._is_expected_lifecycle_runtime_error(e):
                            raise
                        log.warning("telegram_updater_already_stopped", error=str(e))
                try:
                    await self.app.stop()
                except RuntimeError as e:
                    if not self._is_expected_lifecycle_runtime_error(e):
                        raise
                    log.warning("telegram_app_already_stopped", error=str(e))
                await self.app.shutdown()
            self._shutdown_complete = True
            log.info("telegram_bot_stopped")

    async def run(self, stop_event: asyncio.Event | None = None) -> None:
        """Start polling. Integrates with external stop_event for clean shutdown."""
        retry_delay = TELEGRAM_POLLING_RETRY_INITIAL_SECONDS
        while not (stop_event and stop_event.is_set()):
            if not self.app:
                self.build()
            assert self.app is not None
            self._shutdown_complete = False
            try:
                log.info("telegram_bot_starting_polling", kill_priority="absolute")
                await self.app.initialize()
                await self.app.start()
                await self.app.updater.start_polling(
                    allowed_updates=Update.ALL_TYPES,
                    bootstrap_retries=-1,
                    error_callback=self._polling_error_callback,
                )
                log.info("bot_polling_active_kill_live")
                retry_delay = TELEGRAM_POLLING_RETRY_INITIAL_SECONDS
                if stop_event:
                    await stop_event.wait()
                    await self.shutdown()
                    return
                while True:
                    await asyncio.sleep(3600)
            except asyncio.CancelledError:
                raise
            except (KeyboardInterrupt, SystemExit):
                raise
            except Exception as e:
                if stop_event and stop_event.is_set():
                    log.info("telegram_polling_stopped_after_stop_event", error=str(e))
                    await self.shutdown()
                    return
                if not self._is_retryable_polling_error(e):
                    log.exception(
                        "telegram_polling_non_retryable_failure",
                        error_type=type(e).__name__,
                        error=str(e),
                    )
                    raise
                log.warning(
                    "telegram_polling_failed_retrying",
                    error_type=type(e).__name__,
                    error=str(e),
                    retry_delay_seconds=retry_delay,
                    kill_priority="os_signal_still_active_telegram_reconnecting",
                )
                await self._teardown_app_for_reconnect()
                if await self._sleep_until_retry_or_stop(stop_event, retry_delay):
                    await self.shutdown()
                    return
                retry_delay = min(retry_delay * 2, TELEGRAM_POLLING_RETRY_MAX_SECONDS)
        await self.shutdown()
