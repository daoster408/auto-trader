"""
Minimal critical safety tests for kill + HALTED persistence (Reviewer requirement).

These tests must actually exercise real DB save → load roundtrips
and verify the safety default to HALTED on failure.
"""
import asyncio
import tempfile
from pathlib import Path

import pytest
from pydantic import ValidationError

from auto_trader.core.models import SystemState, KillResult, TradeIntent
from auto_trader.core.risk_engine import RiskEngine
from auto_trader.broker.alpaca_adapter import AlpacaAdapter
from auto_trader.comms.telegram_bot import TelegramBot
from auto_trader.config.settings import Settings
from auto_trader.core.state_machine import StateMachine
from auto_trader.execution.order_manager import OrderManager
from auto_trader.__main__ import _handle_signal_shutdown, _should_emergency_halt_on_shutdown
from auto_trader.scheduler.trading_supervisor import TradingSupervisor
from auto_trader.persistence.db import (
    configure_db_path,
    count_entry_orders_since,
    get_latest_order_records,
    init_db,
    load_system_state,
    reconcile_broker_orders,
    save_system_state,
    upsert_order_record,
)


class DummySettings:
    risk_per_trade_pct = 0.5
    max_new_positions_per_day = 1
    max_gross_exposure_pct = 25.0


class DummySupervisorSettings(DummySettings):
    reconcile_interval_seconds = 300
    reconcile_lookback_days = 2
    position_monitor_interval_seconds = 60
    supervisor_tick_timeout_seconds = 20
    auto_entry_enabled = False
    auto_exit_enabled = False
    position_max_loss_pct = -5.0
    position_take_profit_pct = 8.0
    position_trailing_stop_pct = 6.0
    position_max_hold_days = 10
    report_timezone = "America/Los_Angeles"


class DummySnapshot:
    equity = 100.0
    open_positions = []
    today_new_entries = 0


class DummySnapshotWithOpenPosition:
    equity = 100.0
    open_positions = [{"symbol": "AMPX", "qty": 0.832986, "market_value": 20.0}]
    today_new_entries = 0


class DummySnapshotWithTodayEntry:
    equity = 100.0
    open_positions = []
    today_new_entries = 1


class DummySnapshotMissingTodayEntry:
    equity = 100.0
    open_positions = []


class FakeTelegramIdentity:
    def __init__(self, id):
        self.id = id
        self.username = "test"


class FakeTelegramMessage:
    def __init__(self):
        self.replies = []

    async def reply_text(self, text):
        self.replies.append(text)


class FakeTelegramUpdate:
    def __init__(self, chat_id=123, user_id=456):
        self.effective_chat = FakeTelegramIdentity(chat_id)
        self.effective_user = FakeTelegramIdentity(user_id)
        self.message = FakeTelegramMessage()


@pytest.mark.asyncio
async def test_fresh_db_defaults_to_halted():
    """A brand-new DB must not start ACTIVE without an intentional /resume."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "fresh.db"
        configure_db_path(db_path)
        await init_db()

        restored, _ = await load_system_state()
        assert restored == SystemState.HALTED


@pytest.mark.asyncio
async def test_halted_state_survives_real_restart():
    """Save HALTED, then simulate process restart by creating a fresh StateMachine + load."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test_kill.db"
        configure_db_path(db_path)
        await init_db()

        # First "process" does a kill
        sm1 = StateMachine(initial_state=SystemState.ACTIVE, persist_hook=save_system_state)
        await sm1.halt("/kill test", flatten_callback=None)
        assert sm1.state == SystemState.HALTED

        # Simulate full restart: new process loads from disk
        restored, _ = await load_system_state()
        assert restored == SystemState.HALTED, "HALTED must survive real DB roundtrip"


@pytest.mark.asyncio
async def test_load_failure_or_corruption_defaults_to_halted():
    """If the DB is garbage or unreadable, we must still default to HALTED (never ACTIVE)."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "corrupt.db"
        configure_db_path(db_path)
        await init_db()

        # Corrupt the database file
        db_path.write_bytes(b"this is not a valid sqlite file \x00\x01\xff")

        state, meta = await load_system_state()
        assert state == SystemState.HALTED
        assert "load_failed" in str(meta.get("reason", "")) or meta == {"reason": "load_failed_or_missing"}


@pytest.mark.asyncio
async def test_emergency_halt_path_persists_and_calls_flatten():
    """The dual-path emergency kill mechanism must persist HALTED and invoke the flatten callback."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test_emergency.db"
        configure_db_path(db_path)
        await init_db()

        sm = StateMachine(initial_state=SystemState.ACTIVE, persist_hook=save_system_state)

        flatten_called = {"count": 0}

        async def fake_flatten() -> KillResult:
            flatten_called["count"] += 1
            return KillResult(
                success=True,
                orders_cancelled=3,
                positions_flattened=1,
                reason="test",
                incident_report="test flatten",
                timestamp=sm._last_halt_at or __import__("datetime").datetime.now(__import__("datetime").timezone.utc),
            )

        result = await sm.halt("test_emergency", flatten_callback=fake_flatten)
        assert sm.state == SystemState.HALTED
        assert flatten_called["count"] == 1
        assert result.positions_flattened == 1

        # Restart simulation
        restored, _ = await load_system_state()
        assert restored == SystemState.HALTED


def test_shutdown_emergency_halt_decision_defaults_to_production_safety():
    settings = type("Settings", (), {"shutdown_flatten_on_exit": True})()

    assert _should_emergency_halt_on_shutdown(settings, StateMachine(initial_state=SystemState.ACTIVE)) is True
    assert _should_emergency_halt_on_shutdown(settings, StateMachine(initial_state=SystemState.PAUSED)) is True
    assert _should_emergency_halt_on_shutdown(settings, StateMachine(initial_state=SystemState.HALTED)) is False


def test_shutdown_emergency_halt_can_be_disabled_for_supervised_local_runs():
    settings = type("Settings", (), {"shutdown_flatten_on_exit": False})()

    assert _should_emergency_halt_on_shutdown(settings, StateMachine(initial_state=SystemState.ACTIVE)) is False
    assert _should_emergency_halt_on_shutdown(settings, StateMachine(initial_state=SystemState.PAUSED)) is False


@pytest.mark.asyncio
async def test_signal_shutdown_skips_flatten_when_local_opt_out_enabled():
    settings = type("Settings", (), {"shutdown_flatten_on_exit": False})()
    sm = StateMachine(initial_state=SystemState.ACTIVE)
    stop_event = asyncio.Event()

    class FakeAdapter:
        def __init__(self):
            self.cancel_calls = 0
            self.flatten_calls = 0

        async def cancel_all_orders(self):
            self.cancel_calls += 1
            return 0

        async def flatten_all_positions(self):
            self.flatten_calls += 1
            return 0

    adapter = FakeAdapter()

    await _handle_signal_shutdown(
        sig=2,
        settings=settings,
        state_machine=sm,
        adapter=adapter,
        stop_event=stop_event,
    )

    assert sm.state == SystemState.ACTIVE
    assert adapter.cancel_calls == 0
    assert adapter.flatten_calls == 0
    assert stop_event.is_set()


@pytest.mark.asyncio
async def test_signal_shutdown_defaults_to_emergency_flatten():
    settings = type("Settings", (), {"shutdown_flatten_on_exit": True})()
    sm = StateMachine(initial_state=SystemState.ACTIVE)
    stop_event = asyncio.Event()

    class FakeAdapter:
        def __init__(self):
            self.cancel_calls = 0
            self.flatten_calls = 0

        async def cancel_all_orders(self):
            self.cancel_calls += 1
            return 2

        async def flatten_all_positions(self):
            self.flatten_calls += 1
            return 1

    adapter = FakeAdapter()

    await _handle_signal_shutdown(
        sig=15,
        settings=settings,
        state_machine=sm,
        adapter=adapter,
        stop_event=stop_event,
    )

    assert sm.state == SystemState.HALTED
    assert adapter.cancel_calls == 1
    assert adapter.flatten_calls == 1
    assert stop_event.is_set()


@pytest.mark.asyncio
async def test_stock_snapshots_accepts_top_level_alpaca_payload(monkeypatch):
    """Alpaca snapshots may return symbols at top level instead of under a snapshots key."""
    payload = b'{"AAPL":{"latestTrade":{"p":195.0}},"MSFT":{"latestTrade":{"p":420.0}}}'

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return payload

    def fake_urlopen(req, timeout):
        return FakeResponse()

    monkeypatch.setattr("auto_trader.broker.alpaca_adapter.urlopen", fake_urlopen)

    adapter = AlpacaAdapter("key", "secret", paper=True)
    snapshots = await adapter.get_stock_snapshots(["AAPL", "MSFT"])

    assert set(snapshots) == {"AAPL", "MSFT"}
    assert snapshots["AAPL"]["latestTrade"]["p"] == 195.0


@pytest.mark.asyncio
async def test_positions_snapshot_strict_raises_on_broker_failure(monkeypatch):
    """Pre-trade duplicate guards must not treat broker failure as no positions."""
    adapter = AlpacaAdapter("key", "secret", paper=True)

    def fail_client():
        raise RuntimeError("broker unavailable")

    monkeypatch.setattr(adapter, "_get_client", fail_client)

    assert await adapter.get_positions_snapshot() == []
    with pytest.raises(RuntimeError, match="broker unavailable"):
        await adapter.get_positions_snapshot(strict=True)


def test_risk_engine_sizes_fractional_quantity_under_early_cap():
    """Small paper accounts should use fractional size instead of forcing one share."""
    sm = StateMachine(initial_state=SystemState.ACTIVE)
    risk = RiskEngine(sm, DummySettings())
    intent = TradeIntent(
        symbol="AMPX",
        side="long",
        entry_price=23.89,
        rationale="test",
    )

    decision = risk.evaluate(intent, DummySnapshot())

    assert decision.approved is True
    assert decision.sized_quantity is not None
    assert decision.sized_quantity < 1.0
    assert decision.sized_quantity * intent.entry_price <= DummySnapshot.equity * 0.05


def test_risk_engine_blocks_duplicate_open_position():
    sm = StateMachine(initial_state=SystemState.ACTIVE)
    risk = RiskEngine(sm, DummySettings())
    intent = TradeIntent(symbol="AMPX", side="long", entry_price=23.89)

    decision = risk.evaluate(intent, DummySnapshotWithOpenPosition())

    assert decision.approved is False
    assert decision.reason == "Symbol already has an open position"


def test_risk_engine_blocks_durable_daily_entry_limit_after_restart():
    sm = StateMachine(initial_state=SystemState.ACTIVE)
    risk = RiskEngine(sm, DummySettings())
    intent = TradeIntent(symbol="MSFT", side="long", entry_price=23.89)

    decision = risk.evaluate(intent, DummySnapshotWithTodayEntry())

    assert decision.approved is False
    assert decision.reason == "Durable daily new position limit reached"


def test_risk_engine_blocks_missing_durable_daily_entry_count():
    sm = StateMachine(initial_state=SystemState.ACTIVE)
    risk = RiskEngine(sm, DummySettings())
    intent = TradeIntent(symbol="MSFT", side="long", entry_price=23.89)

    decision = risk.evaluate(intent, DummySnapshotMissingTodayEntry())

    assert decision.approved is False
    assert decision.reason == "Durable daily entry count unavailable"


@pytest.mark.asyncio
async def test_entry_order_count_uses_persisted_orders():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "orders.db"
        configure_db_path(db_path)
        await init_db()

        await upsert_order_record(
            {
                "id": "order-1",
                "symbol": "AMPX",
                "side": "buy",
                "qty": 0.5,
                "order_type": "market",
                "status": "filled",
                "filled_qty": 0.5,
                "submitted_at": "2026-06-02T14:33:19+00:00",
            }
        )

        assert await count_entry_orders_since("2026-06-02T07:00:00+00:00") == 1
        assert await count_entry_orders_since("2026-06-03T07:00:00+00:00") == 0

        latest = await get_latest_order_records(limit=1)
        assert latest[0]["symbol"] == "AMPX"
        assert latest[0]["status"] == "filled"


@pytest.mark.asyncio
async def test_entry_order_count_failure_raises():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "directory-not-db"
        db_path.mkdir()
        configure_db_path(db_path)

        with pytest.raises(Exception):
            await count_entry_orders_since("2026-06-02T07:00:00+00:00")


@pytest.mark.asyncio
async def test_reconcile_broker_orders_counts_only_successful_writes(monkeypatch):
    calls = {"count": 0}

    async def fake_upsert(order):
        calls["count"] += 1
        return calls["count"] == 1

    monkeypatch.setattr("auto_trader.persistence.db.upsert_order_record", fake_upsert)

    reconciled = await reconcile_broker_orders(
        [
            {"id": "ok", "symbol": "AMPX"},
            {"id": "failed", "symbol": "MSFT"},
        ]
    )

    assert reconciled == 1


@pytest.mark.asyncio
async def test_order_manager_pauses_on_post_submit_persistence_failure(monkeypatch):
    """If broker accepts an order but SQLite cannot save it, stop new entries immediately."""
    sm = StateMachine(initial_state=SystemState.ACTIVE)
    risk = RiskEngine(sm, DummySettings())

    class FakeAdapter:
        async def submit_order(self, **kwargs):
            return {
                "id": "broker-ok",
                "client_order_id": "broker-ok",
                "symbol": kwargs["symbol"],
                "qty": kwargs["qty"],
                "side": kwargs["side"],
                "order_type": kwargs["order_type"],
                "status": "accepted",
            }

    async def fake_log_risk_decision(**kwargs):
        return 42

    async def fake_upsert_order_record(*args, **kwargs):
        return False

    monkeypatch.setattr("auto_trader.execution.order_manager.log_risk_decision", fake_log_risk_decision)
    monkeypatch.setattr("auto_trader.execution.order_manager.upsert_order_record", fake_upsert_order_record)

    manager = OrderManager(risk, FakeAdapter())
    result = await manager.submit_trade_intent(
        TradeIntent(symbol="AMPX", side="long", entry_price=23.89),
        DummySnapshot(),
    )

    assert result["order"]["id"] == "broker-ok"
    assert result["persistence"]["order_record_saved"] is False
    assert result["risk_decision"]["approved"] is False
    assert sm.state == SystemState.PAUSED


def test_telegram_status_and_report_include_account_position_and_order():
    sm = StateMachine(initial_state=SystemState.ACTIVE)
    bot = TelegramBot(
        token="token",
        state_machine=sm,
        risk_engine=RiskEngine(sm, DummySettings()),
        adapter=object(),
        resume_token="resume",
    )
    snapshot = {
        "health": {"status": "CONNECTED", "paper": True, "market_open": True},
        "account": {
            "status": "CONNECTED",
            "account_status": "AccountStatus.ACTIVE",
            "equity": 100.0,
            "cash": 80.0,
            "buying_power": 80.0,
            "trading_blocked": False,
            "account_blocked": False,
        },
        "positions": [{"symbol": "AMPX", "qty": 0.832986, "market_value": 20.07, "unrealized_pl": -0.04}],
        "orders": [
            {
                "broker_order_id": "eaf99d3e-c577-4b2d-8f4f-74cd74be4178",
                "symbol": "AMPX",
                "status": "filled",
                "filled_qty": 0.832986,
                "avg_fill_price": 24.134,
            }
        ],
        "reconciled": 1,
        "today_new_entries": 0,
        "errors": [],
    }

    status = bot._build_status_message(snapshot)
    report = bot._build_report_message(snapshot)

    assert "AUTO-TRADER STATUS" in status
    assert "Equity: $100.00" in status
    assert "State allows trading: True" in status
    assert "New entries: blocked by open-position limit" in status
    assert "AMPX: qty 0.832986" in status
    assert "filled" in status
    assert "Orders reconciled: 1" in status
    assert "DAILY REPORT" in report
    assert "Open unrealized P/L: $-0.04" in report


def test_telegram_status_surfaces_warnings():
    sm = StateMachine(initial_state=SystemState.PAUSED)
    bot = TelegramBot(
        token="token",
        state_machine=sm,
        risk_engine=RiskEngine(sm, DummySettings()),
        adapter=object(),
        resume_token="resume",
    )
    status = bot._build_status_message(
        {
            "health": {"status": "ERROR", "paper": True, "market_open": False},
            "account": {"status": "ERROR"},
            "positions": [],
            "orders": [],
            "reconciled": None,
            "today_new_entries": None,
            "errors": ["positions unavailable"],
        }
    )

    assert "State: PAUSED" in status
    assert "New entries: blocked by system state" in status
    assert "Warnings: positions unavailable" in status


def test_telegram_status_blocks_entries_when_risk_data_unavailable():
    sm = StateMachine(initial_state=SystemState.ACTIVE)
    bot = TelegramBot(
        token="token",
        state_machine=sm,
        risk_engine=RiskEngine(sm, DummySettings()),
        adapter=object(),
        resume_token="resume",
    )

    status = bot._build_status_message(
        {
            "health": {"status": "CONNECTED", "paper": True, "market_open": True},
            "account": {
                "status": "CONNECTED",
                "account_status": "AccountStatus.ACTIVE",
                "equity": 100.0,
                "cash": 80.0,
                "buying_power": 80.0,
                "trading_blocked": False,
                "account_blocked": False,
            },
            "positions": [],
            "orders": [],
            "reconciled": None,
            "today_new_entries": 0,
            "errors": ["positions unavailable"],
        }
    )

    assert "New entries: blocked by unavailable risk data" in status


def test_telegram_status_blocks_entries_at_durable_daily_limit():
    sm = StateMachine(initial_state=SystemState.ACTIVE)
    bot = TelegramBot(
        token="token",
        state_machine=sm,
        risk_engine=RiskEngine(sm, DummySettings()),
        adapter=object(),
        resume_token="resume",
    )

    status = bot._build_status_message(
        {
            "health": {"status": "CONNECTED", "paper": True, "market_open": True},
            "account": {
                "status": "CONNECTED",
                "account_status": "AccountStatus.ACTIVE",
                "equity": 100.0,
                "cash": 80.0,
                "buying_power": 80.0,
                "trading_blocked": False,
                "account_blocked": False,
            },
            "positions": [],
            "orders": [],
            "reconciled": 0,
            "today_new_entries": 1,
            "errors": [],
        }
    )

    assert "New entries: blocked by daily-entry limit" in status
    assert "Today new entries: 1 / 1" in status


@pytest.mark.asyncio
async def test_telegram_unauthorized_status_does_not_read_broker():
    sm = StateMachine(initial_state=SystemState.ACTIVE)
    called = {"snapshot": 0}
    bot = TelegramBot(
        token="token",
        state_machine=sm,
        risk_engine=RiskEngine(sm, DummySettings()),
        adapter=object(),
        resume_token="resume",
        allowed_ids=[999],
    )

    async def fake_snapshot():
        called["snapshot"] += 1
        return {}

    bot._bounded_snapshot = fake_snapshot
    update = FakeTelegramUpdate(chat_id=123, user_id=456)

    await bot._status_handler(update, object())

    assert called["snapshot"] == 0
    assert update.message.replies == ["Unauthorized."]


@pytest.mark.asyncio
async def test_telegram_allowlisted_user_id_does_not_authorize_group_chat():
    sm = StateMachine(initial_state=SystemState.ACTIVE)
    called = {"snapshot": 0}
    bot = TelegramBot(
        token="token",
        state_machine=sm,
        risk_engine=RiskEngine(sm, DummySettings()),
        adapter=object(),
        resume_token="resume",
        allowed_ids=[456],
    )

    async def fake_snapshot():
        called["snapshot"] += 1
        return {}

    bot._bounded_snapshot = fake_snapshot
    update = FakeTelegramUpdate(chat_id=-100123, user_id=456)

    await bot._status_handler(update, object())

    assert called["snapshot"] == 0
    assert update.message.replies == ["Unauthorized."]


@pytest.mark.asyncio
async def test_telegram_authorized_status_reads_snapshot(monkeypatch):
    sm = StateMachine(initial_state=SystemState.ACTIVE)
    bot = TelegramBot(
        token="token",
        state_machine=sm,
        risk_engine=RiskEngine(sm, DummySettings()),
        adapter=object(),
        resume_token="resume",
        allowed_ids=[123],
    )

    async def fake_snapshot():
        return {
            "health": {"status": "CONNECTED", "paper": True, "market_open": True},
            "account": {
                "status": "CONNECTED",
                "account_status": "AccountStatus.ACTIVE",
                "equity": 100.0,
                "cash": 80.0,
                "buying_power": 80.0,
                "trading_blocked": False,
                "account_blocked": False,
            },
            "positions": [],
            "orders": [],
            "reconciled": 0,
            "today_new_entries": 0,
            "errors": [],
        }

    bot._bounded_snapshot = fake_snapshot
    update = FakeTelegramUpdate(chat_id=123, user_id=456)

    await bot._status_handler(update, object())

    assert "AUTO-TRADER STATUS" in update.message.replies[0]
    assert "New entries: allowed" in update.message.replies[0]


@pytest.mark.asyncio
async def test_telegram_unauthorized_kill_does_not_halt():
    sm = StateMachine(initial_state=SystemState.ACTIVE)
    bot = TelegramBot(
        token="token",
        state_machine=sm,
        risk_engine=RiskEngine(sm, DummySettings()),
        adapter=object(),
        resume_token="resume",
        allowed_ids=[999],
    )
    update = FakeTelegramUpdate(chat_id=123, user_id=456)

    await bot._kill_handler(update, object())

    assert sm.state == SystemState.ACTIVE
    assert update.message.replies == ["Unauthorized."]


@pytest.mark.asyncio
async def test_telegram_snapshot_gather_reconciles_and_reads_once(monkeypatch):
    sm = StateMachine(initial_state=SystemState.ACTIVE)

    class FakeAdapter:
        paper = True

        def __init__(self):
            self.calls = []

        async def get_account_snapshot(self):
            self.calls.append("account")
            return {
                "status": "CONNECTED",
                "account_status": "AccountStatus.ACTIVE",
                "equity": 100.0,
                "cash": 80.0,
                "buying_power": 80.0,
                "trading_blocked": False,
                "account_blocked": False,
            }

        async def get_clock(self):
            self.calls.append("clock")
            return {"is_open": True}

        async def get_recent_orders(self, days=7):
            self.calls.append(f"orders:{days}")
            return [{"id": "order-1", "symbol": "AMPX"}]

        async def get_positions_snapshot(self, *, strict=False):
            self.calls.append(f"positions:{strict}")
            return [{"symbol": "AMPX", "qty": 0.832986, "market_value": 20.0, "unrealized_pl": -0.1}]

    async def fake_reconcile(orders):
        assert orders == [{"id": "order-1", "symbol": "AMPX"}]
        return 1

    async def fake_latest(limit=3):
        return [{"symbol": "AMPX", "status": "filled", "filled_qty": 0.832986, "avg_fill_price": 24.134}]

    async def fake_count(start_utc_iso):
        return 0

    monkeypatch.setattr("auto_trader.comms.telegram_bot.reconcile_broker_orders", fake_reconcile)
    monkeypatch.setattr("auto_trader.comms.telegram_bot.get_latest_order_records", fake_latest)
    monkeypatch.setattr("auto_trader.comms.telegram_bot.count_entry_orders_since", fake_count)

    adapter = FakeAdapter()
    bot = TelegramBot(
        token="token",
        state_machine=sm,
        risk_engine=RiskEngine(sm, DummySettings()),
        adapter=adapter,
        resume_token="resume",
    )

    snapshot = await bot._reconcile_and_snapshot()

    assert snapshot["reconciled"] == 1
    assert snapshot["positions"][0]["symbol"] == "AMPX"
    assert snapshot["orders"][0]["status"] == "filled"
    assert snapshot["today_new_entries"] == 0
    assert snapshot["errors"] == []
    assert adapter.calls == ["account", "clock", "orders:7", "positions:True"]


@pytest.mark.asyncio
async def test_telegram_snapshot_gather_surfaces_account_and_clock_failures(monkeypatch):
    sm = StateMachine(initial_state=SystemState.ACTIVE)

    class FakeAdapter:
        paper = True

        async def get_account_snapshot(self):
            raise RuntimeError("account down")

        async def get_clock(self):
            raise RuntimeError("clock down")

        async def get_recent_orders(self, days=7):
            return []

        async def get_positions_snapshot(self, *, strict=False):
            return []

    async def fake_reconcile(orders):
        return 0

    async def fake_latest(limit=3):
        return []

    async def fake_count(start_utc_iso):
        return 0

    monkeypatch.setattr("auto_trader.comms.telegram_bot.reconcile_broker_orders", fake_reconcile)
    monkeypatch.setattr("auto_trader.comms.telegram_bot.get_latest_order_records", fake_latest)
    monkeypatch.setattr("auto_trader.comms.telegram_bot.count_entry_orders_since", fake_count)

    bot = TelegramBot(
        token="token",
        state_machine=sm,
        risk_engine=RiskEngine(sm, DummySettings()),
        adapter=FakeAdapter(),
        resume_token="resume",
    )

    snapshot = await bot._reconcile_and_snapshot()

    assert snapshot["account"]["status"] == "ERROR"
    assert "account unavailable" in snapshot["errors"][0]
    assert "market clock unavailable" in snapshot["errors"][1]


@pytest.mark.asyncio
async def test_telegram_snapshot_gather_surfaces_returned_error_dicts(monkeypatch):
    sm = StateMachine(initial_state=SystemState.ACTIVE)

    class FakeAdapter:
        paper = True

        async def get_account_snapshot(self):
            return {"equity": 0.0, "cash": 0.0, "status": "ERROR", "error": "account returned error"}

        async def get_clock(self):
            return {"is_open": False, "source": "error", "error": "clock returned error"}

        async def get_recent_orders(self, days=7):
            return []

        async def get_positions_snapshot(self, *, strict=False):
            return []

    async def fake_reconcile(orders):
        return 0

    async def fake_latest(limit=3):
        return []

    async def fake_count(start_utc_iso):
        return 0

    monkeypatch.setattr("auto_trader.comms.telegram_bot.reconcile_broker_orders", fake_reconcile)
    monkeypatch.setattr("auto_trader.comms.telegram_bot.get_latest_order_records", fake_latest)
    monkeypatch.setattr("auto_trader.comms.telegram_bot.count_entry_orders_since", fake_count)

    bot = TelegramBot(
        token="token",
        state_machine=sm,
        risk_engine=RiskEngine(sm, DummySettings()),
        adapter=FakeAdapter(),
        resume_token="resume",
    )

    snapshot = await bot._reconcile_and_snapshot()
    status = bot._build_status_message(snapshot)

    assert "account unavailable: account returned error" in snapshot["errors"]
    assert "market clock unavailable: clock returned error" in snapshot["errors"]
    assert snapshot["health"]["market_open"] is None
    assert "Market open: None" in status
    assert "Warnings: account unavailable: account returned error; market clock unavailable: clock returned error" in status


@pytest.mark.asyncio
async def test_supervisor_reconciles_and_dry_run_exit_signal(monkeypatch):
    sm = StateMachine(initial_state=SystemState.ACTIVE)
    notifications = []

    class FakeAdapter:
        paper = True

        def __init__(self):
            self.close_calls = 0

        async def get_account_snapshot(self):
            return {
                "status": "CONNECTED",
                "account_status": "AccountStatus.ACTIVE",
                "equity": 100.0,
                "cash": 80.0,
                "trading_blocked": False,
                "account_blocked": False,
            }

        async def get_clock(self):
            return {"is_open": True, "source": "alpaca"}

        async def get_recent_orders(self, days=2):
            return [{"id": "order-1", "symbol": "AMPX"}]

        async def get_positions_snapshot(self, *, strict=False):
            return [
                {
                    "symbol": "AMPX",
                    "qty": 1,
                    "market_value": 94.0,
                    "unrealized_pl": -6.0,
                    "cost_basis": 100.0,
                }
            ]

        async def close_position(self, *args, **kwargs):
            self.close_calls += 1
            return {}

    async def fake_reconcile(orders):
        return len(orders)

    async def fake_count(start_utc_iso):
        return 0

    async def fake_latest_entry(symbol):
        return None

    async def fake_notify(message):
        notifications.append(message)

    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.reconcile_broker_orders", fake_reconcile)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.count_entry_orders_since", fake_count)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_latest_entry_order_for_symbol", fake_latest_entry)

    adapter = FakeAdapter()
    supervisor = TradingSupervisor(
        settings=DummySupervisorSettings(),
        state_machine=sm,
        adapter=adapter,
        order_manager=object(),
        notifier=fake_notify,
    )

    result = await supervisor.tick_once()

    assert result.reconciled_orders == 1
    assert result.exit_decisions[0].should_exit is True
    assert result.exit_decisions[0].reason == "position max loss reached"
    assert adapter.close_calls == 0
    assert any("EXIT SIGNAL (dry run): AMPX" in message for message in notifications)


def test_supervisor_exit_rule_blocks_max_hold():
    sm = StateMachine(initial_state=SystemState.ACTIVE)
    supervisor = TradingSupervisor(
        settings=DummySupervisorSettings(),
        state_machine=sm,
        adapter=object(),
        order_manager=object(),
    )

    decision = supervisor.evaluate_exit_rules(
        {
            "symbol": "AMPX",
            "qty": 1,
            "market_value": 100.0,
            "unrealized_pl": 0.0,
            "cost_basis": 100.0,
            "entry_age_days": 10.1,
        }
    )

    assert decision.should_exit is True
    assert decision.reason == "position max hold reached"


@pytest.mark.asyncio
async def test_supervisor_auto_exit_closes_and_persists(monkeypatch):
    sm = StateMachine(initial_state=SystemState.ACTIVE)
    notifications = []

    class ExitSettings(DummySupervisorSettings):
        auto_exit_enabled = True

    class FakeAdapter:
        paper = True

        def __init__(self):
            self.close_calls = 0

        async def get_account_snapshot(self):
            return {
                "status": "CONNECTED",
                "account_status": "AccountStatus.ACTIVE",
                "equity": 100.0,
                "cash": 80.0,
                "trading_blocked": False,
                "account_blocked": False,
            }

        async def get_clock(self):
            return {"is_open": True, "source": "alpaca"}

        async def get_recent_orders(self, days=2):
            return []

        async def get_positions_snapshot(self, *, strict=False):
            return [
                {
                    "symbol": "AMPX",
                    "qty": 1,
                    "market_value": 110.0,
                    "unrealized_pl": 10.0,
                    "cost_basis": 100.0,
                }
            ]

        async def close_position(self, symbol, reason):
            self.close_calls += 1
            return {
                "id": "exit-1",
                "client_order_id": "exit-1",
                "broker_order_id": "exit-1",
                "symbol": symbol,
                "side": "sell",
                "qty": 1,
                "status": "submitted",
                "rationale": reason,
            }

    async def fake_reconcile(orders):
        return 0

    async def fake_count(start_utc_iso):
        return 0

    async def fake_latest_entry(symbol):
        return None

    async def fake_upsert(order, **kwargs):
        assert order["id"] == "exit-1"
        return True

    async def fake_notify(message):
        notifications.append(message)

    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.reconcile_broker_orders", fake_reconcile)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.count_entry_orders_since", fake_count)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_latest_entry_order_for_symbol", fake_latest_entry)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.upsert_order_record", fake_upsert)

    adapter = FakeAdapter()
    supervisor = TradingSupervisor(
        settings=ExitSettings(),
        state_machine=sm,
        adapter=adapter,
        order_manager=object(),
        notifier=fake_notify,
    )

    result = await supervisor.tick_once()
    result2 = await supervisor.tick_once()

    assert result.exit_decisions[0].reason == "position take profit reached"
    assert result2.exit_decisions[0].reason == "position take profit reached"
    assert adapter.close_calls == 1
    assert any("EXIT SUBMITTED: AMPX" in message for message in notifications)
    assert any("EXIT SUPPRESSED: close order already submitted for AMPX" in message for message in notifications)


@pytest.mark.asyncio
async def test_supervisor_pending_exit_survives_position_snapshot_failure(monkeypatch):
    sm = StateMachine(initial_state=SystemState.ACTIVE)
    notifications = []

    class ExitSettings(DummySupervisorSettings):
        auto_exit_enabled = True

    class FakeAdapter:
        paper = True

        def __init__(self):
            self.close_calls = 0
            self.position_calls = 0

        async def get_account_snapshot(self):
            return {
                "status": "CONNECTED",
                "account_status": "AccountStatus.ACTIVE",
                "equity": 100.0,
                "cash": 80.0,
                "trading_blocked": False,
                "account_blocked": False,
            }

        async def get_clock(self):
            return {"is_open": True, "source": "alpaca"}

        async def get_recent_orders(self, days=2):
            return []

        async def get_positions_snapshot(self, *, strict=False):
            self.position_calls += 1
            if self.position_calls == 2:
                raise RuntimeError("broker positions temporarily unavailable")
            return [
                {
                    "symbol": "AMPX",
                    "qty": 1,
                    "market_value": 110.0,
                    "unrealized_pl": 10.0,
                    "cost_basis": 100.0,
                }
            ]

        async def close_position(self, symbol, reason):
            self.close_calls += 1
            return {
                "id": f"exit-{self.close_calls}",
                "client_order_id": f"exit-{self.close_calls}",
                "broker_order_id": f"exit-{self.close_calls}",
                "symbol": symbol,
                "side": "sell",
                "qty": 1,
                "status": "submitted",
                "rationale": reason,
            }

    async def fake_reconcile(orders):
        return 0

    async def fake_count(start_utc_iso):
        return 0

    async def fake_latest_entry(symbol):
        return None

    async def fake_upsert(order, **kwargs):
        return True

    async def fake_notify(message):
        notifications.append(message)

    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.reconcile_broker_orders", fake_reconcile)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.count_entry_orders_since", fake_count)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_latest_entry_order_for_symbol", fake_latest_entry)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.upsert_order_record", fake_upsert)

    adapter = FakeAdapter()
    supervisor = TradingSupervisor(
        settings=ExitSettings(),
        state_machine=sm,
        adapter=adapter,
        order_manager=object(),
        notifier=fake_notify,
    )

    first = await supervisor.tick_once()
    second = await supervisor.tick_once()
    third = await supervisor.tick_once()

    assert first.exit_decisions[0].reason == "position take profit reached"
    assert second.exit_decisions == []
    assert third.exit_decisions[0].reason == "position take profit reached"
    assert adapter.close_calls == 1
    assert any("positions unavailable" in error for error in second.errors)
    assert any("EXIT SUPPRESSED: close order already submitted for AMPX" in message for message in notifications)


@pytest.mark.asyncio
async def test_supervisor_halted_suppresses_auto_exit(monkeypatch):
    sm = StateMachine(initial_state=SystemState.HALTED)
    notifications = []

    class ExitSettings(DummySupervisorSettings):
        auto_exit_enabled = True

    class FakeAdapter:
        paper = True

        def __init__(self):
            self.close_calls = 0

        async def get_account_snapshot(self):
            return {
                "status": "CONNECTED",
                "account_status": "AccountStatus.ACTIVE",
                "equity": 100.0,
                "cash": 80.0,
                "trading_blocked": False,
                "account_blocked": False,
            }

        async def get_clock(self):
            return {"is_open": True, "source": "alpaca"}

        async def get_recent_orders(self, days=2):
            return []

        async def get_positions_snapshot(self, *, strict=False):
            return [
                {
                    "symbol": "AMPX",
                    "qty": 1,
                    "market_value": 110.0,
                    "unrealized_pl": 10.0,
                    "cost_basis": 100.0,
                }
            ]

        async def close_position(self, symbol, reason):
            self.close_calls += 1
            return {"id": "exit-1", "symbol": symbol, "rationale": reason}

    async def fake_reconcile(orders):
        return 0

    async def fake_count(start_utc_iso):
        return 0

    async def fake_latest_entry(symbol):
        return None

    async def fake_notify(message):
        notifications.append(message)

    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.reconcile_broker_orders", fake_reconcile)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.count_entry_orders_since", fake_count)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_latest_entry_order_for_symbol", fake_latest_entry)

    adapter = FakeAdapter()
    supervisor = TradingSupervisor(
        settings=ExitSettings(),
        state_machine=sm,
        adapter=adapter,
        order_manager=object(),
        notifier=fake_notify,
    )

    result = await supervisor.tick_once()

    assert result.exit_decisions[0].reason == "position take profit reached"
    assert adapter.close_calls == 0
    assert any("HALTED POSITION WARNING" in message for message in notifications)
    assert any("EXIT SUPPRESSED: system is HALTED" in message for message in notifications)


def test_supervisor_config_rejects_hot_loop_intervals():
    with pytest.raises(ValidationError):
        Settings(
            ALPACA_API_KEY="key",
            ALPACA_API_SECRET="secret",
            TELEGRAM_BOT_TOKEN="token",
            RESUME_TOKEN="resume",
            RECONCILE_INTERVAL_SECONDS=0,
            POSITION_MONITOR_INTERVAL_SECONDS=0,
            SUPERVISOR_TICK_TIMEOUT_SECONDS=0,
        )


def test_shutdown_flatten_opt_out_is_allowed_in_paper_mode_only():
    settings = Settings(
        ALPACA_API_KEY="key",
        ALPACA_API_SECRET="secret",
        ALPACA_PAPER=True,
        TELEGRAM_BOT_TOKEN="token",
        RESUME_TOKEN="resume",
        SHUTDOWN_FLATTEN_ON_EXIT=False,
    )

    assert settings.shutdown_flatten_on_exit is False


def test_shutdown_flatten_opt_out_rejected_in_live_mode():
    with pytest.raises(ValidationError):
        Settings(
            ALPACA_API_KEY="key",
            ALPACA_API_SECRET="secret",
            ALPACA_PAPER=False,
            TELEGRAM_BOT_TOKEN="token",
            RESUME_TOKEN="resume",
            SHUTDOWN_FLATTEN_ON_EXIT=False,
        )


@pytest.mark.asyncio
async def test_supervisor_auto_entry_uses_order_manager(monkeypatch):
    sm = StateMachine(initial_state=SystemState.ACTIVE)

    class EntrySettings(DummySupervisorSettings):
        auto_entry_enabled = True

    class FakeAdapter:
        paper = True

        async def get_account_snapshot(self):
            return {
                "status": "CONNECTED",
                "account_status": "AccountStatus.ACTIVE",
                "equity": 100.0,
                "cash": 80.0,
                "trading_blocked": False,
                "account_blocked": False,
            }

        async def get_clock(self):
            return {"is_open": True, "source": "alpaca"}

        async def get_recent_orders(self, days=2):
            return []

        async def get_positions_snapshot(self, *, strict=False):
            return []

    class FakeOrderManager:
        def __init__(self):
            self.calls = []

        async def submit_trade_intent(self, intent, snapshot):
            self.calls.append((intent, snapshot))
            return {"order": {"id": "entry-1"}, "risk_decision": {"approved": True}}

    async def fake_reconcile(orders):
        return 0

    async def fake_count(start_utc_iso):
        return 0

    async def fake_latest_entry(symbol):
        return None

    async def fake_signals(adapter, max_signals=1):
        return [TradeIntent(symbol="AMPX", side="long", entry_price=20.0)]

    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.reconcile_broker_orders", fake_reconcile)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.count_entry_orders_since", fake_count)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_latest_entry_order_for_symbol", fake_latest_entry)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_simple_rules_signals", fake_signals)

    manager = FakeOrderManager()
    supervisor = TradingSupervisor(
        settings=EntrySettings(),
        state_machine=sm,
        adapter=FakeAdapter(),
        order_manager=manager,
    )

    result = await supervisor.tick_once()

    assert result.entry_result["order"]["id"] == "entry-1"
    assert manager.calls[0][0].symbol == "AMPX"


@pytest.mark.asyncio
async def test_supervisor_blocks_auto_entry_when_positions_unavailable(monkeypatch):
    sm = StateMachine(initial_state=SystemState.ACTIVE)

    class EntrySettings(DummySupervisorSettings):
        auto_entry_enabled = True

    class FakeAdapter:
        paper = True

        async def get_account_snapshot(self):
            return {
                "status": "CONNECTED",
                "account_status": "AccountStatus.ACTIVE",
                "equity": 100.0,
                "cash": 80.0,
                "trading_blocked": False,
                "account_blocked": False,
            }

        async def get_clock(self):
            return {"is_open": True, "source": "alpaca"}

        async def get_recent_orders(self, days=2):
            return []

        async def get_positions_snapshot(self, *, strict=False):
            raise RuntimeError("positions down")

    class FakeOrderManager:
        def __init__(self):
            self.calls = 0

        async def submit_trade_intent(self, intent, snapshot):
            self.calls += 1
            return {"order": {"id": "entry-1"}}

    async def fake_reconcile(orders):
        return 0

    async def fake_count(start_utc_iso):
        return 0

    async def fake_signals(adapter, max_signals=1):
        return [TradeIntent(symbol="AMPX", side="long", entry_price=20.0)]

    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.reconcile_broker_orders", fake_reconcile)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.count_entry_orders_since", fake_count)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_simple_rules_signals", fake_signals)

    manager = FakeOrderManager()
    supervisor = TradingSupervisor(
        settings=EntrySettings(),
        state_machine=sm,
        adapter=FakeAdapter(),
        order_manager=manager,
    )

    result = await supervisor.tick_once()

    assert result.entry_result is None
    assert manager.calls == 0
    assert any("positions unavailable" in error for error in result.errors)
