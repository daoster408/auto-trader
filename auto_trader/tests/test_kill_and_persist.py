"""
Minimal critical safety tests for kill + HALTED persistence (Reviewer requirement).

These tests must actually exercise real DB save → load roundtrips
and verify the safety default to HALTED on failure.
"""
import asyncio
import json
import sqlite3
import tempfile
from pathlib import Path

import pytest
from pydantic import ValidationError

from auto_trader.core.models import SystemState, KillResult, TradeIntent
from auto_trader.account_risk_validate import (
    AccountRiskScenario,
    build_account_risk_validation_report,
    evaluate_account_risk_scenario,
    validation_exit_code as account_risk_validation_exit_code,
)
from auto_trader.day3_validate import build_day3_validation_report, validation_exit_code
from auto_trader.core.risk_engine import RiskEngine
from auto_trader.broker.alpaca_adapter import AlpacaAdapter
from auto_trader.comms.telegram_bot import TelegramBot
from auto_trader.config.settings import Settings
from auto_trader.core.state_machine import StateMachine
from auto_trader.execution.order_manager import OrderManager
from auto_trader.intelligence.finnhub_client import FinnhubClient
from auto_trader.intelligence.rules_fallback import DiscoveryCandidate, get_simple_rules_signals
from auto_trader.__main__ import _acquire_single_instance_lock, _handle_signal_shutdown, _should_emergency_halt_on_shutdown
from auto_trader.scheduler.trading_supervisor import TradingSupervisor
from auto_trader.persistence.db import (
    append_journal_entry,
    clear_pending_exit,
    configure_db_path,
    count_entry_orders_since,
    get_latest_journal_entries,
    get_latest_order_records,
    get_pending_exits,
    get_pending_exit_for_symbol,
    get_pending_exit_symbols,
    get_runtime_config_bool,
    get_runtime_config_int,
    get_runtime_config_value,
    get_runtime_config_values,
    init_db,
    log_signal,
    load_system_state,
    reconcile_broker_orders,
    save_system_state,
    set_runtime_config_value,
    upsert_pending_exit,
    upsert_order_record,
    update_account_risk_state,
)


class DummySettings:
    risk_per_trade_pct = 0.5
    max_new_positions_per_day = 1
    max_gross_exposure_pct = 25.0
    daily_loss_halt_pct = -1.75
    weekly_loss_halt_pct = -4.0
    peak_drawdown_halt_pct = -6.0
    consecutive_sl_halt = 2


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


class DummyDay3Settings:
    alpaca_paper = True
    auto_entry_enabled = False
    auto_exit_enabled = True


def _day3_account_snapshot():
    return {
        "status": "CONNECTED",
        "account_status": "AccountStatus.ACTIVE",
        "trading_blocked": False,
        "account_blocked": False,
    }


def _day3_close_order(order_id="close-1", status="accepted"):
    return {
        "id": order_id,
        "symbol": "AMPX",
        "side": "sell",
        "qty": 0.832986,
        "status": status,
    }


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


def patch_empty_pending_exit_state(monkeypatch):
    async def fake_pending_symbols():
        return set()

    async def fake_pending_lookup(symbol):
        return None

    async def fake_pending_upsert(symbol, order=None, reason=None):
        return True

    async def fake_pending_clear(symbol):
        return True

    async def fake_journal_entry(**kwargs):
        return 1

    async def fake_log_signal(**kwargs):
        return 1

    async def fake_account_risk_state(*, equity, day_date, week_start_date):
        return {
            "equity": equity,
            "day_date": day_date,
            "day_start_equity": equity,
            "daily_loss_pct": 0.0,
            "week_start_date": week_start_date,
            "week_start_equity": equity,
            "weekly_loss_pct": 0.0,
            "peak_equity": equity,
            "peak_drawdown_pct": 0.0,
        }

    async def fake_runtime_config_bool(key, *, default):
        return default

    async def fake_runtime_config_int(key, *, default, minimum=None, maximum=None):
        return default

    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_pending_exit_symbols", fake_pending_symbols)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_pending_exit_for_symbol", fake_pending_lookup)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.upsert_pending_exit", fake_pending_upsert)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.clear_pending_exit", fake_pending_clear)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.append_journal_entry", fake_journal_entry)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.log_signal", fake_log_signal)
    monkeypatch.setattr(
        "auto_trader.scheduler.trading_supervisor.update_account_risk_state",
        fake_healthy_account_risk_state,
    )
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_runtime_config_bool", fake_runtime_config_bool)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_runtime_config_int", fake_runtime_config_int)


async def fake_healthy_account_risk_state(*, equity, day_date, week_start_date):
    return {
        "equity": equity,
        "day_date": day_date,
        "day_start_equity": equity,
        "daily_loss_pct": 0.0,
        "week_start_date": week_start_date,
        "week_start_equity": equity,
        "weekly_loss_pct": 0.0,
        "peak_equity": equity,
        "peak_drawdown_pct": 0.0,
    }


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


class FakeTelegramContext:
    def __init__(self, args=None):
        self.args = args or []


def test_single_instance_lock_rejects_duplicate_for_same_db():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = str(Path(tmp) / "auto_trader.db")
        lock_path, first_handle = _acquire_single_instance_lock(db_path)
        try:
            with pytest.raises(RuntimeError, match="already running"):
                _acquire_single_instance_lock(db_path)
        finally:
            first_handle.close()
            lock_path.unlink(missing_ok=True)


def test_day3_validation_waits_when_position_open_and_pending_exit_present():
    report, gates = build_day3_validation_report(
        symbol="AMPX",
        settings=DummyDay3Settings(),
        account=_day3_account_snapshot(),
        clock={"is_open": False},
        positions=[{"symbol": "AMPX", "qty": 0.832986, "market_value": 19.08}],
        broker_orders=[_day3_close_order()],
        local_orders=[],
        pending_exits=[
            {
                "symbol": "AMPX",
                "broker_order_id": "close-1",
                "reason": "position max loss reached",
            }
        ],
        reconciled_orders=2,
    )

    assert "Overall: WARN" in report
    assert "WAITING: close lifecycle is still pending" in report
    assert "[PASS] duplicate close count" in report
    assert "[PASS] pending-exit marker" in report
    assert validation_exit_code(gates) == 0


def test_account_risk_validation_dry_run_scenarios_pass():
    report, gates = build_account_risk_validation_report(settings=DummySettings(), base_equity=400.0)

    assert "Overall: PASS" in report
    assert "[PASS] healthy" in report
    assert "[PASS] daily-loss-breach" in report
    assert "[PASS] peak-drawdown-breach" in report
    assert account_risk_validation_exit_code(gates) == 0


def test_account_risk_validation_reports_failed_expectation():
    report, gates = build_account_risk_validation_report(
        settings=DummySettings(),
        base_equity=400.0,
        scenarios=[
            AccountRiskScenario(
                name="bad-expectation",
                equity=390.0,
                day_start_equity=400.0,
                week_start_equity=400.0,
                peak_equity=400.0,
                expected_halt=False,
            )
        ],
    )

    assert "Overall: FAIL" in report
    assert "[FAIL] bad-expectation" in report
    assert account_risk_validation_exit_code(gates) == 2


def test_account_risk_scenario_evaluates_all_thresholds():
    decision = evaluate_account_risk_scenario(
        AccountRiskScenario(
            name="multi-breach",
            equity=380.0,
            day_start_equity=400.0,
            week_start_equity=400.0,
            peak_equity=410.0,
            expected_halt=True,
        ),
        daily_loss_halt_pct=-1.75,
        weekly_loss_halt_pct=-4.0,
        peak_drawdown_halt_pct=-6.0,
    )

    assert decision.should_halt is True
    assert decision.daily_loss_pct == pytest.approx(-5.0)
    assert decision.weekly_loss_pct == pytest.approx(-5.0)
    assert decision.peak_drawdown_pct == pytest.approx(-7.31707317)
    assert len(decision.breaches) == 3


def test_day3_validation_fails_duplicate_close_orders():
    report, gates = build_day3_validation_report(
        symbol="AMPX",
        settings=DummyDay3Settings(),
        account=_day3_account_snapshot(),
        clock={"is_open": True},
        positions=[{"symbol": "AMPX", "qty": 0.832986, "market_value": 19.08}],
        broker_orders=[_day3_close_order("close-1"), _day3_close_order("close-2")],
        local_orders=[],
        pending_exits=[{"symbol": "AMPX", "broker_order_id": "close-1"}],
        reconciled_orders=2,
    )

    assert "[FAIL] duplicate close count" in report
    assert "2 non-failed close order(s) found for AMPX" in report
    assert validation_exit_code(gates) == 2


def test_day3_validation_passes_after_position_gone_and_pending_clear():
    report, gates = build_day3_validation_report(
        symbol="AMPX",
        settings=DummyDay3Settings(),
        account=_day3_account_snapshot(),
        clock={"is_open": True},
        positions=[],
        broker_orders=[_day3_close_order(status="filled")],
        local_orders=[],
        pending_exits=[],
        reconciled_orders=2,
    )

    assert "Overall: PASS" in report
    assert "PASSED: close filled, position gone, pending marker clear" in report
    assert "[PASS] pending-exit marker" in report
    assert validation_exit_code(gates) == 0


def test_day3_validation_fails_when_required_snapshot_is_unavailable():
    report, gates = build_day3_validation_report(
        symbol="AMPX",
        settings=DummyDay3Settings(),
        account=_day3_account_snapshot(),
        clock={"is_open": True},
        positions=[],
        broker_orders=[_day3_close_order()],
        local_orders=[],
        pending_exits=[],
        reconciled_orders=1,
        errors=["positions unavailable: broker down"],
    )

    assert "[FAIL] data availability: positions unavailable: broker down" in report
    assert validation_exit_code(gates) == 2


def test_close_position_submit_is_not_retry_wrapped():
    assert not hasattr(AlpacaAdapter.close_position, "retry")
    assert hasattr(AlpacaAdapter.get_open_orders, "retry")


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
async def test_account_risk_state_tracks_daily_weekly_and_peak_baselines():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "risk_state.db"
        configure_db_path(db_path)
        await init_db()

        first = await update_account_risk_state(
            equity=100.0,
            day_date="2026-06-03",
            week_start_date="2026-06-01",
        )
        assert first["day_start_equity"] == 100.0
        assert first["week_start_equity"] == 100.0
        assert first["peak_equity"] == 100.0
        assert first["daily_loss_pct"] == 0.0

        lower = await update_account_risk_state(
            equity=98.0,
            day_date="2026-06-03",
            week_start_date="2026-06-01",
        )
        assert lower["day_start_equity"] == 100.0
        assert lower["week_start_equity"] == 100.0
        assert lower["peak_equity"] == 100.0
        assert lower["daily_loss_pct"] == pytest.approx(-2.0)
        assert lower["weekly_loss_pct"] == pytest.approx(-2.0)
        assert lower["peak_drawdown_pct"] == pytest.approx(-2.0)

        higher = await update_account_risk_state(
            equity=105.0,
            day_date="2026-06-03",
            week_start_date="2026-06-01",
        )
        assert higher["peak_equity"] == 105.0

        next_day = await update_account_risk_state(
            equity=104.0,
            day_date="2026-06-04",
            week_start_date="2026-06-01",
        )
        assert next_day["day_start_equity"] == 104.0
        assert next_day["week_start_equity"] == 100.0
        assert next_day["peak_equity"] == 105.0


@pytest.mark.asyncio
async def test_runtime_config_bool_roundtrip_and_default():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "runtime_config.db"
        configure_db_path(db_path)
        await init_db()

        assert await get_runtime_config_value("auto_entry_enabled") is None
        assert await get_runtime_config_bool("auto_entry_enabled", default=False) is False
        assert await set_runtime_config_value("auto_entry_enabled", "true") is True
        assert await get_runtime_config_value("auto_entry_enabled") == "true"
        assert await get_runtime_config_bool("auto_entry_enabled", default=False) is True
        assert await get_runtime_config_values() == {"auto_entry_enabled": "true"}


@pytest.mark.asyncio
async def test_runtime_config_int_roundtrip_and_bounds():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "runtime_config_int.db"
        configure_db_path(db_path)
        await init_db()

        assert await get_runtime_config_int("max_new_positions_per_day", default=1, minimum=1, maximum=3) == 1
        assert await set_runtime_config_value("max_new_positions_per_day", "3") is True
        assert await get_runtime_config_int("max_new_positions_per_day", default=1, minimum=1, maximum=3) == 3

        assert await set_runtime_config_value("max_new_positions_per_day", "4") is True
        with pytest.raises(ValueError, match="above maximum"):
            await get_runtime_config_int("max_new_positions_per_day", default=1, minimum=1, maximum=3)


@pytest.mark.asyncio
async def test_signal_log_persists_features_json():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "signals.db"
        configure_db_path(db_path)
        await init_db()

        signal_id = await log_signal(
            symbol="poet",
            thesis="rules found momentum",
            confidence=0.72,
            source="rules_fallback",
            model_tag="rules_fallback/v0",
            features={"finnhub": {"quote": {"current": 14.2}}},
        )

        assert signal_id is not None
        with sqlite3.connect(db_path) as conn:
            row = conn.execute(
                "SELECT symbol, source, features_json FROM signals WHERE id = ?",
                (signal_id,),
            ).fetchone()

        assert row[0] == "POET"
        assert row[1] == "rules_fallback"
        assert json.loads(row[2])["finnhub"]["quote"]["current"] == 14.2


@pytest.mark.asyncio
async def test_finnhub_client_enriches_symbol_with_compact_payload(monkeypatch):
    client = FinnhubClient("test-key")

    async def fake_get_json(path, params, *, endpoint):
        if endpoint == "quote":
            return {"c": 14.2, "d": 0.3, "dp": 2.1, "h": 14.5, "l": 13.7, "o": 13.9, "pc": 13.9}
        if endpoint == "profile2":
            return {
                "name": "POET Technologies Inc",
                "ticker": "POET",
                "exchange": "NASDAQ",
                "finnhubIndustry": "Semiconductors",
                "marketCapitalization": 900.0,
                "shareOutstanding": 65.0,
            }
        if endpoint == "company-news":
            return [{"headline": "POET headline", "source": "Wire", "datetime": 1_717_200_000, "url": "https://example.com"}]
        raise AssertionError(endpoint)

    monkeypatch.setattr(client, "_get_json", fake_get_json)

    enriched = await client.enrich_symbol("POET")

    assert enriched["enabled"] is True
    assert enriched["quote"]["current"] == 14.2
    assert enriched["profile"]["industry"] == "Semiconductors"
    assert enriched["news"][0]["headline"] == "POET headline"


@pytest.mark.asyncio
async def test_rules_signals_attach_finnhub_enrichment(monkeypatch):
    async def fake_discover(adapter, *, max_assets=750, batch_size=100, max_candidates=10):
        return [
            DiscoveryCandidate(
                symbol="POET",
                price=14.2,
                score=6.0,
                dollar_volume=3_000_000,
                rel_volume=2.0,
                change_pct=0.04,
                spread_pct=0.002,
                rationale="candidate rationale",
            )
        ]

    class FakeFinnhub:
        enabled = True

        async def enrich_symbol(self, symbol):
            return {"provider": "finnhub", "enabled": True, "quote": {"current": 14.2}}

    monkeypatch.setattr("auto_trader.intelligence.rules_fallback.discover_dynamic_candidates", fake_discover)

    signals = await get_simple_rules_signals(object(), max_signals=1, finnhub_client=FakeFinnhub())

    assert signals[0].symbol == "POET"
    assert signals[0].features["discovery"]["provider"] == "alpaca"
    assert signals[0].features["finnhub"]["quote"]["current"] == 14.2


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


def test_risk_engine_uses_runtime_entry_cap_from_snapshot():
    sm = StateMachine(initial_state=SystemState.ACTIVE)
    risk = RiskEngine(sm, DummySettings())
    intent = TradeIntent(symbol="MSFT", side="long", entry_price=23.89)

    class RuntimeCapSnapshot:
        equity = 100.0
        open_positions = []
        today_new_entries = 1
        max_new_positions_per_day = 3

    decision = risk.evaluate(intent, RuntimeCapSnapshot())

    assert decision.approved is True
    assert decision.sized_quantity is not None


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
                "broker_order_id": "d08fb2a8-7df4-4da5-b3b5-d4c939be1fde",
                "symbol": "AMPX",
                "status": "accepted",
                "qty": 0.832986,
                "avg_fill_price": None,
            }
        ],
        "broker_orders": [
            {
                "broker_order_id": "d08fb2a8-7df4-4da5-b3b5-d4c939be1fde",
                "symbol": "AMPX",
                "status": "accepted",
                "qty": 0.832986,
            }
        ],
        "pending_exits": [
            {
                "symbol": "AMPX",
                "broker_order_id": "d08fb2a8-7df4-4da5-b3b5-d4c939be1fde",
                "reason": "position max loss reached",
                "qty": 0.832986,
                "status": "pending",
            }
        ],
        "journal_entries": [
            {
                "created_at": "2026-06-02T20:34:23Z",
                "content": "Auto-exit submitted for AMPX: position max loss reached.",
            }
        ],
        "reconciled": 1,
        "today_new_entries": 0,
        "runtime_config": {"auto_entry_enabled": "true"},
        "errors": [],
    }

    status = bot._build_status_message(snapshot)
    report = bot._build_report_message(snapshot)

    assert "AUTO-TRADER STATUS" in status
    assert "Equity: $100.00" in status
    assert "State allows trading: True" in status
    assert "New entries: blocked by open-position limit" in status
    assert "AMPX: qty 0.832986" in status
    assert "accepted" in status
    assert "Pending exits:" in status
    assert "duplicate exits suppressed" in status
    assert "Orders reconciled: 1" in status
    assert "DAILY REPORT" in report
    assert "Open unrealized P/L: $-0.04" in report
    assert "Journal:" in report
    assert "Auto-exit submitted for AMPX" in report


@pytest.mark.asyncio
async def test_telegram_shutdown_is_idempotent():
    sm = StateMachine(initial_state=SystemState.ACTIVE)
    bot = TelegramBot(
        token="token",
        state_machine=sm,
        risk_engine=RiskEngine(sm, DummySettings()),
        adapter=object(),
        resume_token="resume",
    )

    class FakeUpdater:
        def __init__(self):
            self.stop_calls = 0

        async def stop(self):
            self.stop_calls += 1
            if self.stop_calls > 1:
                raise RuntimeError("This Updater is not running!")

    class FakeApp:
        def __init__(self):
            self.updater = FakeUpdater()
            self.stop_calls = 0
            self.shutdown_calls = 0

        async def stop(self):
            self.stop_calls += 1

        async def shutdown(self):
            self.shutdown_calls += 1

    app = FakeApp()
    bot.app = app

    await bot.shutdown()
    await bot.shutdown()

    assert app.updater.stop_calls == 1
    assert app.stop_calls == 1
    assert app.shutdown_calls == 1


@pytest.mark.asyncio
async def test_telegram_shutdown_tolerates_already_stopped_updater():
    sm = StateMachine(initial_state=SystemState.ACTIVE)
    bot = TelegramBot(
        token="token",
        state_machine=sm,
        risk_engine=RiskEngine(sm, DummySettings()),
        adapter=object(),
        resume_token="resume",
    )

    class FakeUpdater:
        async def stop(self):
            raise RuntimeError("This Updater is not running!")

    class FakeApp:
        def __init__(self):
            self.updater = FakeUpdater()
            self.stop_calls = 0
            self.shutdown_calls = 0

        async def stop(self):
            self.stop_calls += 1

        async def shutdown(self):
            self.shutdown_calls += 1

    app = FakeApp()
    bot.app = app

    await bot.shutdown()

    assert app.stop_calls == 1
    assert app.shutdown_calls == 1


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
            "runtime_config": {"auto_entry_enabled": "true"},
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
            "runtime_config": {"auto_entry_enabled": "true"},
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
            "runtime_config": {"auto_entry_enabled": "true"},
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
            "runtime_config": {"auto_entry_enabled": "true", "max_new_positions_per_day": "3"},
            "errors": [],
        }

    bot._bounded_snapshot = fake_snapshot
    update = FakeTelegramUpdate(chat_id=123, user_id=456)

    await bot._status_handler(update, object())

    assert "AUTO-TRADER STATUS" in update.message.replies[0]
    assert "New entries: allowed" in update.message.replies[0]
    assert "Today new entries: 0 / 3" in update.message.replies[0]


def test_telegram_status_surfaces_runtime_auto_entry_disabled():
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
            "today_new_entries": 0,
            "runtime_config": {"auto_entry_enabled": "false"},
            "errors": [],
        }
    )

    assert "Runtime auto-entry: False" in status
    assert "New entries: disabled by runtime config" in status


@pytest.mark.asyncio
async def test_telegram_config_handler_sets_auto_entry(monkeypatch):
    sm = StateMachine(initial_state=SystemState.ACTIVE)
    stored = {}
    journal = []

    async def fake_set(key, value):
        stored[key] = value
        return True

    async def fake_values():
        return stored

    async def fake_bool(key, *, default):
        value = stored.get(key)
        return default if value is None else value == "true"

    async def fake_int(key, *, default, minimum=None, maximum=None):
        value = stored.get(key)
        return default if value is None else int(value)

    async def fake_journal(**kwargs):
        journal.append(kwargs["content"])
        return 1

    monkeypatch.setattr("auto_trader.comms.telegram_bot.set_runtime_config_value", fake_set)
    monkeypatch.setattr("auto_trader.comms.telegram_bot.get_runtime_config_values", fake_values)
    monkeypatch.setattr("auto_trader.comms.telegram_bot.get_runtime_config_bool", fake_bool)
    monkeypatch.setattr("auto_trader.comms.telegram_bot.get_runtime_config_int", fake_int)
    monkeypatch.setattr("auto_trader.comms.telegram_bot.append_journal_entry", fake_journal)

    bot = TelegramBot(
        token="token",
        state_machine=sm,
        risk_engine=RiskEngine(sm, DummySettings()),
        adapter=object(),
        resume_token="resume",
        allowed_ids=[123],
    )
    update = FakeTelegramUpdate(chat_id=123, user_id=456)

    await bot._config_handler(update, FakeTelegramContext(["auto_entry", "on"]))

    assert stored == {"auto_entry_enabled": "true"}
    assert update.message.replies == ["Runtime auto-entry set to True."]
    assert journal == ["Runtime config updated: auto_entry_enabled=True."]


@pytest.mark.asyncio
async def test_telegram_config_handler_sets_max_entries(monkeypatch):
    sm = StateMachine(initial_state=SystemState.ACTIVE)
    stored = {}
    journal = []

    class PaperAdapter:
        paper = True

    async def fake_set(key, value):
        stored[key] = value
        return True

    async def fake_journal(**kwargs):
        journal.append(kwargs["content"])
        return 1

    monkeypatch.setattr("auto_trader.comms.telegram_bot.set_runtime_config_value", fake_set)
    monkeypatch.setattr("auto_trader.comms.telegram_bot.append_journal_entry", fake_journal)

    bot = TelegramBot(
        token="token",
        state_machine=sm,
        risk_engine=RiskEngine(sm, DummySettings()),
        adapter=PaperAdapter(),
        resume_token="resume",
        allowed_ids=[123],
    )
    update = FakeTelegramUpdate(chat_id=123, user_id=456)

    await bot._config_handler(update, FakeTelegramContext(["max_entries", "3"]))

    assert stored == {"max_new_positions_per_day": "3"}
    assert update.message.replies == ["Runtime max entries per day set to 3."]
    assert journal == ["Runtime config updated: max_new_positions_per_day=3."]


@pytest.mark.asyncio
async def test_telegram_config_handler_shows_runtime_config(monkeypatch):
    sm = StateMachine(initial_state=SystemState.ACTIVE)

    async def fake_values():
        return {"auto_entry_enabled": "true", "max_new_positions_per_day": "3"}

    async def fake_bool(key, *, default):
        return True

    async def fake_int(key, *, default, minimum=None, maximum=None):
        return 3

    monkeypatch.setattr("auto_trader.comms.telegram_bot.get_runtime_config_values", fake_values)
    monkeypatch.setattr("auto_trader.comms.telegram_bot.get_runtime_config_bool", fake_bool)
    monkeypatch.setattr("auto_trader.comms.telegram_bot.get_runtime_config_int", fake_int)

    class PaperAdapter:
        paper = True

    bot = TelegramBot(
        token="token",
        state_machine=sm,
        risk_engine=RiskEngine(sm, DummySettings()),
        adapter=PaperAdapter(),
        resume_token="resume",
        allowed_ids=[123],
    )
    update = FakeTelegramUpdate(chat_id=123, user_id=456)

    await bot._config_handler(update, FakeTelegramContext())

    assert "RUNTIME CONFIG" in update.message.replies[0]
    assert "auto_entry_enabled: True (runtime)" in update.message.replies[0]
    assert "max_new_positions_per_day: 3 (runtime)" in update.message.replies[0]


@pytest.mark.asyncio
async def test_telegram_unauthorized_config_does_not_update(monkeypatch):
    sm = StateMachine(initial_state=SystemState.ACTIVE)
    called = {"set": 0}

    async def fake_set(key, value):
        called["set"] += 1
        return True

    monkeypatch.setattr("auto_trader.comms.telegram_bot.set_runtime_config_value", fake_set)

    bot = TelegramBot(
        token="token",
        state_machine=sm,
        risk_engine=RiskEngine(sm, DummySettings()),
        adapter=object(),
        resume_token="resume",
        allowed_ids=[999],
    )
    update = FakeTelegramUpdate(chat_id=123, user_id=456)

    await bot._config_handler(update, FakeTelegramContext(["auto_entry", "on"]))

    assert called["set"] == 0
    assert update.message.replies == ["Unauthorized."]


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

    async def fake_pending_exits(limit=5):
        return [{"symbol": "AMPX", "broker_order_id": "order-1", "reason": "position max loss reached", "qty": 0.832986}]

    async def fake_journal(limit=3):
        return [{"content": "Auto-exit submitted for AMPX.", "created_at": "2026-06-02T20:34:23Z"}]

    async def fake_runtime_config():
        return {"auto_entry_enabled": "true"}

    monkeypatch.setattr("auto_trader.comms.telegram_bot.reconcile_broker_orders", fake_reconcile)
    monkeypatch.setattr("auto_trader.comms.telegram_bot.get_latest_order_records", fake_latest)
    monkeypatch.setattr("auto_trader.comms.telegram_bot.count_entry_orders_since", fake_count)
    monkeypatch.setattr("auto_trader.comms.telegram_bot.get_pending_exits", fake_pending_exits)
    monkeypatch.setattr("auto_trader.comms.telegram_bot.get_latest_journal_entries", fake_journal)
    monkeypatch.setattr("auto_trader.comms.telegram_bot.get_runtime_config_values", fake_runtime_config)

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
    assert snapshot["pending_exits"][0]["symbol"] == "AMPX"
    assert snapshot["journal_entries"][0]["content"] == "Auto-exit submitted for AMPX."
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

    async def fake_pending_exits(limit=5):
        return []

    async def fake_journal(limit=3):
        return []

    async def fake_runtime_config():
        return {}

    monkeypatch.setattr("auto_trader.comms.telegram_bot.reconcile_broker_orders", fake_reconcile)
    monkeypatch.setattr("auto_trader.comms.telegram_bot.get_latest_order_records", fake_latest)
    monkeypatch.setattr("auto_trader.comms.telegram_bot.count_entry_orders_since", fake_count)
    monkeypatch.setattr("auto_trader.comms.telegram_bot.get_pending_exits", fake_pending_exits)
    monkeypatch.setattr("auto_trader.comms.telegram_bot.get_latest_journal_entries", fake_journal)
    monkeypatch.setattr("auto_trader.comms.telegram_bot.get_runtime_config_values", fake_runtime_config)

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

    async def fake_pending_exits(limit=5):
        return []

    async def fake_journal(limit=3):
        return []

    async def fake_runtime_config():
        return {}

    monkeypatch.setattr("auto_trader.comms.telegram_bot.reconcile_broker_orders", fake_reconcile)
    monkeypatch.setattr("auto_trader.comms.telegram_bot.get_latest_order_records", fake_latest)
    monkeypatch.setattr("auto_trader.comms.telegram_bot.count_entry_orders_since", fake_count)
    monkeypatch.setattr("auto_trader.comms.telegram_bot.get_pending_exits", fake_pending_exits)
    monkeypatch.setattr("auto_trader.comms.telegram_bot.get_latest_journal_entries", fake_journal)
    monkeypatch.setattr("auto_trader.comms.telegram_bot.get_runtime_config_values", fake_runtime_config)

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
    patch_empty_pending_exit_state(monkeypatch)

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

        async def get_open_orders(self):
            return []

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
    patch_empty_pending_exit_state(monkeypatch)

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
    assert not any("EXIT SUPPRESSED: close order already submitted for AMPX" in message for message in notifications)


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

        async def get_open_orders(self):
            return []

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
    patch_empty_pending_exit_state(monkeypatch)

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
    assert not any("EXIT SUPPRESSED: close order already submitted for AMPX" in message for message in notifications)


@pytest.mark.asyncio
async def test_pending_exit_persistence_roundtrip():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "pending_exit.db"
        configure_db_path(db_path)
        await init_db()

        assert await get_pending_exit_symbols() == set()
        assert await upsert_pending_exit(
            "AMPX",
            {"id": "exit-1", "client_order_id": "exit-1", "broker_order_id": "exit-1", "qty": 1},
            reason="position max loss reached",
        )

        pending = await get_pending_exit_for_symbol("AMPX")
        assert pending is not None
        assert pending["symbol"] == "AMPX"
        assert pending["broker_order_id"] == "exit-1"
        assert await get_pending_exit_symbols() == {"AMPX"}
        pending_list = await get_pending_exits()
        assert pending_list[0]["symbol"] == "AMPX"
        assert pending_list[0]["reason"] == "position max loss reached"

        assert await clear_pending_exit("AMPX")
        assert await get_pending_exit_for_symbol("AMPX") is None
        assert await get_pending_exit_symbols() == set()


@pytest.mark.asyncio
async def test_journal_entry_roundtrip():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "journal.db"
        configure_db_path(db_path)
        await init_db()

        entry_id = await append_journal_entry(
            date="2026-06-02",
            content="Auto-exit submitted for AMPX: position max loss reached.",
        )

        assert entry_id is not None
        entries = await get_latest_journal_entries()
        assert entries[0]["date"] == "2026-06-02"
        assert entries[0]["kind"] == "daily"
        assert "AMPX" in entries[0]["content"]


@pytest.mark.asyncio
async def test_supervisor_persisted_pending_exit_suppresses_close_after_restart(monkeypatch):
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
                    "market_value": 94.0,
                    "unrealized_pl": -6.0,
                    "cost_basis": 100.0,
                }
            ]

        async def get_open_orders(self):
            return [
                {
                    "id": "prior-exit",
                    "client_order_id": "prior-exit",
                    "broker_order_id": "prior-exit",
                    "symbol": "AMPX",
                    "side": "sell",
                    "qty": 1,
                    "status": "accepted",
                }
            ]

        async def close_position(self, symbol, reason):
            self.close_calls += 1
            return {"id": "exit-duplicate", "symbol": symbol, "rationale": reason}

    async def fake_reconcile(orders):
        return 0

    async def fake_count(start_utc_iso):
        return 0

    async def fake_latest_entry(symbol):
        return None

    async def fake_pending_symbols():
        return {"AMPX"}

    async def fake_pending_lookup(symbol):
        return {"symbol": symbol, "broker_order_id": "prior-exit"}

    async def fake_pending_clear(symbol):
        return True

    async def fake_account_risk_state(*, equity, day_date, week_start_date):
        return {
            "equity": equity,
            "day_date": day_date,
            "day_start_equity": equity,
            "daily_loss_pct": 0.0,
            "week_start_date": week_start_date,
            "week_start_equity": equity,
            "weekly_loss_pct": 0.0,
            "peak_equity": equity,
            "peak_drawdown_pct": 0.0,
        }

    async def fake_runtime_config_bool(key, *, default):
        return default

    async def fake_runtime_config_int(key, *, default, minimum=None, maximum=None):
        return default

    async def fake_notify(message):
        notifications.append(message)

    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.reconcile_broker_orders", fake_reconcile)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.count_entry_orders_since", fake_count)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_latest_entry_order_for_symbol", fake_latest_entry)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_pending_exit_symbols", fake_pending_symbols)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_pending_exit_for_symbol", fake_pending_lookup)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.clear_pending_exit", fake_pending_clear)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.update_account_risk_state", fake_account_risk_state)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_runtime_config_bool", fake_runtime_config_bool)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_runtime_config_int", fake_runtime_config_int)

    adapter = FakeAdapter()
    supervisor = TradingSupervisor(
        settings=ExitSettings(),
        state_machine=sm,
        adapter=adapter,
        order_manager=object(),
        notifier=fake_notify,
    )

    result = await supervisor.tick_once()

    assert result.exit_decisions[0].reason == "position max loss reached"
    assert adapter.close_calls == 0
    assert notifications == []


@pytest.mark.asyncio
async def test_supervisor_unresolved_persisted_pending_exit_pauses_for_review(monkeypatch):
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
                    "market_value": 94.0,
                    "unrealized_pl": -6.0,
                    "cost_basis": 100.0,
                }
            ]

        async def get_open_orders(self):
            return []

        async def close_position(self, symbol, reason):
            self.close_calls += 1
            return {"id": "exit-duplicate", "symbol": symbol, "rationale": reason}

    async def fake_reconcile(orders):
        return 0

    async def fake_count(start_utc_iso):
        return 0

    async def fake_latest_entry(symbol):
        return None

    async def fake_pending_symbols():
        return {"AMPX"}

    async def fake_pending_lookup(symbol):
        return {"symbol": symbol, "broker_order_id": "old-pending-close"}

    async def fake_pending_clear(symbol):
        return True

    async def fake_account_risk_state(*, equity, day_date, week_start_date):
        return {
            "equity": equity,
            "day_date": day_date,
            "day_start_equity": equity,
            "daily_loss_pct": 0.0,
            "week_start_date": week_start_date,
            "week_start_equity": equity,
            "weekly_loss_pct": 0.0,
            "peak_equity": equity,
            "peak_drawdown_pct": 0.0,
        }

    async def fake_notify(message):
        notifications.append(message)

    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.reconcile_broker_orders", fake_reconcile)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.count_entry_orders_since", fake_count)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_latest_entry_order_for_symbol", fake_latest_entry)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_pending_exit_symbols", fake_pending_symbols)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_pending_exit_for_symbol", fake_pending_lookup)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.clear_pending_exit", fake_pending_clear)

    adapter = FakeAdapter()
    supervisor = TradingSupervisor(
        settings=ExitSettings(),
        state_machine=sm,
        adapter=adapter,
        order_manager=object(),
        notifier=fake_notify,
    )

    result = await supervisor.tick_once()

    assert result.exit_decisions[0].reason == "position max loss reached"
    assert adapter.close_calls == 0
    assert sm.state == SystemState.PAUSED
    assert any("EXIT NEEDS REVIEW: persisted pending close exists for AMPX" in message for message in notifications)


@pytest.mark.asyncio
async def test_supervisor_unmatched_open_close_order_does_not_resolve_persisted_pending(monkeypatch):
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
                    "market_value": 94.0,
                    "unrealized_pl": -6.0,
                    "cost_basis": 100.0,
                }
            ]

        async def get_open_orders(self):
            return [
                {
                    "id": "manual-other-close",
                    "client_order_id": "manual-other-close",
                    "broker_order_id": "manual-other-close",
                    "symbol": "AMPX",
                    "side": "sell",
                    "qty": 0.5,
                    "status": "accepted",
                }
            ]

        async def close_position(self, symbol, reason):
            self.close_calls += 1
            return {"id": "exit-duplicate", "symbol": symbol, "rationale": reason}

    async def fake_reconcile(orders):
        return 0

    async def fake_count(start_utc_iso):
        return 0

    async def fake_latest_entry(symbol):
        return None

    async def fake_pending_symbols():
        return {"AMPX"}

    async def fake_pending_lookup(symbol):
        return {"symbol": symbol, "broker_order_id": "old-pending-close", "client_order_id": "old-pending-close"}

    async def fake_pending_clear(symbol):
        return True

    async def fake_account_risk_state(*, equity, day_date, week_start_date):
        return {
            "equity": equity,
            "day_date": day_date,
            "day_start_equity": equity,
            "daily_loss_pct": 0.0,
            "week_start_date": week_start_date,
            "week_start_equity": equity,
            "weekly_loss_pct": 0.0,
            "peak_equity": equity,
            "peak_drawdown_pct": 0.0,
        }

    async def fake_notify(message):
        notifications.append(message)

    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.reconcile_broker_orders", fake_reconcile)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.count_entry_orders_since", fake_count)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_latest_entry_order_for_symbol", fake_latest_entry)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_pending_exit_symbols", fake_pending_symbols)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_pending_exit_for_symbol", fake_pending_lookup)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.clear_pending_exit", fake_pending_clear)

    adapter = FakeAdapter()
    supervisor = TradingSupervisor(
        settings=ExitSettings(),
        state_machine=sm,
        adapter=adapter,
        order_manager=object(),
        notifier=fake_notify,
    )

    result = await supervisor.tick_once()

    assert result.exit_decisions[0].reason == "position max loss reached"
    assert adapter.close_calls == 0
    assert sm.state == SystemState.PAUSED
    assert any("EXIT NEEDS REVIEW: persisted pending close exists for AMPX" in message for message in notifications)


@pytest.mark.asyncio
async def test_supervisor_broker_open_close_order_suppresses_and_persists(monkeypatch):
    sm = StateMachine(initial_state=SystemState.ACTIVE)
    notifications = []
    pending_upserts = []

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
                    "market_value": 94.0,
                    "unrealized_pl": -6.0,
                    "cost_basis": 100.0,
                }
            ]

        async def get_open_orders(self):
            return [
                {
                    "id": "broker-open-close",
                    "client_order_id": "broker-open-close",
                    "broker_order_id": "broker-open-close",
                    "symbol": "AMPX",
                    "side": "sell",
                    "qty": 1,
                    "status": "accepted",
                }
            ]

        async def close_position(self, symbol, reason):
            self.close_calls += 1
            return {"id": "exit-duplicate", "symbol": symbol, "rationale": reason}

    async def fake_reconcile(orders):
        return 0

    async def fake_count(start_utc_iso):
        return 0

    async def fake_latest_entry(symbol):
        return None

    async def fake_pending_symbols():
        return set()

    async def fake_pending_lookup(symbol):
        return None

    async def fake_pending_upsert(symbol, order=None, reason=None):
        pending_upserts.append((symbol, order, reason))
        return True

    async def fake_pending_clear(symbol):
        return True

    async def fake_runtime_config_bool(key, *, default):
        return default

    async def fake_runtime_config_int(key, *, default, minimum=None, maximum=None):
        return default

    async def fake_notify(message):
        notifications.append(message)

    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.reconcile_broker_orders", fake_reconcile)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.count_entry_orders_since", fake_count)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_latest_entry_order_for_symbol", fake_latest_entry)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_pending_exit_symbols", fake_pending_symbols)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_pending_exit_for_symbol", fake_pending_lookup)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.upsert_pending_exit", fake_pending_upsert)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.clear_pending_exit", fake_pending_clear)
    monkeypatch.setattr(
        "auto_trader.scheduler.trading_supervisor.update_account_risk_state",
        fake_healthy_account_risk_state,
    )
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_runtime_config_bool", fake_runtime_config_bool)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_runtime_config_int", fake_runtime_config_int)

    adapter = FakeAdapter()
    supervisor = TradingSupervisor(
        settings=ExitSettings(),
        state_machine=sm,
        adapter=adapter,
        order_manager=object(),
        notifier=fake_notify,
    )

    result = await supervisor.tick_once()

    assert result.exit_decisions[0].reason == "position max loss reached"
    assert adapter.close_calls == 0
    assert pending_upserts[0][0] == "AMPX"
    assert pending_upserts[0][1]["broker_order_id"] == "broker-open-close"
    assert notifications == []


@pytest.mark.asyncio
async def test_supervisor_broker_open_close_order_persistence_failure_pauses_and_alerts(monkeypatch):
    sm = StateMachine(initial_state=SystemState.ACTIVE)
    notifications = []

    class ExitSettings(DummySupervisorSettings):
        auto_exit_enabled = True

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
            return [
                {
                    "symbol": "AMPX",
                    "qty": 1,
                    "market_value": 94.0,
                    "unrealized_pl": -6.0,
                    "cost_basis": 100.0,
                }
            ]

        async def get_open_orders(self):
            return [
                {
                    "id": "broker-open-close",
                    "client_order_id": "broker-open-close",
                    "broker_order_id": "broker-open-close",
                    "symbol": "AMPX",
                    "side": "sell",
                    "qty": 1,
                    "status": "accepted",
                }
            ]

        async def close_position(self, symbol, reason):
            raise AssertionError("close_position must not be called when broker already has a close order")

    async def fake_reconcile(orders):
        return 0

    async def fake_count(start_utc_iso):
        return 0

    async def fake_latest_entry(symbol):
        return None

    async def fake_pending_symbols():
        return set()

    async def fake_pending_lookup(symbol):
        return None

    async def fake_pending_upsert(symbol, order=None, reason=None):
        return False

    async def fake_pending_clear(symbol):
        return True

    async def fake_notify(message):
        notifications.append(message)

    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.reconcile_broker_orders", fake_reconcile)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.count_entry_orders_since", fake_count)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_latest_entry_order_for_symbol", fake_latest_entry)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_pending_exit_symbols", fake_pending_symbols)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_pending_exit_for_symbol", fake_pending_lookup)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.upsert_pending_exit", fake_pending_upsert)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.clear_pending_exit", fake_pending_clear)

    supervisor = TradingSupervisor(
        settings=ExitSettings(),
        state_machine=sm,
        adapter=FakeAdapter(),
        order_manager=object(),
        notifier=fake_notify,
    )

    result = await supervisor.tick_once()

    assert result.exit_decisions[0].reason == "position max loss reached"
    assert sm.state == SystemState.PAUSED
    assert any("local pending-exit persistence failed" in message for message in notifications)


@pytest.mark.asyncio
async def test_supervisor_clears_failed_pending_exit_and_retries_close(monkeypatch):
    sm = StateMachine(initial_state=SystemState.ACTIVE)
    notifications = []
    pending_symbols = {"AMPX"}
    pending = {"symbol": "AMPX", "broker_order_id": "failed-close", "client_order_id": "failed-close"}

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
            return [
                {
                    "id": "failed-close",
                    "client_order_id": "failed-close",
                    "broker_order_id": "failed-close",
                    "symbol": "AMPX",
                    "side": "sell",
                    "qty": 1,
                    "status": "rejected",
                }
            ]

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

        async def get_open_orders(self):
            return []

        async def close_position(self, symbol, reason):
            self.close_calls += 1
            return {
                "id": "replacement-close",
                "client_order_id": "replacement-close",
                "broker_order_id": "replacement-close",
                "symbol": symbol,
                "side": "sell",
                "qty": 1,
                "status": "submitted",
                "rationale": reason,
            }

    async def fake_reconcile(orders):
        return len(orders)

    async def fake_count(start_utc_iso):
        return 0

    async def fake_latest_entry(symbol):
        return None

    async def fake_pending_symbols():
        return set(pending_symbols)

    async def fake_pending_lookup(symbol):
        return pending if symbol == "AMPX" and symbol in pending_symbols else None

    async def fake_pending_upsert(symbol, order=None, reason=None):
        pending_symbols.add(symbol)
        pending["broker_order_id"] = order["broker_order_id"]
        pending["client_order_id"] = order["client_order_id"]
        return True

    async def fake_pending_clear(symbol):
        pending_symbols.discard(symbol)
        return True

    async def fake_upsert_order(order, **kwargs):
        return True

    async def fake_notify(message):
        notifications.append(message)

    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.reconcile_broker_orders", fake_reconcile)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.count_entry_orders_since", fake_count)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_latest_entry_order_for_symbol", fake_latest_entry)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_pending_exit_symbols", fake_pending_symbols)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_pending_exit_for_symbol", fake_pending_lookup)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.upsert_pending_exit", fake_pending_upsert)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.clear_pending_exit", fake_pending_clear)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.upsert_order_record", fake_upsert_order)

    adapter = FakeAdapter()
    supervisor = TradingSupervisor(
        settings=ExitSettings(),
        state_machine=sm,
        adapter=adapter,
        order_manager=object(),
        notifier=fake_notify,
    )

    result = await supervisor.tick_once()

    assert result.exit_decisions[0].reason == "position max loss reached"
    assert adapter.close_calls == 1
    assert pending["broker_order_id"] == "replacement-close"
    assert any("EXIT PENDING CLEARED: prior close for AMPX is rejected" in message for message in notifications)
    assert any("EXIT SUBMITTED: AMPX" in message for message in notifications)


@pytest.mark.asyncio
async def test_supervisor_clears_filled_pending_exit_from_reconciliation(monkeypatch):
    sm = StateMachine(initial_state=SystemState.ACTIVE)
    notifications = []
    journal_entries = []
    pending_symbols = {"AMPX"}
    pending = {"symbol": "AMPX", "broker_order_id": "filled-close", "client_order_id": "filled-close"}

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
            return [
                {
                    "id": "filled-close",
                    "client_order_id": "filled-close",
                    "broker_order_id": "filled-close",
                    "symbol": "AMPX",
                    "side": "sell",
                    "qty": 0.832986,
                    "status": "filled",
                }
            ]

        async def get_positions_snapshot(self, *, strict=False):
            return []

    async def fake_reconcile(orders):
        return len(orders)

    async def fake_count(start_utc_iso):
        return 0

    async def fake_pending_symbols():
        return set(pending_symbols)

    async def fake_pending_lookup(symbol):
        return pending if symbol == "AMPX" and symbol in pending_symbols else None

    async def fake_pending_clear(symbol):
        pending_symbols.discard(symbol)
        return True

    async def fake_journal_entry(**kwargs):
        journal_entries.append(kwargs["content"])
        return len(journal_entries)

    async def fake_notify(message):
        notifications.append(message)

    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.reconcile_broker_orders", fake_reconcile)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.count_entry_orders_since", fake_count)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_pending_exit_symbols", fake_pending_symbols)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_pending_exit_for_symbol", fake_pending_lookup)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.clear_pending_exit", fake_pending_clear)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.append_journal_entry", fake_journal_entry)

    supervisor = TradingSupervisor(
        settings=DummySupervisorSettings(),
        state_machine=sm,
        adapter=FakeAdapter(),
        order_manager=object(),
        notifier=fake_notify,
    )
    supervisor._pending_exit_symbols.add("AMPX")

    result = await supervisor.tick_once()

    assert result.positions == []
    assert result.exit_decisions == []
    assert pending_symbols == set()
    assert supervisor._pending_exit_symbols == set()
    assert any("EXIT COMPLETED: close order for AMPX is filled" in message for message in notifications)
    assert any("Auto-exit completed for AMPX" in entry for entry in journal_entries)


@pytest.mark.asyncio
async def test_supervisor_clears_persisted_pending_exit_after_position_disappears(monkeypatch):
    sm = StateMachine(initial_state=SystemState.ACTIVE)
    cleared = []

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

    async def fake_reconcile(orders):
        return 0

    async def fake_count(start_utc_iso):
        return 0

    async def fake_pending_symbols():
        return {"AMPX"}

    async def fake_pending_lookup(symbol):
        return {"symbol": symbol, "broker_order_id": "exit-1"}

    async def fake_pending_clear(symbol):
        cleared.append(symbol)
        return True

    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.reconcile_broker_orders", fake_reconcile)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.count_entry_orders_since", fake_count)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_pending_exit_symbols", fake_pending_symbols)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_pending_exit_for_symbol", fake_pending_lookup)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.clear_pending_exit", fake_pending_clear)

    supervisor = TradingSupervisor(
        settings=DummySupervisorSettings(),
        state_machine=sm,
        adapter=FakeAdapter(),
        order_manager=object(),
    )
    supervisor._pending_exit_symbols.add("AMPX")

    result = await supervisor.tick_once()

    assert result.positions == []
    assert result.exit_decisions == []
    assert cleared == ["AMPX"]
    assert supervisor._pending_exit_symbols == set()


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
    patch_empty_pending_exit_state(monkeypatch)

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

        async def get_open_orders(self):
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

    async def fake_signals(adapter, max_signals=1, finnhub_client=None):
        return [TradeIntent(symbol="AMPX", side="long", entry_price=20.0)]

    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.reconcile_broker_orders", fake_reconcile)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.count_entry_orders_since", fake_count)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_latest_entry_order_for_symbol", fake_latest_entry)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_simple_rules_signals", fake_signals)
    patch_empty_pending_exit_state(monkeypatch)

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
async def test_supervisor_auto_entry_uses_runtime_entry_cap(monkeypatch):
    sm = StateMachine(initial_state=SystemState.ACTIVE)

    class EntrySettings(DummySupervisorSettings):
        auto_entry_enabled = True
        max_new_positions_per_day = 1

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

        async def get_open_orders(self):
            return []

    class FakeOrderManager:
        def __init__(self):
            self.snapshots = []

        async def submit_trade_intent(self, intent, snapshot):
            self.snapshots.append(snapshot)
            return {"order": {"id": "entry-runtime-cap"}, "risk_decision": {"approved": True}}

    async def fake_reconcile(orders):
        return 0

    async def fake_count(start_utc_iso):
        return 1

    async def fake_latest_entry(symbol):
        return None

    async def fake_signals(adapter, max_signals=1, finnhub_client=None):
        return [TradeIntent(symbol="POET", side="long", entry_price=20.0)]

    async def fake_runtime_config_int(key, *, default, minimum=None, maximum=None):
        return 3

    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.reconcile_broker_orders", fake_reconcile)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.count_entry_orders_since", fake_count)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_latest_entry_order_for_symbol", fake_latest_entry)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_simple_rules_signals", fake_signals)
    patch_empty_pending_exit_state(monkeypatch)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_runtime_config_int", fake_runtime_config_int)

    manager = FakeOrderManager()
    supervisor = TradingSupervisor(
        settings=EntrySettings(),
        state_machine=sm,
        adapter=FakeAdapter(),
        order_manager=manager,
    )

    result = await supervisor.tick_once()

    assert result.entry_result["order"]["id"] == "entry-runtime-cap"
    assert manager.snapshots[0].today_new_entries == 1
    assert manager.snapshots[0].max_new_positions_per_day == 3


@pytest.mark.asyncio
async def test_supervisor_blocks_auto_entry_when_broker_has_open_entry_order(monkeypatch):
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

        async def get_open_orders(self):
            return [
                {
                    "id": "entry-open-1",
                    "symbol": "POET",
                    "side": "buy",
                    "qty": 1.0,
                    "status": "accepted",
                }
            ]

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

    async def fake_signals(adapter, max_signals=1, finnhub_client=None):
        raise AssertionError("signals should not be evaluated while an entry order is open")

    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.reconcile_broker_orders", fake_reconcile)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.count_entry_orders_since", fake_count)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_simple_rules_signals", fake_signals)
    patch_empty_pending_exit_state(monkeypatch)

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


@pytest.mark.asyncio
async def test_supervisor_account_risk_halt_flattens_and_blocks_entry(monkeypatch):
    persisted = []
    notifications = []

    async def persist(state, reason):
        persisted.append((state, reason))

    sm = StateMachine(initial_state=SystemState.ACTIVE, persist_hook=persist)

    class EntrySettings(DummySupervisorSettings):
        auto_entry_enabled = True

    class FakeAdapter:
        paper = True

        def __init__(self):
            self.cancel_calls = 0
            self.flatten_calls = 0

        async def get_account_snapshot(self):
            return {
                "status": "CONNECTED",
                "account_status": "AccountStatus.ACTIVE",
                "equity": 98.0,
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

        async def cancel_all_orders(self):
            self.cancel_calls += 1
            return 1

        async def flatten_all_positions(self):
            self.flatten_calls += 1
            return 1

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

    async def fake_account_risk_state(*, equity, day_date, week_start_date):
        return {
            "equity": equity,
            "day_date": day_date,
            "day_start_equity": 100.0,
            "daily_loss_pct": -2.0,
            "week_start_date": week_start_date,
            "week_start_equity": 100.0,
            "weekly_loss_pct": -2.0,
            "peak_equity": 100.0,
            "peak_drawdown_pct": -2.0,
        }

    async def fake_signals(adapter, max_signals=1, finnhub_client=None):
        raise AssertionError("signals should not be evaluated after an account risk halt")

    async def fake_notify(message):
        notifications.append(message)

    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.reconcile_broker_orders", fake_reconcile)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.count_entry_orders_since", fake_count)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.update_account_risk_state", fake_account_risk_state)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_simple_rules_signals", fake_signals)
    patch_empty_pending_exit_state(monkeypatch)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.update_account_risk_state", fake_account_risk_state)

    adapter = FakeAdapter()
    manager = FakeOrderManager()
    supervisor = TradingSupervisor(
        settings=EntrySettings(),
        state_machine=sm,
        adapter=adapter,
        order_manager=manager,
        notifier=fake_notify,
    )

    result = await supervisor.tick_once()

    assert sm.state == SystemState.HALTED
    assert result.entry_result is None
    assert manager.calls == 0
    assert adapter.cancel_calls == 1
    assert adapter.flatten_calls == 1
    assert persisted[0][0] == SystemState.HALTED
    assert "account risk halt" in persisted[0][1]
    assert any("ACCOUNT RISK HALT" in message for message in notifications)


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

    async def fake_signals(adapter, max_signals=1, finnhub_client=None):
        return [TradeIntent(symbol="AMPX", side="long", entry_price=20.0)]

    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.reconcile_broker_orders", fake_reconcile)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.count_entry_orders_since", fake_count)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_simple_rules_signals", fake_signals)
    patch_empty_pending_exit_state(monkeypatch)

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
