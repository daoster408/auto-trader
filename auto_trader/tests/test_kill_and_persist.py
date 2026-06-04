"""
Minimal critical safety tests for kill + HALTED persistence (Reviewer requirement).

These tests must actually exercise real DB save → load roundtrips
and verify the safety default to HALTED on failure.
"""
import asyncio
import inspect
import json
import sqlite3
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
from pydantic import ValidationError

from auto_trader.core.models import SystemState, KillResult, TradeIntent
from auto_trader.account_risk_validate import (
    AccountRiskScenario,
    ValidationGate as AccountRiskValidationGate,
    build_account_risk_validation_report,
    evaluate_account_risk_scenario,
    rehearse_supervisor_account_halt,
    validation_exit_code as account_risk_validation_exit_code,
)
from auto_trader.day3_validate import build_day3_validation_report, validation_exit_code
from auto_trader.ai_research_preflight import (
    build_ai_research_preflight_report,
    render_ai_research_preflight,
    run_ai_research_preflight,
)
import auto_trader.ai_research_smoke as ai_research_smoke
import auto_trader.ai_entry_gate_rehearsal as ai_entry_gate_rehearsal
from auto_trader.ai_entry_gate_rehearsal import run_ai_entry_gate_rehearsal, render_ai_entry_gate_rehearsal
from auto_trader.ai_research_smoke import run_ai_research_smoke
from auto_trader.ai_research_smoke import render_ai_research_smoke
from auto_trader.live_preflight import build_live_preflight_report, rehearse_halt_drill
from auto_trader.core.risk_engine import RiskEngine
from auto_trader.broker.alpaca_adapter import AlpacaAdapter
from auto_trader.comms.telegram_bot import TelegramBot
from auto_trader.config.settings import Settings
from auto_trader.core.state_machine import StateMachine
from auto_trader.execution.order_manager import OrderManager
from auto_trader.intelligence.ai_committee import (
    AnthropicResearchCommittee,
    GeminiResearchCommittee,
    MultiProviderResearchCommittee,
    OpenAIResearchCommittee,
    ResearchMemo,
    ShadowResearchCommittee,
    XAIResearchCommittee,
    aggregate_research_memos,
    build_research_packet,
    normalize_committee_output,
    packet_hash,
    create_research_committee,
    validate_committee_output,
)
from auto_trader.intelligence.finnhub_client import FinnhubClient
from auto_trader.intelligence.fred_client import CORE_RISK_SERIES, FredClient
from auto_trader.intelligence.rules_fallback import DiscoveryCandidate, get_simple_rules_signals
from auto_trader.__main__ import _acquire_single_instance_lock, _handle_signal_shutdown, _should_emergency_halt_on_shutdown
from auto_trader.scheduler.trading_supervisor import TradingSupervisor
from auto_trader.persistence.db import (
    append_journal_entry,
    consume_planned_maintenance_shutdown,
    clear_pending_exit,
    configure_db_path,
    count_ai_research_chargeable_attempts,
    count_ai_research_memos,
    count_entry_orders_since,
    get_latest_journal_entries,
    get_latest_ai_research_memos,
    get_latest_order_records,
    get_pending_exits,
    get_pending_exit_for_symbol,
    get_pending_exit_symbols,
    get_runtime_config_bool,
    get_runtime_config_int,
    get_runtime_config_value,
    get_runtime_config_values,
    init_db,
    log_risk_decision,
    log_ai_research_memo,
    log_signal,
    load_system_state,
    record_runtime_capabilities,
    reconcile_broker_orders,
    request_planned_maintenance_shutdown,
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
    ai_research_enabled = False
    ai_entry_gate_enabled = False
    ai_research_provider = "shadow"
    ai_research_providers = ""
    ai_research_model = ""
    ai_research_openai_model = ""
    ai_research_xai_model = ""
    ai_research_anthropic_model = ""
    ai_research_gemini_model = ""
    ai_research_timeout_seconds = 8
    ai_research_max_calls_per_day = 0
    ai_research_est_input_tokens = 15000
    ai_research_est_output_tokens = 2000
    ai_research_input_price_per_mtok = 5.0
    ai_research_output_price_per_mtok = 25.0
    db_path = "auto_trader.db"
    fred_api_key = None


class DummyDay3Settings:
    alpaca_paper = True
    auto_entry_enabled = False
    auto_exit_enabled = True
    shutdown_flatten_on_exit = True
    max_new_positions_per_day = 1


class DummyLivePreflightSettings(DummyDay3Settings):
    pass


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
    assert "[PASS] weekly-loss-breach" in report
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


@pytest.mark.asyncio
async def test_account_risk_supervisor_halt_rehearsal_passes():
    report, gates = await rehearse_supervisor_account_halt(
        settings=DummySupervisorSettings(),
        base_equity=400.0,
        shock_pct=-2.0,
    )

    assert "Overall: PASS" in report
    assert "[PASS] daily-loss expected breach" in report
    assert "[PASS] weekly-loss expected breach" in report
    assert "[PASS] peak-drawdown expected breach" in report
    assert "[PASS] daily-loss cancel orders called" in report
    assert "[PASS] weekly-loss flatten positions called" in report
    assert "[PASS] peak-drawdown notification emitted" in report
    assert "[PASS] peak-drawdown journal entry written" in report
    assert account_risk_validation_exit_code(gates) == 0


@pytest.mark.asyncio
async def test_live_preflight_halt_drill_passes_without_broker():
    drill = await rehearse_halt_drill()

    assert "Overall: PASS" in drill.report
    assert "[PASS] drill state halted" in drill.report
    assert "[PASS] drill cancel path called" in drill.report
    assert "[PASS] drill flatten path called" in drill.report
    assert "[PASS] drill broker isolated" in drill.report
    assert account_risk_validation_exit_code(drill.gates) == 0


def test_live_preflight_report_passes_clean_cutover_state():
    _, account_risk_gates = build_account_risk_validation_report(settings=DummySettings(), base_equity=400.0)
    halt_drill_gates = [
        AccountRiskValidationGate(name="drill", status="PASS", detail="ok"),
    ]

    report, gates = build_live_preflight_report(
        settings=DummyLivePreflightSettings(),
        system_state=SystemState.ACTIVE,
        system_meta={},
        account={
            "status": "CONNECTED",
            "account_status": "AccountStatus.ACTIVE",
            "trading_blocked": False,
            "account_blocked": False,
            "equity": 398.0,
        },
        clock={"is_open": True, "source": "alpaca"},
        positions=[],
        open_orders=[],
        pending_exits=[],
        runtime_config={
            "auto_entry_enabled": "true",
            "auto_exit_enabled": "true",
            "max_new_positions_per_day": "3",
            "runtime_capability_planned_maintenance_shutdown": "true",
            "runtime_capability_planned_maintenance_pid": "123",
        },
        account_risk_gates=account_risk_gates,
        halt_drill_gates=halt_drill_gates,
        active_service_pid=123,
        service_pid_detail="systemd MainPID=123",
        max_equity=500.0,
        max_new_positions=3,
    )

    assert "Overall: PASS" in report
    assert "[PASS] open positions clear" in report
    assert "[PASS] account-risk rehearsal passed" in report
    assert account_risk_validation_exit_code(gates) == 0


def test_live_preflight_report_fails_unsafe_cutover_state():
    _, account_risk_gates = build_account_risk_validation_report(settings=DummySettings(), base_equity=400.0)
    halt_drill_gates = [
        AccountRiskValidationGate(name="drill", status="PASS", detail="ok"),
    ]

    report, gates = build_live_preflight_report(
        settings=type(
            "LiveSettings",
            (),
            {
                "alpaca_paper": False,
                "shutdown_flatten_on_exit": False,
                "auto_entry_enabled": True,
                "auto_exit_enabled": True,
                "max_new_positions_per_day": 5,
            },
        )(),
        system_state=SystemState.PAUSED,
        system_meta={"halt_reason": "not ready"},
        account={
            "status": "CONNECTED",
            "account_status": "AccountStatus.ACTIVE",
            "trading_blocked": False,
            "account_blocked": False,
            "equity": 750.0,
        },
        clock={"is_open": True, "source": "alpaca"},
        positions=[{"symbol": "POET", "qty": 1.0}],
        open_orders=[{"symbol": "POET", "status": "accepted"}],
        pending_exits=[{"symbol": "POET"}],
        runtime_config={
            "auto_exit_enabled": "true",
            "max_new_positions_per_day": "5",
        },
        account_risk_gates=account_risk_gates,
        halt_drill_gates=halt_drill_gates,
        active_service_pid=None,
        service_pid_detail="systemctl unavailable",
        max_equity=500.0,
        max_new_positions=3,
    )

    assert "Overall: FAIL" in report
    assert "[FAIL] current mode safe for preflight" in report
    assert "[FAIL] shutdown flatten enabled" in report
    assert "[FAIL] system state active" in report
    assert "[FAIL] open positions clear" in report
    assert "[FAIL] pending exits clear" in report
    assert "[FAIL] planned deploy capability active" in report
    assert "[FAIL] auto-entry runtime intent set" in report
    assert account_risk_validation_exit_code(gates) == 2


def test_live_preflight_report_rejects_inactive_account_status_substring():
    _, account_risk_gates = build_account_risk_validation_report(settings=DummySettings(), base_equity=400.0)
    halt_drill_gates = [
        AccountRiskValidationGate(name="drill", status="PASS", detail="ok"),
    ]

    report, gates = build_live_preflight_report(
        settings=DummyLivePreflightSettings(),
        system_state=SystemState.ACTIVE,
        system_meta={},
        account={
            "status": "CONNECTED",
            "account_status": "AccountStatus.INACTIVE",
            "trading_blocked": False,
            "account_blocked": False,
            "equity": 398.0,
        },
        clock={"is_open": True, "source": "alpaca"},
        positions=[],
        open_orders=[],
        pending_exits=[],
        runtime_config={
            "auto_entry_enabled": "true",
            "auto_exit_enabled": "true",
            "max_new_positions_per_day": "3",
            "runtime_capability_planned_maintenance_shutdown": "true",
            "runtime_capability_planned_maintenance_pid": "123",
        },
        account_risk_gates=account_risk_gates,
        halt_drill_gates=halt_drill_gates,
        active_service_pid=123,
        service_pid_detail="systemd MainPID=123",
        max_equity=500.0,
        max_new_positions=3,
    )

    assert "Overall: FAIL" in report
    assert "[FAIL] broker account tradable" in report
    assert account_risk_validation_exit_code(gates) == 2


def test_live_preflight_report_rejects_stale_planned_capability_pid():
    _, account_risk_gates = build_account_risk_validation_report(settings=DummySettings(), base_equity=400.0)
    halt_drill_gates = [
        AccountRiskValidationGate(name="drill", status="PASS", detail="ok"),
    ]

    report, gates = build_live_preflight_report(
        settings=DummyLivePreflightSettings(),
        system_state=SystemState.ACTIVE,
        system_meta={},
        account={
            "status": "CONNECTED",
            "account_status": "AccountStatus.ACTIVE",
            "trading_blocked": False,
            "account_blocked": False,
            "equity": 398.0,
        },
        clock={"is_open": True, "source": "alpaca"},
        positions=[],
        open_orders=[],
        pending_exits=[],
        runtime_config={
            "auto_entry_enabled": "true",
            "auto_exit_enabled": "true",
            "max_new_positions_per_day": "3",
            "runtime_capability_planned_maintenance_shutdown": "true",
            "runtime_capability_planned_maintenance_pid": "123",
        },
        account_risk_gates=account_risk_gates,
        halt_drill_gates=halt_drill_gates,
        active_service_pid=456,
        service_pid_detail="systemd MainPID=456",
        max_equity=500.0,
        max_new_positions=3,
    )

    assert "Overall: FAIL" in report
    assert "[FAIL] planned deploy capability active" in report
    assert "marker_pid=123" in report
    assert "active_pid=456" in report
    assert account_risk_validation_exit_code(gates) == 2


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
async def test_runtime_capabilities_record_current_process_pid():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "runtime_capabilities.db"
        configure_db_path(db_path)
        await init_db()

        await record_runtime_capabilities(pid=12345)

        values = await get_runtime_config_values()
        assert values["runtime_capability_planned_maintenance_shutdown"] == "true"
        assert values["runtime_capability_planned_maintenance_pid"] == "12345"


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
async def test_ai_research_memo_persistence_roundtrip():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "ai_research.db"
        configure_db_path(db_path)
        await init_db()

        signal_id = await log_signal(
            symbol="POET",
            thesis="rules found momentum",
            confidence=0.72,
            source="rules_fallback",
            features={"discovery": {"score": 6.0}},
        )
        memo_id = await log_ai_research_memo(
            signal_id=signal_id,
            symbol="POET",
            provider="shadow",
            model_tag="shadow_ai_committee/v0",
            prompt_version="ai_research_committee/v0",
            input_hash="abc123",
            verdict="watch",
            confidence=0.66,
            used_only_provided_data=True,
            validation_passed=True,
            memo={"committee": {"judge_summary": "advisory only"}},
        )

        memos = await get_latest_ai_research_memos(limit=5)

        assert memo_id is not None
        assert len(memos) == 1
        assert memos[0]["symbol"] == "POET"
        assert memos[0]["provider"] == "shadow"
        assert memos[0]["verdict"] == "watch"
        assert memos[0]["validation_passed"] is True
        assert memos[0]["memo"]["committee"]["judge_summary"] == "advisory only"


@pytest.mark.asyncio
async def test_ai_research_chargeable_count_excludes_budget_and_shadow_rows():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "ai_chargeable.db"
        configure_db_path(db_path)
        await init_db()

        rows = [
            ("anthropic", "ai_research_budget/v0", "skip1"),
            ("anthropic", "ai_research_committee/v0", "paid1"),
            ("anthropic", "ai_research_failure/v0", "paid2"),
            ("shadow", "ai_research_committee/v0", "free1"),
            ("multi", "ai_research_failure/v0", "aggregate1"),
            ("openai", "ai_research_committee/v0", "paid3"),
        ]
        for provider, prompt_version, input_hash in rows:
            await log_ai_research_memo(
                signal_id=None,
                symbol="POET",
                provider=provider,
                model_tag=f"{provider}/model",
                prompt_version=prompt_version,
                input_hash=input_hash,
                verdict="watch",
                confidence=None,
                used_only_provided_data=True,
                validation_passed=prompt_version == "ai_research_committee/v0",
                memo={"committee": {"judge_summary": "audit"}},
            )

        assert await count_ai_research_memos(provider="anthropic", today_utc=True) == 3
        assert await count_ai_research_chargeable_attempts(provider="anthropic", today_utc=True) == 2
        assert await count_ai_research_chargeable_attempts(provider="openai", today_utc=True) == 1
        assert await count_ai_research_chargeable_attempts(provider="shadow", today_utc=True) == 0
        assert await count_ai_research_chargeable_attempts(provider="multi", today_utc=True) == 0
        assert await count_ai_research_chargeable_attempts(today_utc=True) == 3


@pytest.mark.asyncio
async def test_shadow_research_committee_validates_advisory_memo():
    committee = ShadowResearchCommittee()
    intent = TradeIntent(
        symbol="POET",
        side="long",
        entry_price=14.2,
        rationale="rules found momentum",
        confidence=0.75,
        features={
            "discovery": {
                "score": 5.5,
                "rel_volume": 2.1,
                "change_pct": 0.04,
                "spread_pct": 0.002,
            },
            "finnhub": {"provider": "finnhub", "enabled": True},
        },
    )

    memo = await committee.research(intent, signal_id=7)

    assert memo.symbol == "POET"
    assert memo.provider == "shadow"
    assert memo.prompt_version == "ai_research_committee/v0"
    assert memo.verdict == "approve"
    assert memo.used_only_provided_data is True
    assert memo.validation_passed is True
    assert memo.memo["input_packet"]["signal_id"] == 7
    context = memo.memo["input_packet"]["verified_research_context"]
    assert context["data_quality"]["uses_only_verified_packet_data"] is True
    assert "fundamental" in context["data_quality"]["missing_sections"]
    assert memo.memo["committee"]["judge_summary"]


def test_research_packet_surfaces_verified_context_lanes():
    intent = TradeIntent(
        symbol="POET",
        side="long",
        entry_price=14.2,
        rationale="rules found momentum",
        confidence=0.75,
        features={
            "research_context": {
                "market": {"provider": "alpaca", "feed": "iex"},
                "technical": {"rel_volume": 2.1, "change_pct": 0.04},
                "risk": {"positions": {"open_count": 0}},
                "macro": {"fred": {"enabled": False}},
            },
            "finnhub": {
                "provider": "finnhub",
                "enabled": True,
                "profile": {"name": "POET Technologies", "industry": "Semiconductors"},
                "news": [{"headline": "POET headline", "source": "Wire"}],
            },
        },
    )

    packet = build_research_packet(intent, signal_id=99)
    context = packet["verified_research_context"]

    assert context["market"]["provider"] == "alpaca"
    assert context["technical"]["rel_volume"] == 2.1
    assert context["fundamental"]["industry"] == "Semiconductors"
    assert context["news"][0]["headline"] == "POET headline"
    assert context["risk"]["positions"]["open_count"] == 0
    assert "market" not in context["data_quality"]["missing_sections"]
    assert "fundamental" not in context["data_quality"]["missing_sections"]


def test_research_context_tolerates_malformed_provider_shapes():
    intent = TradeIntent(
        symbol="POET",
        side="long",
        entry_price=14.2,
        features={
            "research_context": {
                "market": {"quote": "bad-shape"},
                "risk": {"positions": {"symbols": ["POET"]}},
            },
            "finnhub": {
                "provider": "finnhub",
                "enabled": True,
                "quote": "bad-shape",
                "profile": ["bad-shape"],
                "news": [{"headline": "valid headline"}, "bad item"],
            },
        },
    )

    packet = build_research_packet(intent, signal_id=100)
    context = packet["verified_research_context"]

    assert context["market"]["quote"] == "bad-shape"
    assert context["news"] == [{"headline": "valid headline", "source": None, "published_at": None, "url": None}]
    assert "fundamental" in context["data_quality"]["missing_sections"]


def test_ai_committee_validator_rejects_unverified_data():
    valid, errors = validate_committee_output(
        "POET",
        {"symbol": "POET", "verdict": "approve", "confidence": 0.7, "used_only_provided_data": False},
    )

    assert valid is False
    assert "used_unverified_data" in errors


def test_ai_committee_validator_rejects_missing_required_fields():
    valid, errors = validate_committee_output(
        "POET",
        {"symbol": "POET", "verdict": "watch", "used_only_provided_data": True},
    )

    assert valid is False
    assert "missing_confidence" in errors
    assert "missing_bull_case" in errors
    assert "missing_bear_case" in errors
    assert "missing_judge_summary" in errors


def test_research_committee_factory_wires_real_providers_and_requires_keys():
    class Base:
        ai_research_model = "explicit-model"
        ai_research_timeout_seconds = 4
        openai_api_key = "openai-key"
        xai_api_key = "xai-key"
        anthropic_api_key = "anthropic-key"
        gemini_api_key = "gemini-key"

    class OpenAISettings(Base):
        ai_research_provider = "openai"

    class XAISettings(Base):
        ai_research_provider = "xai"

    class AnthropicSettings(Base):
        ai_research_provider = "anthropic"

    class GeminiSettings(Base):
        ai_research_provider = "gemini"

    class MissingOpenAI(Base):
        ai_research_provider = "openai"
        openai_api_key = ""

    class MissingModel(Base):
        ai_research_provider = "openai"
        ai_research_model = ""

    assert isinstance(create_research_committee(OpenAISettings()), OpenAIResearchCommittee)
    assert create_research_committee(OpenAISettings()).model == "explicit-model"
    assert create_research_committee(OpenAISettings()).timeout_seconds == 4
    assert isinstance(create_research_committee(XAISettings()), XAIResearchCommittee)
    assert create_research_committee(XAISettings()).model == "explicit-model"
    assert isinstance(create_research_committee(AnthropicSettings()), AnthropicResearchCommittee)
    assert create_research_committee(AnthropicSettings()).model == "explicit-model"
    assert isinstance(create_research_committee(GeminiSettings()), GeminiResearchCommittee)
    assert create_research_committee(GeminiSettings()).model == "explicit-model"
    with pytest.raises(ValueError, match="requires OPENAI_API_KEY"):
        create_research_committee(MissingOpenAI())
    with pytest.raises(ValueError, match="requires AI_RESEARCH_OPENAI_MODEL or AI_RESEARCH_MODEL"):
        create_research_committee(MissingModel())


def test_research_committee_factory_wires_multi_provider_models():
    class MultiSettings(DummySupervisorSettings):
        ai_research_enabled = True
        ai_research_providers = "anthropic,openai,xai"
        ai_research_model = ""
        ai_research_anthropic_model = "claude-opus-4-8"
        ai_research_openai_model = "gpt-5.5"
        ai_research_xai_model = "grok-4.3"
        anthropic_api_key = "anthropic-key"
        openai_api_key = "openai-key"
        xai_api_key = "xai-key"

    committee = create_research_committee(MultiSettings())

    assert isinstance(committee, MultiProviderResearchCommittee)
    assert committee.provider_names == ["anthropic", "openai", "xai"]
    assert [member.model for member in committee.members] == ["claude-opus-4-8", "gpt-5.5", "grok-4.3"]


def _provider_memo(provider: str, verdict: str, *, confidence: float = 0.7, valid: bool = True) -> ResearchMemo:
    return ResearchMemo(
        symbol="POET",
        provider=provider,
        model_tag=f"{provider}/model",
        prompt_version="ai_research_committee/v0" if valid else "ai_research_failure/v0",
        input_hash="hash123",
        verdict=verdict,
        confidence=confidence if valid else None,
        used_only_provided_data=True,
        validation_passed=valid,
        memo={
            "committee": {
                "symbol": "POET",
                "verdict": verdict,
                "confidence": confidence if valid else None,
                "used_only_provided_data": True,
                "bull_case": f"{provider} bull",
                "bear_case": f"{provider} bear",
                "judge_summary": f"{provider} summary",
                "validation_errors": [] if valid else ["ai_research_provider_failed"],
            }
        },
    )


def test_multi_provider_aggregate_approves_only_two_valid_approves_no_reject():
    packet = build_research_packet(TradeIntent(symbol="POET", side="long", entry_price=10.0, confidence=0.7))
    aggregate = aggregate_research_memos(
        "POET",
        [
            _provider_memo("anthropic", "approve", confidence=0.72),
            _provider_memo("openai", "approve", confidence=0.68),
            _provider_memo("xai", "watch", valid=False),
        ],
        packet=packet,
        input_hash=packet_hash(packet),
    )

    assert aggregate.provider == "multi"
    assert aggregate.prompt_version == "ai_research_aggregate/v0"
    assert aggregate.verdict == "approve"
    assert aggregate.validation_passed is True
    assert aggregate.memo["quorum"]["approve_count"] == 2


def test_multi_provider_aggregate_reject_overrides_approve_quorum():
    packet = build_research_packet(TradeIntent(symbol="POET", side="long", entry_price=10.0, confidence=0.7))
    aggregate = aggregate_research_memos(
        "POET",
        [
            _provider_memo("anthropic", "approve", confidence=0.72),
            _provider_memo("openai", "approve", confidence=0.68),
            _provider_memo("xai", "reject", confidence=0.7),
        ],
        packet=packet,
        input_hash=packet_hash(packet),
    )

    assert aggregate.verdict == "reject"
    assert aggregate.memo["quorum"]["reject_count"] == 1


def test_multi_provider_aggregate_invalid_output_cannot_force_approve():
    packet = build_research_packet(TradeIntent(symbol="POET", side="long", entry_price=10.0, confidence=0.7))
    aggregate = aggregate_research_memos(
        "POET",
        [
            _provider_memo("anthropic", "approve", confidence=0.72),
            _provider_memo("openai", "approve", confidence=0.9, valid=False),
            _provider_memo("xai", "watch", confidence=0.7),
        ],
        packet=packet,
        input_hash=packet_hash(packet),
    )

    assert aggregate.verdict == "watch"
    assert aggregate.memo["quorum"]["approve_count"] == 1


@pytest.mark.asyncio
async def test_multi_provider_research_round_runs_members_sequentially():
    events = []

    class FakeMember:
        def __init__(self, provider: str) -> None:
            self.provider = provider
            self.model_tag = f"{provider}/model"

        async def research(self, intent, *, signal_id=None):
            events.append(f"start:{self.provider}")
            events.append(f"end:{self.provider}")
            return _provider_memo(self.provider, "watch", confidence=0.7)

    committee = MultiProviderResearchCommittee(
        [FakeMember("anthropic"), FakeMember("openai"), FakeMember("xai")]
    )
    intent = TradeIntent(symbol="POET", side="long", entry_price=10.0, confidence=0.7)

    await committee.research_round(intent)

    assert events == [
        "start:anthropic",
        "end:anthropic",
        "start:openai",
        "end:openai",
        "start:xai",
        "end:xai",
    ]


def test_real_provider_extractors_parse_structured_json():
    payload = {
        "symbol": "POET",
        "verdict": "watch",
        "confidence": 0.5,
        "used_only_provided_data": True,
        "bull_case": "Provided packet shows constructive momentum.",
        "bear_case": "Catalyst is unverified.",
        "judge_summary": "Watch only.",
    }
    text = json.dumps(payload)

    openai = OpenAIResearchCommittee("key", model="gpt-5.1")
    xai = XAIResearchCommittee("key", model="grok-4.3")
    anthropic = AnthropicResearchCommittee("key", model="claude-sonnet-4-20250514")
    gemini = GeminiResearchCommittee("key", model="gemini-3.5-flash")

    assert openai._extract_output({"output_text": text}) == payload
    assert xai._extract_output({"choices": [{"message": {"content": text}}]}) == payload
    assert anthropic._extract_output({"content": [{"type": "text", "text": text}]}) == payload
    assert gemini._extract_output({"candidates": [{"content": {"parts": [{"text": text}]}}]}) == payload


def _opus_nested_committee_output(**committee_overrides):
    committee = {
        "advisory_note": (
            "This is advisory research only. No order sizing or order submission is provided. "
            "All conclusions are limited strictly to the supplied data packet."
        ),
        "assessment": {
            "confidence_provided": 0.8428879654203442,
            "entry_price": 4.67,
            "side": "long",
            "supporting_factors": [
                "Positive intraday momentum with change_pct of +8.86%",
                "Relative volume of 1.25 indicates above-average participation",
                "Tight spread of 0.21% suggests adequate liquidity for entry/exit",
            ],
            "risk_factors": [
                "Low absolute price can increase volatility.",
                "Modest dollar volume may widen realized execution slippage.",
                "Chasing an extended move increases reversal risk.",
            ],
        },
        "data_limitations": [
            "No historical price context.",
            "No broader market/sector context.",
            "Single snapshot metrics only.",
            "No fundamental/catalyst data.",
        ],
    }
    committee.update(committee_overrides)
    return {"committee": committee}


def test_normalizes_opus_nested_shape_but_requires_explicit_provided_data():
    packet = {"candidate": {"symbol": "SPCE"}}

    normalized, meta = normalize_committee_output(_opus_nested_committee_output(), packet)
    valid, errors = validate_committee_output("SPCE", normalized)

    assert meta["source_shape"] == "committee_wrapper"
    assert normalized["symbol"] == "SPCE"
    assert normalized["verdict"] == "watch"
    assert normalized["confidence"] == 0.8428879654203442
    assert normalized["used_only_provided_data"] is False
    assert "Positive intraday momentum" in normalized["bull_case"]
    assert "No historical price context" in normalized["bear_case"]
    assert "advisory research only" in normalized["judge_summary"]
    assert "normalized_missing_verdict" in normalized["normalization_errors"]
    assert valid is False
    assert "used_unverified_data" in errors


def test_normalized_nested_shape_validates_with_explicit_provided_data_true():
    packet = {"candidate": {"symbol": "SPCE"}}
    output = _opus_nested_committee_output(used_only_provided_data=True)

    normalized, _meta = normalize_committee_output(output, packet)
    valid, errors = validate_committee_output("SPCE", normalized)

    assert normalized["verdict"] == "watch"
    assert normalized["used_only_provided_data"] is True
    assert valid is True
    assert errors == []


def test_normalized_nested_shape_keeps_conflicting_provider_symbol_invalid():
    packet = {"candidate": {"symbol": "SPCE"}}
    output = _opus_nested_committee_output(symbol="TSLA", used_only_provided_data=True)

    normalized, _meta = normalize_committee_output(output, packet)
    valid, errors = validate_committee_output("SPCE", normalized)

    assert normalized["symbol"] == "TSLA"
    assert valid is False
    assert "symbol_mismatch" in errors


def test_normalized_nested_shape_does_not_invent_missing_confidence():
    packet = {"candidate": {"symbol": "SPCE"}}
    output = _opus_nested_committee_output(used_only_provided_data=True)
    output["committee"]["assessment"].pop("confidence_provided")

    normalized, _meta = normalize_committee_output(output, packet)
    valid, errors = validate_committee_output("SPCE", normalized)

    assert "confidence" not in normalized
    assert "normalized_missing_confidence" in normalized["normalization_errors"]
    assert valid is False
    assert "missing_confidence" in errors


def test_normalized_nested_shape_bounds_provider_text():
    packet = {"candidate": {"symbol": "SPCE"}}
    long_factor = "momentum " * 200
    output = _opus_nested_committee_output(used_only_provided_data=True)
    output["committee"]["assessment"]["supporting_factors"] = [long_factor]

    normalized, _meta = normalize_committee_output(output, packet)

    assert len(normalized["bull_case"]) <= 800
    assert normalized["bull_case"].endswith("...")


@pytest.mark.asyncio
async def test_http_research_memo_preserves_raw_provider_output():
    raw_output = _opus_nested_committee_output(used_only_provided_data=True)
    anthropic = AnthropicResearchCommittee("key", model="claude-opus-4-8")

    def fake_call_provider(packet):
        return {
            "id": "msg_test",
            "content": [{"type": "text", "text": json.dumps(raw_output)}],
        }

    anthropic._call_provider = fake_call_provider
    intent = TradeIntent(
        symbol="SPCE",
        side="long",
        entry_price=4.67,
        rationale="rules found momentum",
        confidence=0.7,
        features={},
    )

    memo = await anthropic.research(intent, signal_id=3)

    assert memo.validation_passed is True
    assert memo.memo["raw_committee_output"] == raw_output
    assert memo.memo["normalization"]["source_shape"] == "committee_wrapper"
    assert memo.memo["response_id"] == "msg_test"


def test_anthropic_prompt_requires_exact_schema_without_wrapper_keys():
    payload = {
        "symbol": "SPCE",
        "verdict": "watch",
        "confidence": 0.5,
        "used_only_provided_data": True,
        "bull_case": "Provided packet shows momentum.",
        "bear_case": "Catalyst is unverified.",
        "judge_summary": "Watch only.",
    }
    packet = {"candidate": {"symbol": "SPCE"}}
    captured = {}
    anthropic = AnthropicResearchCommittee("key", model="claude-opus-4-8")

    def fake_post(url, body, headers):
        captured["body"] = body
        return {"content": [{"type": "text", "text": json.dumps(payload)}]}

    anthropic._post_json = fake_post
    assert anthropic._call_provider(packet)["content"]

    body = captured["body"]
    assert "Return exactly one top-level JSON object" in body["system"]
    assert "Do not wrap the object in committee" in body["system"]
    assert "Return exactly the required JSON object" in body["messages"][0]["content"]
    assert "no wrapper keys" in body["messages"][0]["content"]


def test_provider_http_errors_redact_api_keys(monkeypatch):
    def fake_urlopen(request, timeout):
        raise RuntimeError("provider rejected secret-key-value")

    monkeypatch.setattr("auto_trader.intelligence.ai_committee.urlopen", fake_urlopen)
    gemini = GeminiResearchCommittee("secret-key-value", model="gemini-2.5-flash")

    with pytest.raises(RuntimeError) as exc:
        gemini._post_json(
            "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent",
            {},
            {"x-goog-api-key": "secret-key-value"},
        )

    assert "secret-key-value" not in str(exc.value)
    assert "[REDACTED]" in str(exc.value)


def test_ai_research_packet_hash_ignores_volatile_metadata():
    base = {
        "generated_at": "2026-06-04T15:00:00Z",
        "signal_id": 1,
        "candidate": {"symbol": "POET", "confidence": 0.7},
        "features": {"discovery": {"score": 5.0}},
    }
    later = {
        **base,
        "generated_at": "2026-06-04T15:01:00Z",
        "signal_id": 2,
    }

    assert packet_hash(base) == packet_hash(later)


def test_ai_research_preflight_shadow_default_not_ready_and_no_secret():
    class ShadowSettings(DummySupervisorSettings):
        ai_research_enabled = True
        ai_research_provider = "shadow"
        ai_research_model = ""
        ai_research_max_calls_per_day = 0
        anthropic_api_key = "super-secret"

    report = build_ai_research_preflight_report(settings=ShadowSettings(), used_calls=0)
    text = render_ai_research_preflight(report)

    assert report.ready is False
    assert "State: NOT_READY" in text
    assert "Provider: shadow" in text
    assert "super-secret" not in text
    assert "Key present: false" in text


def test_ai_research_preflight_missing_model_not_ready():
    class MissingModelSettings(DummySupervisorSettings):
        ai_research_enabled = True
        ai_research_provider = "anthropic"
        ai_research_model = ""
        ai_research_max_calls_per_day = 1
        anthropic_api_key = "anthropic-key"

    report = build_ai_research_preflight_report(settings=MissingModelSettings(), used_calls=0)

    assert report.ready is False
    assert any(gate.name == "Explicit model" and gate.status == "FAIL" for gate in report.gates)


def test_ai_research_preflight_missing_key_not_ready():
    class MissingKeySettings(DummySupervisorSettings):
        ai_research_enabled = True
        ai_research_provider = "anthropic"
        ai_research_model = "claude-opus-4-8"
        ai_research_max_calls_per_day = 1
        anthropic_api_key = ""

    report = build_ai_research_preflight_report(settings=MissingKeySettings(), used_calls=0)

    assert report.ready is False
    assert any(gate.name == "Provider key present" and gate.status == "FAIL" for gate in report.gates)


def test_ai_research_preflight_zero_budget_not_ready():
    class ZeroBudgetSettings(DummySupervisorSettings):
        ai_research_enabled = True
        ai_research_provider = "anthropic"
        ai_research_model = "claude-opus-4-8"
        ai_research_max_calls_per_day = 0
        anthropic_api_key = "anthropic-key"

    report = build_ai_research_preflight_report(settings=ZeroBudgetSettings(), used_calls=0)

    assert report.ready is False
    assert report.remaining_calls == 0
    assert any(gate.name == "Daily call budget" and gate.status == "FAIL" for gate in report.gates)


def test_ai_research_preflight_count_failure_fails_closed():
    class CountFailureSettings(DummySupervisorSettings):
        ai_research_enabled = True
        ai_research_provider = "anthropic"
        ai_research_model = "claude-opus-4-8"
        ai_research_max_calls_per_day = 1
        anthropic_api_key = "anthropic-key"

    report = build_ai_research_preflight_report(settings=CountFailureSettings(), used_calls=None)
    text = render_ai_research_preflight(report)

    assert report.ready is False
    assert "budget count unavailable" in text
    assert any(gate.name == "Budget count available" and gate.status == "FAIL" for gate in report.gates)


def test_ai_research_preflight_configured_anthropic_opus_estimate_no_secret():
    class AnthropicSettings(DummySupervisorSettings):
        ai_research_enabled = True
        ai_research_provider = "anthropic"
        ai_research_model = "claude-opus-4-8"
        ai_research_max_calls_per_day = 5
        ai_research_est_input_tokens = 15000
        ai_research_est_output_tokens = 2000
        ai_research_input_price_per_mtok = 5.0
        ai_research_output_price_per_mtok = 25.0
        anthropic_api_key = "super-secret"

    report = build_ai_research_preflight_report(settings=AnthropicSettings(), used_calls=1)
    text = render_ai_research_preflight(report)

    assert report.ready is True
    assert report.remaining_calls == 4
    assert report.cost.estimated_cost_per_memo == pytest.approx(0.125)
    assert report.estimated_daily_cost == pytest.approx(0.625)
    assert "State: READY" in text
    assert "Key present: true" in text
    assert "AI entry gate enabled: false" in text
    assert "super-secret" not in text
    assert "Estimated cost per memo: $0.1250" in text
    assert "Estimated worst-case daily cost: $0.6250" in text


def test_ai_research_preflight_renders_ai_entry_gate_enabled():
    class AnthropicSettings(DummySupervisorSettings):
        ai_research_enabled = True
        ai_entry_gate_enabled = True
        ai_research_provider = "anthropic"
        ai_research_model = "claude-opus-4-8"
        ai_research_max_calls_per_day = 5
        anthropic_api_key = "anthropic-key"

    report = build_ai_research_preflight_report(settings=AnthropicSettings(), used_calls=0)
    text = render_ai_research_preflight(report)

    assert report.ready is True
    assert report.ai_entry_gate_enabled is True
    assert "AI entry gate enabled: true" in text


def test_ai_research_preflight_multi_provider_round_budget_and_no_secret():
    class CommitteeSettings(DummySupervisorSettings):
        ai_research_enabled = True
        ai_research_providers = "anthropic,openai,xai"
        ai_research_anthropic_model = "claude-opus-4-8"
        ai_research_openai_model = "gpt-5.5"
        ai_research_xai_model = "grok-4.3"
        ai_research_max_calls_per_day = 6
        ai_research_est_input_tokens = 15000
        ai_research_est_output_tokens = 2000
        ai_research_input_price_per_mtok = 5.0
        ai_research_output_price_per_mtok = 25.0
        anthropic_api_key = "anthropic-secret"
        openai_api_key = "openai-secret"
        xai_api_key = "xai-secret"

    report = build_ai_research_preflight_report(settings=CommitteeSettings(), used_calls=3)
    text = render_ai_research_preflight(report)

    assert report.ready is True
    assert report.provider == "multi"
    assert report.attempts_per_round == 3
    assert report.remaining_calls == 3
    assert report.remaining_rounds == 1
    assert report.estimated_cost_per_round == pytest.approx(0.375)
    assert "Provider: multi" in text
    assert "Chargeable calls per round: 3" in text
    assert "Full rounds remaining: 1" in text
    assert "Estimated cost per round: $0.3750" in text
    assert "- anthropic: model=claude-opus-4-8, key_present=true" in text
    assert text.index("Providers:") < text.index("Gates:")
    assert "anthropic-secret" not in text
    assert "openai-secret" not in text
    assert "xai-secret" not in text


@pytest.mark.asyncio
async def test_ai_research_preflight_count_failure_uses_no_provider_call(monkeypatch):
    class AnthropicSettings(DummySupervisorSettings):
        ai_research_enabled = True
        ai_research_provider = "anthropic"
        ai_research_model = "claude-opus-4-8"
        ai_research_max_calls_per_day = 1
        anthropic_api_key = "anthropic-key"

    async def fake_count(**kwargs):
        return None

    monkeypatch.setattr("auto_trader.ai_research_preflight.count_ai_research_chargeable_attempts", fake_count)
    report_text, gates = await run_ai_research_preflight(settings=AnthropicSettings())

    assert "State: NOT_READY" in report_text
    assert "budget count unavailable" in report_text
    assert any(gate.name == "Budget count available" and gate.status == "FAIL" for gate in gates)


class FakeRehearsalAdapter:
    async def get_account_snapshot(self):
        return {
            "status": "CONNECTED",
            "account_status": "AccountStatus.ACTIVE",
            "equity": 100.0,
            "cash": 90.0,
            "buying_power": 90.0,
        }

    async def get_clock(self):
        return {"is_open": True, "next_close": "2026-06-04T20:00:00+00:00"}

    async def get_positions_snapshot(self, *, strict=False):
        return []


class FakeRehearsalCommittee:
    provider = "openai"
    model_tag = "openai/model"

    def __init__(self, verdict: str):
        self.verdict = verdict

    async def research(self, intent, *, signal_id=None):
        return _provider_memo("openai", self.verdict, confidence=0.8)


@pytest.mark.asyncio
async def test_ai_entry_gate_rehearsal_approve_would_continue(monkeypatch):
    with tempfile.TemporaryDirectory() as tmp:
        configure_db_path(Path(tmp) / "ai_entry_gate_rehearsal.db")

        class RehearsalSettings(DummySupervisorSettings):
            db_path = str(Path(tmp) / "ai_entry_gate_rehearsal.db")
            ai_research_enabled = True
            ai_research_provider = "openai"
            ai_research_model = "gpt-5.5"
            ai_research_max_calls_per_day = 1
            openai_api_key = "openai-key"

        async def fake_signals(adapter, max_signals=1, finnhub_client=None, fred_client=None):
            return [
                TradeIntent(
                    symbol="POET",
                    side="long",
                    entry_price=10.0,
                    confidence=0.8,
                    features={"research_context": {"market": {"provider": "alpaca"}}},
                )
            ]

        async def fake_entry_count(start_utc_iso):
            return 0

        monkeypatch.setattr("auto_trader.ai_entry_gate_rehearsal.get_simple_rules_signals", fake_signals)
        monkeypatch.setattr("auto_trader.ai_entry_gate_rehearsal.count_entry_orders_since", fake_entry_count)

        result = await run_ai_entry_gate_rehearsal(
            settings=RehearsalSettings(),
            adapter=FakeRehearsalAdapter(),
            committee=FakeRehearsalCommittee("approve"),
        )
        text = render_ai_entry_gate_rehearsal(result)

        assert result.ok is True
        assert result.called_provider is True
        assert result.would_continue_to_risk_engine is True
        assert result.verdict == "approve"
        assert "State: WOULD_CONTINUE_TO_RISKENGINE" in text
        assert "No orders submitted" in text
        memos = await get_latest_ai_research_memos(limit=1)
        assert memos[0]["provider"] == "openai"


@pytest.mark.asyncio
async def test_ai_entry_gate_rehearsal_reject_would_block(monkeypatch):
    with tempfile.TemporaryDirectory() as tmp:
        configure_db_path(Path(tmp) / "ai_entry_gate_rehearsal_reject.db")

        class RehearsalSettings(DummySupervisorSettings):
            db_path = str(Path(tmp) / "ai_entry_gate_rehearsal_reject.db")
            ai_research_enabled = True
            ai_research_provider = "openai"
            ai_research_model = "gpt-5.5"
            ai_research_max_calls_per_day = 1
            openai_api_key = "openai-key"

        async def fake_signals(adapter, max_signals=1, finnhub_client=None, fred_client=None):
            return [TradeIntent(symbol="POET", side="long", entry_price=10.0, confidence=0.8)]

        async def fake_entry_count(start_utc_iso):
            return 0

        monkeypatch.setattr("auto_trader.ai_entry_gate_rehearsal.get_simple_rules_signals", fake_signals)
        monkeypatch.setattr("auto_trader.ai_entry_gate_rehearsal.count_entry_orders_since", fake_entry_count)

        result = await run_ai_entry_gate_rehearsal(
            settings=RehearsalSettings(),
            adapter=FakeRehearsalAdapter(),
            committee=FakeRehearsalCommittee("reject"),
        )
        text = render_ai_entry_gate_rehearsal(result)

        assert result.ok is True
        assert result.would_continue_to_risk_engine is False
        assert result.verdict == "reject"
        assert "State: WOULD_BLOCK_BEFORE_RISKENGINE" in text


@pytest.mark.asyncio
async def test_ai_entry_gate_rehearsal_provider_failure_is_chargeable(monkeypatch):
    with tempfile.TemporaryDirectory() as tmp:
        configure_db_path(Path(tmp) / "ai_entry_gate_rehearsal_failure.db")

        class RehearsalSettings(DummySupervisorSettings):
            db_path = str(Path(tmp) / "ai_entry_gate_rehearsal_failure.db")
            ai_research_enabled = True
            ai_research_provider = "openai"
            ai_research_model = "gpt-5.5"
            ai_research_max_calls_per_day = 3
            openai_api_key = "openai-key"

        class FailingRehearsalCommittee:
            provider = "openai"
            model_tag = "openai/model"

            async def research(self, intent, *, signal_id=None):
                raise RuntimeError("provider timeout")

        async def fake_signals(adapter, max_signals=1, finnhub_client=None, fred_client=None):
            return [TradeIntent(symbol="POET", side="long", entry_price=10.0, confidence=0.8)]

        async def fake_entry_count(start_utc_iso):
            return 0

        monkeypatch.setattr("auto_trader.ai_entry_gate_rehearsal.get_simple_rules_signals", fake_signals)
        monkeypatch.setattr("auto_trader.ai_entry_gate_rehearsal.count_entry_orders_since", fake_entry_count)

        result = await run_ai_entry_gate_rehearsal(
            settings=RehearsalSettings(),
            adapter=FakeRehearsalAdapter(),
            committee=FailingRehearsalCommittee(),
        )

        assert result.ok is True
        assert result.called_provider is True
        assert result.would_continue_to_risk_engine is False
        assert result.prompt_version == "ai_research_failure/v0"
        assert result.used_before == 0
        assert result.used_after == 1
        assert await count_ai_research_chargeable_attempts(provider="openai", today_utc=True) == 1
        memos = await get_latest_ai_research_memos(limit=1)
        assert memos[0]["prompt_version"] == "ai_research_failure/v0"


@pytest.mark.asyncio
async def test_ai_entry_gate_rehearsal_zero_budget_does_not_call_provider(monkeypatch):
    with tempfile.TemporaryDirectory() as tmp:
        configure_db_path(Path(tmp) / "ai_entry_gate_rehearsal_zero.db")

        class RehearsalSettings(DummySupervisorSettings):
            db_path = str(Path(tmp) / "ai_entry_gate_rehearsal_zero.db")
            ai_research_enabled = True
            ai_research_provider = "openai"
            ai_research_model = "gpt-5.5"
            ai_research_max_calls_per_day = 0
            openai_api_key = "openai-key"

        class ExplodingCommittee:
            provider = "openai"
            model_tag = "openai/model"

            async def research(self, intent, *, signal_id=None):
                raise AssertionError("provider should not be called with zero budget")

        result = await run_ai_entry_gate_rehearsal(
            settings=RehearsalSettings(),
            adapter=FakeRehearsalAdapter(),
            committee=ExplodingCommittee(),
        )

        assert result.ok is False
        assert result.called_provider is False
        assert result.reason == "AI_RESEARCH_MAX_CALLS_PER_DAY must be positive"


def test_ai_entry_gate_rehearsal_does_not_import_order_or_risk_stack():
    source = inspect.getsource(ai_entry_gate_rehearsal)

    assert "OrderManager" not in source
    assert "RiskEngine" not in source
    assert "submit_trade_intent" not in source
    assert "submit_order" not in source


def test_supervisor_does_not_instantiate_real_provider_when_ai_disabled():
    class DisabledAISettings(DummySupervisorSettings):
        ai_research_enabled = False
        ai_research_provider = "openai"
        ai_research_model = ""
        openai_api_key = ""

    supervisor = TradingSupervisor(
        settings=DisabledAISettings(),
        state_machine=StateMachine(initial_state=SystemState.ACTIVE),
        adapter=object(),
        order_manager=object(),
    )

    assert isinstance(supervisor.research_committee, ShadowResearchCommittee)


@pytest.mark.asyncio
async def test_real_provider_budget_zero_skips_and_persists_audit_row():
    with tempfile.TemporaryDirectory() as tmp:
        configure_db_path(Path(tmp) / "ai_budget.db")
        await init_db()

        class BudgetSettings(DummySupervisorSettings):
            ai_research_enabled = True
            ai_research_provider = "openai"
            ai_research_model = "gpt-5.5"
            ai_research_max_calls_per_day = 0
            openai_api_key = "openai-key"

        supervisor = TradingSupervisor(
            settings=BudgetSettings(),
            state_machine=StateMachine(initial_state=SystemState.ACTIVE),
            adapter=object(),
            order_manager=object(),
        )
        intent = TradeIntent(symbol="POET", side="long", entry_price=10.0, confidence=0.7)

        await supervisor._run_ai_research(intent, signal_id=12)

        memos = await get_latest_ai_research_memos(limit=1)
        assert len(memos) == 1
        assert memos[0]["provider"] == "openai"
        assert memos[0]["prompt_version"] == "ai_research_budget/v0"
        assert memos[0]["validation_passed"] is False
        assert memos[0]["memo"]["budget"]["daily_max"] == 0


@pytest.mark.asyncio
async def test_multi_provider_supervisor_requires_budget_for_full_round():
    with tempfile.TemporaryDirectory() as tmp:
        configure_db_path(Path(tmp) / "ai_multi_budget.db")
        await init_db()

        class BudgetSettings(DummySupervisorSettings):
            ai_research_enabled = True
            ai_research_providers = "anthropic,openai,xai"
            ai_research_anthropic_model = "claude-opus-4-8"
            ai_research_openai_model = "gpt-5.5"
            ai_research_xai_model = "grok-4.3"
            ai_research_max_calls_per_day = 2
            anthropic_api_key = "anthropic-key"
            openai_api_key = "openai-key"
            xai_api_key = "xai-key"

        class ExplodingMember:
            def __init__(self, provider: str) -> None:
                self.provider = provider
                self.model_tag = f"{provider}/model"

            async def research(self, intent, *, signal_id=None):
                raise AssertionError("provider should not be called without enough budget for the full round")

        supervisor = TradingSupervisor(
            settings=BudgetSettings(),
            state_machine=StateMachine(initial_state=SystemState.ACTIVE),
            adapter=object(),
            order_manager=object(),
        )
        supervisor.research_committee = MultiProviderResearchCommittee(
            [ExplodingMember("anthropic"), ExplodingMember("openai"), ExplodingMember("xai")]
        )
        intent = TradeIntent(symbol="POET", side="long", entry_price=10.0, confidence=0.7)

        await supervisor._run_ai_research(intent, signal_id=12)
        memos = await get_latest_ai_research_memos(limit=5)

        assert len(memos) == 1
        assert memos[0]["provider"] == "multi"
        assert memos[0]["prompt_version"] == "ai_research_budget/v0"
        assert memos[0]["memo"]["budget"]["attempts_needed"] == 3


@pytest.mark.asyncio
async def test_multi_provider_supervisor_logs_members_and_aggregate():
    with tempfile.TemporaryDirectory() as tmp:
        configure_db_path(Path(tmp) / "ai_multi_supervisor.db")
        await init_db()

        class BudgetSettings(DummySupervisorSettings):
            ai_research_enabled = True
            ai_research_providers = "anthropic,openai,xai"
            ai_research_anthropic_model = "claude-opus-4-8"
            ai_research_openai_model = "gpt-5.5"
            ai_research_xai_model = "grok-4.3"
            ai_research_max_calls_per_day = 3
            anthropic_api_key = "anthropic-key"
            openai_api_key = "openai-key"
            xai_api_key = "xai-key"

        class FakeMember:
            def __init__(self, provider: str, verdict: str) -> None:
                self.provider = provider
                self.model_tag = f"{provider}/model"
                self.verdict = verdict

            async def research(self, intent, *, signal_id=None):
                return _provider_memo(self.provider, self.verdict, confidence=0.7)

        supervisor = TradingSupervisor(
            settings=BudgetSettings(),
            state_machine=StateMachine(initial_state=SystemState.ACTIVE),
            adapter=object(),
            order_manager=object(),
        )
        supervisor.research_committee = MultiProviderResearchCommittee(
            [
                FakeMember("anthropic", "approve"),
                FakeMember("openai", "approve"),
                FakeMember("xai", "watch"),
            ]
        )
        intent = TradeIntent(symbol="POET", side="long", entry_price=10.0, confidence=0.7)

        await supervisor._run_ai_research(intent, signal_id=12)
        memos = await get_latest_ai_research_memos(limit=10)

        assert len(memos) == 4
        assert {memo["provider"] for memo in memos} == {"anthropic", "openai", "xai", "multi"}
        aggregate = next(memo for memo in memos if memo["provider"] == "multi")
        assert aggregate["prompt_version"] == "ai_research_aggregate/v0"
        assert aggregate["verdict"] == "approve"
        assert len(aggregate["memo"]["provider_memo_ids"]) == 3
        assert {row["provider"] for row in aggregate["memo"]["provider_memo_ids"]} == {"anthropic", "openai", "xai"}
        assert await count_ai_research_chargeable_attempts(today_utc=True) == 3


@pytest.mark.asyncio
async def test_real_provider_budget_count_failure_skips_provider_call(monkeypatch):
    class BudgetSettings(DummySupervisorSettings):
        ai_research_enabled = True
        ai_research_provider = "openai"
        ai_research_model = "gpt-5.5"
        ai_research_max_calls_per_day = 5
        openai_api_key = "openai-key"

    class ExplodingCommittee:
        provider = "openai"
        model_tag = "openai/gpt-5.5"

        async def research(self, intent, *, signal_id=None):
            raise AssertionError("provider should not be called when budget count fails")

    async def fake_count(**kwargs):
        return None

    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.count_ai_research_memos", fake_count)
    supervisor = TradingSupervisor(
        settings=BudgetSettings(),
        state_machine=StateMachine(initial_state=SystemState.ACTIVE),
        adapter=object(),
        order_manager=object(),
    )
    supervisor.research_committee = ExplodingCommittee()
    intent = TradeIntent(symbol="POET", side="long", entry_price=10.0, confidence=0.7)

    await supervisor._run_ai_research(intent, signal_id=12)


@pytest.mark.asyncio
async def test_real_provider_chargeable_budget_count_failure_skips_provider_call(monkeypatch):
    class BudgetSettings(DummySupervisorSettings):
        ai_research_enabled = True
        ai_research_provider = "openai"
        ai_research_model = "gpt-5.5"
        ai_research_max_calls_per_day = 5
        openai_api_key = "openai-key"

    class ExplodingCommittee:
        provider = "openai"
        model_tag = "openai/gpt-5.5"

        async def research(self, intent, *, signal_id=None):
            raise AssertionError("provider should not be called when chargeable budget count fails")

    async def fake_audit_count(**kwargs):
        return 0

    async def fake_chargeable_count(**kwargs):
        return None

    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.count_ai_research_memos", fake_audit_count)
    monkeypatch.setattr(
        "auto_trader.scheduler.trading_supervisor.count_ai_research_chargeable_attempts",
        fake_chargeable_count,
    )
    supervisor = TradingSupervisor(
        settings=BudgetSettings(),
        state_machine=StateMachine(initial_state=SystemState.ACTIVE),
        adapter=object(),
        order_manager=object(),
    )
    supervisor.research_committee = ExplodingCommittee()
    intent = TradeIntent(symbol="POET", side="long", entry_price=10.0, confidence=0.7)

    await supervisor._run_ai_research(intent, signal_id=12)


@pytest.mark.asyncio
async def test_ai_research_smoke_refuses_zero_budget(monkeypatch):
    class SmokeSettings(DummySupervisorSettings):
        ai_research_provider = "anthropic"
        ai_research_model = "claude-opus-4-8"
        ai_research_max_calls_per_day = 0
        anthropic_api_key = "anthropic-key"

    def exploding_factory(settings):
        raise AssertionError("committee should not be constructed when budget is zero")

    monkeypatch.setattr("auto_trader.ai_research_smoke.create_research_committee", exploding_factory)

    result = await run_ai_research_smoke(settings=SmokeSettings(), symbol="POET", price=10.0)

    assert result.ok is False
    assert result.called_provider is False
    assert "MAX_CALLS_PER_DAY" in result.reason


@pytest.mark.asyncio
async def test_ai_research_smoke_refuses_count_failure(monkeypatch):
    class SmokeSettings(DummySupervisorSettings):
        ai_research_provider = "anthropic"
        ai_research_model = "claude-opus-4-8"
        ai_research_max_calls_per_day = 1
        anthropic_api_key = "anthropic-key"

    async def fake_count(**kwargs):
        return None

    def exploding_factory(settings):
        raise AssertionError("committee should not be constructed when count fails")

    monkeypatch.setattr("auto_trader.ai_research_smoke.count_ai_research_chargeable_attempts", fake_count)
    monkeypatch.setattr("auto_trader.ai_research_smoke.create_research_committee", exploding_factory)

    result = await run_ai_research_smoke(settings=SmokeSettings(), symbol="POET", price=10.0)

    assert result.ok is False
    assert result.called_provider is False
    assert result.reason == "chargeable budget count unavailable"


@pytest.mark.asyncio
async def test_ai_research_smoke_persists_one_chargeable_memo(monkeypatch):
    with tempfile.TemporaryDirectory() as tmp:
        configure_db_path(Path(tmp) / "ai_smoke.db")

        class SmokeSettings(DummySupervisorSettings):
            db_path = str(Path(tmp) / "ai_smoke.db")
            ai_research_provider = "anthropic"
            ai_research_model = "claude-opus-4-8"
            ai_research_max_calls_per_day = 1
            anthropic_api_key = "anthropic-key"

        class FakeCommittee:
            provider = "anthropic"
            model_tag = "anthropic/claude-opus-4-8"

            async def research(self, intent, *, signal_id=None):
                return ResearchMemo(
                    symbol=intent.symbol,
                    provider=self.provider,
                    model_tag=self.model_tag,
                    prompt_version="ai_research_committee/v0",
                    input_hash="hash123456789",
                    verdict="watch",
                    confidence=0.8,
                    used_only_provided_data=True,
                    validation_passed=True,
                    memo={
                        "committee": {"judge_summary": "advisory only"},
                        "normalization": {"markers": ["normalized_invalid_verdict"]},
                    },
                )

        monkeypatch.setattr("auto_trader.ai_research_smoke.create_research_committee", lambda settings: FakeCommittee())

        result = await run_ai_research_smoke(settings=SmokeSettings(), symbol="POET", price=10.0)
        memos = await get_latest_ai_research_memos(limit=5)

        assert result.ok is True
        assert result.called_provider is True
        assert result.memo_id is not None
        assert result.used_before == 0
        assert result.used_after == 1
        assert result.remaining_after == 0
        assert result.normalization_markers == ["normalized_invalid_verdict"]
        assert len(memos) == 1
        assert memos[0]["prompt_version"] == "ai_research_committee/v0"
        assert await count_ai_research_chargeable_attempts(provider="anthropic", today_utc=True) == 1


@pytest.mark.asyncio
async def test_ai_research_smoke_multi_provider_persists_members_and_aggregate(monkeypatch):
    with tempfile.TemporaryDirectory() as tmp:
        configure_db_path(Path(tmp) / "ai_smoke_multi.db")

        class SmokeSettings(DummySupervisorSettings):
            db_path = str(Path(tmp) / "ai_smoke_multi.db")
            ai_research_providers = "anthropic,openai,xai"
            ai_research_anthropic_model = "claude-opus-4-8"
            ai_research_openai_model = "gpt-5.5"
            ai_research_xai_model = "grok-4.3"
            ai_research_max_calls_per_day = 3
            anthropic_api_key = "anthropic-key"
            openai_api_key = "openai-key"
            xai_api_key = "xai-key"

        class FakeMember:
            def __init__(self, provider: str, verdict: str) -> None:
                self.provider = provider
                self.model_tag = f"{provider}/model"
                self.verdict = verdict

            async def research(self, intent, *, signal_id=None):
                return _provider_memo(self.provider, self.verdict, confidence=0.7)

        committee = MultiProviderResearchCommittee(
            [
                FakeMember("anthropic", "approve"),
                FakeMember("openai", "approve"),
                FakeMember("xai", "watch"),
            ]
        )
        monkeypatch.setattr("auto_trader.ai_research_smoke.create_research_committee", lambda settings: committee)

        result = await run_ai_research_smoke(settings=SmokeSettings(), symbol="POET", price=10.0)
        text = render_ai_research_smoke(result)
        memos = await get_latest_ai_research_memos(limit=10)

        assert result.ok is True
        assert result.provider == "multi"
        assert result.verdict == "approve"
        assert result.attempts_needed == 3
        assert result.used_before == 0
        assert result.used_after == 3
        assert len(result.provider_results) == 3
        assert "Estimated cost per round: $0.3750" in text
        assert {memo["provider"] for memo in memos} == {"anthropic", "openai", "xai", "multi"}
        aggregate = next(memo for memo in memos if memo["provider"] == "multi")
        assert len(aggregate["memo"]["provider_memo_ids"]) == 3
        assert await count_ai_research_chargeable_attempts(today_utc=True) == 3
        assert await count_ai_research_chargeable_attempts(provider="multi", today_utc=True) == 0


@pytest.mark.asyncio
async def test_ai_research_smoke_failure_consumes_chargeable_budget(monkeypatch):
    with tempfile.TemporaryDirectory() as tmp:
        configure_db_path(Path(tmp) / "ai_smoke_failure.db")

        class SmokeSettings(DummySupervisorSettings):
            db_path = str(Path(tmp) / "ai_smoke_failure.db")
            ai_research_provider = "anthropic"
            ai_research_model = "claude-opus-4-8"
            ai_research_max_calls_per_day = 1
            anthropic_api_key = "anthropic-key"

        class FailingCommittee:
            async def research(self, intent, *, signal_id=None):
                raise RuntimeError("provider unavailable")

        monkeypatch.setattr("auto_trader.ai_research_smoke.create_research_committee", lambda settings: FailingCommittee())

        result = await run_ai_research_smoke(settings=SmokeSettings(), symbol="POET", price=10.0)

        assert result.ok is False
        assert result.called_provider is True
        assert result.prompt_version == "ai_research_failure/v0"
        assert result.used_before == 0
        assert result.used_after == 1
        assert await count_ai_research_chargeable_attempts(provider="anthropic", today_utc=True) == 1


def test_ai_research_smoke_does_not_import_order_or_risk_stack():
    source = inspect.getsource(ai_research_smoke)

    assert "TradingSupervisor" not in source
    assert "OrderManager" not in source
    assert "RiskEngine" not in source
    assert "AlpacaAdapter" not in source


@pytest.mark.asyncio
async def test_risk_decision_persists_signal_id_link():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "signal_link.db"
        configure_db_path(db_path)
        await init_db()

        signal_id = await log_signal(
            symbol="POET",
            thesis="rules found momentum",
            confidence=0.72,
            source="rules_fallback",
            features={"discovery": {"score": 6.0}},
        )
        risk_decision_id = await log_risk_decision(
            signal_id=signal_id,
            approved=True,
            reason="Passed v1 risk gates",
            symbol="POET",
            side="long",
            proposed_qty=1.0,
            sized_qty=1.0,
            equity_snapshot=100.0,
            risk_metrics={"state": "ACTIVE"},
            model_tag="rules_fallback/v0",
            trace_id="trace1234",
        )

        assert signal_id is not None
        assert risk_decision_id is not None
        with sqlite3.connect(db_path) as conn:
            row = conn.execute(
                "SELECT signal_id FROM risk_decisions WHERE id = ?",
                (risk_decision_id,),
            ).fetchone()

        assert row[0] == signal_id


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
async def test_finnhub_client_preserves_partial_endpoint_failures(monkeypatch):
    client = FinnhubClient("test-key")

    async def fake_get_json(path, params, *, endpoint):
        if endpoint == "quote":
            return {"c": 14.2, "d": 0.3, "dp": 2.1, "h": 14.5, "l": 13.7, "o": 13.9, "pc": 13.9}
        if endpoint == "profile2":
            return {"name": "POET Technologies Inc", "ticker": "POET"}
        if endpoint == "company-news":
            raise RuntimeError("news endpoint unavailable token=secret123&symbol=POET")
        raise AssertionError(endpoint)

    monkeypatch.setattr(client, "_get_json", fake_get_json)

    enriched = await client.enrich_symbol("POET")

    assert enriched["quote"]["current"] == 14.2
    assert enriched["profile"]["name"] == "POET Technologies Inc"
    assert enriched["news"]["error"] == "news endpoint unavailable token=[REDACTED]&symbol=POET"


@pytest.mark.asyncio
async def test_fred_client_returns_cached_core_risk_pack(monkeypatch):
    client = FredClient("fred-key")
    calls = []

    async def fake_get_json(path, params, *, endpoint):
        calls.append((path, params["series_id"], endpoint))
        return {"observations": [{"date": "2026-06-03", "value": "4.25"}]}

    monkeypatch.setattr(client, "_get_json", fake_get_json)

    first = await client.macro_context()
    second = await client.macro_context()

    assert first is second
    assert first["enabled"] is True
    assert set(first["series"]) == set(CORE_RISK_SERIES)
    assert first["series"]["ten_year_treasury_yield"]["value"] == 4.25
    assert first["regime"]["advisory_only"] is True
    assert len(calls) == len(CORE_RISK_SERIES)


@pytest.mark.asyncio
async def test_fred_client_missing_key_returns_macro_error_without_network():
    client = FredClient(None)

    context = await client.macro_context()

    assert context["enabled"] is False
    assert context["error"] == "FRED_API_KEY is not configured"
    assert context["series"] == {}


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
                research_context={
                    "market": {"provider": "alpaca", "feed": "iex"},
                    "technical": {"rel_volume": 2.0, "change_pct": 0.04},
                },
            )
        ]

    class FakeFinnhub:
        enabled = True

        async def enrich_symbol(self, symbol):
            return {
                "provider": "finnhub",
                "enabled": True,
                "quote": {"current": 14.2},
                "profile": {"name": "POET Technologies", "industry": "Semiconductors"},
                "news": [{"headline": "POET headline", "source": "Wire"}],
            }

    class FakeFred:
        async def macro_context(self):
            return {
                "provider": "fred",
                "enabled": True,
                "series": {"ten_year_treasury_yield": {"series_id": "DGS10", "value": 4.2}},
                "regime": {"risk_backdrop": "normal_or_unknown", "advisory_only": True},
            }

    monkeypatch.setattr("auto_trader.intelligence.rules_fallback.discover_dynamic_candidates", fake_discover)

    signals = await get_simple_rules_signals(
        object(),
        max_signals=1,
        finnhub_client=FakeFinnhub(),
        fred_client=FakeFred(),
    )

    assert signals[0].symbol == "POET"
    assert signals[0].features["discovery"]["provider"] == "alpaca"
    assert signals[0].features["finnhub"]["quote"]["current"] == 14.2
    assert signals[0].features["research_context"]["market"]["provider"] == "alpaca"
    assert signals[0].features["research_context"]["fundamental"]["industry"] == "Semiconductors"
    assert signals[0].features["research_context"]["news"][0]["headline"] == "POET headline"
    assert signals[0].features["research_context"]["macro"]["series"]["ten_year_treasury_yield"]["value"] == 4.2


@pytest.mark.asyncio
async def test_rules_signals_attach_fred_missing_key_macro_context(monkeypatch):
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

    monkeypatch.setattr("auto_trader.intelligence.rules_fallback.discover_dynamic_candidates", fake_discover)

    signals = await get_simple_rules_signals(object(), max_signals=1, fred_client=FredClient(None))
    packet = build_research_packet(signals[0], signal_id=33)

    assert signals[0].features["fred"]["enabled"] is False
    assert signals[0].features["research_context"]["macro"]["error"] == "FRED_API_KEY is not configured"
    assert "macro" in packet["verified_research_context"]["data_quality"]["missing_sections"]


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
async def test_signal_shutdown_skips_flatten_when_local_opt_out_enabled(monkeypatch):
    async def fake_consume_marker(*, alpaca_paper):
        return None

    monkeypatch.setattr("auto_trader.__main__.consume_planned_maintenance_shutdown", fake_consume_marker)
    settings = type("Settings", (), {"shutdown_flatten_on_exit": False, "alpaca_paper": True})()
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
async def test_signal_shutdown_defaults_to_emergency_flatten(monkeypatch):
    async def fake_consume_marker(*, alpaca_paper):
        return None

    monkeypatch.setattr("auto_trader.__main__.consume_planned_maintenance_shutdown", fake_consume_marker)
    settings = type("Settings", (), {"shutdown_flatten_on_exit": True, "alpaca_paper": True})()
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
async def test_planned_maintenance_marker_roundtrip_consumes_once():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "maintenance.db"
        configure_db_path(db_path)
        await init_db()

        marker = await request_planned_maintenance_shutdown(
            reason="deploy redaction fix",
            ttl_seconds=60,
        )

        assert marker["planned_maintenance_shutdown"] == "true"
        consumed = await consume_planned_maintenance_shutdown(alpaca_paper=True)
        assert consumed is not None
        assert consumed["planned_maintenance_reason"] == "deploy redaction fix"
        assert await consume_planned_maintenance_shutdown(alpaca_paper=True) is None


@pytest.mark.asyncio
async def test_expired_planned_maintenance_marker_does_not_skip_shutdown():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "maintenance_expired.db"
        configure_db_path(db_path)
        await init_db()

        await request_planned_maintenance_shutdown(reason="expired deploy", ttl_seconds=60)
        expired_at = (datetime.now(UTC) - timedelta(seconds=1)).isoformat().replace("+00:00", "Z")
        with sqlite3.connect(db_path) as db:
            db.execute(
                "UPDATE runtime_config SET value = ? WHERE key = 'planned_maintenance_expires_at'",
                (expired_at,),
            )
        assert await consume_planned_maintenance_shutdown(alpaca_paper=True) is None


@pytest.mark.asyncio
async def test_live_planned_maintenance_requires_allow_live_marker():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "maintenance_live.db"
        configure_db_path(db_path)
        await init_db()

        await request_planned_maintenance_shutdown(reason="live deploy", ttl_seconds=60)
        assert await consume_planned_maintenance_shutdown(alpaca_paper=False) is None

        await request_planned_maintenance_shutdown(
            reason="live deploy",
            ttl_seconds=60,
            allow_live=True,
        )
        assert await consume_planned_maintenance_shutdown(alpaca_paper=False) is not None


@pytest.mark.asyncio
async def test_signal_shutdown_uses_planned_maintenance_marker_once(monkeypatch):
    settings = type("Settings", (), {"shutdown_flatten_on_exit": True, "alpaca_paper": True})()
    sm = StateMachine(initial_state=SystemState.ACTIVE)
    stop_event = asyncio.Event()
    shutdown_context = {"planned_maintenance": False}
    consumed = {"count": 0}

    async def fake_consume_marker(*, alpaca_paper):
        assert alpaca_paper is True
        consumed["count"] += 1
        return {"planned_maintenance_reason": "deploy"} if consumed["count"] == 1 else None

    monkeypatch.setattr("auto_trader.__main__.consume_planned_maintenance_shutdown", fake_consume_marker)

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
        sig=15,
        settings=settings,
        state_machine=sm,
        adapter=adapter,
        stop_event=stop_event,
        shutdown_context=shutdown_context,
    )

    assert sm.state == SystemState.ACTIVE
    assert adapter.cancel_calls == 0
    assert adapter.flatten_calls == 0
    assert stop_event.is_set()
    assert shutdown_context["planned_maintenance"] is True
    assert _should_emergency_halt_on_shutdown(
        settings,
        sm,
        planned_maintenance=shutdown_context["planned_maintenance"],
    ) is False


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


@pytest.mark.asyncio
async def test_order_manager_persists_signal_id_in_risk_decision(monkeypatch):
    sm = StateMachine(initial_state=SystemState.ACTIVE)
    risk = RiskEngine(sm, DummySettings())
    captured = {}

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
        captured.update(kwargs)
        return 42

    async def fake_upsert_order_record(*args, **kwargs):
        return True

    monkeypatch.setattr("auto_trader.execution.order_manager.log_risk_decision", fake_log_risk_decision)
    monkeypatch.setattr("auto_trader.execution.order_manager.upsert_order_record", fake_upsert_order_record)

    manager = OrderManager(risk, FakeAdapter())
    result = await manager.submit_trade_intent(
        TradeIntent(symbol="AMPX", side="long", entry_price=23.89),
        DummySnapshot(),
        signal_id=7,
    )

    assert captured["signal_id"] == 7
    assert result["signal_id"] == 7
    assert result["risk_decision"]["risk_decision_id"] == 42


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
    assert "Runtime AI entry gate: False" in status
    assert "New entries: disabled by runtime config" in status


def test_telegram_status_clamps_stale_runtime_cap_in_live_mode():
    sm = StateMachine(initial_state=SystemState.ACTIVE)

    class LiveAdapter:
        paper = False

    bot = TelegramBot(
        token="token",
        state_machine=sm,
        risk_engine=RiskEngine(sm, DummySettings()),
        adapter=LiveAdapter(),
        resume_token="resume",
    )

    status = bot._build_status_message(
        {
            "health": {"status": "CONNECTED", "paper": False, "market_open": True},
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
            "runtime_config": {"auto_entry_enabled": "true", "max_new_positions_per_day": "3"},
            "errors": [],
        }
    )

    assert "Today new entries: 1 / 1" in status
    assert "New entries: blocked by daily-entry limit" in status


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
async def test_telegram_config_handler_sets_ai_entry_gate(monkeypatch):
    sm = StateMachine(initial_state=SystemState.ACTIVE)
    stored = {}
    journal = []

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
        adapter=object(),
        resume_token="resume",
        allowed_ids=[123],
    )
    update = FakeTelegramUpdate(chat_id=123, user_id=456)

    await bot._config_handler(update, FakeTelegramContext(["ai_gate", "on"]))

    assert stored == {"ai_entry_gate_enabled": "true"}
    assert update.message.replies == [
        "Runtime AI entry gate set to True. Gate is fail-closed; only valid real-provider approve can continue to RiskEngine."
    ]
    assert journal == ["Runtime config updated: ai_entry_gate_enabled=True."]


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
        return {"auto_entry_enabled": "true", "ai_entry_gate_enabled": "true", "max_new_positions_per_day": "3"}

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
    assert "ai_entry_gate_enabled: True (runtime)" in update.message.replies[0]
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
                "buying_power": 80.0,
                "trading_blocked": False,
                "account_blocked": False,
            }

        async def get_clock(self):
            return {"is_open": True, "source": "alpaca", "next_close": "2026-06-04T20:00:00+00:00"}

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


def test_supervisor_last_risk_sweep_window_runs_once():
    sm = StateMachine(initial_state=SystemState.ACTIVE)
    supervisor = TradingSupervisor(
        settings=DummySupervisorSettings(),
        state_machine=sm,
        adapter=object(),
        order_manager=object(),
    )
    local_tz = ZoneInfo("America/Los_Angeles")
    sweep_time = datetime(2026, 6, 3, 12, 55, tzinfo=local_tz)

    assert supervisor._should_run_last_risk_sweep({"is_open": True, "source": "alpaca"}, sweep_time) is True

    supervisor._mark_last_risk_sweep_complete(sweep_time)

    assert supervisor._should_run_last_risk_sweep({"is_open": True, "source": "alpaca"}, sweep_time) is False
    assert (
        supervisor._should_run_last_risk_sweep(
            {"is_open": True, "source": "alpaca"},
            datetime(2026, 6, 4, 12, 54, tzinfo=local_tz),
        )
        is False
    )
    assert (
        supervisor._should_run_last_risk_sweep(
            {"is_open": False, "source": "alpaca"},
            datetime(2026, 6, 4, 12, 55, tzinfo=local_tz),
        )
        is False
    )


@pytest.mark.asyncio
async def test_supervisor_last_risk_sweep_forces_reconciliation(monkeypatch):
    sm = StateMachine(initial_state=SystemState.ACTIVE)
    reconcile_calls = 0

    class FakeAdapter:
        paper = True

        async def get_account_snapshot(self):
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
            return {"is_open": True, "source": "alpaca", "next_close": "2026-06-04T20:00:00+00:00"}

        async def get_recent_orders(self, days=2):
            return [{"id": "recent-1", "symbol": "AMPX"}]

        async def get_positions_snapshot(self, *, strict=False):
            return []

    async def fake_reconcile(orders):
        nonlocal reconcile_calls
        reconcile_calls += 1
        return len(orders)

    async def fake_count(start_utc_iso):
        return 0

    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.reconcile_broker_orders", fake_reconcile)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.count_entry_orders_since", fake_count)
    patch_empty_pending_exit_state(monkeypatch)

    supervisor = TradingSupervisor(
        settings=DummySupervisorSettings(),
        state_machine=sm,
        adapter=FakeAdapter(),
        order_manager=object(),
    )
    supervisor._last_reconcile_at = datetime.now(UTC)
    monkeypatch.setattr(supervisor, "_should_run_last_risk_sweep", lambda clock, local_now: True)

    result = await supervisor.tick_once()

    assert reconcile_calls == 1
    assert result.reconciled_orders == 1
    assert supervisor._last_risk_sweep_dates == {datetime.now(ZoneInfo("America/Los_Angeles")).date().isoformat()}


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
async def test_supervisor_auto_exit_does_not_close_when_market_closed(monkeypatch):
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
            return {"is_open": False, "source": "alpaca", "next_open": "2026-06-04T13:30:00+00:00"}

        async def get_recent_orders(self, days=2):
            return []

        async def get_positions_snapshot(self, *, strict=False):
            return [
                {
                    "symbol": "REPL",
                    "qty": 1,
                    "market_value": 89.0,
                    "unrealized_pl": -11.0,
                    "cost_basis": 100.0,
                }
            ]

        async def close_position(self, symbol, reason):
            self.close_calls += 1
            raise AssertionError("close_position must not run outside regular market hours")

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

    assert result.exit_decisions[0].should_exit is True
    assert result.exit_decisions[0].reason == "position max loss reached"
    assert adapter.close_calls == 0
    assert not any("EXIT SUBMITTED: REPL" in message for message in notifications)


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

        async def submit_trade_intent(self, intent, snapshot, signal_id=None):
            self.calls.append((intent, snapshot))
            return {"order": {"id": "entry-1"}, "risk_decision": {"approved": True}}

    async def fake_reconcile(orders):
        return 0

    async def fake_count(start_utc_iso):
        return 0

    async def fake_latest_entry(symbol):
        return None

    async def fake_signals(adapter, max_signals=1, finnhub_client=None, fred_client=None):
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
async def test_supervisor_auto_entry_logs_shadow_ai_research(monkeypatch):
    sm = StateMachine(initial_state=SystemState.ACTIVE)
    logged_memos = []

    class EntrySettings(DummySupervisorSettings):
        auto_entry_enabled = True
        ai_research_enabled = True

    class FakeAdapter:
        paper = True

        async def get_account_snapshot(self):
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
            return {"is_open": True, "source": "alpaca", "next_close": "2026-06-04T20:00:00+00:00"}

        async def get_recent_orders(self, days=2):
            return []

        async def get_positions_snapshot(self, *, strict=False):
            return []

        async def get_open_orders(self):
            return []

    class FakeOrderManager:
        def __init__(self):
            self.intents = []

        async def submit_trade_intent(self, intent, snapshot, signal_id=None):
            self.intents.append(intent)
            return {"order": {"id": "entry-1"}, "risk_decision": {"approved": True}}

    async def fake_reconcile(orders):
        return 0

    async def fake_count(start_utc_iso):
        return 0

    async def fake_latest_entry(symbol):
        return None

    async def fake_signals(adapter, max_signals=1, finnhub_client=None, fred_client=None):
        return [
            TradeIntent(
                symbol="AMPX",
                side="long",
                entry_price=20.0,
                confidence=0.8,
                features={"discovery": {"score": 5.0, "rel_volume": 2.0, "change_pct": 0.03, "spread_pct": 0.002}},
            )
        ]

    async def fake_ai_memo(**kwargs):
        logged_memos.append(kwargs)
        return 1

    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.reconcile_broker_orders", fake_reconcile)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.count_entry_orders_since", fake_count)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_latest_entry_order_for_symbol", fake_latest_entry)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_simple_rules_signals", fake_signals)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.log_ai_research_memo", fake_ai_memo)
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
    assert logged_memos[0]["symbol"] == "AMPX"
    assert logged_memos[0]["provider"] == "shadow"
    assert logged_memos[0]["validation_passed"] is True
    risk_context = logged_memos[0]["memo"]["input_packet"]["verified_research_context"]["risk"]
    assert risk_context["account"]["equity"] == 100.0
    assert risk_context["account"]["cash"] == 80.0
    assert risk_context["account"]["buying_power"] == 80.0
    assert risk_context["market_clock"]["is_open"] is True
    assert risk_context["market_clock"]["next_close"] == "2026-06-04T20:00:00+00:00"
    assert risk_context["positions"]["open_count"] == 0
    assert risk_context["entry_limits"]["today_new_entries"] == 0
    assert risk_context["entry_limits"]["max_new_positions_per_day"] == 1
    assert manager.intents[0].features["research_context"]["risk"] == risk_context


@pytest.mark.asyncio
async def test_ai_entry_gate_approve_continues_to_order_manager(monkeypatch):
    class GateSettings(DummySupervisorSettings):
        auto_entry_enabled = True
        ai_research_enabled = True
        ai_entry_gate_enabled = True
        ai_research_provider = "openai"
        ai_research_model = "gpt-5.5"
        ai_research_max_calls_per_day = 1
        openai_api_key = "openai-key"

    class FakeAdapter:
        paper = True

        async def get_open_orders(self):
            return []

    class FakeOrderManager:
        def __init__(self):
            self.calls = []

        async def submit_trade_intent(self, intent, snapshot, signal_id=None):
            self.calls.append((intent, snapshot, signal_id))
            return {"order": {"id": "entry-approved"}, "risk_decision": {"approved": True}}

    class ApprovingCommittee:
        provider = "openai"
        model_tag = "openai/gpt-5.5"

        async def research(self, intent, *, signal_id=None):
            return _provider_memo("openai", "approve", confidence=0.8)

    async def fake_signals(adapter, max_signals=1, finnhub_client=None, fred_client=None):
        return [TradeIntent(symbol="POET", side="long", entry_price=10.0, confidence=0.8)]

    async def fake_bool(key, *, default):
        return True

    async def fake_count(**kwargs):
        return 0

    async def fake_log_signal(**kwargs):
        return 44

    async def fake_log_ai(**kwargs):
        return 55

    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_runtime_config_bool", fake_bool)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_simple_rules_signals", fake_signals)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.count_ai_research_memos", fake_count)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.count_ai_research_chargeable_attempts", fake_count)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.log_signal", fake_log_signal)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.log_ai_research_memo", fake_log_ai)

    manager = FakeOrderManager()
    supervisor = TradingSupervisor(
        settings=GateSettings(),
        state_machine=StateMachine(initial_state=SystemState.ACTIVE),
        adapter=FakeAdapter(),
        order_manager=manager,
    )
    supervisor.research_committee = ApprovingCommittee()

    result = await supervisor._maybe_submit_entry(
        account={"status": "CONNECTED", "account_status": "AccountStatus.ACTIVE", "equity": 100.0},
        clock={"is_open": True},
        positions=[],
        today_new_entries=0,
        max_new_positions_per_day=1,
    )

    assert result["order"]["id"] == "entry-approved"
    assert manager.calls[0][0].symbol == "POET"
    assert manager.calls[0][2] == 44


@pytest.mark.asyncio
async def test_ai_entry_gate_reject_blocks_before_order_manager(monkeypatch):
    class GateSettings(DummySupervisorSettings):
        auto_entry_enabled = True
        ai_research_enabled = True
        ai_entry_gate_enabled = True
        ai_research_provider = "openai"
        ai_research_model = "gpt-5.5"
        ai_research_max_calls_per_day = 1
        openai_api_key = "openai-key"

    class FakeAdapter:
        paper = True

        async def get_open_orders(self):
            return []

    class FakeOrderManager:
        def __init__(self):
            self.calls = []

        async def submit_trade_intent(self, intent, snapshot, signal_id=None):
            self.calls.append((intent, snapshot, signal_id))
            raise AssertionError("OrderManager should not be called when AI gate rejects")

    class RejectingCommittee:
        provider = "openai"
        model_tag = "openai/gpt-5.5"

        async def research(self, intent, *, signal_id=None):
            return _provider_memo("openai", "reject", confidence=0.8)

    journal_entries = []

    async def fake_signals(adapter, max_signals=1, finnhub_client=None, fred_client=None):
        return [TradeIntent(symbol="POET", side="long", entry_price=10.0, confidence=0.8)]

    async def fake_bool(key, *, default):
        return True

    async def fake_count(**kwargs):
        return 0

    async def fake_log_signal(**kwargs):
        return 45

    async def fake_log_ai(**kwargs):
        return 56

    async def fake_journal(content):
        journal_entries.append(content)

    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_runtime_config_bool", fake_bool)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_simple_rules_signals", fake_signals)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.count_ai_research_memos", fake_count)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.count_ai_research_chargeable_attempts", fake_count)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.log_signal", fake_log_signal)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.log_ai_research_memo", fake_log_ai)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.append_journal_entry", fake_journal)

    manager = FakeOrderManager()
    supervisor = TradingSupervisor(
        settings=GateSettings(),
        state_machine=StateMachine(initial_state=SystemState.ACTIVE),
        adapter=FakeAdapter(),
        order_manager=manager,
    )
    supervisor.research_committee = RejectingCommittee()

    result = await supervisor._maybe_submit_entry(
        account={"status": "CONNECTED", "account_status": "AccountStatus.ACTIVE", "equity": 100.0},
        clock={"is_open": True},
        positions=[],
        today_new_entries=0,
        max_new_positions_per_day=1,
    )

    assert result["blocked"] is True
    assert result["ai_gate"]["verdict"] == "reject"
    assert manager.calls == []
    assert "AI entry gate blocked POET" in journal_entries[0]


@pytest.mark.asyncio
async def test_ai_entry_gate_runtime_config_enables_gate(monkeypatch):
    class GateSettings(DummySupervisorSettings):
        auto_entry_enabled = True
        ai_research_enabled = True
        ai_entry_gate_enabled = False
        ai_research_provider = "openai"
        ai_research_model = "gpt-5.5"
        ai_research_max_calls_per_day = 1
        openai_api_key = "openai-key"

    class FakeAdapter:
        paper = True

        async def get_open_orders(self):
            return []

    class FakeOrderManager:
        async def submit_trade_intent(self, intent, snapshot, signal_id=None):
            raise AssertionError("OrderManager should not be called when runtime AI gate rejects")

    class RejectingCommittee:
        provider = "openai"
        model_tag = "openai/gpt-5.5"

        async def research(self, intent, *, signal_id=None):
            return _provider_memo("openai", "reject", confidence=0.8)

    journal_entries = []

    async def fake_signals(adapter, max_signals=1, finnhub_client=None, fred_client=None):
        return [TradeIntent(symbol="POET", side="long", entry_price=10.0, confidence=0.8)]

    async def fake_bool(key, *, default):
        if key == "auto_entry_enabled":
            return True
        if key == "ai_entry_gate_enabled":
            return True
        return default

    async def fake_count(**kwargs):
        return 0

    async def fake_log_signal(**kwargs):
        return 52

    async def fake_log_ai(**kwargs):
        return 61

    async def fake_journal(content):
        journal_entries.append(content)

    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_runtime_config_bool", fake_bool)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_simple_rules_signals", fake_signals)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.count_ai_research_memos", fake_count)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.count_ai_research_chargeable_attempts", fake_count)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.log_signal", fake_log_signal)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.log_ai_research_memo", fake_log_ai)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.append_journal_entry", fake_journal)

    supervisor = TradingSupervisor(
        settings=GateSettings(),
        state_machine=StateMachine(initial_state=SystemState.ACTIVE),
        adapter=FakeAdapter(),
        order_manager=FakeOrderManager(),
    )
    supervisor.research_committee = RejectingCommittee()

    result = await supervisor._maybe_submit_entry(
        account={"status": "CONNECTED", "account_status": "AccountStatus.ACTIVE", "equity": 100.0},
        clock={"is_open": True},
        positions=[],
        today_new_entries=0,
        max_new_positions_per_day=1,
    )

    assert result["blocked"] is True
    assert result["ai_gate"]["verdict"] == "reject"
    assert "AI entry gate blocked POET" in journal_entries[0]


@pytest.mark.asyncio
async def test_ai_entry_gate_budget_exhausted_fails_closed(monkeypatch):
    class GateSettings(DummySupervisorSettings):
        auto_entry_enabled = True
        ai_research_enabled = True
        ai_entry_gate_enabled = True
        ai_research_provider = "openai"
        ai_research_model = "gpt-5.5"
        ai_research_max_calls_per_day = 0
        openai_api_key = "openai-key"

    class FakeAdapter:
        paper = True

        async def get_open_orders(self):
            return []

    class FakeOrderManager:
        async def submit_trade_intent(self, intent, snapshot, signal_id=None):
            raise AssertionError("OrderManager should not be called when AI budget is exhausted")

    class ExplodingCommittee:
        provider = "openai"
        model_tag = "openai/gpt-5.5"

        async def research(self, intent, *, signal_id=None):
            raise AssertionError("provider should not be called without budget")

    journal_entries = []
    memos = []

    async def fake_signals(adapter, max_signals=1, finnhub_client=None, fred_client=None):
        return [TradeIntent(symbol="POET", side="long", entry_price=10.0, confidence=0.8)]

    async def fake_bool(key, *, default):
        return True

    async def fake_count(**kwargs):
        return 0

    async def fake_log_signal(**kwargs):
        return 46

    async def fake_log_ai(**kwargs):
        memos.append(kwargs)
        return 57

    async def fake_journal(content):
        journal_entries.append(content)

    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_runtime_config_bool", fake_bool)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_simple_rules_signals", fake_signals)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.count_ai_research_memos", fake_count)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.count_ai_research_chargeable_attempts", fake_count)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.log_signal", fake_log_signal)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.log_ai_research_memo", fake_log_ai)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.append_journal_entry", fake_journal)

    supervisor = TradingSupervisor(
        settings=GateSettings(),
        state_machine=StateMachine(initial_state=SystemState.ACTIVE),
        adapter=FakeAdapter(),
        order_manager=FakeOrderManager(),
    )
    supervisor.research_committee = ExplodingCommittee()

    result = await supervisor._maybe_submit_entry(
        account={"status": "CONNECTED", "account_status": "AccountStatus.ACTIVE", "equity": 100.0},
        clock={"is_open": True},
        positions=[],
        today_new_entries=0,
        max_new_positions_per_day=1,
    )

    assert result["blocked"] is True
    assert result["ai_gate"]["reason"] == "ai_research_budget_exhausted"
    assert memos[0]["prompt_version"] == "ai_research_budget/v0"
    assert "ai_research_budget_exhausted" in journal_entries[0]


@pytest.mark.asyncio
async def test_ai_entry_gate_requires_real_provider(monkeypatch):
    class GateSettings(DummySupervisorSettings):
        auto_entry_enabled = True
        ai_research_enabled = True
        ai_entry_gate_enabled = True
        ai_research_provider = "shadow"

    class FakeAdapter:
        paper = True

        async def get_open_orders(self):
            return []

    class FakeOrderManager:
        async def submit_trade_intent(self, intent, snapshot, signal_id=None):
            raise AssertionError("OrderManager should not be called when gate has only shadow research")

    journal_entries = []

    async def fake_signals(adapter, max_signals=1, finnhub_client=None, fred_client=None):
        return [
            TradeIntent(
                symbol="POET",
                side="long",
                entry_price=10.0,
                confidence=0.8,
                features={"discovery": {"score": 6.0, "rel_volume": 2.0, "change_pct": 0.02, "spread_pct": 0.002}},
            )
        ]

    async def fake_bool(key, *, default):
        return True

    async def fake_log_signal(**kwargs):
        return 47

    async def fake_log_ai(**kwargs):
        return 58

    async def fake_journal(content):
        journal_entries.append(content)

    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_runtime_config_bool", fake_bool)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_simple_rules_signals", fake_signals)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.log_signal", fake_log_signal)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.log_ai_research_memo", fake_log_ai)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.append_journal_entry", fake_journal)

    supervisor = TradingSupervisor(
        settings=GateSettings(),
        state_machine=StateMachine(initial_state=SystemState.ACTIVE),
        adapter=FakeAdapter(),
        order_manager=FakeOrderManager(),
    )

    result = await supervisor._maybe_submit_entry(
        account={"status": "CONNECTED", "account_status": "AccountStatus.ACTIVE", "equity": 100.0},
        clock={"is_open": True},
        positions=[],
        today_new_entries=0,
        max_new_positions_per_day=1,
    )

    assert result["blocked"] is True
    assert result["ai_gate"]["reason"] == "real_ai_provider_required_for_entry_gate"
    assert "real_ai_provider_required_for_entry_gate" in journal_entries[0]


@pytest.mark.asyncio
async def test_ai_entry_gate_invalid_output_fails_closed(monkeypatch):
    class GateSettings(DummySupervisorSettings):
        auto_entry_enabled = True
        ai_research_enabled = True
        ai_entry_gate_enabled = True
        ai_research_provider = "openai"
        ai_research_model = "gpt-5.5"
        ai_research_max_calls_per_day = 1
        openai_api_key = "openai-key"

    class FakeAdapter:
        paper = True

        async def get_open_orders(self):
            return []

    class FakeOrderManager:
        async def submit_trade_intent(self, intent, snapshot, signal_id=None):
            raise AssertionError("OrderManager should not be called when AI output is invalid")

    class InvalidCommittee:
        provider = "openai"
        model_tag = "openai/gpt-5.5"

        async def research(self, intent, *, signal_id=None):
            return _provider_memo("openai", "approve", confidence=0.8, valid=False)

    journal_entries = []

    async def fake_signals(adapter, max_signals=1, finnhub_client=None, fred_client=None):
        return [TradeIntent(symbol="POET", side="long", entry_price=10.0, confidence=0.8)]

    async def fake_bool(key, *, default):
        return True

    async def fake_count(**kwargs):
        return 0

    async def fake_log_signal(**kwargs):
        return 48

    async def fake_log_ai(**kwargs):
        return 59

    async def fake_journal(content):
        journal_entries.append(content)

    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_runtime_config_bool", fake_bool)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_simple_rules_signals", fake_signals)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.count_ai_research_memos", fake_count)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.count_ai_research_chargeable_attempts", fake_count)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.log_signal", fake_log_signal)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.log_ai_research_memo", fake_log_ai)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.append_journal_entry", fake_journal)

    supervisor = TradingSupervisor(
        settings=GateSettings(),
        state_machine=StateMachine(initial_state=SystemState.ACTIVE),
        adapter=FakeAdapter(),
        order_manager=FakeOrderManager(),
    )
    supervisor.research_committee = InvalidCommittee()

    result = await supervisor._maybe_submit_entry(
        account={"status": "CONNECTED", "account_status": "AccountStatus.ACTIVE", "equity": 100.0},
        clock={"is_open": True},
        positions=[],
        today_new_entries=0,
        max_new_positions_per_day=1,
    )

    assert result["blocked"] is True
    assert result["ai_gate"]["reason"] == "ai_research_approve"
    assert result["ai_gate"]["validation_passed"] is False
    assert "validation_passed=False" in journal_entries[0]


@pytest.mark.asyncio
async def test_ai_entry_gate_provider_failure_fails_closed(monkeypatch):
    class GateSettings(DummySupervisorSettings):
        auto_entry_enabled = True
        ai_research_enabled = True
        ai_entry_gate_enabled = True
        ai_research_provider = "openai"
        ai_research_model = "gpt-5.5"
        ai_research_max_calls_per_day = 1
        openai_api_key = "openai-key"

    class FakeAdapter:
        paper = True

        async def get_open_orders(self):
            return []

    class FakeOrderManager:
        async def submit_trade_intent(self, intent, snapshot, signal_id=None):
            raise AssertionError("OrderManager should not be called when AI provider fails")

    class FailingCommittee:
        provider = "openai"
        model_tag = "openai/gpt-5.5"

        async def research(self, intent, *, signal_id=None):
            raise RuntimeError("provider unavailable")

    journal_entries = []
    memos = []

    async def fake_signals(adapter, max_signals=1, finnhub_client=None, fred_client=None):
        return [TradeIntent(symbol="POET", side="long", entry_price=10.0, confidence=0.8)]

    async def fake_bool(key, *, default):
        return True

    async def fake_count(**kwargs):
        return 0

    async def fake_log_signal(**kwargs):
        return 49

    async def fake_log_ai(**kwargs):
        memos.append(kwargs)
        return 60

    async def fake_journal(content):
        journal_entries.append(content)

    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_runtime_config_bool", fake_bool)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_simple_rules_signals", fake_signals)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.count_ai_research_memos", fake_count)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.count_ai_research_chargeable_attempts", fake_count)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.log_signal", fake_log_signal)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.log_ai_research_memo", fake_log_ai)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.append_journal_entry", fake_journal)

    supervisor = TradingSupervisor(
        settings=GateSettings(),
        state_machine=StateMachine(initial_state=SystemState.ACTIVE),
        adapter=FakeAdapter(),
        order_manager=FakeOrderManager(),
    )
    supervisor.research_committee = FailingCommittee()

    result = await supervisor._maybe_submit_entry(
        account={"status": "CONNECTED", "account_status": "AccountStatus.ACTIVE", "equity": 100.0},
        clock={"is_open": True},
        positions=[],
        today_new_entries=0,
        max_new_positions_per_day=1,
    )

    assert result["blocked"] is True
    assert result["ai_gate"]["reason"] == "ai_research_provider_failed"
    assert memos[0]["prompt_version"] == "ai_research_failure/v0"
    assert "ai_research_provider_failed" in journal_entries[0]


@pytest.mark.asyncio
async def test_ai_entry_gate_research_disabled_fails_closed(monkeypatch):
    class GateSettings(DummySupervisorSettings):
        auto_entry_enabled = True
        ai_research_enabled = False
        ai_entry_gate_enabled = True
        ai_research_provider = "openai"
        ai_research_model = "gpt-5.5"
        ai_research_max_calls_per_day = 1
        openai_api_key = "openai-key"

    class FakeAdapter:
        paper = True

        async def get_open_orders(self):
            return []

    class FakeOrderManager:
        async def submit_trade_intent(self, intent, snapshot, signal_id=None):
            raise AssertionError("OrderManager should not be called when AI research is disabled")

    journal_entries = []

    async def fake_signals(adapter, max_signals=1, finnhub_client=None, fred_client=None):
        return [TradeIntent(symbol="POET", side="long", entry_price=10.0, confidence=0.8)]

    async def fake_bool(key, *, default):
        return True

    async def fake_log_signal(**kwargs):
        return 50

    async def fake_journal(content):
        journal_entries.append(content)

    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_runtime_config_bool", fake_bool)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_simple_rules_signals", fake_signals)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.log_signal", fake_log_signal)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.append_journal_entry", fake_journal)

    supervisor = TradingSupervisor(
        settings=GateSettings(),
        state_machine=StateMachine(initial_state=SystemState.ACTIVE),
        adapter=FakeAdapter(),
        order_manager=FakeOrderManager(),
    )

    result = await supervisor._maybe_submit_entry(
        account={"status": "CONNECTED", "account_status": "AccountStatus.ACTIVE", "equity": 100.0},
        clock={"is_open": True},
        positions=[],
        today_new_entries=0,
        max_new_positions_per_day=1,
    )

    assert result["blocked"] is True
    assert result["ai_gate"]["reason"] == "ai_research_disabled"
    assert "ai_research_disabled" in journal_entries[0]


@pytest.mark.asyncio
async def test_ai_entry_gate_budget_count_failure_fails_closed(monkeypatch):
    class GateSettings(DummySupervisorSettings):
        auto_entry_enabled = True
        ai_research_enabled = True
        ai_entry_gate_enabled = True
        ai_research_provider = "openai"
        ai_research_model = "gpt-5.5"
        ai_research_max_calls_per_day = 1
        openai_api_key = "openai-key"

    class FakeAdapter:
        paper = True

        async def get_open_orders(self):
            return []

    class FakeOrderManager:
        async def submit_trade_intent(self, intent, snapshot, signal_id=None):
            raise AssertionError("OrderManager should not be called when AI budget count fails")

    class ExplodingCommittee:
        provider = "openai"
        model_tag = "openai/gpt-5.5"

        async def research(self, intent, *, signal_id=None):
            raise AssertionError("provider should not be called when budget count fails")

    journal_entries = []

    async def fake_signals(adapter, max_signals=1, finnhub_client=None, fred_client=None):
        return [TradeIntent(symbol="POET", side="long", entry_price=10.0, confidence=0.8)]

    async def fake_bool(key, *, default):
        return True

    async def fake_memo_count(**kwargs):
        return 0

    async def fake_budget_count(**kwargs):
        return None

    async def fake_log_signal(**kwargs):
        return 51

    async def fake_journal(content):
        journal_entries.append(content)

    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_runtime_config_bool", fake_bool)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.get_simple_rules_signals", fake_signals)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.count_ai_research_memos", fake_memo_count)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.count_ai_research_chargeable_attempts", fake_budget_count)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.log_signal", fake_log_signal)
    monkeypatch.setattr("auto_trader.scheduler.trading_supervisor.append_journal_entry", fake_journal)

    supervisor = TradingSupervisor(
        settings=GateSettings(),
        state_machine=StateMachine(initial_state=SystemState.ACTIVE),
        adapter=FakeAdapter(),
        order_manager=FakeOrderManager(),
    )
    supervisor.research_committee = ExplodingCommittee()

    result = await supervisor._maybe_submit_entry(
        account={"status": "CONNECTED", "account_status": "AccountStatus.ACTIVE", "equity": 100.0},
        clock={"is_open": True},
        positions=[],
        today_new_entries=0,
        max_new_positions_per_day=1,
    )

    assert result["blocked"] is True
    assert result["ai_gate"]["reason"] == "ai_research_budget_count_failed"
    assert "ai_research_budget_count_failed" in journal_entries[0]


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

        async def submit_trade_intent(self, intent, snapshot, signal_id=None):
            self.snapshots.append(snapshot)
            return {"order": {"id": "entry-runtime-cap"}, "risk_decision": {"approved": True}}

    async def fake_reconcile(orders):
        return 0

    async def fake_count(start_utc_iso):
        return 1

    async def fake_latest_entry(symbol):
        return None

    async def fake_signals(adapter, max_signals=1, finnhub_client=None, fred_client=None):
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

        async def submit_trade_intent(self, intent, snapshot, signal_id=None):
            self.calls += 1
            return {"order": {"id": "entry-1"}}

    async def fake_reconcile(orders):
        return 0

    async def fake_count(start_utc_iso):
        return 0

    async def fake_signals(adapter, max_signals=1, finnhub_client=None, fred_client=None):
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

        async def submit_trade_intent(self, intent, snapshot, signal_id=None):
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

    async def fake_signals(adapter, max_signals=1, finnhub_client=None, fred_client=None):
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

        async def submit_trade_intent(self, intent, snapshot, signal_id=None):
            self.calls += 1
            return {"order": {"id": "entry-1"}}

    async def fake_reconcile(orders):
        return 0

    async def fake_count(start_utc_iso):
        return 0

    async def fake_signals(adapter, max_signals=1, finnhub_client=None, fred_client=None):
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
