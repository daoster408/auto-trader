"""
Minimal critical safety tests for kill + HALTED persistence (Reviewer requirement).

These tests must actually exercise real DB save → load roundtrips
and verify the safety default to HALTED on failure.
"""
import tempfile
from pathlib import Path

import pytest

from auto_trader.core.models import SystemState, KillResult, TradeIntent
from auto_trader.core.risk_engine import RiskEngine
from auto_trader.broker.alpaca_adapter import AlpacaAdapter
from auto_trader.core.state_machine import StateMachine
from auto_trader.execution.order_manager import OrderManager
from auto_trader.persistence.db import (
    configure_db_path,
    count_entry_orders_since,
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
