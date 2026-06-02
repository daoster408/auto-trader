"""
Minimal critical safety tests for kill + HALTED persistence (Reviewer requirement).

These tests must actually exercise real DB save → load roundtrips
and verify the safety default to HALTED on failure.
"""
import tempfile
from pathlib import Path

import pytest

from auto_trader.core.models import SystemState, KillResult
from auto_trader.core.state_machine import StateMachine
from auto_trader.persistence.db import (
    configure_db_path,
    init_db,
    load_system_state,
    save_system_state,
)


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
