import json
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from auto_trader.edge_report import build_edge_report
from auto_trader.persistence.db import (
    configure_db_path,
    create_decision_context,
    init_db,
    log_ai_research_memo,
    log_risk_decision,
    log_signal,
    reconcile_broker_orders,
    upsert_order_record,
)
from auto_trader.persistence.provenance import (
    config_fingerprint,
    redacted_config_snapshot,
    start_runtime_provenance,
)


class ProvenanceSettings:
    def __init__(self, *, alpaca_key: str = "alpaca-secret", xai_key: str = "xai-secret"):
        self.alpaca_key = alpaca_key
        self.xai_key = xai_key

    def model_dump(self, *, by_alias: bool, mode: str):
        assert by_alias is True
        assert mode == "json"
        return {
            "ALPACA_API_KEY": self.alpaca_key,
            "ALPACA_API_SECRET": "broker-secret",
            "ALPACA_PAPER": True,
            "RESUME_TOKEN": "resume-secret",
            "TELEGRAM_BOT_TOKEN": "telegram-secret",
            "XAI_API_KEY": self.xai_key,
            "AI_ENTRY_GATE_ENABLED": True,
            "AI_RESEARCH_PROVIDER": "xai",
            "AI_RESEARCH_XAI_MODEL": "grok-latest",
            "AI_RESEARCH_XAI_TIMEOUT_SECONDS": 60,
            "AI_PROVIDER_FAILURE_COOLDOWN_SECONDS": 300,
            "SUPERVISOR_TICK_TIMEOUT_SECONDS": 90,
            "RISK_PROFILE": "aggressive",
        }


def test_config_snapshot_redacts_secrets_and_hashes_redacted_form():
    first = redacted_config_snapshot(ProvenanceSettings(alpaca_key="first", xai_key="one"))
    second = redacted_config_snapshot(ProvenanceSettings(alpaca_key="second", xai_key="two"))
    serialized = json.dumps(first, sort_keys=True)

    assert first["ALPACA_API_KEY"] == "<redacted>"
    assert first["XAI_API_KEY"] == "<redacted>"
    assert first["AI_ENTRY_GATE_ENABLED"] is True
    assert first["AI_RESEARCH_XAI_TIMEOUT_SECONDS"] == 60
    assert first["AI_PROVIDER_FAILURE_COOLDOWN_SECONDS"] == 300
    assert first["SUPERVISOR_TICK_TIMEOUT_SECONDS"] == 90
    assert first["RISK_PROFILE"] == "aggressive"
    assert "first" not in serialized
    assert "one" not in serialized
    assert config_fingerprint(first) == config_fingerprint(second)

    effective = redacted_config_snapshot(
        ProvenanceSettings(),
        effective={"risk_profile_source": "runtime_override", "unsafe_secret": "do-not-store"},
    )
    assert effective["EFFECTIVE"]["risk_profile_source"] == "runtime_override"
    assert effective["EFFECTIVE"]["unsafe_secret"] == "<redacted>"


@pytest.mark.asyncio
async def test_legacy_context_is_single_inferred_bucket(tmp_path: Path):
    db_path = tmp_path / "legacy.db"
    configure_db_path(db_path)
    await init_db()
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            "INSERT INTO signals (id, symbol, source) VALUES (20, 'OLD', 'rules')"
        )
        connection.execute(
            """
            INSERT INTO ai_research_memos (
                signal_id, symbol, provider, model_tag, prompt_version, input_hash,
                verdict, used_only_provided_data, validation_passed, memo_json
            )
            VALUES (20, 'OLD', 'xai', 'xai/grok-latest', 'ai_research_single/v1',
                    'old', 'approve', 1, 1, '{}')
            """
        )
        connection.execute(
            """
            INSERT INTO risk_decisions (
                signal_id, approved, reason, symbol, side, equity_snapshot
            )
            VALUES (20, 1, 'approved', 'OLD', 'long', 400)
            """
        )
        risk_id = connection.execute(
            "SELECT id FROM risk_decisions WHERE signal_id = 20"
        ).fetchone()[0]
        connection.execute(
            """
            INSERT INTO orders (
                client_order_id, symbol, side, qty, status, risk_decision_id
            )
            VALUES ('old-order', 'OLD', 'buy', 1, 'filled', ?)
            """,
            (risk_id,),
        )
        connection.commit()

    configure_db_path(db_path)
    await init_db()
    with sqlite3.connect(db_path) as connection:
        contexts = {
            connection.execute(
                f"SELECT decision_context_id FROM {table} LIMIT 1"
            ).fetchone()[0]
            for table in ("signals", "ai_research_memos", "risk_decisions", "orders")
        }
        assert len(contexts) == 1
        context_id = contexts.pop()
        context = connection.execute(
            "SELECT decision_source, inferred FROM decision_contexts WHERE id = ?",
            (context_id,),
        ).fetchone()
        session = connection.execute(
            """
            SELECT s.inferred
            FROM runtime_sessions AS s
            JOIN decision_contexts AS c ON c.runtime_session_id = s.id
            WHERE c.id = ?
            """,
            (context_id,),
        ).fetchone()
    assert context == ("legacy_supervisor_entry", 1)
    assert session == (1,)


@pytest.mark.asyncio
async def test_captured_context_links_trading_records_and_preserves_gate_source(tmp_path: Path):
    db_path = tmp_path / "captured.db"
    configure_db_path(db_path)
    await init_db()
    settings = ProvenanceSettings()
    session = await start_runtime_provenance(
        settings,
        process_role="test_supervisor",
        execution_mode="paper",
    )
    assert session is not None
    snapshot = redacted_config_snapshot(
        settings,
        effective={"ai_entry_gate_enabled": True, "ai_entry_gate_source": "runtime_override"},
    )
    context_id = await create_decision_context(
        runtime_session_id=session.session_id,
        decision_source="supervisor_entry",
        ai_entry_gate_enabled=True,
        ai_entry_gate_source="runtime_override",
        ai_research_enabled=True,
        simplified_runtime_enabled=True,
        execution_mode="paper",
        provider="xai",
        model_tag="xai/grok-latest",
        prompt_version="ai_research_single/v1",
        risk_profile="aggressive",
        config_hash=config_fingerprint(snapshot),
        config_snapshot=snapshot,
    )
    assert context_id is not None
    signal_id = await log_signal(
        symbol="CTX",
        thesis="context test",
        confidence=0.8,
        source="rules",
        decision_context_id=context_id,
    )
    risk_id = await log_risk_decision(
        signal_id=signal_id,
        approved=True,
        reason="approved",
        symbol="CTX",
        side="long",
        equity_snapshot=400,
        decision_context_id=context_id,
    )
    await log_ai_research_memo(
        signal_id=signal_id,
        symbol="CTX",
        provider="xai",
        model_tag="xai/grok-latest",
        prompt_version="ai_research_single/v1",
        input_hash="ctx",
        verdict="approve",
        confidence=0.8,
        used_only_provided_data=True,
        validation_passed=True,
        memo={},
        decision_context_id=context_id,
        decision_source="supervisor_entry",
    )
    await upsert_order_record(
        {
            "id": "ctx-order",
            "symbol": "CTX",
            "side": "buy",
            "qty": 1,
            "status": "filled",
            "paper": True,
        },
        risk_decision_id=risk_id,
        decision_context_id=context_id,
    )

    with sqlite3.connect(db_path) as connection:
        references = [
            connection.execute(
                f"SELECT decision_context_id FROM {table} ORDER BY rowid DESC LIMIT 1"
            ).fetchone()[0]
            for table in ("signals", "ai_research_memos", "risk_decisions", "orders")
        ]
        context = connection.execute(
            """
            SELECT decision_source, inferred, ai_entry_gate_enabled,
                   ai_entry_gate_source, execution_mode
            FROM decision_contexts WHERE id = ?
            """,
            (context_id,),
        ).fetchone()
    assert references == [context_id] * 4
    assert context == ("supervisor_entry", 0, 1, "runtime_override", "paper")


@pytest.mark.asyncio
async def test_reconciliation_preserves_captured_order_context(tmp_path: Path):
    db_path = tmp_path / "reconcile_context.db"
    configure_db_path(db_path)
    await init_db()
    settings = ProvenanceSettings()
    session = await start_runtime_provenance(
        settings,
        process_role="test_supervisor",
        execution_mode="paper",
    )
    assert session is not None
    snapshot = redacted_config_snapshot(settings)
    context_id = await create_decision_context(
        runtime_session_id=session.session_id,
        decision_source="supervisor_entry",
        ai_entry_gate_enabled=True,
        ai_entry_gate_source="env",
        ai_research_enabled=True,
        simplified_runtime_enabled=True,
        execution_mode="paper",
        provider="xai",
        model_tag="xai/grok-latest",
        prompt_version="ai_research_single/v2",
        risk_profile="aggressive",
        config_hash=config_fingerprint(snapshot),
        config_snapshot=snapshot,
    )
    assert context_id is not None
    order = {
        "id": "broker-reconcile",
        "client_order_id": "captured-order",
        "symbol": "KEEP",
        "side": "buy",
        "qty": 1,
        "filled_qty": 1,
        "avg_fill_price": 20,
        "status": "filled",
        "paper": True,
    }
    assert await upsert_order_record(order, decision_context_id=context_id)

    assert await reconcile_broker_orders([order]) == 1

    with sqlite3.connect(db_path) as connection:
        stored_context = connection.execute(
            "SELECT decision_context_id FROM orders WHERE client_order_id = 'captured-order'"
        ).fetchone()[0]
        admin_contexts = connection.execute(
            "SELECT COUNT(*) FROM decision_contexts WHERE decision_source = 'broker_reconciliation'"
        ).fetchone()[0]
    assert stored_context == context_id
    assert admin_contexts == 0

    placeholder = {**order, "id": "placeholder-broker", "client_order_id": "placeholder"}
    assert await upsert_order_record(
        placeholder,
        decision_source="broker_reconciliation",
    )
    assert await upsert_order_record(placeholder, decision_context_id=context_id)
    with sqlite3.connect(db_path) as connection:
        upgraded_context = connection.execute(
            "SELECT decision_context_id FROM orders WHERE client_order_id = 'placeholder'"
        ).fetchone()[0]
    assert upgraded_context == context_id


@pytest.mark.asyncio
async def test_startup_repairs_only_proven_entry_context_for_edge(tmp_path: Path):
    db_path = tmp_path / "repair_context.db"
    configure_db_path(db_path)
    await init_db()
    settings = ProvenanceSettings()
    session = await start_runtime_provenance(
        settings,
        process_role="test_supervisor",
        execution_mode="paper",
    )
    assert session is not None
    snapshot = redacted_config_snapshot(settings)
    supervisor_context_id = await create_decision_context(
        runtime_session_id=session.session_id,
        decision_source="supervisor_entry",
        ai_entry_gate_enabled=True,
        ai_entry_gate_source="env",
        ai_research_enabled=True,
        simplified_runtime_enabled=True,
        execution_mode="paper",
        provider="xai",
        model_tag="xai/grok-latest",
        prompt_version="ai_research_single/v2",
        risk_profile="aggressive",
        config_hash=config_fingerprint(snapshot),
        config_snapshot=snapshot,
    )
    assert supervisor_context_id is not None
    signal_id = await log_signal(
        symbol="REPAIR",
        thesis="repair test",
        confidence=0.8,
        source="rules",
        decision_context_id=supervisor_context_id,
    )
    risk_id = await log_risk_decision(
        signal_id=signal_id,
        approved=True,
        reason="approved",
        symbol="REPAIR",
        side="long",
        equity_snapshot=400,
        decision_context_id=supervisor_context_id,
    )
    entry_at = datetime.now(UTC) - timedelta(days=2)
    exit_at = datetime.now(UTC) - timedelta(days=1)
    assert await upsert_order_record(
        {
            "id": "repair-entry",
            "symbol": "REPAIR",
            "side": "buy",
            "qty": 1,
            "filled_qty": 1,
            "avg_fill_price": 20,
            "filled_at": entry_at.isoformat(),
            "status": "filled",
            "paper": True,
        },
        risk_decision_id=risk_id,
        decision_context_id=supervisor_context_id,
    )
    assert await upsert_order_record(
        {
            "id": "repair-exit",
            "symbol": "REPAIR",
            "side": "sell",
            "qty": 1,
            "filled_qty": 1,
            "avg_fill_price": 22,
            "filled_at": exit_at.isoformat(),
            "status": "filled",
            "paper": True,
        },
        decision_source="broker_reconciliation",
    )
    assert await upsert_order_record(
        {
            "id": "unproven-entry",
            "symbol": "ORPHAN",
            "side": "buy",
            "qty": 1,
            "status": "filled",
            "paper": True,
        },
        decision_source="broker_reconciliation",
    )
    with sqlite3.connect(db_path) as connection:
        broker_context_id = connection.execute(
            "SELECT decision_context_id FROM orders WHERE client_order_id = 'repair-exit'"
        ).fetchone()[0]
        connection.execute(
            "UPDATE orders SET decision_context_id = ? WHERE client_order_id = 'repair-entry'",
            (broker_context_id,),
        )
        connection.commit()

    configure_db_path(db_path)
    await init_db()
    configure_db_path(db_path)
    await init_db()

    with sqlite3.connect(db_path) as connection:
        contexts = dict(
            connection.execute(
                "SELECT client_order_id, decision_context_id FROM orders"
            )
        )
    assert contexts["repair-entry"] == supervisor_context_id
    assert contexts["repair-exit"] == broker_context_id
    assert contexts["unproven-entry"] == broker_context_id

    report = await build_edge_report(window_days=14)
    assert len(report.closed_trades) == 1
    assert report.closed_trades[0].symbol == "REPAIR"
    assert report.closed_trades[0].decision_source == "supervisor_entry"
    assert report.closed_trades[0].execution_mode == "paper"


@pytest.mark.asyncio
async def test_edge_excludes_offline_records_but_keeps_legacy_bucket(tmp_path: Path):
    db_path = tmp_path / "edge_sources.db"
    configure_db_path(db_path)
    await init_db()
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            "INSERT INTO signals (symbol, source) VALUES ('LEGACY', 'rules')"
        )
        connection.commit()

    configure_db_path(db_path)
    await init_db()
    await log_signal(
        symbol="SMOKE",
        thesis="offline check",
        confidence=0.5,
        source="smoke",
        decision_source="ai_research_smoke",
    )

    report = await build_edge_report(window_days=30)
    legacy = await build_edge_report(window_days=30, execution_mode="legacy")

    assert [row.symbol for row in report.opportunities] == ["LEGACY"]
    assert [row.symbol for row in legacy.opportunities] == ["LEGACY"]
    assert legacy.opportunities[0].context_inferred is True
    assert report.provenance_counts["legacy_trades"] == 0


@pytest.mark.asyncio
async def test_edge_mode_filter_isolates_opportunities_and_ai_outcomes(tmp_path: Path):
    db_path = tmp_path / "edge_modes.db"
    configure_db_path(db_path)
    await init_db()
    settings = ProvenanceSettings()

    for symbol, mode in (("PAPER", "paper"), ("LIVE", "live")):
        session = await start_runtime_provenance(
            settings,
            process_role="test_supervisor",
            execution_mode=mode,
        )
        assert session is not None
        snapshot = redacted_config_snapshot(
            settings,
            effective={"decision_source": "supervisor_entry", "execution_mode": mode},
        )
        context_id = await create_decision_context(
            runtime_session_id=session.session_id,
            decision_source="supervisor_entry",
            ai_entry_gate_enabled=True,
            ai_entry_gate_source="env",
            ai_research_enabled=True,
            simplified_runtime_enabled=True,
            execution_mode=mode,
            provider="xai",
            model_tag="xai/grok-latest",
            prompt_version="ai_research_single/v1",
            risk_profile="aggressive",
            config_hash=config_fingerprint(snapshot),
            config_snapshot=snapshot,
        )
        assert context_id is not None
        signal_id = await log_signal(
            symbol=symbol,
            thesis=f"{mode} candidate",
            confidence=0.8,
            source="rules",
            decision_context_id=context_id,
        )
        await log_ai_research_memo(
            signal_id=signal_id,
            symbol=symbol,
            provider="xai",
            model_tag="xai/grok-latest",
            prompt_version="ai_research_single/v1",
            input_hash=mode,
            verdict="approve",
            confidence=0.8,
            used_only_provided_data=True,
            validation_passed=True,
            memo={
                "input_packet": {
                    "verified_research_context": {
                        "market": {"quote": {"ask_price": 10.0}}
                    }
                }
            },
            decision_context_id=context_id,
            decision_source="supervisor_entry",
        )

    paper = await build_edge_report(window_days=30, execution_mode="paper")
    live = await build_edge_report(window_days=30, execution_mode="live")

    assert [row.symbol for row in paper.opportunities] == ["PAPER"]
    assert [row.symbol for row in paper.candidate_outcomes] == ["PAPER"]
    assert [row.symbol for row in live.opportunities] == ["LIVE"]
    assert [row.symbol for row in live.candidate_outcomes] == ["LIVE"]
