import json
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace

import aiosqlite
import pytest

from auto_trader.broker import alpaca_adapter as alpaca_module
from auto_trader.broker.alpaca_adapter import AlpacaAdapter
from auto_trader.core.models import SystemState, TradeIntent
from auto_trader.core.risk_engine import RiskEngine
from auto_trader.core.state_machine import StateMachine
from auto_trader.edge_report import (
    CandidateOutcomeEvidence,
    ClosedTradeEvidence,
    EdgeReport,
    _build_closed_trades,
    _fetch_rows,
    render_edge_report,
)
from auto_trader.intelligence.candidate_outcomes import (
    resolve_candidate_outcomes,
    resolve_outcome_row,
)
from auto_trader.persistence.db import (
    configure_db_path,
    init_db,
    log_ai_research_memo,
)


def _memo(price: float = 10.0) -> dict:
    return {
        "input_packet": {
            "verified_research_context": {
                "market": {
                    "quote": {
                        "ask_price": price,
                        "latest_trade_price": price - 0.01,
                    }
                },
                "technical": {"price": price - 0.02},
            }
        },
        "committee": {"verdict": "approve"},
    }


async def _outcome_rows(db_path) -> list[dict]:
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM ai_candidate_outcomes ORDER BY id")
        return [dict(row) for row in await cur.fetchall()]


@pytest.mark.asyncio
async def test_edge_fetch_bounds_mixed_timestamps_and_keeps_old_trade_metadata(tmp_path):
    db_path = tmp_path / "edge-window.db"
    configure_db_path(db_path)
    await init_db()

    signals = [
        (1, "2026-07-01 12:00:00", "OLD", "rules"),
        (2, "2026-07-20 12:00:00", "SPACE", "rules"),
        (3, "2026-07-20T13:00:00Z", "ISO", "rules"),
        (4, "2026-07-01T12:00:00Z", "OLDREF", "rules"),
    ]
    memos = [
        (101, "2026-07-01 12:01:00", 1, "OLD"),
        (102, "2026-07-20 12:01:00", 2, "SPACE"),
        (103, "2026-07-20T13:01:00Z", 3, "ISO"),
        (104, "2026-07-01T12:01:00Z", 4, "OLDREF"),
    ]
    async with aiosqlite.connect(db_path) as db:
        await db.executemany(
            """
            INSERT INTO signals (id, created_at, symbol, source)
            VALUES (?, ?, ?, ?)
            """,
            signals,
        )
        await db.executemany(
            """
            INSERT INTO ai_research_memos (
                id, created_at, signal_id, symbol, provider, model_tag, prompt_version,
                input_hash, verdict, confidence, used_only_provided_data,
                validation_passed, memo_json
            )
            VALUES (?, ?, ?, ?, 'xai', 'grok-latest', 'ai_research_single/v1',
                    'hash', 'approve', 0.8, 1, 1, '{}')
            """,
            memos,
        )
        await db.execute(
            """
            INSERT INTO risk_decisions (
                id, created_at, signal_id, approved, reason, symbol, side,
                equity_snapshot
            )
            VALUES (201, '2026-07-01 12:02:00', 4, 1, 'approved', 'OLDREF', 'long', 400)
            """
        )
        await db.executemany(
            """
            INSERT INTO orders (
                client_order_id, symbol, side, qty, status, filled_qty,
                avg_fill_price, submitted_at, filled_at, risk_decision_id
            )
            VALUES (?, 'OLDREF', ?, 1, 'filled', 1, ?, ?, ?, ?)
            """,
            [
                ("entry", "buy", 10.0, "2026-07-01 12:03:00", "2026-07-01 12:03:00", 201),
                ("exit", "sell", 11.0, "2026-07-20 15:55:00", "2026-07-20 15:55:00", None),
            ],
        )
        await db.executemany(
            """
            INSERT INTO ai_candidate_outcomes (
                memo_id, signal_id, symbol, provider, model_tag, policy_tag,
                decision_at, decision_session_date, verdict, reference_price,
                price_source
            )
            VALUES (?, ?, ?, 'xai', 'grok-latest', 'single_xai', ?, ?, 'approve', 10, 'test')
            """,
            [
                (101, 1, "OLD", "2026-07-01 12:01:00", "2026-07-01"),
                (102, 2, "SPACE", "2026-07-20 12:01:00", "2026-07-20"),
                (103, 3, "ISO", "2026-07-20T13:01:00Z", "2026-07-20"),
                (104, 4, "OLDREF", "2026-07-01T12:01:00Z", "2026-07-01"),
            ],
        )
        await db.commit()

    since = datetime(2026, 7, 14, tzinfo=UTC)
    rows = await _fetch_rows(db_path, since=since)

    assert {row["symbol"] for row in rows["signals"]} == {"SPACE", "ISO", "OLDREF"}
    assert {row["symbol"] for row in rows["ai_memos"]} == {"SPACE", "ISO", "OLDREF"}
    assert {row["symbol"] for row in rows["candidate_outcomes"]} == {"SPACE", "ISO"}
    closed = _build_closed_trades(rows, since=since)
    assert [(trade.symbol, trade.pnl, trade.ai_verdict) for trade in closed] == [
        ("OLDREF", pytest.approx(1.0), "xai:approve")
    ]


@pytest.mark.asyncio
async def test_valid_single_provider_memo_registers_once_per_symbol_session(tmp_path):
    db_path = tmp_path / "outcomes.db"
    configure_db_path(db_path)
    await init_db()

    for digest in ("one", "two"):
        await log_ai_research_memo(
            signal_id=None,
            symbol="XYZ",
            provider="xai",
            model_tag="grok-latest",
            prompt_version="ai_research_single/v1",
            input_hash=digest,
            verdict="approve",
            confidence=0.8,
            used_only_provided_data=True,
            validation_passed=True,
            memo=_memo(),
        )

    rows = await _outcome_rows(db_path)
    assert len(rows) == 1
    assert rows[0]["provider"] == "xai"
    assert rows[0]["policy_tag"] == "single_xai"
    assert rows[0]["reference_price"] == pytest.approx(10.0)
    assert rows[0]["price_source"] == "model_packet.market.quote.ask_price"
    assert rows[0]["comparison_notional"] == pytest.approx(30.0)


@pytest.mark.asyncio
async def test_invalid_and_non_single_memos_do_not_register_outcomes(tmp_path):
    db_path = tmp_path / "excluded.db"
    configure_db_path(db_path)
    await init_db()
    cases = [
        ("prefilter", "ai_research_single/v1", True),
        ("shadow", "ai_research_single/v1", True),
        ("multi", "ai_research_single/v1", True),
        ("xai", "ai_research_failure/v0", False),
        ("xai", "ai_research_budget/v0", False),
        ("xai", "ai_research_single/v1", False),
    ]
    for index, (provider, prompt_version, valid) in enumerate(cases):
        await log_ai_research_memo(
            signal_id=None,
            symbol=f"X{index}",
            provider=provider,
            model_tag="model",
            prompt_version=prompt_version,
            input_hash=str(index),
            verdict="watch",
            confidence=None,
            used_only_provided_data=True,
            validation_passed=valid,
            memo=_memo(),
        )
    assert await _outcome_rows(db_path) == []


@pytest.mark.asyncio
async def test_schema_upgrade_backfills_existing_valid_single_provider_memos(tmp_path):
    db_path = tmp_path / "backfill.db"
    configure_db_path(db_path)
    await init_db()
    async with aiosqlite.connect(db_path) as db:
        await db.execute(
            "INSERT INTO signals (id, symbol, source) VALUES (1, 'OLD', 'rules')"
        )
        await db.execute(
            """
            INSERT INTO ai_research_memos (
                signal_id, symbol, provider, model_tag, prompt_version, input_hash,
                verdict, confidence, used_only_provided_data, validation_passed, memo_json
            )
            VALUES (1, 'OLD', 'xai', 'grok-latest', 'ai_research_single/v1',
                    'old-hash', 'reject', 0.7, 1, 1, ?)
            """,
            (json.dumps(_memo()),),
        )
        await db.commit()

    configure_db_path(db_path)
    await init_db()
    rows = await _outcome_rows(db_path)
    assert len(rows) == 1
    assert rows[0]["symbol"] == "OLD"
    assert rows[0]["verdict"] == "reject"


def test_resolve_outcome_row_uses_trading_sessions_across_weekend():
    row = {
        "id": 1,
        "decision_session_date": "2026-07-24",
        "reference_price": 10.0,
        "comparison_notional": 30.0,
    }
    bars = [
        {"t": "2026-07-24T04:00:00Z", "c": 10.5},
        {"t": "2026-07-27T04:00:00Z", "c": 11.0},
        {"t": "2026-07-28T04:00:00Z", "c": 9.0},
        {"t": "2026-07-29T04:00:00Z", "c": 12.0},
        {"t": "2026-07-30T04:00:00Z", "c": 10.0},
        {"t": "2026-07-31T04:00:00Z", "c": 13.0},
    ]
    result = resolve_outcome_row(row, bars, completed_through=date(2026, 7, 31))
    assert result["status"] == "resolved"
    assert result["horizons"][0]["session_date"] == "2026-07-24"
    assert result["horizons"][1]["session_date"] == "2026-07-27"
    assert result["horizons"][3]["session_date"] == "2026-07-29"
    assert result["horizons"][5]["session_date"] == "2026-07-31"
    assert result["horizons"][5]["return_pct"] == pytest.approx(30.0)
    assert result["horizons"][5]["hypothetical_pnl"] == pytest.approx(9.0)


@pytest.mark.asyncio
async def test_batched_resolver_updates_rows_without_ai_or_orders(tmp_path):
    db_path = tmp_path / "resolver.db"
    configure_db_path(db_path)
    await init_db()
    await log_ai_research_memo(
        signal_id=None,
        symbol="XYZ",
        provider="xai",
        model_tag="grok-latest",
        prompt_version="ai_research_single/v1",
        input_hash="resolver",
        verdict="reject",
        confidence=0.7,
        used_only_provided_data=True,
        validation_passed=True,
        memo=_memo(),
    )
    row = (await _outcome_rows(db_path))[0]
    start = date.fromisoformat(row["decision_session_date"])
    sessions = [start + timedelta(days=offset) for offset in range(8)]
    sessions = [session for session in sessions if session.weekday() < 5][:6]

    class FakeAdapter:
        calls = 0

        async def get_stock_daily_bars(self, symbols, *, start, end):
            self.calls += 1
            assert symbols == ["XYZ"]
            return {
                "XYZ": [
                    {"t": f"{session.isoformat()}T04:00:00Z", "c": 10.0 + index}
                    for index, session in enumerate(sessions)
                ]
            }

    adapter = FakeAdapter()
    summary = await resolve_candidate_outcomes(adapter, completed_through=sessions[-1])
    updated = (await _outcome_rows(db_path))[0]
    assert adapter.calls == 1
    assert summary.resolved_rows == 1
    assert updated["status"] == "resolved"
    assert updated["d5_session_date"] == sessions[-1].isoformat()


@pytest.mark.asyncio
async def test_daily_bar_adapter_parses_multi_symbol_payload(monkeypatch):
    payload = {
        "bars": {
            "AAA": [{"t": "2026-07-24T04:00:00Z", "c": 10.0}],
            "BBB": [{"t": "2026-07-24T04:00:00Z", "c": 20.0}],
        },
        "next_page_token": None,
    }

    class FakeResponse:
        def read(self):
            return json.dumps(payload).encode()

    @contextmanager
    def fake_urlopen(request, timeout):
        assert "timeframe=1Day" in request.full_url
        assert timeout == 20
        yield FakeResponse()

    monkeypatch.setattr(alpaca_module, "urlopen", fake_urlopen)
    adapter = AlpacaAdapter("key", "secret")
    bars = await adapter.get_stock_daily_bars(
        ["AAA", "BBB"],
        start=date(2026, 7, 24),
        end=date(2026, 7, 25),
    )
    assert bars["AAA"][0]["c"] == 10.0
    assert bars["BBB"][0]["c"] == 20.0


def test_edge_report_labels_hypothetical_outcomes_and_selection_lift():
    outcomes = [
        CandidateOutcomeEvidence(
            symbol="AAA",
            provider="xai",
            model_tag="grok-latest",
            policy_tag="single_xai",
            verdict="approve",
            decision_at=datetime.fromisoformat("2026-07-20T15:00:00+00:00"),
            comparison_notional=30.0,
            price_source="model_packet.market.quote.ask_price",
            status="resolved",
            horizon_returns={5: 10.0},
            horizon_pnl={5: 3.0},
        ),
        CandidateOutcomeEvidence(
            symbol="BBB",
            provider="xai",
            model_tag="grok-latest",
            policy_tag="single_xai",
            verdict="reject",
            decision_at=datetime.fromisoformat("2026-07-20T15:00:00+00:00"),
            comparison_notional=30.0,
            price_source="model_packet.market.quote.ask_price",
            status="resolved",
            horizon_returns={5: -5.0},
            horizon_pnl={5: -1.5},
        ),
    ]
    rendered = render_edge_report(
        EdgeReport(window_days=14, closed_trades=[], opportunities=[], candidate_outcomes=outcomes)
    )
    assert "observed hypothetical; no rejected stock was purchased" in rendered
    assert "fixed $30 hypothetical position" in rendered
    assert "Observed D5 AI selection lift: $4.50" in rendered
    assert "selection spread +15.00%" in rendered


def test_positive_expectancy_does_not_claim_win_rate_clears_breakeven():
    trades = [
        ClosedTradeEvidence(
            symbol="WIN",
            qty=1.0,
            entry_price=10.0,
            exit_price=12.0,
            pnl=2.0,
            pnl_pct=20.0,
            entry_time=None,
            exit_time=None,
            exit_reason="take profit",
            ai_verdict="xai:approve",
            risk_profile="aggressive",
            signal_id=1,
        ),
        ClosedTradeEvidence(
            symbol="LOSS",
            qty=1.0,
            entry_price=10.0,
            exit_price=9.0,
            pnl=-1.0,
            pnl_pct=-10.0,
            entry_time=None,
            exit_time=None,
            exit_reason="max loss",
            ai_verdict="xai:approve",
            risk_profile="aggressive",
            signal_id=2,
        ),
        ClosedTradeEvidence(
            symbol="FLAT",
            qty=1.0,
            entry_price=10.0,
            exit_price=10.0,
            pnl=0.0,
            pnl_pct=0.0,
            entry_time=None,
            exit_time=None,
            exit_reason="stagnation",
            ai_verdict="xai:approve",
            risk_profile="aggressive",
            signal_id=3,
        ),
        ClosedTradeEvidence(
            symbol="FLAT2",
            qty=1.0,
            entry_price=10.0,
            exit_price=10.0,
            pnl=0.0,
            pnl_pct=0.0,
            entry_time=None,
            exit_time=None,
            exit_reason="stagnation",
            ai_verdict="xai:approve",
            risk_profile="aggressive",
            signal_id=4,
        ),
    ]
    rendered = render_edge_report(EdgeReport(window_days=14, closed_trades=trades, opportunities=[]))
    assert "Realized expectancy is positive" in rendered
    assert "Win rate is clearing the breakeven bar" not in rendered


def test_runtime_cap_12_cannot_bypass_live_gross_exposure_limit():
    settings = SimpleNamespace(
        alpaca_paper=False,
        simplified_runtime_enabled=True,
        max_position_notional_pct=7.5,
        max_new_positions_per_day=12,
        max_gross_exposure_pct=100.0,
        risk_per_trade_pct=0.5,
        risk_profile="aggressive",
    )
    engine = RiskEngine(StateMachine(SystemState.ACTIVE), settings)
    snapshot = SimpleNamespace(
        equity=400.0,
        open_positions=[
            {"symbol": f"P{index}", "qty": 1.0, "market_value": 20.0}
            for index in range(5)
        ],
        today_new_entries=5,
        max_new_positions_per_day=12,
        risk_profile="aggressive",
    )
    decision = engine.evaluate(
        TradeIntent(symbol="NEW", side="long", entry_price=20.0),
        snapshot,
        consume_daily_counter=False,
    )
    assert decision.approved is False
    assert decision.reason == "Gross exposure limit would be breached"
    assert decision.risk_metrics["max_gross_exposure_pct"] == 25.0
