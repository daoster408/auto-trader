"""Risk Engine - THE ONLY PATH to any order.

All trade intents MUST go through evaluate() before OrderManager sees them.
No exceptions. Ever.
"""
from __future__ import annotations

import uuid
from math import floor


from auto_trader.core.models import RiskDecision, TradeIntent
from auto_trader.core.risk_profile import get_risk_profile
from auto_trader.core.state_machine import StateMachine
from auto_trader.utils.logging import get_logger

log = get_logger("auto_trader.core.risk_engine")


class RiskEngine:
    """v1 risk gate. Strict, auditable, and non-bypassable."""

    def __init__(self, state_machine: StateMachine, settings) -> None:
        self.sm = state_machine
        self.settings = settings
        self._daily_new_positions = 0  # reset at EOD (later)
        log.info("risk_engine_initialized", model_tag="risk/v0")

    def evaluate(self, intent: TradeIntent, snapshot, *, consume_daily_counter: bool = True) -> RiskDecision:
        """Core decision point. Returns approved/rejected with full audit trail."""
        trace_id = str(uuid.uuid4())[:8]
        model_tag = "rules_fallback/v0"  # will be overridden when LLM wired

        if intent.entry_price <= 0:
            return RiskDecision(
                approved=False,
                reason="Invalid or missing entry price",
                sized_quantity=None,
                risk_metrics={"entry_price": intent.entry_price},
                model_tag=model_tag,
                trace_id=trace_id,
            )

        # 1. State gate (highest priority)
        if not self.sm.can_trade():
            return RiskDecision(
                approved=False,
                reason=f"System state is {self.sm.state.value} - no new entries allowed",
                sized_quantity=None,
                risk_metrics={"state": self.sm.state.value},
                model_tag=model_tag,
                trace_id=trace_id,
            )

        open_positions = getattr(snapshot, "open_positions", [])
        max_new_positions_per_day = int(
            getattr(snapshot, "max_new_positions_per_day", self.settings.max_new_positions_per_day)
        )
        existing_position_symbols = {
            str(p.get("symbol", "")).upper()
            for p in open_positions
            if abs(float(p.get("qty", 0) or 0)) > 0
        }
        if intent.symbol.upper() in existing_position_symbols:
            return RiskDecision(
                approved=False,
                reason="Symbol already has an open position",
                sized_quantity=None,
                risk_metrics={"symbol": intent.symbol, "open_positions": sorted(existing_position_symbols)},
                model_tag=model_tag,
                trace_id=trace_id,
            )
        if len(existing_position_symbols) >= max_new_positions_per_day:
            return RiskDecision(
                approved=False,
                reason="Open position limit reached for v1",
                sized_quantity=None,
                risk_metrics={
                    "open_positions": sorted(existing_position_symbols),
                    "max_new_positions_per_day": max_new_positions_per_day,
                },
                model_tag=model_tag,
                trace_id=trace_id,
            )

        durable_today_entries_raw = getattr(snapshot, "today_new_entries", None)
        if durable_today_entries_raw is None:
            return RiskDecision(
                approved=False,
                reason="Durable daily entry count unavailable",
                sized_quantity=None,
                risk_metrics={"today_new_entries": None},
                model_tag=model_tag,
                trace_id=trace_id,
            )
        durable_today_entries = int(durable_today_entries_raw)
        if durable_today_entries >= max_new_positions_per_day:
            return RiskDecision(
                approved=False,
                reason="Durable daily new position limit reached",
                sized_quantity=None,
                risk_metrics={
                    "today_new_entries": durable_today_entries,
                    "max_new_positions_per_day": max_new_positions_per_day,
                },
                model_tag=model_tag,
                trace_id=trace_id,
            )

        profile = get_risk_profile(
            getattr(snapshot, "risk_profile", getattr(self.settings, "risk_profile", "conservative")),
            paper=bool(getattr(self.settings, "alpaca_paper", True)),
        )

        # 2. Basic per-trade risk budget (v1 bootstrap: fractional long, no leverage)
        early_notional_cap = snapshot.equity * profile.early_notional_cap_pct
        sized_qty = floor((early_notional_cap / intent.entry_price) * 1_000_000) / 1_000_000
        proposed_notional = intent.entry_price * sized_qty
        max_risk_dollars = snapshot.equity * (self.settings.risk_per_trade_pct / 100.0)
        if proposed_notional < 1.0:
            return RiskDecision(
                approved=False,
                reason="Proposed size below minimum bootstrap notional",
                sized_quantity=None,
                risk_metrics={
                    "proposed_notional": proposed_notional,
                    "early_notional_cap": early_notional_cap,
                    "max_risk": max_risk_dollars,
                },
                model_tag=model_tag,
                trace_id=trace_id,
            )
        if proposed_notional > early_notional_cap:
            return RiskDecision(
                approved=False,
                reason="Proposed size exceeds early profile limit",
                sized_quantity=None,
                risk_metrics={
                    "proposed_notional": proposed_notional,
                    "early_notional_cap": early_notional_cap,
                    "max_risk": max_risk_dollars,
                    "risk_profile": profile.name,
                },
                model_tag=model_tag,
                trace_id=trace_id,
            )

        # 3. Max new positions per day (v1 = 1)
        if self._daily_new_positions >= max_new_positions_per_day:
            return RiskDecision(
                approved=False,
                reason="Daily new position limit reached",
                sized_quantity=None,
                risk_metrics={"daily_new": self._daily_new_positions},
                model_tag=model_tag,
                trace_id=trace_id,
            )

        # 4. Gross exposure guard (very loose for bootstrap)
        current_exposure = sum(abs(float(p.get("market_value", 0) or 0)) for p in open_positions)
        projected = current_exposure + proposed_notional
        projected_exposure_pct = (projected / snapshot.equity) * 100 if snapshot.equity else 0.0
        max_gross_exposure_pct = self._effective_max_gross_exposure_pct()
        if projected > snapshot.equity * (max_gross_exposure_pct / 100):
            return RiskDecision(
                approved=False,
                reason="Gross exposure limit would be breached",
                sized_quantity=None,
                risk_metrics={
                    "current": current_exposure,
                    "projected": projected,
                    "projected_exposure_pct": projected_exposure_pct,
                    "max_gross_exposure_pct": max_gross_exposure_pct,
                },
                model_tag=model_tag,
                trace_id=trace_id,
            )

        # APPROVED (one share for first paper-trade bootstrap)
        decision = RiskDecision(
            approved=True,
            reason="Passed v1 risk gates",
            sized_quantity=sized_qty,
            risk_metrics={
                "state": "ACTIVE",
                "daily_new_after": self._daily_new_positions + 1,
                "risk_profile": profile.name,
                "early_notional_cap_pct": profile.early_notional_cap_pct,
                "current_gross_exposure": current_exposure,
                "projected_gross_exposure": projected,
                "projected_gross_exposure_pct": projected_exposure_pct,
                "max_gross_exposure_pct": max_gross_exposure_pct,
            },
            model_tag=model_tag,
            trace_id=trace_id,
        )

        if consume_daily_counter:
            self._daily_new_positions += 1
        log.info(
            "risk_decision_approved",
            symbol=intent.symbol,
            trace_id=trace_id,
            sized_qty=sized_qty,
            model_tag=model_tag,
        )
        return decision

    def reset_daily_counters(self) -> None:
        self._daily_new_positions = 0
        log.info("daily_risk_counters_reset")

    def _effective_max_gross_exposure_pct(self) -> float:
        configured = float(self.settings.max_gross_exposure_pct)
        if bool(getattr(self.settings, "alpaca_paper", True)):
            return configured
        return min(configured, 25.0)
