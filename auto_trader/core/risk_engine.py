"""Risk Engine - THE ONLY PATH to any order.

All trade intents MUST go through evaluate() before OrderManager sees them.
No exceptions. Ever.
"""
from __future__ import annotations

import uuid


from auto_trader.core.models import RiskDecision, TradeIntent
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

    def evaluate(self, intent: TradeIntent, snapshot) -> RiskDecision:
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

        # 2. Basic per-trade risk budget (v1 first paper trade: one share, no leverage)
        sized_qty = 1.0
        proposed_notional = intent.entry_price * sized_qty
        max_risk_dollars = snapshot.equity * (self.settings.risk_per_trade_pct / 100.0)
        if proposed_notional > snapshot.equity * 0.05:  # conservative 5% cap early
            return RiskDecision(
                approved=False,
                reason="Proposed size exceeds early conservative limit",
                sized_quantity=None,
                risk_metrics={"proposed_notional": proposed_notional, "max_risk": max_risk_dollars},
                model_tag=model_tag,
                trace_id=trace_id,
            )

        # 3. Max new positions per day (v1 = 1)
        if self._daily_new_positions >= self.settings.max_new_positions_per_day:
            return RiskDecision(
                approved=False,
                reason="Daily new position limit reached",
                sized_quantity=None,
                risk_metrics={"daily_new": self._daily_new_positions},
                model_tag=model_tag,
                trace_id=trace_id,
            )

        # 4. Gross exposure guard (very loose for bootstrap)
        current_exposure = sum(p.get("market_value", 0) for p in snapshot.open_positions)
        projected = current_exposure + proposed_notional
        if projected > snapshot.equity * (self.settings.max_gross_exposure_pct / 100):
            return RiskDecision(
                approved=False,
                reason="Gross exposure limit would be breached",
                sized_quantity=None,
                risk_metrics={"current": current_exposure, "projected": projected},
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
            },
            model_tag=model_tag,
            trace_id=trace_id,
        )

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
