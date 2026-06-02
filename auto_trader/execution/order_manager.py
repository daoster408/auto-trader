"""Order Manager - thin execution layer.

Responsibilities:
- Receive a TradeIntent (after signal generation)
- Run it through RiskEngine (the only gate)
- If approved, call AlpacaAdapter.submit_order()
- Log everything with model tag and trace_id for audit
- Return structured result

This is the only place that should ever call adapter.submit_order().
"""
from datetime import UTC, datetime
from typing import Any

from auto_trader.broker.alpaca_adapter import AlpacaAdapter
from auto_trader.core.models import RiskDecision, TradeIntent
from auto_trader.core.risk_engine import RiskEngine
from auto_trader.utils.logging import get_logger


class OrderManager:
    """Minimal OrderManager for v1 paper trading."""

    def __init__(self, risk_engine: RiskEngine, adapter: AlpacaAdapter):
        self.risk = risk_engine
        self.adapter = adapter
        self.log = get_logger("auto_trader.execution.order_manager")

    async def submit_trade_intent(
        self,
        intent: TradeIntent,
        snapshot: Any,  # SystemSnapshot or dict for now
    ) -> dict[str, Any]:
        """
        Main entry point for submitting a trade.

        1. Risk check (mandatory)
        2. If approved → real order submission
        3. Return result with full audit info
        """
        decision: RiskDecision = self.risk.evaluate(intent, snapshot)

        result: dict[str, Any] = {
            "intent": {
                "symbol": intent.symbol,
                "side": intent.side,
                "entry_price": intent.entry_price,
                "rationale": intent.rationale,
            },
            "risk_decision": {
                "approved": decision.approved,
                "reason": decision.reason,
                "sized_quantity": decision.sized_quantity,
                "trace_id": decision.trace_id,
                "model_tag": decision.model_tag,
            },
            "order": None,
            "timestamp": datetime.now(UTC).isoformat() + "Z",
        }

        if not decision.approved:
            self.log.warning("order_rejected_by_risk", **result["risk_decision"])
            return result

        if not decision.sized_quantity or decision.sized_quantity <= 0:
            result["risk_decision"]["approved"] = False
            result["risk_decision"]["reason"] = "RiskEngine returned invalid size"
            self.log.error("order_blocked_invalid_risk_size", trace_id=decision.trace_id)
            return result

        # Risk approved → submit real order
        try:
            order_result = await self.adapter.submit_order(
                symbol=intent.symbol,
                qty=decision.sized_quantity,
                side=intent.side,
                order_type="market",
            )
            result["order"] = order_result
            self.log.info("order_submitted_via_manager", order=order_result, trace_id=decision.trace_id)
        except Exception as e:
            self.log.exception("order_submission_failed_after_risk_approval", error=str(e))
            result["order"] = {"error": str(e)}
            result["risk_decision"]["approved"] = False  # mark as failed post-approval

        return result
