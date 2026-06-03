"""Core domain models (immutable where possible)."""
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Literal


class SystemState(str, Enum):
    ACTIVE = "ACTIVE"
    PAUSED = "PAUSED"
    HALTED = "HALTED"


@dataclass(frozen=True)
class RiskDecision:
    """Result of risk engine evaluation. Never bypass this."""
    approved: bool
    reason: str
    sized_quantity: float | None
    risk_metrics: dict
    model_tag: str | None = None
    trace_id: str | None = None


@dataclass(frozen=True)
class TradeIntent:
    """Proposed trade before risk gate."""
    symbol: str
    side: Literal["long"]  # v1 cash account only long
    entry_price: float
    stop_price: float | None = None
    rationale: str = ""
    confidence: float = 0.0
    features: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SystemSnapshot:
    """Point-in-time view for risk and status."""
    state: SystemState
    equity: float
    cash: float
    open_positions: list[dict]
    daily_pnl: float
    weekly_pnl: float
    peak_equity: float
    consecutive_losses: int
    updated_at: datetime


@dataclass(frozen=True)
class KillResult:
    """Outcome of /kill execution."""
    success: bool
    orders_cancelled: int
    positions_flattened: int
    reason: str
    incident_report: str
    timestamp: datetime
