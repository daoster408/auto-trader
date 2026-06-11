"""Risk profile controls for paper/live experimentation.

Profiles can widen opportunity search and sizing, but they never bypass the
state machine, account halts, broker blocks, duplicate guards, AI gate, or
RiskEngine.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


RiskProfileName = Literal["conservative", "aggressive", "risky"]

VALID_RISK_PROFILES: tuple[RiskProfileName, ...] = ("conservative", "aggressive", "risky")


@dataclass(frozen=True)
class DiscoveryProfile:
    min_price: float
    max_price: float
    min_dollar_volume: float
    max_spread_pct: float
    min_change_pct: float
    max_change_pct: float
    min_intraday_pct: float
    max_assets: int
    max_candidates: int


@dataclass(frozen=True)
class PaidAIPrefilterProfile:
    min_rel_volume: float
    strong_rel_volume: float
    high_buffer_pct: float
    block_inverse_overlap: bool


@dataclass(frozen=True)
class RiskProfile:
    name: RiskProfileName
    label: str
    paper_only: bool
    early_notional_cap_pct: float
    discovery: DiscoveryProfile
    paid_ai_prefilter: PaidAIPrefilterProfile


CONSERVATIVE_PROFILE = RiskProfile(
    name="conservative",
    label="Conservative",
    paper_only=False,
    early_notional_cap_pct=0.05,
    discovery=DiscoveryProfile(
        min_price=3.0,
        max_price=120.0,
        min_dollar_volume=2_000_000,
        max_spread_pct=0.006,
        min_change_pct=0.008,
        max_change_pct=0.12,
        min_intraday_pct=-0.02,
        max_assets=750,
        max_candidates=10,
    ),
    paid_ai_prefilter=PaidAIPrefilterProfile(
        min_rel_volume=1.0,
        strong_rel_volume=2.5,
        high_buffer_pct=0.002,
        block_inverse_overlap=True,
    ),
)

AGGRESSIVE_PROFILE = RiskProfile(
    name="aggressive",
    label="Aggressive",
    paper_only=True,
    early_notional_cap_pct=0.075,
    discovery=DiscoveryProfile(
        min_price=2.0,
        max_price=150.0,
        min_dollar_volume=1_000_000,
        max_spread_pct=0.01,
        min_change_pct=0.005,
        max_change_pct=0.18,
        min_intraday_pct=-0.035,
        max_assets=900,
        max_candidates=12,
    ),
    paid_ai_prefilter=PaidAIPrefilterProfile(
        min_rel_volume=0.8,
        strong_rel_volume=2.0,
        high_buffer_pct=0.003,
        block_inverse_overlap=True,
    ),
)

RISKY_PROFILE = RiskProfile(
    name="risky",
    label="Risky",
    paper_only=True,
    early_notional_cap_pct=0.10,
    discovery=DiscoveryProfile(
        min_price=1.0,
        max_price=200.0,
        min_dollar_volume=500_000,
        max_spread_pct=0.015,
        min_change_pct=0.003,
        max_change_pct=0.25,
        min_intraday_pct=-0.06,
        max_assets=1_100,
        max_candidates=15,
    ),
    paid_ai_prefilter=PaidAIPrefilterProfile(
        min_rel_volume=0.6,
        strong_rel_volume=1.6,
        high_buffer_pct=0.005,
        block_inverse_overlap=True,
    ),
)

_PROFILES: dict[str, RiskProfile] = {
    CONSERVATIVE_PROFILE.name: CONSERVATIVE_PROFILE,
    AGGRESSIVE_PROFILE.name: AGGRESSIVE_PROFILE,
    RISKY_PROFILE.name: RISKY_PROFILE,
}


def normalize_risk_profile(value: object, *, paper: bool) -> RiskProfileName:
    requested = str(value or "conservative").strip().lower()
    if requested not in _PROFILES:
        requested = "conservative"
    profile = _PROFILES[requested]
    if profile.paper_only and not paper:
        return "conservative"
    return profile.name


def get_risk_profile(value: object, *, paper: bool) -> RiskProfile:
    return _PROFILES[normalize_risk_profile(value, paper=paper)]
