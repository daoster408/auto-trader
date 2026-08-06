"""Secret-safe immutable runtime and decision provenance."""
from __future__ import annotations

import hashlib
import json
import os
import socket
import uuid
from dataclasses import dataclass
from typing import Any

from auto_trader.persistence.db import create_decision_context, create_runtime_session


SAFE_CONFIG_ALIASES = frozenset(
    {
        "ALPACA_PAPER",
        "SIMPLIFIED_RUNTIME_ENABLED",
        "RISK_PER_TRADE_PCT",
        "RISK_PROFILE",
        "MAX_POSITION_NOTIONAL_PCT",
        "MAX_NEW_POSITIONS_PER_DAY",
        "MAX_GROSS_EXPOSURE_PCT",
        "DAILY_LOSS_HALT_PCT",
        "WEEKLY_LOSS_HALT_PCT",
        "PEAK_DRAWDOWN_HALT_PCT",
        "CONSECUTIVE_SL_HALT",
        "AUTO_ENTRY_ENABLED",
        "AUTO_EXIT_ENABLED",
        "POSITION_MAX_LOSS_PCT",
        "POSITION_TAKE_PROFIT_PCT",
        "POSITION_TRAILING_STOP_PCT",
        "POSITION_MAX_HOLD_DAYS",
        "POSITION_STAGNATION_EXIT_ENABLED",
        "POSITION_STAGNATION_MIN_HOLD_DAYS",
        "POSITION_STAGNATION_MIN_PNL_PCT",
        "POSITION_STAGNATION_MAX_PNL_PCT",
        "POSITION_STAGNATION_MAX_REL_VOLUME",
        "POSITION_STAGNATION_MAX_DAILY_RANGE_PCT",
        "AI_RESEARCH_ENABLED",
        "AI_ENTRY_GATE_ENABLED",
        "AI_RESEARCH_PROVIDER",
        "AI_RESEARCH_PROVIDERS",
        "AI_RESEARCH_MODEL",
        "AI_RESEARCH_OPENAI_MODEL",
        "AI_RESEARCH_XAI_MODEL",
        "AI_RESEARCH_ANTHROPIC_MODEL",
        "AI_RESEARCH_GEMINI_MODEL",
        "AI_RESEARCH_MAX_CALLS_PER_DAY",
        "AI_HIGH_EXPOSURE_UNANIMOUS_THRESHOLD_PCT",
        "AI_UNANIMOUS_AFTER_BUDGET_PCT",
    }
)

SAFE_EFFECTIVE_KEYS = frozenset(
    {
        "ai_entry_gate_enabled",
        "ai_entry_gate_source",
        "ai_research_enabled",
        "auto_entry_enabled",
        "auto_entry_source",
        "decision_source",
        "execution_mode",
        "max_gross_exposure_pct",
        "max_gross_exposure_source",
        "max_new_positions_per_day",
        "max_new_positions_source",
        "model_tag",
        "prompt_version",
        "provider",
        "risk_profile",
        "risk_profile_source",
        "simplified_runtime_enabled",
    }
)


@dataclass(frozen=True)
class RuntimeProvenance:
    session_id: str
    config_hash: str
    config_snapshot: dict[str, Any]


def redacted_config_snapshot(
    settings: Any,
    *,
    effective: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return an allowlisted snapshot; every other configured value is redacted."""
    if hasattr(settings, "model_dump"):
        raw = settings.model_dump(by_alias=True, mode="json")
    else:
        raw = {
            str(key).upper(): value
            for key, value in vars(settings).items()
            if not str(key).startswith("_")
        }
    snapshot = {
        str(key): value if str(key) in SAFE_CONFIG_ALIASES else "<redacted>"
        for key, value in sorted(raw.items(), key=lambda item: str(item[0]))
    }
    snapshot["EFFECTIVE"] = {
        str(key): value if str(key) in SAFE_EFFECTIVE_KEYS else "<redacted>"
        for key, value in sorted((effective or {}).items(), key=lambda item: str(item[0]))
    }
    snapshot["SNAPSHOT_POLICY"] = "explicit_allowlist_v1"
    return snapshot


def config_fingerprint(snapshot: dict[str, Any]) -> str:
    canonical = json.dumps(snapshot, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


async def start_runtime_provenance(
    settings: Any,
    *,
    process_role: str,
    execution_mode: str,
) -> RuntimeProvenance | None:
    snapshot = redacted_config_snapshot(settings)
    config_hash = config_fingerprint(snapshot)
    session_id = str(uuid.uuid4())
    persisted = await create_runtime_session(
        session_id=session_id,
        host_name=socket.gethostname(),
        process_id=os.getpid(),
        process_role=process_role,
        execution_mode=execution_mode,
        config_hash=config_hash,
        config_snapshot=snapshot,
    )
    if persisted is None:
        return None
    return RuntimeProvenance(
        session_id=session_id,
        config_hash=config_hash,
        config_snapshot=snapshot,
    )


async def capture_decision_provenance(
    session: RuntimeProvenance,
    settings: Any,
    *,
    decision_source: str,
    ai_entry_gate_enabled: bool | None,
    ai_entry_gate_source: str | None,
    ai_research_enabled: bool | None,
    simplified_runtime_enabled: bool | None,
    execution_mode: str,
    provider: str | None,
    model_tag: str | None,
    prompt_version: str | None,
    risk_profile: str | None,
    effective_config: dict[str, Any] | None = None,
) -> int | None:
    effective = {
        "ai_entry_gate_enabled": ai_entry_gate_enabled,
        "ai_entry_gate_source": ai_entry_gate_source,
        "ai_research_enabled": ai_research_enabled,
        "decision_source": decision_source,
        "execution_mode": execution_mode,
        "model_tag": model_tag,
        "prompt_version": prompt_version,
        "provider": provider,
        "risk_profile": risk_profile,
        "simplified_runtime_enabled": simplified_runtime_enabled,
    }
    effective.update(effective_config or {})
    snapshot = redacted_config_snapshot(settings, effective=effective)
    config_hash = config_fingerprint(snapshot)
    return await create_decision_context(
        runtime_session_id=session.session_id,
        decision_source=decision_source,
        ai_entry_gate_enabled=ai_entry_gate_enabled,
        ai_entry_gate_source=ai_entry_gate_source,
        ai_research_enabled=ai_research_enabled,
        simplified_runtime_enabled=simplified_runtime_enabled,
        execution_mode=execution_mode,
        provider=provider,
        model_tag=model_tag,
        prompt_version=prompt_version,
        risk_profile=risk_profile,
        config_hash=config_hash,
        config_snapshot=snapshot,
    )
