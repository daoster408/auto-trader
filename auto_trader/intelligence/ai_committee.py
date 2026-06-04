"""AI research committee shadow mode.

The committee is advisory only. It can write research memos for candidates,
but RiskEngine remains the only authority that can approve sizing/order flow.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol
from urllib.request import Request, urlopen

from auto_trader.core.models import TradeIntent
from auto_trader.utils.logging import get_logger

log = get_logger("auto_trader.intelligence.ai_committee")

PROMPT_VERSION = "ai_research_committee/v0"
VALID_VERDICTS = {"approve", "reject", "watch"}


class ResearchCommittee(Protocol):
    async def research(self, intent: TradeIntent, *, signal_id: int | None = None) -> "ResearchMemo":
        ...


@dataclass(frozen=True)
class ResearchMemo:
    symbol: str
    provider: str
    model_tag: str
    prompt_version: str
    input_hash: str
    verdict: str
    confidence: float | None
    used_only_provided_data: bool
    validation_passed: bool
    memo: dict[str, Any]


def build_research_packet(intent: TradeIntent, *, signal_id: int | None = None) -> dict[str, Any]:
    """Build the verified data packet handed to an AI/shadow committee."""
    return {
        "generated_at": datetime.now(UTC).isoformat() + "Z",
        "signal_id": signal_id,
        "candidate": {
            "symbol": intent.symbol.upper(),
            "side": intent.side,
            "entry_price": intent.entry_price,
            "confidence": intent.confidence,
            "rationale": intent.rationale,
        },
        "features": intent.features,
        "rules": {
            "advisory_only": True,
            "cannot_submit_orders": True,
            "cannot_size_orders": True,
            "must_use_only_provided_data": True,
        },
    }


def packet_hash(packet: dict[str, Any]) -> str:
    payload = json.dumps(packet, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def validate_committee_output(symbol: str, output: dict[str, Any]) -> tuple[bool, list[str]]:
    errors: list[str] = []
    if str(output.get("symbol", "")).upper() != symbol.upper():
        errors.append("symbol_mismatch")
    if output.get("verdict") not in VALID_VERDICTS:
        errors.append("invalid_verdict")
    if output.get("used_only_provided_data") is not True:
        errors.append("used_unverified_data")
    confidence = output.get("confidence")
    if confidence is not None:
        try:
            value = float(confidence)
            if value < 0 or value > 1:
                errors.append("confidence_out_of_range")
        except (TypeError, ValueError):
            errors.append("confidence_not_numeric")
    return not errors, errors


class ShadowResearchCommittee:
    """Zero-cost committee that writes structured memos from verified packet data."""

    provider = "shadow"
    model_tag = "shadow_ai_committee/v0"

    async def research(self, intent: TradeIntent, *, signal_id: int | None = None) -> ResearchMemo:
        packet = build_research_packet(intent, signal_id=signal_id)
        digest = packet_hash(packet)
        features = intent.features or {}
        discovery = features.get("discovery") or {}
        finnhub = features.get("finnhub") or {}
        score = _float(discovery.get("score"))
        rel_volume = _float(discovery.get("rel_volume"))
        change_pct = _float(discovery.get("change_pct"))
        spread_pct = _float(discovery.get("spread_pct"))

        verdict = "watch"
        confidence = min(max(float(intent.confidence or 0.0), 0.0), 1.0)
        if confidence >= 0.7 and score >= 4.0 and (spread_pct is None or spread_pct <= 0.004):
            verdict = "approve"
        elif confidence < 0.45 or change_pct > 0.11:
            verdict = "reject"

        output = {
            "symbol": intent.symbol.upper(),
            "verdict": verdict,
            "confidence": confidence,
            "used_only_provided_data": True,
            "bull_case": (
                f"Rules packet shows constructive momentum with rel_volume={rel_volume:.2f} "
                f"and change_pct={change_pct:.2%}."
            ),
            "bear_case": (
                "Shadow committee cannot verify catalysts beyond provided packet; "
                f"spread_pct={spread_pct} and news context must remain advisory."
            ),
            "judge_summary": "Advisory memo only; deterministic RiskEngine remains the execution gate.",
            "data_sources": sorted(k for k, v in features.items() if v),
            "finnhub_enabled": bool(finnhub.get("enabled")) if isinstance(finnhub, dict) else False,
        }
        validation_passed, validation_errors = validate_committee_output(intent.symbol, output)
        if validation_errors:
            output["validation_errors"] = validation_errors
        return ResearchMemo(
            symbol=intent.symbol.upper(),
            provider=self.provider,
            model_tag=self.model_tag,
            prompt_version=PROMPT_VERSION,
            input_hash=digest,
            verdict=verdict,
            confidence=confidence,
            used_only_provided_data=True,
            validation_passed=validation_passed,
            memo={"input_packet": packet, "committee": output},
        )


class OpenAIResearchCommittee:
    """Optional OpenAI Responses API committee. Network/API failures raise to caller."""

    provider = "openai"

    def __init__(self, api_key: str, *, model: str, timeout_seconds: float = 20.0) -> None:
        self.api_key = api_key
        self.model = model
        self.timeout_seconds = timeout_seconds

    @property
    def model_tag(self) -> str:
        return f"openai/{self.model}"

    async def research(self, intent: TradeIntent, *, signal_id: int | None = None) -> ResearchMemo:
        packet = build_research_packet(intent, signal_id=signal_id)
        digest = packet_hash(packet)
        raw = await asyncio.to_thread(self._call_openai, packet)
        output = _extract_json_output(raw)
        validation_passed, validation_errors = validate_committee_output(intent.symbol, output)
        if validation_errors:
            output["validation_errors"] = validation_errors
        return ResearchMemo(
            symbol=intent.symbol.upper(),
            provider=self.provider,
            model_tag=self.model_tag,
            prompt_version=PROMPT_VERSION,
            input_hash=digest,
            verdict=str(output.get("verdict", "watch")),
            confidence=_nullable_float(output.get("confidence")),
            used_only_provided_data=output.get("used_only_provided_data") is True,
            validation_passed=validation_passed,
            memo={"input_packet": packet, "committee": output, "response_id": raw.get("id")},
        )

    def _call_openai(self, packet: dict[str, Any]) -> dict[str, Any]:
        schema = {
            "type": "object",
            "additionalProperties": False,
            "required": [
                "symbol",
                "verdict",
                "confidence",
                "used_only_provided_data",
                "bull_case",
                "bear_case",
                "judge_summary",
            ],
            "properties": {
                "symbol": {"type": "string"},
                "verdict": {"type": "string", "enum": ["approve", "reject", "watch"]},
                "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                "used_only_provided_data": {"type": "boolean"},
                "bull_case": {"type": "string"},
                "bear_case": {"type": "string"},
                "judge_summary": {"type": "string"},
            },
        }
        body = {
            "model": self.model,
            "instructions": (
                "You are an advisory trading research committee. Use only the provided JSON packet. "
                "Do not invent market facts. Do not recommend order size. Return only structured JSON."
            ),
            "input": json.dumps(packet, sort_keys=True, default=str),
            "text": {
                "format": {
                    "type": "json_schema",
                    "name": "ai_research_memo",
                    "schema": schema,
                    "strict": True,
                }
            },
        }
        request = Request(
            "https://api.openai.com/v1/responses",
            data=json.dumps(body).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
                "User-Agent": "auto-trader/0.1",
            },
            method="POST",
        )
        with urlopen(request, timeout=self.timeout_seconds) as response:
            return json.loads(response.read().decode("utf-8"))


def create_research_committee(settings: Any) -> ResearchCommittee:
    provider = str(getattr(settings, "ai_research_provider", "shadow") or "shadow").lower()
    if provider == "openai" and getattr(settings, "openai_api_key", None):
        return OpenAIResearchCommittee(
            str(getattr(settings, "openai_api_key")),
            model=str(getattr(settings, "ai_research_model", "gpt-4.1-mini")),
        )
    return ShadowResearchCommittee()


def _extract_json_output(response: dict[str, Any]) -> dict[str, Any]:
    if isinstance(response.get("output_text"), str):
        return json.loads(response["output_text"])
    for item in response.get("output", []) or []:
        if item.get("type") != "message":
            continue
        for content in item.get("content", []) or []:
            text = content.get("text")
            if isinstance(text, str):
                return json.loads(text)
    raise ValueError("OpenAI response did not contain JSON text output")


def _float(value: Any, default: float = 0.0) -> float:
    parsed = _nullable_float(value)
    return default if parsed is None else parsed


def _nullable_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None
