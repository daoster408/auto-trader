"""AI research committee shadow mode.

The committee is advisory only. It can write research memos for candidates,
but RiskEngine remains the only authority that can approve sizing/order flow.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import socket
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol
from urllib.parse import quote
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from auto_trader.core.models import TradeIntent
from auto_trader.intelligence.brain_guidance import load_brain_guidance_context
from auto_trader.intelligence.research_context import build_verified_research_context
from auto_trader.intelligence.scoreboard_memory import load_scoreboard_memory_context
from auto_trader.utils.logging import get_logger

log = get_logger("auto_trader.intelligence.ai_committee")

PROMPT_VERSION = "ai_research_committee/v3"
SIMPLIFIED_PROMPT_VERSION = "ai_research_single/v2"
AGGREGATE_PROMPT_VERSION = "ai_research_aggregate/v5"
PATTERN_MEMORY_VERSION = "postmortem_edge_memory/v2"
CANDIDATE_MEMORY_MATCH_VERSION = "candidate_memory_match/v3"
CANDIDATE_MEMORY_MATCH_MAX_BYTES = 5000
VALID_VERDICTS = {"approve", "reject", "watch"}
REAL_PROVIDERS = ("openai", "xai", "anthropic", "gemini")
MIN_APPROVE_CONFIDENCE = 0.65
EDGE_MEMORY_ACTIONS = {"amplify", "neutral", "conflict_review", "memory_unavailable", "defer_to_current_packet"}
COMMITTEE_INSTRUCTIONS = (
    "You are an advisory trading research committee. Use only the provided JSON packet. "
    "Do not invent market facts. Do not recommend order size. Use any loaded "
    "scoreboard_memory, brain_guidance/pattern_memory_v2, and candidate_memory_match/v3 "
    "as advisory observed-edge context only; "
    "current verified candidate data has priority, and stale/missing memory is never "
    "a standalone reason to approve or reject. When candidate_memory_match/v3 is available, use "
    "that compact pre-match first, then pattern_memory_v2, before setting edge_memory_* fields. "
    "Return only valid JSON. "
    "Return exactly one top-level JSON object with these keys only: symbol, verdict, "
    "confidence, used_only_provided_data, bull_case, bear_case, edge_memory_alignment, "
    "edge_memory_conflicts, edge_memory_action, judge_summary. edge_memory_action must be "
    "one of: amplify, neutral, conflict_review, memory_unavailable, defer_to_current_packet. "
    "Do not wrap the object in committee, assessment, analysis, result, or any other key."
)
SIMPLIFIED_COMMITTEE_INSTRUCTIONS = (
    "You are the sole advisory research model for one aggressive, dollar-outcome-first trading candidate. "
    "Use only the provided JSON packet, cite its concrete favorable and adverse evidence concisely, do not invent "
    "market facts, and do not recommend order size. Approve when verified evidence supports a plausible favorable "
    "setup and there is no concrete disqualifier. Reject when verified evidence shows a concrete adverse, "
    "contradictory, stale, unusable, or untradeable setup. Use watch only when the supplied directional evidence "
    "is genuinely contradictory or too unusable to choose; ordinary uncertainty is not enough for watch. Missing "
    "optional macro, fundamental, or news lanes increase uncertainty but are never an automatic veto. Positions "
    "may be held across sessions under deterministic exits, so late-session timing is not automatically "
    "disqualifying. Aggressive means a lower uncertainty bar, not approval without favorable evidence. AI is "
    "advisory only: RiskEngine retains sizing and order authority. No historical scoreboard or postmortem memory "
    "is provided; judge the current verified candidate packet directly. Return only valid JSON with exactly these "
    "top-level keys: symbol, verdict, confidence, used_only_provided_data, bull_case, bear_case, "
    "edge_memory_alignment, edge_memory_conflicts, edge_memory_action, judge_summary. Set edge_memory_action to "
    "memory_unavailable and state plainly in both edge_memory text fields that memory is disabled. Verdict must be "
    "approve, watch, or reject. Do not wrap the object in another key and do not include extra prose."
)
COMMITTEE_REQUIRED_FIELDS = [
    "symbol",
    "verdict",
    "confidence",
    "used_only_provided_data",
    "bull_case",
    "bear_case",
    "edge_memory_alignment",
    "edge_memory_conflicts",
    "edge_memory_action",
    "judge_summary",
]


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


class ProviderRequestError(RuntimeError):
    """Sanitized provider failure with bounded diagnostics."""

    def __init__(
        self,
        message: str,
        *,
        provider: str,
        category: str,
        timeout_seconds: float,
        elapsed_ms: int,
        possible_duplicate_paid_request: bool,
        status_code: int | None = None,
    ) -> None:
        super().__init__(message)
        self.provider = provider
        self.category = category
        self.timeout_seconds = float(timeout_seconds)
        self.elapsed_ms = int(elapsed_ms)
        self.possible_duplicate_paid_request = bool(possible_duplicate_paid_request)
        self.status_code = status_code


@dataclass(frozen=True)
class ResearchRound:
    member_memos: list[ResearchMemo]
    aggregate_memo: ResearchMemo


def build_research_packet(
    intent: TradeIntent,
    *,
    signal_id: int | None = None,
    include_edge_memory: bool = True,
) -> dict[str, Any]:
    """Build the verified data packet handed to an AI/shadow committee."""
    verified_context = build_verified_research_context(intent)
    if include_edge_memory:
        verified_context["scoreboard_memory"] = load_scoreboard_memory_context()
        verified_context["brain_guidance"] = load_brain_guidance_context()
        verified_context["candidate_memory_match"] = build_candidate_memory_match(intent, verified_context)
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
        "verified_research_context": verified_context,
        "features": intent.features,
        "rules": {
            "advisory_only": True,
            "cannot_submit_orders": True,
            "cannot_size_orders": True,
            "must_use_only_provided_data": True,
        },
    }


def build_candidate_memory_match(intent: TradeIntent, verified_context: dict[str, Any]) -> dict[str, Any]:
    """Deterministically pre-match a candidate to loaded V2 memory."""
    tags = _candidate_setup_tags(intent, verified_context)
    base: dict[str, Any] = {
        "version": CANDIDATE_MEMORY_MATCH_VERSION,
        "advisory_only": True,
        "order_authority": "RiskEngine",
        "config_authority": "operator_only",
        "candidate_tags": tags[:12],
    }
    guidance = verified_context.get("brain_guidance") if isinstance(verified_context, dict) else {}
    if not isinstance(guidance, dict) or guidance.get("available") is not True:
        return _cap_candidate_memory_match({
            **base,
            "status": "unavailable",
            "available": False,
            "advisory_action": "memory_unavailable",
            "reason": _bounded_text("Brain guidance is missing, stale, or unavailable; judge current packet directly."),
            "matched_approve_pressure": [],
            "matched_demand_current_evidence": [],
            "provider_memory_notes": [],
            "sample_warnings": [],
        })
    memory = guidance.get("pattern_memory")
    if (
        guidance.get("active_memory_version") != PATTERN_MEMORY_VERSION
        or not isinstance(memory, dict)
        or memory.get("version") != PATTERN_MEMORY_VERSION
    ):
        return _cap_candidate_memory_match({
            **base,
            "status": "neutral",
            "available": False,
            "advisory_action": "memory_unavailable",
            "reason": _bounded_text("No valid V2 pattern memory is loaded; judge current packet directly."),
            "matched_approve_pressure": [],
            "matched_demand_current_evidence": [],
            "provider_memory_notes": [],
            "sample_warnings": _bounded_text_list(memory.get("sample_warnings") if isinstance(memory, dict) else [], limit=3),
        })

    tag_set = set(tags)
    guidance_matches = [
        _candidate_guidance_match(row)
        for row in _list(memory.get("candidate_guidance"))
        if isinstance(row, dict) and str(row.get("pattern") or "") in tag_set
    ]
    guidance_matches = [row for row in guidance_matches if row]
    approve_matches = [row for row in guidance_matches if row.get("action") == "approve_pressure"]
    demand_matches = [row for row in guidance_matches if row.get("action") == "demand_current_evidence"]

    for row in _list(memory.get("winning_patterns")):
        match = _pattern_row_match(row, tag_set=tag_set, action="approve_pressure")
        if match and all(existing.get("pattern") != match.get("pattern") for existing in approve_matches):
            approve_matches.append(match)
    for row in _list(memory.get("weak_patterns")):
        match = _pattern_row_match(row, tag_set=tag_set, action="demand_current_evidence")
        if match and all(existing.get("pattern") != match.get("pattern") for existing in demand_matches):
            demand_matches.append(match)

    if approve_matches and demand_matches:
        advisory_action = "mixed"
        reason = "Candidate matches both recent winner and weak-pattern buckets; judge current evidence carefully."
    elif approve_matches:
        advisory_action = "approve_pressure"
        reason = "Candidate matches recent observed edge buckets; use as positive pressure if current data confirms."
    elif demand_matches:
        advisory_action = "demand_current_evidence"
        reason = "Candidate matches recent weak buckets; require stronger current evidence before approval."
    else:
        advisory_action = "neutral"
        reason = "No candidate-specific V2 pattern bucket matched; judge current packet directly."

    return _cap_candidate_memory_match({
        **base,
        "status": "matched" if approve_matches or demand_matches else "neutral",
        "available": True,
        "source_memory_version": PATTERN_MEMORY_VERSION,
        "advisory_action": advisory_action,
        "reason": _bounded_text(reason),
        "matched_approve_pressure": approve_matches[:4],
        "matched_demand_current_evidence": demand_matches[:4],
        "provider_memory_notes": _provider_memory_notes(memory),
        "sample_warnings": _bounded_text_list(memory.get("sample_warnings"), limit=3),
    })


def packet_hash(packet: dict[str, Any]) -> str:
    stable_packet = dict(packet)
    stable_packet.pop("generated_at", None)
    stable_packet.pop("signal_id", None)
    context = stable_packet.get("verified_research_context")
    if isinstance(context, dict):
        stable_context = dict(context)
        for context_key in ("scoreboard_memory", "brain_guidance", "candidate_memory_match"):
            if not isinstance(stable_context.get(context_key), dict):
                continue
            stable_payload = dict(stable_context[context_key])
            stable_payload.pop("path", None)
            stable_payload.pop("age_seconds", None)
            stable_context[context_key] = stable_payload
        stable_packet["verified_research_context"] = stable_context
    payload = json.dumps(stable_packet, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def validate_committee_output(symbol: str, output: dict[str, Any]) -> tuple[bool, list[str]]:
    errors: list[str] = []
    for field in COMMITTEE_REQUIRED_FIELDS:
        if field not in output:
            errors.append(f"missing_{field}")
    if str(output.get("symbol", "")).upper() != symbol.upper():
        errors.append("symbol_mismatch")
    if output.get("verdict") not in VALID_VERDICTS:
        errors.append("invalid_verdict")
    if output.get("used_only_provided_data") is not True:
        errors.append("used_unverified_data")
    if output.get("edge_memory_action") not in EDGE_MEMORY_ACTIONS:
        errors.append("invalid_edge_memory_action_value")
    confidence = output.get("confidence")
    try:
        value = float(confidence)
        if value < 0 or value > 1:
            errors.append("confidence_out_of_range")
    except (TypeError, ValueError):
        errors.append("confidence_not_numeric")
    for field in (
        "bull_case",
        "bear_case",
        "edge_memory_alignment",
        "edge_memory_conflicts",
        "edge_memory_action",
        "judge_summary",
    ):
        if not isinstance(output.get(field), str) or not output.get(field, "").strip():
            errors.append(f"invalid_{field}")
    return not errors, errors


def normalize_committee_output(
    output: dict[str, Any], packet: dict[str, Any] | None = None
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Convert common provider wrapper shapes into the strict committee schema.

    Normalization preserves useful text but keeps validation conservative: missing
    proof that only packet data was used still fails validation.
    """
    source = _committee_source(output)
    assessment = source.get("assessment") if isinstance(source.get("assessment"), dict) else {}
    normalized: dict[str, Any] = {}
    markers: list[str] = []

    provider_symbol = _first_present(output, source, assessment, key="symbol")
    if isinstance(provider_symbol, str) and provider_symbol.strip():
        normalized["symbol"] = provider_symbol.strip().upper()
    else:
        packet_symbol = (((packet or {}).get("candidate") or {}).get("symbol") if isinstance(packet, dict) else None)
        if isinstance(packet_symbol, str) and packet_symbol.strip():
            normalized["symbol"] = packet_symbol.strip().upper()
            markers.append("normalized_symbol_from_packet")
        else:
            markers.append("normalized_missing_symbol")

    verdict = _first_present(output, source, assessment, key="verdict")
    if isinstance(verdict, str) and verdict.lower() in VALID_VERDICTS:
        normalized["verdict"] = verdict.lower()
    else:
        normalized["verdict"] = "watch"
        markers.append("normalized_missing_verdict" if verdict in (None, "") else "normalized_invalid_verdict")

    confidence = _first_present(output, source, assessment, key="confidence")
    if confidence is None:
        confidence = _first_present(output, source, assessment, key="confidence_provided")
    if confidence is not None:
        normalized["confidence"] = confidence
    else:
        markers.append("normalized_missing_confidence")

    used_only = _explicit_true(output, "used_only_provided_data")
    if not used_only:
        used_only = _explicit_true(source, "used_only_provided_data") or _explicit_true(
            assessment, "used_only_provided_data"
        )
    normalized["used_only_provided_data"] = bool(used_only)
    if not used_only:
        markers.append("normalized_missing_used_only_provided_data")

    bull_case = _bounded_text(
        _first_text(
            source.get("bull_case"),
            assessment.get("bull_case"),
            assessment.get("supporting_factors"),
            source.get("supporting_factors"),
        )
    )
    if bull_case:
        normalized["bull_case"] = bull_case
    else:
        markers.append("normalized_missing_bull_case")

    bear_parts = [
        _first_text(source.get("bear_case"), assessment.get("bear_case"), assessment.get("risk_factors")),
        _first_text(source.get("risk_factors")),
        _first_text(source.get("data_limitations"), assessment.get("data_limitations")),
    ]
    bear_case = _bounded_text(" ".join(part for part in bear_parts if part))
    if bear_case:
        normalized["bear_case"] = bear_case
    else:
        markers.append("normalized_missing_bear_case")

    edge_memory_alignment = _bounded_text(
        _first_text(
            source.get("edge_memory_alignment"),
            assessment.get("edge_memory_alignment"),
            source.get("memory_alignment"),
            assessment.get("memory_alignment"),
            source.get("edge_alignment"),
            assessment.get("edge_alignment"),
        )
    )
    if edge_memory_alignment:
        normalized["edge_memory_alignment"] = edge_memory_alignment
    else:
        markers.append("normalized_missing_edge_memory_alignment")

    edge_memory_conflicts = _bounded_text(
        _first_text(
            source.get("edge_memory_conflicts"),
            assessment.get("edge_memory_conflicts"),
            source.get("memory_conflicts"),
            assessment.get("memory_conflicts"),
            source.get("edge_conflicts"),
            assessment.get("edge_conflicts"),
        )
    )
    if edge_memory_conflicts:
        normalized["edge_memory_conflicts"] = edge_memory_conflicts
    else:
        markers.append("normalized_missing_edge_memory_conflicts")

    edge_memory_action = _normalize_edge_memory_action(
        _first_text(
            source.get("edge_memory_action"),
            assessment.get("edge_memory_action"),
            source.get("memory_action"),
            assessment.get("memory_action"),
            source.get("edge_action"),
            assessment.get("edge_action"),
        )
    )
    if edge_memory_action:
        normalized["edge_memory_action"] = edge_memory_action
    else:
        markers.append("normalized_missing_edge_memory_action")

    judge_summary = _bounded_text(
        _first_text(
            source.get("judge_summary"),
            assessment.get("judge_summary"),
            source.get("advisory_note"),
            source.get("final_judgment"),
            source.get("recommendation"),
            source.get("summary"),
            assessment.get("summary"),
        )
    )
    if judge_summary:
        normalized["judge_summary"] = judge_summary
    else:
        markers.append("normalized_missing_judge_summary")

    if markers:
        normalized["normalization_errors"] = markers
    return normalized, {
        "applied": normalized != output,
        "source_shape": "committee_wrapper" if source is not output else "top_level",
        "markers": markers,
    }


def committee_json_schema(*, strict: bool = True) -> dict[str, Any]:
    schema: dict[str, Any] = {
        "type": "object",
        "required": COMMITTEE_REQUIRED_FIELDS,
        "properties": {
            "symbol": {"type": "string"},
            "verdict": {"type": "string", "enum": ["approve", "reject", "watch"]},
            "confidence": {"type": "number", "minimum": 0, "maximum": 1},
            "used_only_provided_data": {"type": "boolean"},
            "bull_case": {"type": "string"},
            "bear_case": {"type": "string"},
            "edge_memory_alignment": {"type": "string"},
            "edge_memory_conflicts": {"type": "string"},
            "edge_memory_action": {
                "type": "string",
                "enum": sorted(EDGE_MEMORY_ACTIONS),
            },
            "judge_summary": {"type": "string"},
        },
    }
    if strict:
        schema["additionalProperties"] = False
    return schema


def committee_prompt(packet: dict[str, Any], *, include_edge_memory: bool = True) -> str:
    if include_edge_memory:
        memory_direction = (
            "For edge_memory_alignment, state which loaded observed-edge/postmortem/"
            "candidate_memory_match_v3 buckets this candidate confirms. For edge_memory_conflicts, state which "
            "loaded patterns argue against it. For edge_memory_action, return exactly one advisory label: "
            "amplify, neutral, conflict_review, memory_unavailable, or defer_to_current_packet; do not change "
            "sizing or config."
        )
    else:
        memory_direction = (
            "Historical edge memory is intentionally disabled. Set edge_memory_action to memory_unavailable and "
            "state that fact plainly in edge_memory_alignment and edge_memory_conflicts. Judge only the current "
            "verified packet; do not change sizing or config."
        )
    return (
        "Review this verified candidate packet. Return exactly the required JSON object, "
        f"with no wrapper keys and no extra prose. {memory_direction}\n\n"
        f"{json.dumps(packet, sort_keys=True, default=str)}"
    )


def _candidate_setup_tags(intent: TradeIntent, verified_context: dict[str, Any]) -> list[str]:
    features = intent.features or {}
    discovery = features.get("discovery") if isinstance(features.get("discovery"), dict) else {}
    technical = verified_context.get("technical") if isinstance(verified_context.get("technical"), dict) else {}
    risk = verified_context.get("risk") if isinstance(verified_context.get("risk"), dict) else {}
    risk_profile = risk.get("risk_profile") if isinstance(risk.get("risk_profile"), dict) else {}
    tags: list[str] = []

    profile = _bounded_text(risk_profile.get("name") or _nested_get(features, ("research_context", "risk_profile", "name")))
    tags.append(f"profile:{profile or 'unknown'}")

    rel_volume = _first_number(discovery.get("rel_volume"), technical.get("rel_volume"))
    if rel_volume is None:
        tags.append("relvol:missing")
    elif rel_volume < 0.8:
        tags.append("relvol:low")
    elif rel_volume < 2.0:
        tags.append("relvol:normal")
    elif rel_volume < 4.0:
        tags.append("relvol:strong")
    else:
        tags.append("relvol:hot")

    change_pct = _decimal_pct(_first_number(discovery.get("change_pct"), technical.get("change_pct")))
    if change_pct is None:
        tags.append("move:missing")
    elif change_pct < 0.005:
        tags.append("move:cold")
    elif change_pct < 0.08:
        tags.append("move:tradable")
    elif change_pct < 0.15:
        tags.append("move:extended")
    else:
        tags.append("move:parabolic")

    spread_pct = _decimal_pct(_first_number(discovery.get("spread_pct"), technical.get("spread_pct")))
    if spread_pct is None:
        tags.append("spread:missing")
    elif spread_pct <= 0.006:
        tags.append("spread:tight")
    elif spread_pct <= 0.01:
        tags.append("spread:workable")
    else:
        tags.append("spread:wide")

    distance_from_high_pct = _decimal_pct(technical.get("distance_from_high_pct"))
    if distance_from_high_pct is not None and distance_from_high_pct >= -0.02:
        tags.append("near_high")

    news = verified_context.get("news")
    tags.append("news:present" if isinstance(news, list) and bool(news) else "news:missing")

    fundamental = verified_context.get("fundamental")
    tags.append("fundamental:present" if _has_meaningful_dict_values(fundamental) else "fundamental:missing")

    macro = verified_context.get("macro")
    if not isinstance(macro, dict) or not macro:
        tags.append("macro:missing")
    elif macro.get("error"):
        tags.append("macro:error")
    else:
        tags.append("macro:ok")

    return tags


def _candidate_guidance_match(row: dict[str, Any]) -> dict[str, Any] | None:
    pattern = _bounded_text(row.get("pattern"), limit=120)
    if not pattern:
        return None
    action = _bounded_text(row.get("action"), limit=40)
    return {
        "pattern": pattern,
        "action": action if action in {"approve_pressure", "demand_current_evidence", "neutral"} else "neutral",
        "reason": _bounded_text(row.get("reason"), limit=240),
        "source": _bounded_text(row.get("source"), limit=60),
        "sample": _bounded_text(row.get("sample"), limit=40),
    }


def _pattern_row_match(row: Any, *, tag_set: set[str], action: str) -> dict[str, Any] | None:
    if not isinstance(row, dict):
        return None
    key = _bounded_text(row.get("key"), limit=120)
    if not key or key not in tag_set:
        return None
    return {
        "pattern": key,
        "action": action,
        "source": _bounded_text(row.get("source"), limit=60),
        "sample": _bounded_text(row.get("sample"), limit=40),
        "n": _safe_int(row.get("n")),
        "realized_pnl": _safe_round(row.get("realized_pnl")),
        "expectancy": _safe_round(row.get("expectancy")),
        "win_rate": _safe_round(row.get("win_rate")),
    }


def _provider_memory_notes(memory: dict[str, Any]) -> list[dict[str, Any]]:
    notes: list[dict[str, Any]] = []
    for kind, key in (("strength", "provider_strengths"), ("weakness", "provider_weaknesses")):
        for row in _list(memory.get(key))[:3]:
            if not isinstance(row, dict):
                continue
            bucket = _bounded_text(row.get("key"), limit=120)
            if not bucket:
                continue
            notes.append(
                {
                    "type": kind,
                    "bucket": bucket,
                    "source": _bounded_text(row.get("source"), limit=60),
                    "sample": _bounded_text(row.get("sample"), limit=40),
                    "n": _safe_int(row.get("n")),
                    "expectancy": _safe_round(row.get("expectancy")),
                    "win_rate": _safe_round(row.get("win_rate")),
                }
            )
    return notes[:5]


def _cap_candidate_memory_match(payload: dict[str, Any]) -> dict[str, Any]:
    payload["max_bytes"] = CANDIDATE_MEMORY_MATCH_MAX_BYTES
    payload["truncated"] = False
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    if len(encoded) <= CANDIDATE_MEMORY_MATCH_MAX_BYTES:
        return payload

    capped = dict(payload)
    capped["truncated"] = True
    capped["max_bytes"] = CANDIDATE_MEMORY_MATCH_MAX_BYTES
    capped["candidate_tags"] = _list(capped.get("candidate_tags"))[:8]
    capped["matched_approve_pressure"] = _list(capped.get("matched_approve_pressure"))[:2]
    capped["matched_demand_current_evidence"] = _list(capped.get("matched_demand_current_evidence"))[:2]
    capped["provider_memory_notes"] = _list(capped.get("provider_memory_notes"))[:2]
    capped["sample_warnings"] = _bounded_text_list(capped.get("sample_warnings"), limit=1)
    capped["reason"] = _bounded_text(capped.get("reason"), limit=160)
    encoded = json.dumps(capped, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    if len(encoded) <= CANDIDATE_MEMORY_MATCH_MAX_BYTES:
        return capped

    capped["matched_approve_pressure"] = []
    capped["matched_demand_current_evidence"] = []
    capped["provider_memory_notes"] = []
    capped["sample_warnings"] = []
    capped["reason"] = "Candidate memory match exceeded the compact payload cap; judge current packet directly."
    capped["advisory_action"] = "neutral"
    capped["status"] = "neutral"
    encoded = json.dumps(capped, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    if len(encoded) <= CANDIDATE_MEMORY_MATCH_MAX_BYTES:
        return capped

    return {
        "version": CANDIDATE_MEMORY_MATCH_VERSION,
        "available": False,
        "advisory_action": "neutral",
        "truncated": True,
        "max_bytes": CANDIDATE_MEMORY_MATCH_MAX_BYTES,
    }


class ShadowResearchCommittee:
    """Zero-cost committee that writes structured memos from verified packet data."""

    provider = "shadow"
    model_tag = "shadow_ai_committee/v0"

    def __init__(self, *, include_edge_memory: bool = True, prompt_version: str = PROMPT_VERSION) -> None:
        self.include_edge_memory = bool(include_edge_memory)
        self.prompt_version = prompt_version

    async def research(self, intent: TradeIntent, *, signal_id: int | None = None) -> ResearchMemo:
        packet = build_research_packet(
            intent,
            signal_id=signal_id,
            include_edge_memory=self.include_edge_memory,
        )
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
            "edge_memory_alignment": _shadow_edge_memory_alignment(packet),
            "edge_memory_conflicts": _shadow_edge_memory_conflicts(packet),
            "edge_memory_action": _shadow_edge_memory_action(packet),
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
            prompt_version=self.prompt_version,
            input_hash=digest,
            verdict=verdict,
            confidence=confidence,
            used_only_provided_data=True,
            validation_passed=validation_passed,
            memo={"input_packet": packet, "committee": output},
        )


class UnavailableResearchCommittee:
    """Non-decision placeholder used when configured real-provider setup fails."""

    provider = "unavailable"
    model_tag = "unavailable/configuration_error"

    def __init__(self, reason: str, *, prompt_version: str) -> None:
        self.reason = reason
        self.prompt_version = prompt_version
        self.include_edge_memory = False

    async def research(self, intent: TradeIntent, *, signal_id: int | None = None) -> ResearchMemo:
        raise RuntimeError("configured AI research provider is unavailable")


class HTTPResearchCommittee:
    provider = "http"

    def __init__(
        self,
        api_key: str,
        *,
        model: str,
        timeout_seconds: float = 20.0,
        include_edge_memory: bool = True,
        prompt_version: str = PROMPT_VERSION,
        instructions: str = COMMITTEE_INSTRUCTIONS,
    ) -> None:
        if not api_key:
            raise ValueError(f"{self.provider} research provider requires an API key")
        self.api_key = api_key
        self.model = model
        self.timeout_seconds = float(timeout_seconds)
        self.include_edge_memory = bool(include_edge_memory)
        self.prompt_version = prompt_version
        self.instructions = instructions

    @property
    def model_tag(self) -> str:
        return f"{self.provider}/{self.model}"

    async def research(self, intent: TradeIntent, *, signal_id: int | None = None) -> ResearchMemo:
        packet = build_research_packet(
            intent,
            signal_id=signal_id,
            include_edge_memory=self.include_edge_memory,
        )
        digest = packet_hash(packet)
        raw = await asyncio.to_thread(self._call_provider, packet)
        raw_output = self._extract_output(raw)
        output, normalization = normalize_committee_output(raw_output, packet)
        validation_passed, validation_errors = validate_committee_output(intent.symbol, output)
        if validation_errors:
            output["validation_errors"] = validation_errors
        return ResearchMemo(
            symbol=intent.symbol.upper(),
            provider=self.provider,
            model_tag=self.model_tag,
            prompt_version=self.prompt_version,
            input_hash=digest,
            verdict=str(output.get("verdict", "watch")),
            confidence=_nullable_float(output.get("confidence")),
            used_only_provided_data=output.get("used_only_provided_data") is True,
            validation_passed=validation_passed,
            memo={
                "input_packet": packet,
                "committee": output,
                "raw_committee_output": raw_output,
                "normalization": normalization,
                "response_id": raw.get("id"),
                "provider_usage": _provider_usage_metadata(raw),
            },
        )

    def _call_provider(self, packet: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError

    def _extract_output(self, response: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError

    def _post_json(self, url: str, body: dict[str, Any], headers: dict[str, str]) -> dict[str, Any]:
        request = Request(
            url,
            data=json.dumps(body).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "User-Agent": "auto-trader/0.1",
                **headers,
            },
            method="POST",
        )
        started = time.monotonic()
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                return json.loads(response.read().decode("utf-8"))
        except Exception as exc:
            elapsed_ms = int((time.monotonic() - started) * 1000)
            category, possible_duplicate_paid_request, status_code = _provider_failure_category(exc)
            message = _sanitize_provider_error(str(exc), api_key=self.api_key)
            raise ProviderRequestError(
                (
                    f"{self.provider} request failed after {self.timeout_seconds:g}s "
                    f"({category}): {message}"
                ),
                provider=self.provider,
                category=category,
                timeout_seconds=self.timeout_seconds,
                elapsed_ms=elapsed_ms,
                possible_duplicate_paid_request=possible_duplicate_paid_request,
                status_code=status_code,
            ) from exc


class OpenAIResearchCommittee(HTTPResearchCommittee):
    """OpenAI Responses API committee. Network/API failures raise to caller."""

    provider = "openai"

    def _call_provider(self, packet: dict[str, Any]) -> dict[str, Any]:
        body = {
            "model": self.model,
            "instructions": self.instructions,
            "input": committee_prompt(packet, include_edge_memory=self.include_edge_memory),
            "text": {
                "format": {
                    "type": "json_schema",
                    "name": "ai_research_memo",
                    "schema": committee_json_schema(strict=True),
                    "strict": True,
                }
            },
        }
        return self._post_json(
            "https://api.openai.com/v1/responses",
            body,
            {"Authorization": f"Bearer {self.api_key}"},
        )

    def _extract_output(self, response: dict[str, Any]) -> dict[str, Any]:
        return _extract_openai_response_json(response)


class XAIResearchCommittee(HTTPResearchCommittee):
    """xAI/Grok OpenAI-compatible chat completions committee."""

    provider = "xai"

    def _call_provider(self, packet: dict[str, Any]) -> dict[str, Any]:
        body = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": self.instructions},
                {
                    "role": "user",
                    "content": committee_prompt(packet, include_edge_memory=self.include_edge_memory),
                },
            ],
            "response_format": {
                "type": "json_schema",
                "json_schema": {
                    "name": "ai_research_memo",
                    "schema": committee_json_schema(strict=True),
                    "strict": True,
                },
            },
        }
        return self._post_json(
            "https://api.x.ai/v1/chat/completions",
            body,
            {"Authorization": f"Bearer {self.api_key}"},
        )

    def _extract_output(self, response: dict[str, Any]) -> dict[str, Any]:
        return _parse_json_text(response["choices"][0]["message"]["content"])


class AnthropicResearchCommittee(HTTPResearchCommittee):
    """Anthropic Messages API committee."""

    provider = "anthropic"

    def __init__(
        self,
        api_key: str,
        *,
        model: str,
        timeout_seconds: float = 20.0,
        max_tokens: int = 2200,
        include_edge_memory: bool = True,
        prompt_version: str = PROMPT_VERSION,
        instructions: str = COMMITTEE_INSTRUCTIONS,
    ) -> None:
        super().__init__(
            api_key,
            model=model,
            timeout_seconds=timeout_seconds,
            include_edge_memory=include_edge_memory,
            prompt_version=prompt_version,
            instructions=instructions,
        )
        self.max_tokens = int(max_tokens)

    def _call_provider(self, packet: dict[str, Any]) -> dict[str, Any]:
        body = {
            "model": self.model,
            "max_tokens": self.max_tokens,
            "system": self.instructions,
            "messages": [
                {
                    "role": "user",
                    "content": committee_prompt(packet, include_edge_memory=self.include_edge_memory),
                }
            ],
        }
        return self._post_json(
            "https://api.anthropic.com/v1/messages",
            body,
            {
                "x-api-key": self.api_key,
                "anthropic-version": "2023-06-01",
            },
        )

    def _extract_output(self, response: dict[str, Any]) -> dict[str, Any]:
        for item in response.get("content", []) or []:
            text = item.get("text")
            if isinstance(text, str):
                return _parse_json_text(text)
        raise ValueError("Anthropic response did not contain text JSON output")


class GeminiResearchCommittee(HTTPResearchCommittee):
    """Google Gemini generateContent committee."""

    provider = "gemini"

    def _call_provider(self, packet: dict[str, Any]) -> dict[str, Any]:
        body = {
            "contents": [
                {
                    "role": "user",
                    "parts": [
                        {
                            "text": (
                                f"{self.instructions}\n\n"
                                f"{committee_prompt(packet, include_edge_memory=self.include_edge_memory)}"
                            )
                        }
                    ],
                }
            ],
            "generationConfig": {
                "response_mime_type": "application/json",
                "response_schema": committee_json_schema(strict=False),
            },
        }
        model = quote(self.model, safe="")
        return self._post_json(
            f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent",
            body,
            {"x-goog-api-key": self.api_key},
        )

    def _extract_output(self, response: dict[str, Any]) -> dict[str, Any]:
        candidates = response.get("candidates") or []
        if not candidates:
            raise ValueError("Gemini response did not contain candidates")
        parts = ((candidates[0].get("content") or {}).get("parts")) or []
        for part in parts:
            text = part.get("text")
            if isinstance(text, str):
                return _parse_json_text(text)
        raise ValueError("Gemini response did not contain text JSON output")


class MultiProviderResearchCommittee:
    """Independent-provider advisory committee with deterministic quorum."""

    provider = "multi"

    def __init__(self, members: list[ResearchCommittee]) -> None:
        if len(members) < 2:
            raise ValueError("multi-provider research committee requires at least two providers")
        self.members = members

    @property
    def provider_names(self) -> list[str]:
        return [str(getattr(member, "provider", "unknown")) for member in self.members]

    @property
    def model_tag(self) -> str:
        return "multi/" + "+".join(str(getattr(member, "model_tag", "unknown")) for member in self.members)

    async def research_round(self, intent: TradeIntent, *, signal_id: int | None = None) -> ResearchRound:
        packet = build_research_packet(intent, signal_id=signal_id)
        digest = packet_hash(packet)
        member_memos = await asyncio.gather(
            *[
                self._safe_member_research(member, intent, signal_id=signal_id, digest=digest)
                for member in self.members
            ]
        )
        aggregate = aggregate_research_memos(intent.symbol, member_memos, packet=packet, input_hash=digest)
        return ResearchRound(member_memos=list(member_memos), aggregate_memo=aggregate)

    async def research(self, intent: TradeIntent, *, signal_id: int | None = None) -> ResearchMemo:
        return (await self.research_round(intent, signal_id=signal_id)).aggregate_memo

    async def _safe_member_research(
        self,
        member: ResearchCommittee,
        intent: TradeIntent,
        *,
        signal_id: int | None,
        digest: str,
    ) -> ResearchMemo:
        try:
            return await member.research(intent, signal_id=signal_id)
        except Exception as exc:
            provider = str(getattr(member, "provider", "unknown"))
            model_tag = str(getattr(member, "model_tag", provider))
            failure = provider_failure_metadata(exc, provider=provider, member=member)
            validation_errors = ["ai_research_provider_failed", f"ai_research_provider_{failure['category']}"]
            return ResearchMemo(
                symbol=intent.symbol.upper(),
                provider=provider,
                model_tag=model_tag,
                prompt_version="ai_research_failure/v0",
                input_hash=digest,
                verdict="watch",
                confidence=None,
                used_only_provided_data=True,
                validation_passed=False,
                memo={
                    "input_packet": build_research_packet(intent, signal_id=signal_id),
                    "committee": {
                        "symbol": intent.symbol.upper(),
                        "verdict": "watch",
                        "confidence": None,
                        "used_only_provided_data": True,
                        "validation_errors": validation_errors,
                        "judge_summary": (
                            "Provider failed during multi-provider advisory research; "
                            "deterministic RiskEngine remains authoritative."
                        ),
                    },
                    "error": failure["message"],
                    "provider_failure": failure,
                },
            )


def aggregate_research_memos(
    symbol: str,
    member_memos: list[ResearchMemo],
    *,
    packet: dict[str, Any],
    input_hash: str,
) -> ResearchMemo:
    """Aggregate independent provider memos into an auditable advisory verdict."""
    risk_profile = _aggregate_risk_profile(packet)
    unanimous_required = _aggregate_unanimous_required(packet)
    valid_memos = [memo for memo in member_memos if memo.validation_passed]
    invalid_count = len(member_memos) - len(valid_memos)
    invalid_providers = [memo.provider for memo in member_memos if not memo.validation_passed]
    valid_rejects = [memo for memo in valid_memos if memo.verdict == "reject"]
    valid_approves = [
        memo
        for memo in valid_memos
        if memo.verdict == "approve" and memo.confidence is not None and memo.confidence >= MIN_APPROVE_CONFIDENCE
    ]

    if valid_rejects:
        verdict = "reject"
        reason = "valid provider reject overrides approve quorum"
    elif invalid_count:
        verdict = "watch"
        reason = "invalid provider output blocks approve quorum"
    elif unanimous_required and len(valid_approves) == len(member_memos) and len(valid_memos) == len(member_memos):
        verdict = "approve"
        reason = "projected high exposure requires every configured provider to approve"
    elif unanimous_required:
        verdict = "watch"
        reason = "projected high exposure requires unanimous valid provider approve"
    elif _aggregate_approve_quorum_met(risk_profile=risk_profile, approve_count=len(valid_approves)):
        verdict = "approve"
        reason = _aggregate_approve_reason(risk_profile)
    else:
        verdict = "watch"
        reason = "approve quorum not met"

    confidences = [memo.confidence for memo in valid_memos if memo.confidence is not None]
    confidence = round(sum(confidences) / len(confidences), 4) if confidences else 0.0
    votes = [_memo_vote_summary(memo) for memo in member_memos]
    output = {
        "symbol": symbol.upper(),
        "verdict": verdict,
        "confidence": confidence,
        "used_only_provided_data": True,
        "bull_case": _bounded_text(
            "; ".join(
                _committee_text(memo, "bull_case")
                for memo in valid_approves
                if _committee_text(memo, "bull_case")
            )
            or "No valid approve quorum was established from provider memos."
        ),
        "bear_case": _bounded_text(
            "; ".join(
                _committee_text(memo, "bear_case")
                for memo in member_memos
                if _committee_text(memo, "bear_case")
            )
            or "Provider failures, invalid memos, or watch votes keep the advisory result conservative."
        ),
        "edge_memory_alignment": _bounded_text(
            "; ".join(
                _committee_text(memo, "edge_memory_alignment")
                for memo in valid_memos
                if _committee_text(memo, "edge_memory_alignment")
            )
            or "No valid provider supplied edge-memory alignment."
        ),
        "edge_memory_conflicts": _bounded_text(
            "; ".join(
                _committee_text(memo, "edge_memory_conflicts")
                for memo in valid_memos
                if _committee_text(memo, "edge_memory_conflicts")
            )
            or "No valid provider supplied edge-memory conflicts."
        ),
        "edge_memory_action": _aggregate_edge_memory_action(valid_memos),
        "judge_summary": _bounded_text(
            f"Multi-provider advisory result is {verdict}: {reason}. "
            "RiskEngine remains the only order and sizing authority."
        ),
    }
    validation_passed, validation_errors = validate_committee_output(symbol, output)
    if validation_errors:
        output["validation_errors"] = validation_errors
    aggregate_hash = hashlib.sha256(
        (
            input_hash
            + ":"
            + AGGREGATE_PROMPT_VERSION
            + ":"
            + ",".join(memo.provider for memo in member_memos)
        ).encode("utf-8")
    ).hexdigest()
    return ResearchMemo(
        symbol=symbol.upper(),
        provider="multi",
        model_tag="multi/" + "+".join(memo.model_tag for memo in member_memos),
        prompt_version=AGGREGATE_PROMPT_VERSION,
        input_hash=aggregate_hash,
        verdict=verdict,
        confidence=confidence,
        used_only_provided_data=True,
        validation_passed=validation_passed,
        memo={
            "input_packet": packet,
            "committee": output,
            "quorum": {
                "rule": _aggregate_rule_text(risk_profile),
                "reason": reason,
                "risk_profile": risk_profile,
                "unanimous_required": unanimous_required,
                "valid_provider_count": len(valid_memos),
                "invalid_provider_count": invalid_count,
                "invalid_providers": invalid_providers,
                "approve_count": len(valid_approves),
                "reject_count": len(valid_rejects),
                "provider_count": len(member_memos),
            },
            "provider_votes": votes,
        },
    )


def _aggregate_risk_profile(packet: dict[str, Any]) -> str:
    context = packet.get("verified_research_context")
    if isinstance(context, dict):
        risk = context.get("risk")
        if isinstance(risk, dict):
            name = _risk_profile_name(risk.get("risk_profile"))
            if name:
                return name
        name = _risk_profile_name(context.get("risk_profile"))
        if name:
            return name
    return "conservative"


def _aggregate_unanimous_required(packet: dict[str, Any]) -> bool:
    context = packet.get("verified_research_context")
    if not isinstance(context, dict):
        return False
    risk = context.get("risk")
    if not isinstance(risk, dict):
        return False
    return bool(risk.get("ai_unanimous_required"))


def _risk_profile_name(value: Any) -> str | None:
    if isinstance(value, dict):
        name = str(value.get("name") or "").lower()
    else:
        name = str(value or "").lower()
    if name in {"conservative", "aggressive", "risky"}:
        return name
    return None


def _aggregate_approve_quorum_met(*, risk_profile: str, approve_count: int) -> bool:
    if risk_profile == "aggressive":
        return approve_count >= 1
    return approve_count >= 2


def _aggregate_approve_reason(risk_profile: str) -> str:
    if risk_profile == "aggressive":
        return "aggressive profile allows one valid provider approve and no valid provider rejected"
    return "at least two valid providers approved and no valid provider rejected"


def _aggregate_rule_text(risk_profile: str) -> str:
    # High-exposure packets can add a stricter unanimous override; that trigger
    # is recorded separately in quorum.unanimous_required/reason.
    if risk_profile == "aggressive":
        return (
            "aggressive approve requires every provider output to validate, at least one valid approve vote "
            "with confidence >= 0.65, and no valid reject"
        )
    return (
        "approve requires every provider output to validate, at least two valid approve votes "
        "with confidence >= 0.65, and no valid reject"
    )


def _aggregate_edge_memory_action(valid_memos: list[ResearchMemo]) -> str:
    actions = []
    for memo in valid_memos:
        committee = memo.memo.get("committee") if isinstance(memo.memo, dict) else {}
        action = committee.get("edge_memory_action") if isinstance(committee, dict) else None
        if action in EDGE_MEMORY_ACTIONS:
            actions.append(str(action))
    if not actions:
        return "memory_unavailable"
    if "conflict_review" in actions:
        return "conflict_review"
    if "amplify" in actions:
        return "amplify"
    if all(action == "memory_unavailable" for action in actions):
        return "memory_unavailable"
    if "defer_to_current_packet" in actions:
        return "defer_to_current_packet"
    return "neutral"


def _shadow_edge_memory_alignment(packet: dict[str, Any]) -> str:
    context = packet.get("verified_research_context")
    if not isinstance(context, dict):
        return "No loaded edge memory was available in the verified packet."
    parts: list[str] = []
    for key, label in (("brain_guidance", "brain guidance"), ("scoreboard_memory", "scoreboard memory")):
        memory = context.get(key)
        if not isinstance(memory, dict):
            continue
        if memory.get("available") is True and str(memory.get("prompt_context") or "").strip():
            parts.append(f"Loaded {label} is available for advisory comparison.")
        elif memory.get("status"):
            parts.append(f"{label} status={memory.get('status')}; no active edge boost from this source.")
    return " ".join(parts) or "No loaded edge memory was available in the verified packet."


def _shadow_edge_memory_conflicts(packet: dict[str, Any]) -> str:
    context = packet.get("verified_research_context")
    if not isinstance(context, dict):
        return "No loaded edge memory conflict was available in the verified packet."
    stale = []
    for key, label in (("brain_guidance", "brain guidance"), ("scoreboard_memory", "scoreboard memory")):
        memory = context.get(key)
        if isinstance(memory, dict) and memory.get("available") is not True:
            stale.append(f"{label} status={memory.get('status', 'missing')}")
    if stale:
        return "Memory unavailable or stale: " + "; ".join(stale) + ". Current packet data remains primary."
    return "No explicit conflict was derived by the zero-cost shadow committee."


def _shadow_edge_memory_action(packet: dict[str, Any]) -> str:
    context = packet.get("verified_research_context")
    if not isinstance(context, dict):
        return "memory_unavailable"
    for key in ("brain_guidance", "scoreboard_memory"):
        memory = context.get(key)
        if isinstance(memory, dict) and memory.get("available") is True:
            return "neutral"
    return "memory_unavailable"


async def research_committee_round(
    committee: ResearchCommittee,
    intent: TradeIntent,
    *,
    signal_id: int | None = None,
) -> ResearchRound:
    if hasattr(committee, "research_round"):
        return await committee.research_round(intent, signal_id=signal_id)  # type: ignore[attr-defined]
    memo = await committee.research(intent, signal_id=signal_id)
    return ResearchRound(member_memos=[memo], aggregate_memo=memo)


def create_research_committee(settings: Any) -> ResearchCommittee:
    simplified = bool(getattr(settings, "simplified_runtime_enabled", False))
    providers = selected_research_providers(settings)
    if len(providers) > 1:
        return MultiProviderResearchCommittee(
            [_create_single_research_committee(settings, provider, simplified=False) for provider in providers]
        )
    provider = providers[0]
    return _create_single_research_committee(settings, provider, simplified=simplified)


def selected_research_providers(settings: Any) -> list[str]:
    simplified = bool(getattr(settings, "simplified_runtime_enabled", False))
    raw_providers = str(getattr(settings, "ai_research_providers", "") or "").strip()
    if simplified:
        providers = [str(getattr(settings, "ai_research_provider", "shadow") or "shadow").lower()]
    elif raw_providers:
        providers = [provider.strip().lower() for provider in raw_providers.split(",") if provider.strip()]
    else:
        providers = [str(getattr(settings, "ai_research_provider", "shadow") or "shadow").lower()]
    providers = list(dict.fromkeys(providers))
    if not providers:
        return ["shadow"]
    if len(providers) > 1 and "shadow" in providers:
        raise ValueError("AI_RESEARCH_PROVIDERS cannot mix shadow with real providers")
    unsupported = [provider for provider in providers if provider != "shadow" and provider not in REAL_PROVIDERS]
    if unsupported:
        raise ValueError(f"unsupported AI_RESEARCH_PROVIDERS={','.join(unsupported)}")
    return providers


def real_research_providers(committee: ResearchCommittee) -> list[str]:
    if isinstance(committee, MultiProviderResearchCommittee):
        return [provider for provider in committee.provider_names if provider != "shadow"]
    provider = str(getattr(committee, "provider", "shadow"))
    return [] if provider in {"shadow", "unavailable"} else [provider]


def _create_single_research_committee(
    settings: Any,
    provider: str,
    *,
    simplified: bool,
) -> ResearchCommittee:
    include_edge_memory = not simplified
    prompt_version = SIMPLIFIED_PROMPT_VERSION if simplified else PROMPT_VERSION
    instructions = SIMPLIFIED_COMMITTEE_INSTRUCTIONS if simplified else COMMITTEE_INSTRUCTIONS
    if provider == "shadow":
        return ShadowResearchCommittee(
            include_edge_memory=include_edge_memory,
            prompt_version=prompt_version,
        )
    model = _required_model(settings, provider)
    timeout_seconds = research_provider_timeout_seconds(settings, provider)
    if provider == "openai":
        return OpenAIResearchCommittee(
            _required_key(settings, "openai_api_key", provider),
            model=model,
            timeout_seconds=timeout_seconds,
            include_edge_memory=include_edge_memory,
            prompt_version=prompt_version,
            instructions=instructions,
        )
    if provider == "xai":
        return XAIResearchCommittee(
            _required_key(settings, "xai_api_key", provider),
            model=model,
            timeout_seconds=timeout_seconds,
            include_edge_memory=include_edge_memory,
            prompt_version=prompt_version,
            instructions=instructions,
        )
    if provider == "anthropic":
        return AnthropicResearchCommittee(
            _required_key(settings, "anthropic_api_key", provider),
            model=model,
            timeout_seconds=timeout_seconds,
            max_tokens=_anthropic_max_tokens(settings),
            include_edge_memory=include_edge_memory,
            prompt_version=prompt_version,
            instructions=instructions,
        )
    if provider == "gemini":
        return GeminiResearchCommittee(
            _required_key(settings, "gemini_api_key", provider),
            model=model,
            timeout_seconds=timeout_seconds,
            include_edge_memory=include_edge_memory,
            prompt_version=prompt_version,
            instructions=instructions,
        )
    raise ValueError(f"unsupported AI_RESEARCH_PROVIDER={provider}")


def research_provider_timeout_seconds(settings: Any, provider: str) -> float:
    shared = float(getattr(settings, "ai_research_timeout_seconds", 8.0) or 8.0)
    override = getattr(settings, f"ai_research_{provider}_timeout_seconds", None)
    if override is None:
        return shared
    return float(override or shared)


def _anthropic_max_tokens(settings: Any) -> int:
    return int(getattr(settings, "ai_research_anthropic_max_tokens", 2200) or 2200)


def _required_key(settings: Any, attr: str, provider: str) -> str:
    value = str(getattr(settings, attr, "") or "").strip()
    if not value:
        raise ValueError(f"AI_RESEARCH_PROVIDER={provider} requires {attr.upper()}")
    return value


def _required_model(settings: Any, provider: str) -> str:
    value = model_for_provider(settings, provider)
    if not value:
        provider_attr = f"ai_research_{provider}_model"
        raise ValueError(f"AI_RESEARCH_PROVIDER={provider} requires {provider_attr.upper()} or AI_RESEARCH_MODEL")
    return value


def model_for_provider(settings: Any, provider: str) -> str:
    provider_attr = f"ai_research_{provider}_model"
    value = str(getattr(settings, provider_attr, "") or "").strip()
    if value:
        return value
    return str(getattr(settings, "ai_research_model", "") or "").strip()


def _memo_vote_summary(memo: ResearchMemo) -> dict[str, Any]:
    committee = memo.memo.get("committee") if isinstance(memo.memo, dict) else {}
    validation_errors = committee.get("validation_errors", []) if isinstance(committee, dict) else []
    return {
        "provider": memo.provider,
        "model_tag": memo.model_tag,
        "prompt_version": memo.prompt_version,
        "input_hash": memo.input_hash,
        "verdict": memo.verdict,
        "confidence": memo.confidence,
        "validation_passed": memo.validation_passed,
        "used_only_provided_data": memo.used_only_provided_data,
        "validation_errors": validation_errors,
    }


def _committee_text(memo: ResearchMemo, field: str) -> str:
    committee = memo.memo.get("committee") if isinstance(memo.memo, dict) else {}
    value = committee.get(field) if isinstance(committee, dict) else None
    return _stringify_points(value)


def _normalize_edge_memory_action(value: Any) -> str:
    text = _stringify_points(value).strip().lower()
    aliases = {
        "supports_trade": "amplify",
        "support": "amplify",
        "press": "amplify",
        "cautions_trade": "conflict_review",
        "caution": "conflict_review",
        "conflict": "conflict_review",
        "unavailable": "memory_unavailable",
        "missing": "memory_unavailable",
        "stale": "memory_unavailable",
        "defer": "defer_to_current_packet",
    }
    if text in EDGE_MEMORY_ACTIONS:
        return text
    return aliases.get(text, text)


def _extract_openai_response_json(response: dict[str, Any]) -> dict[str, Any]:
    if isinstance(response.get("output_text"), str):
        return _parse_json_text(response["output_text"])
    for item in response.get("output", []) or []:
        if item.get("type") != "message":
            continue
        for content in item.get("content", []) or []:
            text = content.get("text")
            if isinstance(text, str):
                return _parse_json_text(text)
    raise ValueError("OpenAI response did not contain JSON text output")


def _parse_json_text(text: str) -> dict[str, Any]:
    stripped = text.strip()
    if stripped.startswith("```"):
        lines = [line for line in stripped.splitlines() if not line.strip().startswith("```")]
        stripped = "\n".join(lines).strip()
    value = json.loads(stripped)
    if not isinstance(value, dict):
        raise ValueError("committee output must be a JSON object")
    return value


def _committee_source(output: dict[str, Any]) -> dict[str, Any]:
    committee = output.get("committee")
    if isinstance(committee, dict):
        return committee
    return output


def _first_present(*sources: dict[str, Any], key: str) -> Any:
    for source in sources:
        if isinstance(source, dict) and key in source:
            return source.get(key)
    return None


def _explicit_true(source: dict[str, Any], key: str) -> bool:
    return isinstance(source, dict) and source.get(key) is True


def _first_text(*values: Any) -> str:
    for value in values:
        text = _stringify_points(value)
        if text:
            return text
    return ""


def _stringify_points(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return " ".join(value.split())
    if isinstance(value, list):
        parts = [_stringify_points(item) for item in value]
        return " ".join(part for part in parts if part)
    if isinstance(value, dict):
        parts = []
        for key, item in value.items():
            text = _stringify_points(item)
            if text:
                parts.append(f"{key}: {text}")
        return " ".join(parts)
    return str(value).strip()


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _bounded_text_list(value: Any, *, limit: int) -> list[str]:
    return [_bounded_text(item, limit=240) for item in _list(value)[:limit] if _bounded_text(item, limit=240)]


def _nested_get(source: Any, path: tuple[str, ...]) -> Any:
    current = source
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _first_number(*values: Any) -> float | None:
    for value in values:
        parsed = _nullable_float(value)
        if parsed is not None:
            return parsed
    return None


def _decimal_pct(value: Any) -> float | None:
    parsed = _nullable_float(value)
    if parsed is None:
        return None
    return parsed / 100.0 if abs(parsed) > 1 else parsed


def _has_meaningful_dict_values(value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    for item in value.values():
        if item not in (None, "", 0, 0.0, [], {}):
            return True
    return False


def _safe_int(value: Any) -> int:
    try:
        return int(float(value or 0))
    except (TypeError, ValueError):
        return 0


def _safe_round(value: Any) -> float:
    parsed = _nullable_float(value)
    return round(parsed or 0.0, 4)


def _bounded_text(text: str, *, limit: int = 800) -> str:
    compact = " ".join(str(text or "").split())
    if len(compact) <= limit:
        return compact
    return compact[: limit - 3].rstrip() + "..."


def _provider_usage_metadata(response: dict[str, Any]) -> dict[str, Any]:
    usage = response.get("usage")
    if isinstance(usage, dict):
        normalized: dict[str, Any] = {
            key: int(value)
            for key, value in {
                "input_tokens": usage.get("input_tokens") or usage.get("prompt_tokens"),
                "output_tokens": usage.get("output_tokens") or usage.get("completion_tokens"),
                "total_tokens": usage.get("total_tokens"),
            }.items()
            if isinstance(value, int) and value >= 0
        }
        prompt_details = usage.get("prompt_tokens_details") or usage.get("input_tokens_details")
        completion_details = usage.get("completion_tokens_details") or usage.get("output_tokens_details")
        if isinstance(prompt_details, dict) or isinstance(completion_details, dict):
            input_tokens = int(normalized.get("input_tokens", 0))
            output_tokens = int(normalized.get("output_tokens", 0))
            total_tokens = int(normalized.get("total_tokens", 0))
            cached_tokens = _safe_int(
                prompt_details.get("cached_tokens") if isinstance(prompt_details, dict) else 0
            )
            reasoning_tokens = _safe_int(
                completion_details.get("reasoning_tokens") if isinstance(completion_details, dict) else 0
            )
            degraded = cached_tokens > input_tokens
            cached_tokens = min(cached_tokens, input_tokens)
            regular_input_tokens = max(0, input_tokens - cached_tokens)
            if reasoning_tokens and total_tokens >= input_tokens + output_tokens + reasoning_tokens:
                completion_tokens = output_tokens
            elif reasoning_tokens:
                degraded = degraded or reasoning_tokens > output_tokens
                completion_tokens = max(0, output_tokens - reasoning_tokens)
            else:
                completion_tokens = output_tokens
            normalized.update(
                {
                    "regular_input_tokens": regular_input_tokens,
                    "cached_input_tokens": cached_tokens,
                    "completion_tokens": completion_tokens,
                    "reasoning_tokens": reasoning_tokens,
                    "usage_detail": "provider_explicit_degraded" if degraded else "provider_explicit",
                }
            )
        return normalized
    usage = response.get("usageMetadata")
    if isinstance(usage, dict):
        return {
            key: int(value)
            for key, value in {
                "input_tokens": usage.get("promptTokenCount"),
                "output_tokens": usage.get("candidatesTokenCount"),
                "total_tokens": usage.get("totalTokenCount"),
            }.items()
            if isinstance(value, int) and value >= 0
        }
    return {}


def _sanitize_provider_error(message: str, *, api_key: str | None = None, limit: int = 300) -> str:
    sanitized = str(message or "")
    if api_key:
        sanitized = sanitized.replace(api_key, "[REDACTED]")
    lowered = sanitized.lower()
    for marker in ("authorization:", "x-api-key:", "api-key:", "bearer "):
        if marker in lowered:
            sanitized = "provider error contained credential-like text; redacted"
            break
    return _bounded_text(sanitized, limit=limit)


def _provider_failure_category(exc: Exception) -> tuple[str, bool, int | None]:
    message = str(exc).lower()
    status_code: int | None = None
    if isinstance(exc, HTTPError):
        status_code = int(exc.code)
        if status_code in {401, 403}:
            return "auth_error", False, status_code
        if status_code == 408:
            return "timeout", True, status_code
        if status_code == 429:
            return "rate_limited", False, status_code
        if status_code == 404 and "model" in message:
            return "model_unavailable", False, status_code
        if 500 <= status_code <= 599:
            return "provider_5xx", True, status_code
        return "http_error", False, status_code
    if isinstance(exc, (TimeoutError, socket.timeout)) or "timed out" in message or "timeout" in message:
        return "timeout", True, status_code
    if isinstance(exc, json.JSONDecodeError) or "json" in message or "unterminated string" in message:
        return "invalid_json", True, status_code
    if isinstance(exc, URLError):
        return "network_error", True, status_code
    if "rate limit" in message or "too many requests" in message:
        return "rate_limited", False, status_code
    if "model" in message and ("not found" in message or "unavailable" in message):
        return "model_unavailable", False, status_code
    return "unknown", True, status_code


def provider_failure_metadata(exc: Exception, *, provider: str, member: Any) -> dict[str, Any]:
    if isinstance(exc, ProviderRequestError):
        category = exc.category
        possible_duplicate_paid_request = exc.possible_duplicate_paid_request
        status_code = exc.status_code
        timeout_seconds = exc.timeout_seconds
        elapsed_ms = exc.elapsed_ms
        message = _bounded_text(str(exc), limit=300)
    else:
        category, possible_duplicate_paid_request, status_code = _provider_failure_category(exc)
        timeout_seconds = float(getattr(member, "timeout_seconds", 0.0) or 0.0)
        elapsed_ms = None
        message = _sanitize_provider_error(str(exc), api_key=getattr(member, "api_key", None), limit=300)
    metadata: dict[str, Any] = {
        "provider": provider,
        "category": category,
        "message": message,
        "timeout_seconds": timeout_seconds,
        "possible_duplicate_paid_request": possible_duplicate_paid_request,
    }
    if elapsed_ms is not None:
        metadata["elapsed_ms"] = elapsed_ms
    if status_code is not None:
        metadata["status_code"] = status_code
    return metadata


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
