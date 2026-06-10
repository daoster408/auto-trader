"""Explicit paid/offline AI postmortem review for observed trading edge."""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
from dataclasses import replace
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import quote
from urllib.request import Request, urlopen

from auto_trader.brain_review import (
    build_brain_review_bundle,
    default_brain_guidance_path,
    default_brain_review_dir,
    write_brain_review_bundle,
)
from auto_trader.config.settings import get_settings
from auto_trader.edge_report import EdgeReport, build_edge_report
from auto_trader.intelligence.ai_committee import (
    REAL_PROVIDERS,
    _extract_openai_response_json,
    _parse_json_text,
    model_for_provider,
    selected_research_providers,
)
from auto_trader.persistence.db import (
    configure_db_path,
    count_ai_postmortem_chargeable_attempts,
    count_ai_research_memos,
    init_db,
    log_ai_research_memo,
)
from auto_trader.utils.logging import setup_logging

AI_POSTMORTEM_KIND = "ai_postmortem_pack"
AI_POSTMORTEM_PROMPT_VERSION = "ai_postmortem_review/v0"
AI_POSTMORTEM_FAILURE_PROMPT_VERSION = "ai_postmortem_failure/v0"
MAX_POSTMORTEM_PROMPT_CONTEXT_CHARS = 4_000
MAX_POSTMORTEM_ROWS = 12
POSTMORTEM_INSTRUCTIONS = (
    "You are an advisory trading postmortem analyst. Use only the provided JSON packet. "
    "Do not invent market facts. Do not recommend order size. Return only valid JSON. "
    "Return exactly one top-level JSON object with these keys only: used_only_provided_data, "
    "lessons, edge_hypotheses, budget_leaks, provider_notes, operator_recommendations, "
    "judge_summary. All recommendations are review-only."
)

POSTMORTEM_REQUIRED_FIELDS = [
    "used_only_provided_data",
    "lessons",
    "edge_hypotheses",
    "budget_leaks",
    "provider_notes",
    "operator_recommendations",
    "judge_summary",
]


class PostmortemProvider(Protocol):
    provider: str
    model_tag: str

    async def review(self, packet: dict[str, Any]) -> "PostmortemProviderMemo":
        ...


@dataclass(frozen=True)
class PostmortemProviderMemo:
    provider: str
    model_tag: str
    prompt_version: str
    input_hash: str
    used_only_provided_data: bool
    validation_passed: bool
    output: dict[str, Any]
    error: str | None = None


@dataclass(frozen=True)
class PostmortemRunResult:
    pack: dict[str, Any]
    path: Path | None
    guidance_refreshed: bool


def default_ai_postmortem_path(settings: Any | None = None) -> Path:
    override = getattr(settings, "ai_postmortem_path", None) or os.getenv("AUTO_TRADER_AI_POSTMORTEM_PATH")
    if override:
        return Path(override)
    return default_brain_review_dir(settings) / "ai_postmortem_pack.json"


async def run_ai_postmortem_review(
    *,
    settings: Any | None = None,
    providers: list[PostmortemProvider] | None = None,
    run_paid: bool = False,
    confirm_paid_postmortem: bool = False,
    force_paid: bool = False,
    max_paid_calls: int | None = None,
    write_cache: bool = False,
    refresh_brain_guidance: bool = False,
    output_path: Path | None = None,
    window_days: int = 7,
    limit: int = MAX_POSTMORTEM_ROWS,
) -> PostmortemRunResult:
    settings = settings or get_settings()
    configure_db_path(getattr(settings, "db_path", "auto_trader.db"))
    await init_db()
    report = await build_edge_report(window_days=window_days)
    packet = build_postmortem_packet(report, window_days=window_days, limit=limit)
    evidence_hash = postmortem_packet_hash(packet)
    postmortem_path = output_path or default_ai_postmortem_path(settings)
    provider_memos: list[PostmortemProviderMemo] = []
    status = "not_run"
    reason = "paid postmortem requires --run-paid"
    used_before: int | None = None
    used_after: int | None = None
    attempts_needed = 0

    if run_paid:
        daily_limit = int(max_paid_calls if max_paid_calls is not None else getattr(settings, "ai_postmortem_max_calls_per_day", 0) or 0)
        if not confirm_paid_postmortem:
            reason = "paid postmortem requires --confirm-paid-postmortem"
        elif daily_limit <= 0:
            reason = "AI_POSTMORTEM_MAX_CALLS_PER_DAY or --max-paid-calls must be positive"
        else:
            try:
                selected_providers = providers or create_postmortem_providers(settings)
                real_providers = [provider for provider in selected_providers if provider.provider != "shadow"]
                attempts_needed = len(real_providers)
            except Exception as exc:
                status = "invalid"
                reason = f"postmortem provider setup failed: {exc}"
                real_providers = []
            if status == "invalid":
                pass
            elif not real_providers:
                reason = "real provider is required"
            else:
                used_before = await count_ai_postmortem_chargeable_attempts(provider=None, today_utc=True)
                if used_before is None:
                    reason = "postmortem chargeable budget count unavailable"
                elif max(0, daily_limit - used_before) < attempts_needed:
                    reason = "chargeable budget exhausted"
                    used_after = used_before
                else:
                    provider_memos = await _run_paid_postmortem_providers(
                        real_providers,
                        packet,
                        evidence_hash,
                        force_paid=force_paid,
                    )
                    used_after = await count_ai_postmortem_chargeable_attempts(provider=None, today_utc=True)
                    called = [memo for memo in provider_memos if memo.prompt_version != "ai_postmortem_deduped/v0"]
                    if provider_memos and not called:
                        status = "deduped"
                        reason = "postmortem input already reviewed"
                    else:
                        status = "completed" if any(memo.validation_passed for memo in provider_memos) else "invalid"
                        reason = "valid postmortem generated" if status == "completed" else "no valid provider postmortem"

    pack = build_ai_postmortem_pack(
        packet=packet,
        input_hash=evidence_hash,
        status=status,
        reason=reason,
        paid_called=any(
            memo.prompt_version in {AI_POSTMORTEM_PROMPT_VERSION, AI_POSTMORTEM_FAILURE_PROMPT_VERSION}
            for memo in provider_memos
        ),
        provider_memos=provider_memos,
        used_before=used_before,
        used_after=used_after,
        attempts_needed=attempts_needed,
    )
    written_path: Path | None = None
    guidance_refreshed = False
    if write_cache or run_paid:
        written_path = _write_json_atomic(pack, postmortem_path)
        if refresh_brain_guidance:
            bundle = await build_brain_review_bundle(postmortem_pack=pack)
            write_brain_review_bundle(
                bundle,
                default_brain_review_dir(settings),
                guidance_path=default_brain_guidance_path(settings),
            )
            guidance_refreshed = True
    return PostmortemRunResult(pack=pack, path=written_path, guidance_refreshed=guidance_refreshed)


def build_postmortem_packet(report: EdgeReport, *, window_days: int, limit: int = MAX_POSTMORTEM_ROWS) -> dict[str, Any]:
    trades = sorted(report.closed_trades, key=lambda trade: trade.exit_time or datetime.min.replace(tzinfo=UTC))
    opportunities = sorted(report.opportunities, key=lambda opportunity: opportunity.created_at or datetime.min.replace(tzinfo=UTC))
    return {
        "kind": "ai_postmortem_input",
        "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "window_days": int(window_days),
        "closed_trade_count": len(report.closed_trades),
        "opportunity_count": len(report.opportunities),
        "closed_trades": [_trade_row(trade) for trade in trades[-limit:]],
        "missed_or_blocked_opportunities": [
            _opportunity_row(opportunity)
            for opportunity in opportunities
            if opportunity.outcome != "traded"
        ][-limit:],
        "rules": {
            "advisory_only": True,
            "operator_recommendations_review_only": True,
            "cannot_submit_orders": True,
            "cannot_change_config": True,
            "riskengine_remains_authority": True,
            "use_only_provided_data": True,
        },
    }


def postmortem_packet_hash(packet: dict[str, Any]) -> str:
    stable = dict(packet)
    stable.pop("generated_at", None)
    payload = json.dumps(stable, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def build_ai_postmortem_pack(
    *,
    packet: dict[str, Any],
    input_hash: str,
    status: str,
    reason: str,
    paid_called: bool,
    provider_memos: list[PostmortemProviderMemo],
    used_before: int | None,
    used_after: int | None,
    attempts_needed: int,
) -> dict[str, Any]:
    valid_outputs = [memo.output for memo in provider_memos if memo.validation_passed]
    lessons = _merge_rows(valid_outputs, "lessons", limit=6)
    edge_hypotheses = _merge_rows(valid_outputs, "edge_hypotheses", limit=6)
    budget_leaks = _merge_rows(valid_outputs, "budget_leaks", limit=5)
    operator_recommendations = _review_only_rows(_merge_rows(valid_outputs, "operator_recommendations", limit=5))
    provider_notes = _merge_rows(valid_outputs, "provider_notes", limit=6)
    pack = {
        "kind": AI_POSTMORTEM_KIND,
        "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "status": status,
        "reason": reason,
        "advisory_only": True,
        "order_authority": "RiskEngine",
        "config_authority": "operator_only",
        "prompt_version": AI_POSTMORTEM_PROMPT_VERSION,
        "input_hash": input_hash,
        "paid_called": paid_called,
        "chargeable_calls": {
            "before": used_before,
            "after": used_after,
            "attempts_needed": attempts_needed,
        },
        "input_summary": {
            "window_days": packet.get("window_days"),
            "closed_trade_count": packet.get("closed_trade_count"),
            "opportunity_count": packet.get("opportunity_count"),
        },
        "provider_results": [_provider_result(memo) for memo in provider_memos],
        "distilled_lessons": lessons,
        "edge_hypotheses": edge_hypotheses,
        "budget_leaks": budget_leaks,
        "provider_notes": provider_notes,
        "operator_recommendations": operator_recommendations,
    }
    pack["prompt_guidance"] = render_ai_postmortem_guidance(pack)
    return pack


def render_ai_postmortem_guidance(pack: dict[str, Any]) -> str:
    lines = [
        "AI POSTMORTEM GUIDANCE",
        "Advisory lessons from observed outcomes only. Current candidate packet still has priority.",
        "Use lessons to press better asymmetry and waste fewer paid calls; do not change config or sizing.",
    ]
    if pack.get("status") != "completed":
        lines.append(f"Status: {pack.get('status')} ({pack.get('reason')})")
        return "\n".join(lines)
    lines.append("Lessons:")
    lines.extend(_text_rows(pack.get("distilled_lessons"), limit=4) or ["- none"])
    lines.append("Edge hypotheses to test:")
    lines.extend(_text_rows(pack.get("edge_hypotheses"), limit=4) or ["- none"])
    lines.append("Budget leaks:")
    lines.extend(_text_rows(pack.get("budget_leaks"), limit=3) or ["- none"])
    prompt = "\n".join(lines)
    if len(prompt) > MAX_POSTMORTEM_PROMPT_CONTEXT_CHARS:
        return prompt[:MAX_POSTMORTEM_PROMPT_CONTEXT_CHARS].rstrip() + "\n[truncated]"
    return prompt


def validate_postmortem_output(output: dict[str, Any]) -> tuple[bool, list[str]]:
    errors: list[str] = []
    for field in POSTMORTEM_REQUIRED_FIELDS:
        if field not in output:
            errors.append(f"missing_{field}")
    if output.get("used_only_provided_data") is not True:
        errors.append("used_unverified_data")
    for field in ("lessons", "edge_hypotheses", "budget_leaks", "provider_notes", "operator_recommendations"):
        value = output.get(field)
        if not isinstance(value, list):
            errors.append(f"invalid_{field}")
        elif any(not isinstance(row, (str, dict)) or not str(row).strip() for row in value):
            errors.append(f"invalid_{field}_item")
    if not isinstance(output.get("judge_summary"), str) or not output.get("judge_summary", "").strip():
        errors.append("invalid_judge_summary")
    return not errors, errors


def create_postmortem_providers(settings: Any) -> list[PostmortemProvider]:
    providers = selected_research_providers(settings)
    if providers == ["shadow"]:
        return []
    return [_create_postmortem_provider(settings, provider) for provider in providers]


class HTTPPostmortemProvider:
    provider = "http"

    def __init__(self, api_key: str, *, model: str, timeout_seconds: float = 20.0) -> None:
        if not api_key:
            raise ValueError(f"{self.provider} postmortem provider requires an API key")
        self.api_key = api_key
        self.model = model
        self.timeout_seconds = timeout_seconds

    @property
    def model_tag(self) -> str:
        return f"{self.provider}/{self.model}"

    async def review(self, packet: dict[str, Any]) -> PostmortemProviderMemo:
        digest = postmortem_packet_hash(packet)
        try:
            raw = await asyncio.to_thread(self._call_provider, packet)
            output = self._extract_output(raw)
            valid, errors = validate_postmortem_output(output)
            if errors:
                output["validation_errors"] = errors
            memo = PostmortemProviderMemo(
                provider=self.provider,
                model_tag=self.model_tag,
                prompt_version=AI_POSTMORTEM_PROMPT_VERSION,
                input_hash=digest,
                used_only_provided_data=output.get("used_only_provided_data") is True,
                validation_passed=valid,
                output=output,
            )
        except Exception as exc:
            memo = PostmortemProviderMemo(
                provider=self.provider,
                model_tag=self.model_tag,
                prompt_version=AI_POSTMORTEM_FAILURE_PROMPT_VERSION,
                input_hash=digest,
                used_only_provided_data=True,
                validation_passed=False,
                output={
                    "used_only_provided_data": True,
                    "lessons": [],
                    "edge_hypotheses": [],
                    "budget_leaks": [],
                    "provider_notes": [],
                    "operator_recommendations": [],
                    "judge_summary": "Provider failed during explicit postmortem review.",
                    "validation_errors": ["ai_postmortem_provider_failed"],
                },
                error=str(exc),
            )
        return memo

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
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                return json.loads(response.read().decode("utf-8"))
        except Exception as exc:
            raise RuntimeError(str(exc).replace(self.api_key, "[REDACTED]")) from exc


class OpenAIPostmortemProvider(HTTPPostmortemProvider):
    provider = "openai"

    def _call_provider(self, packet: dict[str, Any]) -> dict[str, Any]:
        body = {
            "model": self.model,
            "instructions": POSTMORTEM_INSTRUCTIONS,
            "input": postmortem_prompt(packet),
            "text": {
                "format": {
                    "type": "json_schema",
                    "name": "ai_postmortem_review",
                    "schema": postmortem_json_schema(strict=True),
                    "strict": True,
                }
            },
        }
        return self._post_json("https://api.openai.com/v1/responses", body, {"Authorization": f"Bearer {self.api_key}"})

    def _extract_output(self, response: dict[str, Any]) -> dict[str, Any]:
        return _extract_openai_response_json(response)


class XAIPostmortemProvider(HTTPPostmortemProvider):
    provider = "xai"

    def _call_provider(self, packet: dict[str, Any]) -> dict[str, Any]:
        body = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": POSTMORTEM_INSTRUCTIONS},
                {"role": "user", "content": postmortem_prompt(packet)},
            ],
            "response_format": {
                "type": "json_schema",
                "json_schema": {
                    "name": "ai_postmortem_review",
                    "schema": postmortem_json_schema(strict=True),
                    "strict": True,
                },
            },
        }
        return self._post_json("https://api.x.ai/v1/chat/completions", body, {"Authorization": f"Bearer {self.api_key}"})

    def _extract_output(self, response: dict[str, Any]) -> dict[str, Any]:
        return _parse_json_text(response["choices"][0]["message"]["content"])


class AnthropicPostmortemProvider(HTTPPostmortemProvider):
    provider = "anthropic"

    def _call_provider(self, packet: dict[str, Any]) -> dict[str, Any]:
        body = {
            "model": self.model,
            "max_tokens": 1200,
            "system": POSTMORTEM_INSTRUCTIONS,
            "messages": [{"role": "user", "content": postmortem_prompt(packet)}],
        }
        return self._post_json(
            "https://api.anthropic.com/v1/messages",
            body,
            {"x-api-key": self.api_key, "anthropic-version": "2023-06-01"},
        )

    def _extract_output(self, response: dict[str, Any]) -> dict[str, Any]:
        for item in response.get("content", []) or []:
            text = item.get("text")
            if isinstance(text, str):
                return _parse_json_text(text)
        raise ValueError("Anthropic response did not contain text JSON output")


class GeminiPostmortemProvider(HTTPPostmortemProvider):
    provider = "gemini"

    def _call_provider(self, packet: dict[str, Any]) -> dict[str, Any]:
        body = {
            "contents": [
                {"role": "user", "parts": [{"text": f"{POSTMORTEM_INSTRUCTIONS}\n\n{postmortem_prompt(packet)}"}]}
            ],
            "generationConfig": {
                "response_mime_type": "application/json",
                "response_schema": postmortem_json_schema(strict=False),
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


def postmortem_json_schema(*, strict: bool = True) -> dict[str, Any]:
    array = {"type": "array", "items": {"type": "string"}, "maxItems": 8}
    schema: dict[str, Any] = {
        "type": "object",
        "required": POSTMORTEM_REQUIRED_FIELDS,
        "properties": {
            "used_only_provided_data": {"type": "boolean"},
            "lessons": array,
            "edge_hypotheses": array,
            "budget_leaks": array,
            "provider_notes": array,
            "operator_recommendations": array,
            "judge_summary": {"type": "string"},
        },
    }
    if strict:
        schema["additionalProperties"] = False
    return schema


def postmortem_prompt(packet: dict[str, Any]) -> str:
    return (
        "Review this realized-outcome packet. Return exactly the required JSON object, "
        "with no wrapper keys and no extra prose.\n\n"
        f"{json.dumps(packet, sort_keys=True, default=str)}"
    )


def _create_postmortem_provider(settings: Any, provider: str) -> PostmortemProvider:
    if provider not in REAL_PROVIDERS:
        raise ValueError(f"paid postmortem requires a real provider, got {provider}")
    model = model_for_provider(settings, provider)
    if not model:
        raise ValueError(f"AI_RESEARCH_PROVIDER={provider} requires model setting")
    timeout_seconds = float(getattr(settings, "ai_research_timeout_seconds", 8.0) or 8.0)
    if provider == "openai":
        return OpenAIPostmortemProvider(_required_key(settings, "openai_api_key", provider), model=model, timeout_seconds=timeout_seconds)
    if provider == "xai":
        return XAIPostmortemProvider(_required_key(settings, "xai_api_key", provider), model=model, timeout_seconds=timeout_seconds)
    if provider == "anthropic":
        return AnthropicPostmortemProvider(
            _required_key(settings, "anthropic_api_key", provider),
            model=model,
            timeout_seconds=timeout_seconds,
        )
    return GeminiPostmortemProvider(_required_key(settings, "gemini_api_key", provider), model=model, timeout_seconds=timeout_seconds)


def _required_key(settings: Any, attr: str, provider: str) -> str:
    value = str(getattr(settings, attr, "") or "").strip()
    if not value:
        raise ValueError(f"AI_RESEARCH_PROVIDER={provider} requires {attr.upper()}")
    return value


async def _run_paid_postmortem_providers(
    providers: list[PostmortemProvider],
    packet: dict[str, Any],
    evidence_hash: str,
    *,
    force_paid: bool,
) -> list[PostmortemProviderMemo]:
    memos = []
    for provider in providers:
        attempt_hash = postmortem_attempt_hash(
            evidence_hash=evidence_hash,
            provider=provider.provider,
            model_tag=provider.model_tag,
            window_days=int(packet.get("window_days") or 0),
        )
        if not force_paid:
            existing = await count_ai_research_memos(provider=provider.provider, input_hash=attempt_hash)
            if existing is None:
                memo = _failure_memo(provider, attempt_hash, "postmortem dedupe count unavailable")
                await _log_postmortem_memo(memo, packet)
                memos.append(memo)
                continue
            if existing > 0:
                memos.append(
                    PostmortemProviderMemo(
                        provider=provider.provider,
                        model_tag=provider.model_tag,
                        prompt_version="ai_postmortem_deduped/v0",
                        input_hash=attempt_hash,
                        used_only_provided_data=True,
                        validation_passed=False,
                        output={
                            "used_only_provided_data": True,
                            "lessons": [],
                            "edge_hypotheses": [],
                            "budget_leaks": [],
                            "provider_notes": [],
                            "operator_recommendations": [],
                            "judge_summary": "Existing postmortem attempt found for provider/model/window hash.",
                        },
                    )
                )
                continue
        try:
            memo = await provider.review(packet)
            memo = replace(memo, input_hash=attempt_hash)
        except Exception as exc:
            memo = _failure_memo(provider, attempt_hash, str(exc))
        await _log_postmortem_memo(memo, packet)
        memos.append(memo)
    return memos


def postmortem_attempt_hash(*, evidence_hash: str, provider: str, model_tag: str, window_days: int) -> str:
    payload = {
        "evidence_hash": evidence_hash,
        "provider": provider,
        "model_tag": model_tag,
        "prompt_version": AI_POSTMORTEM_PROMPT_VERSION,
        "window_days": window_days,
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def _failure_memo(provider: PostmortemProvider, attempt_hash: str, error: str) -> PostmortemProviderMemo:
    return PostmortemProviderMemo(
        provider=str(getattr(provider, "provider", "unknown")),
        model_tag=str(getattr(provider, "model_tag", "unknown")),
        prompt_version=AI_POSTMORTEM_FAILURE_PROMPT_VERSION,
        input_hash=attempt_hash,
        used_only_provided_data=True,
        validation_passed=False,
        output={
            "used_only_provided_data": True,
            "lessons": [],
            "edge_hypotheses": [],
            "budget_leaks": [],
            "provider_notes": [],
            "operator_recommendations": [],
            "judge_summary": "Provider failed during explicit postmortem review.",
            "validation_errors": ["ai_postmortem_provider_failed"],
        },
        error=error,
    )


async def _log_postmortem_memo(memo: PostmortemProviderMemo, packet: dict[str, Any]) -> int | None:
    return await log_ai_research_memo(
        signal_id=None,
        symbol="POSTMORTEM",
        provider=memo.provider,
        model_tag=memo.model_tag,
        prompt_version=memo.prompt_version,
        input_hash=memo.input_hash,
        verdict="watch",
        confidence=None,
        used_only_provided_data=memo.used_only_provided_data,
        validation_passed=memo.validation_passed,
        memo={
            "source": "ai_postmortem_review",
            "input_packet": packet,
            "postmortem": memo.output,
            "error": memo.error,
        },
    )


def _trade_row(trade: Any) -> dict[str, Any]:
    return {
        "symbol": trade.symbol,
        "pnl": round(float(trade.pnl), 4),
        "pnl_pct": round(float(trade.pnl_pct), 4),
        "entry_price": round(float(trade.entry_price), 4),
        "exit_price": round(float(trade.exit_price), 4),
        "exit_reason": trade.exit_reason,
        "ai_verdict": trade.ai_verdict,
        "risk_profile": trade.risk_profile,
        "setup_tags": list(trade.setup_tags),
        "provider_votes": list(trade.provider_votes),
    }


def _opportunity_row(opportunity: Any) -> dict[str, Any]:
    return {
        "symbol": opportunity.symbol,
        "outcome": opportunity.outcome,
        "reason": opportunity.reason,
        "ai_verdict": opportunity.ai_verdict,
        "risk_profile": opportunity.risk_profile,
        "setup_tags": list(opportunity.setup_tags),
        "provider_votes": list(opportunity.provider_votes),
    }


def _provider_result(memo: PostmortemProviderMemo) -> dict[str, Any]:
    return {
        "provider": memo.provider,
        "model_tag": memo.model_tag,
        "prompt_version": memo.prompt_version,
        "input_hash": memo.input_hash,
        "validation_passed": memo.validation_passed,
        "used_only_provided_data": memo.used_only_provided_data,
        "validation_errors": memo.output.get("validation_errors", []),
        "error": memo.error,
    }


def _merge_rows(outputs: list[dict[str, Any]], key: str, *, limit: int) -> list[str]:
    rows: list[str] = []
    seen: set[str] = set()
    for output in outputs:
        value = output.get(key)
        if not isinstance(value, list):
            continue
        for item in value:
            text = _row_text(item)
            if not text or text in seen:
                continue
            seen.add(text)
            rows.append(text)
            if len(rows) >= limit:
                return rows
    return rows


def _row_text(value: Any) -> str:
    if isinstance(value, str):
        return " ".join(value.split())[:500]
    if isinstance(value, dict):
        return " ".join(str(value.get("recommendation") or value.get("lesson") or value.get("text") or "").split())[:500]
    return ""


def _review_only_rows(rows: list[str]) -> list[dict[str, Any]]:
    return [{"recommendation": row, "review_only": True} for row in rows]


def _text_rows(value: Any, *, limit: int) -> list[str]:
    if not isinstance(value, list):
        return []
    rows = []
    for item in value[:limit]:
        if isinstance(item, dict):
            text = item.get("recommendation") or item.get("lesson") or item.get("text")
        else:
            text = item
        if str(text or "").strip():
            rows.append(f"- {str(text).strip()}")
    return rows


def _write_json_atomic(payload: dict[str, Any], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        tmp_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        os.replace(tmp_path, path)
    except Exception:
        try:
            tmp_path.unlink(missing_ok=True)
        finally:
            raise
    return path


def render_ai_postmortem_result(result: PostmortemRunResult) -> str:
    pack = result.pack
    calls = pack.get("chargeable_calls") if isinstance(pack.get("chargeable_calls"), dict) else {}
    lines = [
        "AI POSTMORTEM REVIEW",
        "Explicit/offline only. No orders submitted, canceled, resumed, or sized.",
        f"Status: {pack.get('status')}",
        f"Reason: {pack.get('reason')}",
        f"Paid called: {pack.get('paid_called')}",
        f"Input hash: {str(pack.get('input_hash') or '')[:12]}",
        f"Chargeable calls: before {calls.get('before')} / after {calls.get('after')}; needed {calls.get('attempts_needed')}",
        f"Artifact: {result.path or 'not written'}",
        f"Brain guidance refreshed: {result.guidance_refreshed}",
    ]
    if pack.get("distilled_lessons"):
        lines.append("Lessons:")
        lines.extend(_text_rows(pack.get("distilled_lessons"), limit=4))
    if pack.get("edge_hypotheses"):
        lines.append("Edge hypotheses:")
        lines.extend(_text_rows(pack.get("edge_hypotheses"), limit=4))
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run explicit AUTO-TRADER paid AI postmortem review.")
    parser.add_argument("--run-paid", action="store_true", help="Actually call configured paid AI providers.")
    parser.add_argument(
        "--confirm-paid-postmortem",
        action="store_true",
        help="Second confirmation required before paid provider calls.",
    )
    parser.add_argument("--force-paid", action="store_true", help="Bypass provider/model/window dedupe for this run.")
    parser.add_argument("--max-paid-calls", type=int, default=None, help="Override separate postmortem paid-call budget.")
    parser.add_argument("--write-cache", action="store_true", help="Write ai_postmortem_pack.json artifact.")
    parser.add_argument("--refresh-brain-guidance", action="store_true", help="Regenerate brain guidance with postmortem lessons.")
    parser.add_argument("--output-path", type=Path, default=None, help="Override postmortem artifact path.")
    parser.add_argument("--window-days", type=int, default=7, help="Observed outcome lookback window.")
    parser.add_argument("--json", action="store_true", help="Print raw postmortem pack JSON.")
    args = parser.parse_args()
    setup_logging("ERROR")
    result = asyncio.run(
        run_ai_postmortem_review(
            run_paid=args.run_paid,
            confirm_paid_postmortem=args.confirm_paid_postmortem,
            force_paid=args.force_paid,
            max_paid_calls=args.max_paid_calls,
            write_cache=args.write_cache,
            refresh_brain_guidance=args.refresh_brain_guidance,
            output_path=args.output_path,
            window_days=args.window_days,
        )
    )
    if args.json:
        print(json.dumps(result.pack, indent=2, sort_keys=True))
    else:
        print(render_ai_postmortem_result(result))


if __name__ == "__main__":
    main()
