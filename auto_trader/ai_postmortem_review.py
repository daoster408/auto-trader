"""Explicit paid/offline AI postmortem review for observed trading edge."""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import socket
from dataclasses import replace
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Protocol
from urllib.error import HTTPError, URLError
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
    _extract_openai_response_json,
    _parse_json_text,
)
from auto_trader.persistence.db import (
    configure_db_path,
    count_ai_postmortem_chargeable_attempts,
    count_ai_postmortem_escalation_chargeable_attempts,
    count_ai_research_memos,
    init_db,
    log_ai_research_memo,
)
from auto_trader.utils.logging import setup_logging

AI_POSTMORTEM_KIND = "ai_postmortem_pack"
AI_POSTMORTEM_PROMPT_VERSION = "ai_postmortem_review/v0"
AI_POSTMORTEM_FAILURE_PROMPT_VERSION = "ai_postmortem_failure/v0"
AI_POSTMORTEM_ESCALATION_PROMPT_VERSION = "ai_postmortem_escalation/v0"
AI_POSTMORTEM_ESCALATION_FAILURE_PROMPT_VERSION = "ai_postmortem_escalation_failure/v0"
MAX_POSTMORTEM_PROMPT_CONTEXT_CHARS = 4_000
MAX_POSTMORTEM_ESCALATION_CONTEXT_CHARS = 4_000
MAX_POSTMORTEM_ROWS = 12
POSTMORTEM_MAX_PROVIDER_ATTEMPTS = 2
POSTMORTEM_RETRY_BACKOFF_SECONDS = 30.0
POSTMORTEM_MAX_RETRY_AFTER_SECONDS = 120.0
POSTMORTEM_MAX_HTTP_ERROR_BODY_CHARS = 2_000
POSTMORTEM_MAX_HTTP_ERROR_HEADERS = 12
POSTMORTEM_MAX_PROVIDER_JSON_SCAN_CHARS = 24_000
POSTMORTEM_NORMALIZED_ROW_MAX_CHARS = 1_000
POSTMORTEM_OPENAI_MAX_OUTPUT_TOKENS = 2_000
POSTMORTEM_DEEPSEEK_MAX_TOKENS = 5_000
POSTMORTEM_ANTHROPIC_MAX_TOKENS = 2_000
POSTMORTEM_ANTHROPIC_ESCALATION_MAX_TOKENS = 3_000
POSTMORTEM_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
POSTMORTEM_REAL_PROVIDERS = ("openai", "xai", "anthropic", "gemini", "deepseek")
POSTMORTEM_INSTRUCTIONS = (
    "You are an advisory trading postmortem analyst. Use only the provided JSON packet. "
    "Do not invent market facts. Do not recommend order size. Return only valid JSON. "
    "Return exactly one top-level JSON object with these keys only: used_only_provided_data, "
    "lessons, edge_hypotheses, budget_leaks, provider_notes, operator_recommendations, "
    "judge_summary. All recommendations are review-only."
)
POSTMORTEM_ESCALATION_INSTRUCTIONS = (
    "You are the escalation reviewer for an advisory trading postmortem. Use only the provided "
    "JSON packet and compact base postmortem outputs. Find weak or unsupported conclusions, "
    "identify high-confidence lessons, note provider quality issues, and return only valid JSON. "
    "Do not recommend order size. Do not recommend automatic config changes. Return exactly one "
    "top-level JSON object with these keys only: used_only_provided_data, lessons, "
    "edge_hypotheses, budget_leaks, provider_notes, operator_recommendations, judge_summary. "
    "All recommendations are review-only."
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

    async def review(
        self,
        packet: dict[str, Any],
        *,
        prompt_version: str = AI_POSTMORTEM_PROMPT_VERSION,
        failure_prompt_version: str = AI_POSTMORTEM_FAILURE_PROMPT_VERSION,
        instructions: str = POSTMORTEM_INSTRUCTIONS,
    ) -> "PostmortemProviderMemo":
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


class PostmortemProviderRequestError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        retry_after_seconds: float | None = None,
        response_body: str | None = None,
        response_headers: dict[str, str] | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.retry_after_seconds = retry_after_seconds
        self.response_body = response_body
        self.response_headers = response_headers or {}


class PostmortemProviderTimeoutError(RuntimeError):
    def __init__(self, message: str, *, timeout_seconds: float) -> None:
        super().__init__(message)
        self.timeout_seconds = timeout_seconds


class PostmortemProviderOutputError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        error_type: str,
        response_body: str | None = None,
        provider_stop_reason: str | None = None,
    ) -> None:
        super().__init__(message)
        self.output_error_type = error_type
        self.response_body = response_body
        self.provider_stop_reason = provider_stop_reason


def default_ai_postmortem_path(settings: Any | None = None) -> Path:
    override = getattr(settings, "ai_postmortem_path", None) or os.getenv("AUTO_TRADER_AI_POSTMORTEM_PATH")
    if override:
        return Path(override)
    return default_brain_review_dir(settings) / "ai_postmortem_pack.json"


async def run_ai_postmortem_review(
    *,
    settings: Any | None = None,
    providers: list[PostmortemProvider] | None = None,
    escalation_provider: PostmortemProvider | None = None,
    run_paid: bool = False,
    confirm_paid_postmortem: bool = False,
    force_paid: bool = False,
    force_escalation: bool = False,
    max_paid_calls: int | None = None,
    max_escalation_calls: int | None = None,
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
    escalation_review: dict[str, Any] | None = None

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
                        max_http_attempts=max(0, daily_limit - used_before),
                    )
                    used_after = await count_ai_postmortem_chargeable_attempts(provider=None, today_utc=True)
                    called = [memo for memo in provider_memos if memo.prompt_version != "ai_postmortem_deduped/v0"]
                    if provider_memos and not called:
                        status = "deduped"
                        reason = "postmortem input already reviewed"
                    else:
                        status = "completed" if any(memo.validation_passed for memo in provider_memos) else "invalid"
                        reason = "valid postmortem generated" if status == "completed" else "no valid provider postmortem"

    escalation_review = await _maybe_run_postmortem_escalation(
        settings=settings,
        provider=escalation_provider,
        packet=packet,
        evidence_hash=evidence_hash,
        provider_memos=provider_memos,
        run_paid=run_paid,
        confirm_paid_postmortem=confirm_paid_postmortem,
        force_escalation=force_escalation,
        max_escalation_calls=max_escalation_calls,
    )
    escalation_paid_called = bool(escalation_review and escalation_review.get("paid_called"))
    pack = build_ai_postmortem_pack(
        packet=packet,
        input_hash=evidence_hash,
        status=status,
        reason=reason,
        paid_called=(
            any(
                memo.prompt_version in {AI_POSTMORTEM_PROMPT_VERSION, AI_POSTMORTEM_FAILURE_PROMPT_VERSION}
                for memo in provider_memos
            )
            or escalation_paid_called
        ),
        provider_memos=provider_memos,
        used_before=used_before,
        used_after=used_after,
        attempts_needed=attempts_needed,
        escalation_review=escalation_review,
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
    escalation_review: dict[str, Any] | None = None,
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
    if escalation_review is not None:
        pack["escalation_review"] = escalation_review
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
    escalation = pack.get("escalation_review")
    if isinstance(escalation, dict) and escalation.get("status") == "completed":
        lines.append("Escalation review:")
        for lesson in escalation.get("highest_confidence_lessons") or []:
            if isinstance(lesson, str) and lesson.strip():
                lines.append(f"- reviewer lesson: {lesson}")
        for note in escalation.get("provider_quality_notes") or []:
            if isinstance(note, str) and note.strip():
                lines.append(f"- reviewer note: {note}")
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
    return [_create_postmortem_provider(settings, provider) for provider in selected_postmortem_providers(settings)]


def selected_postmortem_providers(settings: Any) -> list[str]:
    raw_providers = str(getattr(settings, "ai_postmortem_providers", "") or "").strip()
    if not raw_providers:
        return []
    providers = [provider.strip().lower() for provider in raw_providers.split(",") if provider.strip()]
    providers = list(dict.fromkeys(providers))
    if not providers or providers == ["shadow"]:
        return []
    if "shadow" in providers:
        raise ValueError("AI_POSTMORTEM_PROVIDERS cannot mix shadow with real providers")
    unsupported = [provider for provider in providers if provider not in POSTMORTEM_REAL_PROVIDERS]
    if unsupported:
        raise ValueError(f"unsupported AI_POSTMORTEM_PROVIDERS={','.join(unsupported)}")
    return providers


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

    async def review(
        self,
        packet: dict[str, Any],
        *,
        prompt_version: str = AI_POSTMORTEM_PROMPT_VERSION,
        failure_prompt_version: str = AI_POSTMORTEM_FAILURE_PROMPT_VERSION,
        instructions: str = POSTMORTEM_INSTRUCTIONS,
    ) -> PostmortemProviderMemo:
        digest = postmortem_packet_hash(packet)
        try:
            raw = await asyncio.to_thread(self._call_provider, packet, instructions)
            output = self._extract_output(raw)
            valid, errors = validate_postmortem_output(output)
            if errors:
                output["validation_errors"] = errors
            memo = PostmortemProviderMemo(
                provider=self.provider,
                model_tag=self.model_tag,
                prompt_version=prompt_version,
                input_hash=digest,
                used_only_provided_data=output.get("used_only_provided_data") is True,
                validation_passed=valid,
                output=output,
            )
        except Exception as exc:
            failure_output = _provider_failure_output(exc)
            memo = PostmortemProviderMemo(
                provider=self.provider,
                model_tag=self.model_tag,
                prompt_version=failure_prompt_version,
                input_hash=digest,
                used_only_provided_data=True,
                validation_passed=False,
                output=failure_output,
                error=str(exc),
            )
        return memo

    def _call_provider(self, packet: dict[str, Any], instructions: str) -> dict[str, Any]:
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
        except HTTPError as exc:
            error = f"HTTP Error {exc.code}: {exc.reason}"
            retry_after = _retry_after_seconds(exc.headers.get("Retry-After") if exc.headers else None)
            response_body = _sanitize_http_error_body(_read_http_error_body(exc), secrets=[self.api_key])
            raise PostmortemProviderRequestError(
                error.replace(self.api_key, "[REDACTED]"),
                status_code=exc.code,
                retry_after_seconds=retry_after,
                response_body=response_body,
                response_headers=_diagnostic_http_headers(exc.headers, secrets=[self.api_key]),
            ) from exc
        except Exception as exc:
            if _is_timeout_exception(exc):
                safe_error = str(exc).replace(self.api_key, "[REDACTED]")
                raise PostmortemProviderTimeoutError(
                    f"Provider request timed out after {self.timeout_seconds:g}s: {safe_error}",
                    timeout_seconds=self.timeout_seconds,
                ) from exc
            raise RuntimeError(str(exc).replace(self.api_key, "[REDACTED]")) from exc


class OpenAIPostmortemProvider(HTTPPostmortemProvider):
    provider = "openai"

    def _call_provider(self, packet: dict[str, Any], instructions: str) -> dict[str, Any]:
        body = {
            "model": self.model,
            "instructions": instructions,
            "input": postmortem_prompt(packet),
            "max_output_tokens": POSTMORTEM_OPENAI_MAX_OUTPUT_TOKENS,
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

    def _call_provider(self, packet: dict[str, Any], instructions: str) -> dict[str, Any]:
        body = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": instructions},
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


class DeepSeekPostmortemProvider(XAIPostmortemProvider):
    provider = "deepseek"

    def _call_provider(self, packet: dict[str, Any], instructions: str) -> dict[str, Any]:
        body = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": instructions},
                {"role": "user", "content": postmortem_prompt(packet)},
            ],
            "max_tokens": POSTMORTEM_DEEPSEEK_MAX_TOKENS,
            "response_format": {"type": "json_object"},
            "thinking": {"type": "disabled"},
        }
        return self._post_json("https://api.deepseek.com/chat/completions", body, {"Authorization": f"Bearer {self.api_key}"})

    def _extract_output(self, response: dict[str, Any]) -> dict[str, Any]:
        choices = response.get("choices") or []
        if not choices:
            raise ValueError("DeepSeek response did not contain choices")
        choice = choices[0] if isinstance(choices[0], dict) else {}
        message = choice.get("message") if isinstance(choice, dict) else {}
        text = _bounded_provider_json_text(
            _chat_message_content_text(message.get("content") if isinstance(message, dict) else None)
        )
        if not text:
            finish_reason = choice.get("finish_reason") or choice.get("stop_reason")
            diagnostic = _deepseek_missing_content_diagnostic(choice)
            error_type = (
                "truncated_provider_json"
                if str(finish_reason or "").lower() in {"length", "max_tokens"}
                else "malformed_provider_json"
            )
            raise PostmortemProviderOutputError(
                "DeepSeek response did not contain text JSON output",
                error_type=error_type,
                response_body=diagnostic,
                provider_stop_reason=str(finish_reason) if finish_reason is not None else None,
            )
        finish_reason = choice.get("finish_reason") or choice.get("stop_reason")
        try:
            return _normalize_deepseek_postmortem_output(_parse_json_text_or_embedded_object(text))
        except Exception as exc:
            error_type = (
                "truncated_provider_json"
                if str(finish_reason or "").lower() in {"length", "max_tokens"}
                else "malformed_provider_json"
            )
            raise PostmortemProviderOutputError(
                f"DeepSeek response JSON could not be parsed: {exc}",
                error_type=error_type,
                response_body=text,
                provider_stop_reason=str(finish_reason) if finish_reason is not None else None,
            ) from exc


def _deepseek_missing_content_diagnostic(choice: dict[str, Any]) -> str:
    message = choice.get("message") if isinstance(choice, dict) else {}
    if not isinstance(message, dict):
        message = {}
    content = message.get("content")
    reasoning = message.get("reasoning_content")
    diagnostic = {
        "finish_reason": choice.get("finish_reason") or choice.get("stop_reason"),
        "message_keys": sorted(str(key) for key in message.keys()),
        "content_type": type(content).__name__,
        "content_length": len(content) if isinstance(content, str) else None,
        "reasoning_content_length": len(reasoning) if isinstance(reasoning, str) else None,
        "has_tool_calls": bool(message.get("tool_calls")),
    }
    return json.dumps(diagnostic, sort_keys=True)


def _normalize_deepseek_postmortem_output(output: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(output)
    normalized_fields: list[str] = []
    for field in ("lessons", "edge_hypotheses", "budget_leaks", "provider_notes", "operator_recommendations"):
        value = normalized.get(field)
        if isinstance(value, str):
            stripped = value.strip()
            normalized[field] = [stripped[:POSTMORTEM_NORMALIZED_ROW_MAX_CHARS]] if stripped else []
            normalized_fields.append(field)
    if normalized_fields:
        normalized["normalization_notes"] = [
            f"deepseek_string_to_singleton_array:{field}" for field in normalized_fields
        ]
    return normalized


class AnthropicPostmortemProvider(HTTPPostmortemProvider):
    provider = "anthropic"

    def _call_provider(self, packet: dict[str, Any], instructions: str) -> dict[str, Any]:
        max_tokens = (
            POSTMORTEM_ANTHROPIC_ESCALATION_MAX_TOKENS
            if instructions == POSTMORTEM_ESCALATION_INSTRUCTIONS
            else POSTMORTEM_ANTHROPIC_MAX_TOKENS
        )
        body = {
            "model": self.model,
            "max_tokens": max_tokens,
            "system": (
                f"{instructions} Keep every array to at most 4 short strings and "
                "keep judge_summary under 500 characters."
            ),
            "messages": [{"role": "user", "content": postmortem_prompt(packet)}],
        }
        return self._post_json(
            "https://api.anthropic.com/v1/messages",
            body,
            {"x-api-key": self.api_key, "anthropic-version": "2023-06-01"},
        )

    def _extract_output(self, response: dict[str, Any]) -> dict[str, Any]:
        stop_reason = response.get("stop_reason")
        for item in response.get("content", []) or []:
            text = item.get("text")
            if isinstance(text, str):
                try:
                    return _parse_json_text(text)
                except Exception as exc:
                    error_type = "truncated_provider_json" if stop_reason == "max_tokens" else "malformed_provider_json"
                    raise PostmortemProviderOutputError(
                        f"Anthropic response JSON could not be parsed: {exc}",
                        error_type=error_type,
                        response_body=text,
                        provider_stop_reason=str(stop_reason) if stop_reason is not None else None,
                    ) from exc
        raise ValueError("Anthropic response did not contain text JSON output")


class GeminiPostmortemProvider(HTTPPostmortemProvider):
    provider = "gemini"

    def _call_provider(self, packet: dict[str, Any], instructions: str) -> dict[str, Any]:
        body = {
            "contents": [
                {"role": "user", "parts": [{"text": f"{instructions}\n\n{postmortem_prompt(packet)}"}]}
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


def _retry_after_seconds(value: str | None) -> float | None:
    if not value:
        return None
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        try:
            retry_at = parsedate_to_datetime(value)
            if retry_at.tzinfo is None:
                retry_at = retry_at.replace(tzinfo=UTC)
            seconds = (retry_at - datetime.now(UTC)).total_seconds()
        except (TypeError, ValueError):
            return None
    return min(max(seconds, 0.0), POSTMORTEM_MAX_RETRY_AFTER_SECONDS)


def _is_timeout_exception(exc: BaseException) -> bool:
    if isinstance(exc, TimeoutError | socket.timeout):
        return True
    if isinstance(exc, URLError):
        reason = getattr(exc, "reason", None)
        if isinstance(reason, TimeoutError | socket.timeout):
            return True
        return "timed out" in str(reason).lower()
    return "timed out" in str(exc).lower()


def _read_http_error_body(exc: HTTPError) -> str | None:
    try:
        raw = exc.read()
    except Exception:
        return None
    if not raw:
        return None
    try:
        return raw.decode("utf-8", errors="replace")
    except Exception:
        return str(raw)


def _sanitize_http_error_body(text: str | None, *, secrets: list[str]) -> str | None:
    if not text:
        return None
    sanitized = _redact_diagnostic_text(text, secrets=secrets)
    sanitized = sanitized.replace("\x00", "")
    if len(sanitized) > POSTMORTEM_MAX_HTTP_ERROR_BODY_CHARS:
        sanitized = sanitized[:POSTMORTEM_MAX_HTTP_ERROR_BODY_CHARS].rstrip() + "\n[truncated]"
    return sanitized


def _diagnostic_http_headers(headers: Any, *, secrets: list[str]) -> dict[str, str]:
    if not headers:
        return {}
    diagnostic: dict[str, str] = {}
    for key, value in headers.items():
        name = str(key).strip()
        normalized = name.lower()
        if _is_forbidden_http_header(normalized):
            continue
        if not _is_diagnostic_http_header(normalized):
            continue
        clean_value = _redact_diagnostic_text(str(value), secrets=secrets)
        diagnostic[name] = clean_value[:500]
        if len(diagnostic) >= POSTMORTEM_MAX_HTTP_ERROR_HEADERS:
            break
    return diagnostic


def _redact_diagnostic_text(text: str, *, secrets: list[str]) -> str:
    redacted = text
    for secret in secrets:
        if secret:
            redacted = redacted.replace(secret, "[REDACTED]")
    redacted = re.sub(r"(?i)\bbearer\s+[A-Za-z0-9._~+/=-]+", "Bearer [REDACTED]", redacted)
    redacted = re.sub(
        r'(?i)("?(?:api[_-]?key|access[_-]?token|secret|token|key)"?\s*[:=]\s*")([^"]+)(")',
        r"\1[REDACTED]\3",
        redacted,
    )
    redacted = re.sub(
        r"(?i)('?(?:api[_-]?key|access[_-]?token|secret|token|key)'?\s*[:=]\s*')([^']+)(')",
        r"\1[REDACTED]\3",
        redacted,
    )
    return redacted


def _is_forbidden_http_header(normalized_name: str) -> bool:
    forbidden = ("authorization", "cookie", "set-cookie", "api-key", "apikey", "token", "secret", "key")
    return any(part in normalized_name for part in forbidden)


def _is_diagnostic_http_header(normalized_name: str) -> bool:
    if normalized_name == "retry-after":
        return True
    if normalized_name in {
        "request-id",
        "x-request-id",
        "x-goog-request-id",
        "x-guploader-uploadid",
        "openai-processing-ms",
    }:
        return True
    return normalized_name.startswith(
        (
            "x-ratelimit-",
            "ratelimit-",
            "x-rate-limit-",
            "x-quota-",
            "quota-",
            "x-request-cost-",
        )
    )


def _provider_failure_output(exc: BaseException | str) -> dict[str, Any]:
    metadata = _provider_failure_metadata(exc)
    validation_errors = ["ai_postmortem_provider_failed"]
    if metadata["error_type"] == "rate_limited":
        validation_errors.append("ai_postmortem_provider_rate_limited")
    elif metadata["error_type"] == "model_unavailable":
        validation_errors.append("ai_postmortem_provider_model_unavailable")
    elif metadata["error_type"] == "truncated_provider_json":
        validation_errors.append("ai_postmortem_provider_json_truncated")
    elif metadata["error_type"] == "malformed_provider_json":
        validation_errors.append("ai_postmortem_provider_json_malformed")
    elif metadata["retryable"]:
        validation_errors.append("ai_postmortem_provider_retryable_failed")
    return {
        "used_only_provided_data": True,
        "lessons": [],
        "edge_hypotheses": [],
        "budget_leaks": [],
        "provider_notes": [],
        "operator_recommendations": [],
        "judge_summary": "Provider failed during explicit postmortem review.",
        "validation_errors": validation_errors,
        **metadata,
    }


def _provider_failure_metadata(exc: BaseException | str) -> dict[str, Any]:
    status_code = getattr(exc, "status_code", None)
    retry_after = getattr(exc, "retry_after_seconds", None)
    output_error_type = getattr(exc, "output_error_type", None)
    provider_stop_reason = getattr(exc, "provider_stop_reason", None)
    timeout_seconds = getattr(exc, "timeout_seconds", None)
    response_body = _sanitize_http_error_body(getattr(exc, "response_body", None), secrets=[])
    response_headers = _diagnostic_http_headers(getattr(exc, "response_headers", None), secrets=[])
    text = str(exc)
    lower = text.lower()
    response_lower = str(response_body or "").lower()
    if status_code is None and "http error 429" in lower:
        status_code = 429
    retryable = bool(status_code in POSTMORTEM_RETRYABLE_STATUS_CODES)
    if status_code == 404 and _response_indicates_model_unavailable(response_lower):
        error_type = "model_unavailable"
        retryable = False
    elif output_error_type in {"malformed_provider_json", "truncated_provider_json"}:
        error_type = str(output_error_type)
        retryable = False
    elif status_code == 429:
        error_type = "rate_limited"
    elif timeout_seconds is not None or "timed out" in lower:
        error_type = "timeout"
        retryable = False
    elif status_code in POSTMORTEM_RETRYABLE_STATUS_CODES:
        error_type = "retryable_provider_error"
    elif status_code is not None:
        error_type = "provider_http_error"
    else:
        error_type = "provider_error"
    return {
        "error_type": error_type,
        "http_status": status_code,
        "retry_after_seconds": retry_after,
        "provider_response_body": response_body,
        "provider_response_headers": response_headers or {},
        "provider_stop_reason": provider_stop_reason,
        "timeout_seconds": timeout_seconds,
        "timeout_remediation": (
            "Raise postmortem timeout or reduce postmortem output size."
            if error_type == "timeout"
            else None
        ),
        "retryable": retryable,
        "possible_duplicate_paid_request": error_type == "timeout",
    }


def _response_indicates_model_unavailable(response_lower: str) -> bool:
    if not response_lower:
        return False
    unavailable_markers = (
        "not available",
        "not_found_error",
        "model_not_found",
        "model unavailable",
        "does not exist",
    )
    return any(marker in response_lower for marker in unavailable_markers)


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


def _chat_message_content_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict) and isinstance(item.get("text"), str):
                parts.append(item["text"])
        return "\n".join(parts).strip()
    return ""


def _parse_json_text_or_embedded_object(text: str) -> dict[str, Any]:
    text = _bounded_provider_json_text(text)
    try:
        return _parse_json_text(text)
    except Exception as strict_exc:
        for embedded in _balanced_json_object_candidates(text):
            try:
                return _parse_json_text(embedded)
            except Exception:
                continue
        raise strict_exc


def _balanced_json_object_candidates(text: str) -> list[str]:
    candidates: list[str] = []
    for start, char in enumerate(text):
        if char != "{":
            continue
        candidate = _balanced_json_object_from(text, start)
        if candidate is not None:
            candidates.append(candidate)
    return candidates


def _balanced_json_object_from(text: str, start: int) -> str | None:
    depth = 0
    in_string = False
    escape = False
    for index in range(start, len(text)):
        char = text[index]
        if in_string:
            if escape:
                escape = False
            elif char == "\\":
                escape = True
            elif char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return text[start : index + 1]
    return None


def _bounded_provider_json_text(text: str) -> str:
    if len(text) <= POSTMORTEM_MAX_PROVIDER_JSON_SCAN_CHARS:
        return text
    return text[:POSTMORTEM_MAX_PROVIDER_JSON_SCAN_CHARS]


def _create_postmortem_provider(settings: Any, provider: str) -> PostmortemProvider:
    if provider not in POSTMORTEM_REAL_PROVIDERS:
        raise ValueError(f"paid postmortem requires a real provider, got {provider}")
    model = postmortem_model_for_provider(settings, provider)
    if not model:
        raise ValueError(f"AI_POSTMORTEM_PROVIDERS={provider} requires postmortem model setting")
    return _create_postmortem_provider_with_model(settings, provider, model)


def create_postmortem_escalation_provider(settings: Any) -> PostmortemProvider:
    provider = str(getattr(settings, "ai_postmortem_escalation_provider", "") or "").strip().lower()
    if provider not in POSTMORTEM_REAL_PROVIDERS:
        raise ValueError(f"unsupported AI_POSTMORTEM_ESCALATION_PROVIDER={provider}")
    model = str(getattr(settings, "ai_postmortem_escalation_model", "") or "").strip()
    if not model:
        raise ValueError("AI_POSTMORTEM_ESCALATION_MODEL is required when escalation runs")
    timeout_seconds = _postmortem_timeout_seconds(
        settings,
        "ai_postmortem_escalation_timeout_seconds",
        default=90.0,
    )
    return _create_postmortem_provider_with_model(settings, provider, model, timeout_seconds=timeout_seconds)


def _create_postmortem_provider_with_model(
    settings: Any,
    provider: str,
    model: str,
    *,
    timeout_seconds: float | None = None,
) -> PostmortemProvider:
    if timeout_seconds is None:
        timeout_seconds = _postmortem_timeout_seconds(settings, "ai_postmortem_timeout_seconds", default=90.0)
    if provider == "openai":
        return OpenAIPostmortemProvider(
            _required_key(settings, "openai_api_key", provider),
            model=model,
            timeout_seconds=timeout_seconds,
        )
    if provider == "xai":
        return XAIPostmortemProvider(_required_key(settings, "xai_api_key", provider), model=model, timeout_seconds=timeout_seconds)
    if provider == "anthropic":
        return AnthropicPostmortemProvider(
            _required_key(settings, "anthropic_api_key", provider),
            model=model,
            timeout_seconds=timeout_seconds,
        )
    if provider == "gemini":
        return GeminiPostmortemProvider(_required_key(settings, "gemini_api_key", provider), model=model, timeout_seconds=timeout_seconds)
    if provider == "deepseek":
        return DeepSeekPostmortemProvider(
            _required_key(settings, "deepseek_api_key", provider),
            model=model,
            timeout_seconds=timeout_seconds,
        )
    raise ValueError(f"unsupported AI_POSTMORTEM_PROVIDER={provider}")


def _postmortem_timeout_seconds(settings: Any, attr: str, *, default: float) -> float:
    try:
        return float(getattr(settings, attr, default) or default)
    except (TypeError, ValueError):
        return default


def postmortem_model_for_provider(settings: Any, provider: str) -> str:
    provider_attr = f"ai_postmortem_{provider}_model"
    value = str(getattr(settings, provider_attr, "") or "").strip()
    if value:
        return value
    return str(getattr(settings, "ai_postmortem_model", "") or "").strip()


def _required_key(settings: Any, attr: str, provider: str) -> str:
    value = str(getattr(settings, attr, "") or "").strip()
    if not value:
        raise ValueError(f"AI_POSTMORTEM_PROVIDER={provider} requires {attr.upper()}")
    return value


def _has_fresh_base_postmortem_memo(provider_memos: list[PostmortemProviderMemo]) -> bool:
    return any(
        memo.prompt_version in {AI_POSTMORTEM_PROMPT_VERSION, AI_POSTMORTEM_FAILURE_PROMPT_VERSION}
        for memo in provider_memos
    )


async def _maybe_run_postmortem_escalation(
    *,
    settings: Any,
    provider: PostmortemProvider | None,
    packet: dict[str, Any],
    evidence_hash: str,
    provider_memos: list[PostmortemProviderMemo],
    run_paid: bool,
    confirm_paid_postmortem: bool,
    force_escalation: bool,
    max_escalation_calls: int | None,
) -> dict[str, Any] | None:
    trigger_reasons = _postmortem_escalation_trigger_reasons(packet, provider_memos)
    enabled = bool(getattr(settings, "ai_postmortem_escalation_enabled", False))
    should_attempt = run_paid and confirm_paid_postmortem and (force_escalation or (enabled and trigger_reasons))
    if not should_attempt:
        return None
    if force_escalation and "operator_forced" not in trigger_reasons:
        trigger_reasons = [*trigger_reasons, "operator_forced"]
    if not trigger_reasons:
        return {
            "status": "not_run",
            "reason": "no escalation trigger",
            "triggered": False,
            "trigger_reasons": [],
            "paid_called": False,
        }
    if not _has_fresh_base_postmortem_memo(provider_memos):
        return {
            "status": "not_run",
            "reason": "postmortem escalation requires a fresh base provider memo",
            "triggered": True,
            "trigger_reasons": trigger_reasons,
            "paid_called": False,
        }
    daily_limit = int(
        max_escalation_calls
        if max_escalation_calls is not None
        else getattr(settings, "ai_postmortem_escalation_max_calls_per_day", 0)
        or 0
    )
    if daily_limit <= 0:
        return {
            "status": "not_run",
            "reason": "AI_POSTMORTEM_ESCALATION_MAX_CALLS_PER_DAY or --max-escalation-calls must be positive",
            "triggered": True,
            "trigger_reasons": trigger_reasons,
            "paid_called": False,
            "chargeable_calls": {"before": None, "after": None, "attempts_needed": 0},
        }
    used_before = await count_ai_postmortem_escalation_chargeable_attempts(provider=None, today_utc=True)
    if used_before is None:
        return {
            "status": "not_run",
            "reason": "postmortem escalation chargeable budget count unavailable",
            "triggered": True,
            "trigger_reasons": trigger_reasons,
            "paid_called": False,
            "chargeable_calls": {"before": None, "after": None, "attempts_needed": 1},
        }
    if max(0, daily_limit - used_before) < 1:
        return {
            "status": "not_run",
            "reason": "postmortem escalation budget exhausted",
            "triggered": True,
            "trigger_reasons": trigger_reasons,
            "paid_called": False,
            "chargeable_calls": {"before": used_before, "after": used_before, "attempts_needed": 1},
        }
    try:
        reviewer = provider or create_postmortem_escalation_provider(settings)
    except Exception as exc:
        return {
            "status": "invalid",
            "reason": f"postmortem escalation provider setup failed: {exc}",
            "triggered": True,
            "trigger_reasons": trigger_reasons,
            "paid_called": False,
            "chargeable_calls": {"before": used_before, "after": used_before, "attempts_needed": 1},
        }
    escalation_packet = build_postmortem_escalation_packet(
        packet=packet,
        evidence_hash=evidence_hash,
        provider_memos=provider_memos,
        trigger_reasons=trigger_reasons,
    )
    base_result_hash = postmortem_base_result_hash(provider_memos)
    attempt_hash = postmortem_escalation_attempt_hash(
        evidence_hash=evidence_hash,
        base_result_hash=base_result_hash,
        provider=reviewer.provider,
        model_tag=reviewer.model_tag,
        window_days=int(packet.get("window_days") or 0),
        trigger_reasons=trigger_reasons,
    )
    existing = await count_ai_research_memos(provider=reviewer.provider, input_hash=attempt_hash)
    if existing is None:
        memo = _failure_memo(
            reviewer,
            attempt_hash,
            "postmortem escalation dedupe count unavailable",
            prompt_version=AI_POSTMORTEM_ESCALATION_FAILURE_PROMPT_VERSION,
        )
    elif existing > 0:
        return {
            "status": "deduped",
            "reason": "postmortem escalation input already reviewed",
            "triggered": True,
            "trigger_reasons": trigger_reasons,
            "paid_called": False,
            "chargeable_calls": {"before": used_before, "after": used_before, "attempts_needed": 1},
            "provider_result": _provider_result(
                PostmortemProviderMemo(
                    provider=reviewer.provider,
                    model_tag=reviewer.model_tag,
                    prompt_version="ai_postmortem_escalation_deduped/v0",
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
                        "judge_summary": "Existing escalation attempt found for provider/model/window hash.",
                    },
                )
            ),
        }
    else:
        memo = await _review_provider_with_retries(
            reviewer,
            escalation_packet,
            prompt_version=AI_POSTMORTEM_ESCALATION_PROMPT_VERSION,
            failure_prompt_version=AI_POSTMORTEM_ESCALATION_FAILURE_PROMPT_VERSION,
            instructions=POSTMORTEM_ESCALATION_INSTRUCTIONS,
            max_attempts=max(1, daily_limit - used_before),
        )
        memo = replace(memo, input_hash=attempt_hash)
    await _log_postmortem_memo(memo, escalation_packet)
    used_after = await count_ai_postmortem_escalation_chargeable_attempts(provider=None, today_utc=True)
    return build_postmortem_escalation_review(
        memo=memo,
        provider_memos=provider_memos,
        trigger_reasons=trigger_reasons,
        used_before=used_before,
        used_after=used_after,
    )


def _postmortem_escalation_trigger_reasons(
    packet: dict[str, Any],
    provider_memos: list[PostmortemProviderMemo],
) -> list[str]:
    reasons: list[str] = []
    if any(memo.prompt_version != "ai_postmortem_deduped/v0" and not memo.validation_passed for memo in provider_memos):
        reasons.append("invalid_provider_output")
    valid_outputs = [memo.output for memo in provider_memos if memo.validation_passed]
    if len(valid_outputs) >= 2:
        lesson_sets = {tuple(_merge_rows([output], "lessons", limit=8)) for output in valid_outputs}
        recommendation_sets = {tuple(_merge_rows([output], "operator_recommendations", limit=8)) for output in valid_outputs}
        if len(lesson_sets) > 1 or len(recommendation_sets) > 1:
            reasons.append("provider_disagreement")
    for trade in packet.get("closed_trades") or []:
        if isinstance(trade, dict) and float(trade.get("pnl_pct") or 0.0) <= -5.0:
            reasons.append("material_loss")
            break
    blocked = [row for row in packet.get("missed_or_blocked_opportunities") or [] if isinstance(row, dict)]
    if len(blocked) >= 5:
        reasons.append("many_blocked_opportunities")
    return list(dict.fromkeys(reasons))


def build_postmortem_escalation_packet(
    *,
    packet: dict[str, Any],
    evidence_hash: str,
    provider_memos: list[PostmortemProviderMemo],
    trigger_reasons: list[str],
) -> dict[str, Any]:
    base_outputs = [memo.output for memo in provider_memos if memo.validation_passed]
    compact_base = _postmortem_escalation_base_distilled(base_outputs)
    escalation_packet = {
        "kind": "ai_postmortem_escalation_input",
        "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "base_input_hash": evidence_hash,
        "window_days": packet.get("window_days"),
        "trigger_reasons": trigger_reasons,
        "input_summary": {
            "closed_trade_count": packet.get("closed_trade_count"),
            "opportunity_count": packet.get("opportunity_count"),
        },
        "base_provider_results": [_provider_result(memo) for memo in provider_memos],
        "base_distilled": compact_base,
        "evidence_excerpt": {
            "closed_trades": (packet.get("closed_trades") or [])[-6:],
            "missed_or_blocked_opportunities": (packet.get("missed_or_blocked_opportunities") or [])[-6:],
        },
        "rules": {
            "advisory_only": True,
            "operator_recommendations_review_only": True,
            "cannot_submit_orders": True,
            "cannot_change_config": True,
            "riskengine_remains_authority": True,
            "use_only_provided_data": True,
        },
    }
    if _postmortem_packet_size(escalation_packet) > MAX_POSTMORTEM_ESCALATION_CONTEXT_CHARS:
        escalation_packet["evidence_excerpt"] = {
            "closed_trades": (packet.get("closed_trades") or [])[-3:],
            "missed_or_blocked_opportunities": (packet.get("missed_or_blocked_opportunities") or [])[-3:],
        }
        escalation_packet["base_distilled"] = _postmortem_escalation_base_distilled(base_outputs, rows=3, chars=240)
        escalation_packet["context_truncated"] = True
    if _postmortem_packet_size(escalation_packet) > MAX_POSTMORTEM_ESCALATION_CONTEXT_CHARS:
        escalation_packet["evidence_excerpt"] = {
            "closed_trades": (packet.get("closed_trades") or [])[-1:],
            "missed_or_blocked_opportunities": (packet.get("missed_or_blocked_opportunities") or [])[-1:],
        }
        escalation_packet["base_distilled"] = _postmortem_escalation_base_distilled(base_outputs, rows=1, chars=160)
        escalation_packet["base_provider_results"] = [_compact_provider_result(memo) for memo in provider_memos]
    if _postmortem_packet_size(escalation_packet) > MAX_POSTMORTEM_ESCALATION_CONTEXT_CHARS:
        escalation_packet["evidence_excerpt"] = {"closed_trades": [], "missed_or_blocked_opportunities": []}
        escalation_packet["base_distilled"] = {
            "lessons": _clip_rows(_merge_rows(base_outputs, "lessons", limit=1), limit=1, chars=120),
            "edge_hypotheses": [],
            "budget_leaks": [],
            "operator_recommendations": _clip_rows(
                _merge_rows(base_outputs, "operator_recommendations", limit=1),
                limit=1,
                chars=120,
            ),
            "provider_notes": [],
        }
    return escalation_packet


def _postmortem_packet_size(packet: dict[str, Any]) -> int:
    return len(json.dumps(packet, sort_keys=True, default=str))


def _postmortem_escalation_base_distilled(
    base_outputs: list[dict[str, Any]],
    *,
    rows: int = 8,
    chars: int = 500,
) -> dict[str, list[str]]:
    return {
        "lessons": _clip_rows(_merge_rows(base_outputs, "lessons", limit=rows), limit=rows, chars=chars),
        "edge_hypotheses": _clip_rows(_merge_rows(base_outputs, "edge_hypotheses", limit=rows), limit=rows, chars=chars),
        "budget_leaks": _clip_rows(_merge_rows(base_outputs, "budget_leaks", limit=min(rows, 6)), limit=min(rows, 6), chars=chars),
        "operator_recommendations": _clip_rows(
            _merge_rows(base_outputs, "operator_recommendations", limit=min(rows, 6)),
            limit=min(rows, 6),
            chars=chars,
        ),
        "provider_notes": _clip_rows(_merge_rows(base_outputs, "provider_notes", limit=rows), limit=rows, chars=chars),
    }


def _clip_rows(rows: list[str], *, limit: int, chars: int) -> list[str]:
    return [row[:chars] for row in rows[:limit]]


def _compact_provider_result(memo: PostmortemProviderMemo) -> dict[str, Any]:
    return {
        "provider": memo.provider[:64],
        "model_tag": memo.model_tag[:120],
        "prompt_version": memo.prompt_version,
        "validation_passed": memo.validation_passed,
        "used_only_provided_data": memo.used_only_provided_data,
    }


def build_postmortem_escalation_review(
    *,
    memo: PostmortemProviderMemo,
    provider_memos: list[PostmortemProviderMemo],
    trigger_reasons: list[str],
    used_before: int | None,
    used_after: int | None,
) -> dict[str, Any]:
    base_outputs = [base_memo.output for base_memo in provider_memos if base_memo.validation_passed]
    output = memo.output if memo.validation_passed else {}
    metrics = _postmortem_escalation_metrics(base_outputs, output)
    return {
        "status": "completed" if memo.validation_passed else "invalid",
        "reason": "valid escalation review generated" if memo.validation_passed else "no valid escalation review",
        "triggered": True,
        "trigger_reasons": trigger_reasons,
        "paid_called": memo.prompt_version
        in {AI_POSTMORTEM_ESCALATION_PROMPT_VERSION, AI_POSTMORTEM_ESCALATION_FAILURE_PROMPT_VERSION},
        "chargeable_calls": {"before": used_before, "after": used_after, "attempts_needed": 1},
        "provider_result": _provider_result(memo),
        "highest_confidence_lessons": _merge_rows([output], "lessons", limit=4) if memo.validation_passed else [],
        "edge_hypotheses": _merge_rows([output], "edge_hypotheses", limit=4) if memo.validation_passed else [],
        "budget_leaks": _merge_rows([output], "budget_leaks", limit=3) if memo.validation_passed else [],
        "provider_quality_notes": _merge_rows([output], "provider_notes", limit=4) if memo.validation_passed else [],
        "operator_recommendations": _review_only_rows(_merge_rows([output], "operator_recommendations", limit=3))
        if memo.validation_passed
        else [],
        "judge_summary": output.get("judge_summary") if memo.validation_passed else "",
        **metrics,
    }


def _postmortem_escalation_metrics(
    base_outputs: list[dict[str, Any]],
    reviewer_output: dict[str, Any],
) -> dict[str, int]:
    base_lessons = set(_merge_rows(base_outputs, "lessons", limit=20))
    reviewer_lessons = set(_merge_rows([reviewer_output], "lessons", limit=20))
    base_recommendations = set(_merge_rows(base_outputs, "operator_recommendations", limit=20))
    reviewer_recommendations = set(_merge_rows([reviewer_output], "operator_recommendations", limit=20))
    notes = _merge_rows([reviewer_output], "provider_notes", limit=20)
    challenged = [
        note
        for note in notes
        if any(keyword in note.lower() for keyword in ("weak", "unsupported", "overstated", "contradict", "not enough"))
    ]
    return {
        "escalation_novel_lesson_count": len(reviewer_lessons - base_lessons),
        "escalation_changed_recommendation_count": len(reviewer_recommendations - base_recommendations),
        "escalation_challenged_lesson_count": len(challenged),
    }


def postmortem_base_result_hash(provider_memos: list[PostmortemProviderMemo]) -> str:
    rows = [
        {
            "provider": memo.provider,
            "model_tag": memo.model_tag,
            "prompt_version": memo.prompt_version,
            "input_hash": memo.input_hash,
            "validation_passed": memo.validation_passed,
            "output": memo.output,
            "error": memo.error,
        }
        for memo in provider_memos
    ]
    return hashlib.sha256(json.dumps(rows, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()


def postmortem_escalation_attempt_hash(
    *,
    evidence_hash: str,
    base_result_hash: str,
    provider: str,
    model_tag: str,
    window_days: int,
    trigger_reasons: list[str],
) -> str:
    payload = {
        "evidence_hash": evidence_hash,
        "base_result_hash": base_result_hash,
        "provider": provider,
        "model_tag": model_tag,
        "prompt_version": AI_POSTMORTEM_ESCALATION_PROMPT_VERSION,
        "window_days": window_days,
        "trigger_reasons": list(trigger_reasons),
        "role": "postmortem_escalation_reviewer",
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


async def _run_paid_postmortem_providers(
    providers: list[PostmortemProvider],
    packet: dict[str, Any],
    evidence_hash: str,
    *,
    force_paid: bool,
    max_http_attempts: int,
) -> list[PostmortemProviderMemo]:
    memos = []
    remaining_http_attempts = max(0, max_http_attempts)
    for index, provider in enumerate(providers):
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
        providers_after = len(providers) - index - 1
        max_attempts_for_provider = min(
            POSTMORTEM_MAX_PROVIDER_ATTEMPTS,
            max(1, remaining_http_attempts - providers_after),
        )
        memo = await _review_provider_with_retries(
            provider,
            packet,
            max_attempts=max_attempts_for_provider,
        )
        remaining_http_attempts = max(
            0,
            remaining_http_attempts - int(memo.output.get("attempt_count") or 1),
        )
        memo = replace(memo, input_hash=attempt_hash)
        await _log_postmortem_memo(memo, packet)
        memos.append(memo)
    return memos


async def _review_provider_with_retries(
    provider: PostmortemProvider,
    packet: dict[str, Any],
    *,
    prompt_version: str = AI_POSTMORTEM_PROMPT_VERSION,
    failure_prompt_version: str = AI_POSTMORTEM_FAILURE_PROMPT_VERSION,
    instructions: str = POSTMORTEM_INSTRUCTIONS,
    max_attempts: int = POSTMORTEM_MAX_PROVIDER_ATTEMPTS,
) -> PostmortemProviderMemo:
    packet_hash = postmortem_packet_hash(packet)
    attempts = 0
    retry_history: list[dict[str, Any]] = []
    max_attempts = max(1, min(max_attempts, POSTMORTEM_MAX_PROVIDER_ATTEMPTS))
    while attempts < max_attempts:
        attempts += 1
        try:
            memo = await provider.review(
                packet,
                prompt_version=prompt_version,
                failure_prompt_version=failure_prompt_version,
                instructions=instructions,
            )
        except Exception as exc:
            memo = _failure_memo(provider, packet_hash, exc, prompt_version=failure_prompt_version)
        memo = _memo_with_attempt_count(memo, attempts)
        if memo.validation_passed or not _memo_is_retryable(memo) or attempts >= max_attempts:
            return _memo_with_retry_history(memo, retry_history)
        retry_history.append(_retry_history_row(memo, attempts))
        await asyncio.sleep(_retry_delay_for_memo(memo, attempts))
    return _memo_with_retry_history(memo, retry_history)


def _memo_with_attempt_count(memo: PostmortemProviderMemo, attempts: int) -> PostmortemProviderMemo:
    output = dict(memo.output)
    output["attempt_count"] = attempts
    output["retry_count"] = max(0, attempts - 1)
    return replace(memo, output=output)


def _memo_is_retryable(memo: PostmortemProviderMemo) -> bool:
    return bool(memo.output.get("retryable"))


def _retry_history_row(memo: PostmortemProviderMemo, attempt: int) -> dict[str, Any]:
    return {
        "attempt": attempt,
        "error_type": memo.output.get("error_type"),
        "http_status": memo.output.get("http_status"),
        "retry_after_seconds": memo.output.get("retry_after_seconds"),
        "possible_duplicate_paid_request": bool(memo.output.get("possible_duplicate_paid_request")),
    }


def _memo_with_retry_history(
    memo: PostmortemProviderMemo,
    retry_history: list[dict[str, Any]],
) -> PostmortemProviderMemo:
    if not retry_history:
        return memo
    output = dict(memo.output)
    output["retry_history"] = list(retry_history)
    last = retry_history[-1]
    output["last_retry_error_type"] = last.get("error_type")
    output["last_retry_http_status"] = last.get("http_status")
    output["last_retry_after_seconds"] = last.get("retry_after_seconds")
    output["possible_duplicate_paid_request"] = bool(
        output.get("possible_duplicate_paid_request")
        or any(row.get("possible_duplicate_paid_request") for row in retry_history)
    )
    return replace(memo, output=output)


def _retry_delay_for_memo(memo: PostmortemProviderMemo, attempts: int) -> float:
    retry_after = memo.output.get("retry_after_seconds")
    if isinstance(retry_after, int | float):
        return min(max(float(retry_after), 0.0), POSTMORTEM_MAX_RETRY_AFTER_SECONDS)
    return min(POSTMORTEM_RETRY_BACKOFF_SECONDS * attempts, POSTMORTEM_MAX_RETRY_AFTER_SECONDS)


def postmortem_attempt_hash(*, evidence_hash: str, provider: str, model_tag: str, window_days: int) -> str:
    payload = {
        "evidence_hash": evidence_hash,
        "provider": provider,
        "model_tag": model_tag,
        "prompt_version": AI_POSTMORTEM_PROMPT_VERSION,
        "window_days": window_days,
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def _failure_memo(
    provider: PostmortemProvider,
    attempt_hash: str,
    error: BaseException | str,
    *,
    prompt_version: str = AI_POSTMORTEM_FAILURE_PROMPT_VERSION,
) -> PostmortemProviderMemo:
    error_text = str(error)
    return PostmortemProviderMemo(
        provider=str(getattr(provider, "provider", "unknown")),
        model_tag=str(getattr(provider, "model_tag", "unknown")),
        prompt_version=prompt_version,
        input_hash=attempt_hash,
        used_only_provided_data=True,
        validation_passed=False,
        output=_provider_failure_output(error),
        error=error_text,
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
        decision_source="ai_postmortem",
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
        "error_type": memo.output.get("error_type"),
        "http_status": memo.output.get("http_status"),
        "retryable": memo.output.get("retryable"),
        "retry_after_seconds": memo.output.get("retry_after_seconds"),
        "provider_response_body": memo.output.get("provider_response_body"),
        "provider_response_headers": memo.output.get("provider_response_headers", {}),
        "provider_stop_reason": memo.output.get("provider_stop_reason"),
        "attempt_count": memo.output.get("attempt_count"),
        "retry_count": memo.output.get("retry_count"),
        "last_retry_error_type": memo.output.get("last_retry_error_type"),
        "last_retry_http_status": memo.output.get("last_retry_http_status"),
        "last_retry_after_seconds": memo.output.get("last_retry_after_seconds"),
        "possible_duplicate_paid_request": memo.output.get("possible_duplicate_paid_request"),
        "retry_history": memo.output.get("retry_history", []),
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
    escalation = pack.get("escalation_review")
    if isinstance(escalation, dict):
        lines.append(
            "Escalation: "
            f"{escalation.get('status')} ({escalation.get('reason')}); "
            f"triggers={','.join(escalation.get('trigger_reasons') or []) or 'none'}; "
            f"paid_called={escalation.get('paid_called')}"
        )
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
    parser.add_argument("--force-escalation", action="store_true", help="Run the configured escalation reviewer when paid review is confirmed.")
    parser.add_argument("--max-paid-calls", type=int, default=None, help="Override separate postmortem paid-call budget.")
    parser.add_argument("--max-escalation-calls", type=int, default=None, help="Override separate escalation reviewer paid-call budget.")
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
            force_escalation=args.force_escalation,
            max_paid_calls=args.max_paid_calls,
            max_escalation_calls=args.max_escalation_calls,
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
