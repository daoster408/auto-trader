"""Compact, secret-safe AI decision display helpers."""
from __future__ import annotations

from typing import Any

PROVIDER_DISPLAY_NAMES = {
    "anthropic": "Claude",
    "openai": "ChatGPT",
    "xai": "Grok",
    "gemini": "Gemini",
    "deepseek": "DeepSeek",
    "multi": "Committee",
    "shadow": "Shadow",
    "prefilter": "Prefilter",
}

VERDICT_DISPLAY = {
    "approve": "Buy",
    "reject": "Reject",
    "watch": "Watch",
}

FAILURE_PREFIX = "ai_research_provider_"


def provider_display_name(provider: Any) -> str:
    """Return an allowlisted provider label for operator-facing messages."""
    return PROVIDER_DISPLAY_NAMES.get(str(provider or "").strip().lower(), "Provider")


def ai_decision_label(
    *,
    verdict: Any,
    validation_passed: Any = True,
    prompt_version: Any = "",
    memo: dict[str, Any] | None = None,
    validation_errors: list[Any] | None = None,
) -> str:
    """Normalize a provider memo/vote into Buy/Reject/Watch/Error text."""
    errors = validation_errors
    if errors is None:
        committee = (memo or {}).get("committee") if isinstance(memo, dict) else {}
        errors = committee.get("validation_errors", []) if isinstance(committee, dict) else []
    is_failure = not bool(validation_passed) or str(prompt_version or "") == "ai_research_failure/v0"
    if is_failure:
        return f"Error ({_failure_category(memo=memo, validation_errors=errors)})"
    return VERDICT_DISPLAY.get(str(verdict or "").strip().lower(), "Watch")


def compact_ai_decision_line(
    *,
    provider: Any,
    verdict: Any,
    validation_passed: Any = True,
    prompt_version: Any = "",
    confidence: Any = None,
    symbol: Any = None,
    memo: dict[str, Any] | None = None,
    validation_errors: list[Any] | None = None,
    include_confidence: bool = False,
) -> str:
    """Render a single compact provider decision line."""
    label = ai_decision_label(
        verdict=verdict,
        validation_passed=validation_passed,
        prompt_version=prompt_version,
        memo=memo,
        validation_errors=validation_errors,
    )
    line = f"{provider_display_name(provider)}: {label}"
    clean_symbol = str(symbol or "").strip().upper()
    if clean_symbol:
        line += f" on {clean_symbol}"
    if include_confidence and isinstance(confidence, int | float):
        line += f" ({float(confidence):.2f})"
    return line


def compact_ai_vote_lines_from_memo(memo: dict[str, Any], *, include_confidence: bool = False) -> list[str]:
    """Return compact provider vote lines from an aggregate or provider memo."""
    if not isinstance(memo, dict):
        return []
    votes = memo.get("provider_votes")
    if isinstance(votes, list) and votes:
        lines = []
        for vote in votes:
            if not isinstance(vote, dict):
                continue
            lines.append(
                compact_ai_decision_line(
                    provider=vote.get("provider"),
                    verdict=vote.get("verdict"),
                    validation_passed=vote.get("validation_passed", True),
                    prompt_version=vote.get("prompt_version", ""),
                    confidence=vote.get("confidence"),
                    validation_errors=vote.get("validation_errors"),
                    include_confidence=include_confidence,
                )
            )
        return lines
    committee = memo.get("committee") if isinstance(memo.get("committee"), dict) else {}
    return [
        compact_ai_decision_line(
            provider=memo.get("provider"),
            verdict=(committee or {}).get("verdict") or memo.get("verdict"),
            validation_passed=memo.get("validation_passed", True),
            prompt_version=memo.get("prompt_version", ""),
            confidence=(committee or {}).get("confidence"),
            symbol=(committee or {}).get("symbol"),
            memo=memo,
            include_confidence=include_confidence,
        )
    ]


def _failure_category(*, memo: dict[str, Any] | None, validation_errors: list[Any] | None) -> str:
    failure = (memo or {}).get("provider_failure") if isinstance(memo, dict) else None
    category = failure.get("category") if isinstance(failure, dict) else None
    if category:
        return _clean_failure_category(category)
    for error in validation_errors or []:
        text = str(error)
        if text.startswith(FAILURE_PREFIX) and text != "ai_research_provider_failed":
            return _clean_failure_category(text.removeprefix(FAILURE_PREFIX))
    return "unknown"


def _clean_failure_category(value: Any) -> str:
    text = str(value or "unknown").strip().lower()
    return "".join(ch for ch in text if ch.isalnum() or ch == "_")[:40] or "unknown"
