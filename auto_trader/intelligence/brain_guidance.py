"""Read-only cached brain guidance for AI packet context."""
from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta
from json import JSONDecodeError
from pathlib import Path
from typing import Any

from auto_trader.config.settings import get_settings
from auto_trader.persistence.db import get_configured_db_path

MAX_BRAIN_GUIDANCE_BYTES = 32_000
BRAIN_GUIDANCE_STALE_AFTER = timedelta(days=8)
MAX_BRAIN_PROMPT_CONTEXT_CHARS = 6_000


def default_brain_guidance_path() -> Path:
    override = os.getenv("AUTO_TRADER_BRAIN_GUIDANCE_PATH")
    if override:
        return Path(override)
    try:
        settings = get_settings()
    except Exception:
        settings = None
    settings_override = getattr(settings, "brain_guidance_path", None)
    if settings_override:
        return Path(settings_override)
    db_path = get_configured_db_path()
    root = db_path.parent if str(db_path.parent) not in {"", "."} else Path(".")
    return root / "runtime" / "brain_reviews" / "brain_guidance_pack.json"


def load_brain_guidance_context(
    *,
    path: Path | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    guidance_path = path or default_brain_guidance_path()
    context: dict[str, Any] = {
        "status": "missing",
        "available": False,
        "path": str(guidance_path),
        "advisory_only": True,
        "order_authority": "RiskEngine",
        "config_authority": "operator_only",
        "max_age_seconds": int(BRAIN_GUIDANCE_STALE_AFTER.total_seconds()),
    }
    try:
        if not guidance_path.exists():
            return context
        size = guidance_path.stat().st_size
        if size > MAX_BRAIN_GUIDANCE_BYTES:
            return {
                **context,
                "status": "oversized",
                "error": "brain_guidance_oversized",
                "size_bytes": size,
                "max_bytes": MAX_BRAIN_GUIDANCE_BYTES,
            }
        try:
            pack = json.loads(guidance_path.read_text(encoding="utf-8"))
        except JSONDecodeError:
            return {**context, "status": "malformed", "error": "brain_guidance_malformed_json"}
        if not isinstance(pack, dict) or pack.get("kind") != "brain_guidance_pack":
            return {**context, "status": "invalid", "error": "invalid_brain_guidance_pack"}
        loaded = _compact_pack(pack, path=guidance_path)
        generated_at = _parse_generated_at(loaded.get("generated_at"))
        if generated_at is None:
            return {**loaded, "status": "invalid", "available": False, "error": "invalid_generated_at"}
        age_seconds = max(0.0, ((now or datetime.now(UTC)) - generated_at).total_seconds())
        loaded["age_seconds"] = round(age_seconds, 3)
        loaded["max_age_seconds"] = int(BRAIN_GUIDANCE_STALE_AFTER.total_seconds())
        if age_seconds > BRAIN_GUIDANCE_STALE_AFTER.total_seconds():
            loaded["status"] = "stale"
            loaded["available"] = False
            loaded["error"] = "brain_guidance_stale"
            loaded["prompt_context"] = (
                "Brain guidance is stale; use only current candidate packet data and safety gates."
            )
        return loaded
    except Exception as exc:
        return {**context, "status": "error", "error": "brain_guidance_load_failed", "detail": str(exc)}


def _compact_pack(pack: dict[str, Any], *, path: Path) -> dict[str, Any]:
    prompt_context = str(pack.get("prompt_context") or "")
    if len(prompt_context) > MAX_BRAIN_PROMPT_CONTEXT_CHARS:
        prompt_context = prompt_context[:MAX_BRAIN_PROMPT_CONTEXT_CHARS].rstrip() + "\n[truncated]"
    return {
        "status": "loaded",
        "available": True,
        "path": str(path),
        "advisory_only": True,
        "order_authority": "RiskEngine",
        "config_authority": "operator_only",
        "generated_at": pack.get("generated_at"),
        "source_labels": _bounded_list(pack.get("source_labels"), limit=4),
        "reviews": _bounded_list(pack.get("reviews"), limit=3),
        "prompt_context": prompt_context,
        "max_age_seconds": int(BRAIN_GUIDANCE_STALE_AFTER.total_seconds()),
    }


def _bounded_list(value: Any, *, limit: int) -> list[Any]:
    if not isinstance(value, list):
        return []
    return value[:limit]


def _parse_generated_at(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)
