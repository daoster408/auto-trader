"""Read-only cached scoreboard memory for AI packet context."""
from __future__ import annotations

import json
import os
from json import JSONDecodeError
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from auto_trader.persistence.db import get_configured_db_path

MAX_SCOREBOARD_MEMORY_BYTES = 24_000
SCOREBOARD_MEMORY_STALE_AFTER = timedelta(hours=36)
MAX_PROMPT_CONTEXT_CHARS = 4_000


def default_scoreboard_memory_path() -> Path:
    override = os.getenv("AUTO_TRADER_SCOREBOARD_MEMORY_PATH")
    if override:
        return Path(override)
    db_path = get_configured_db_path()
    root = db_path.parent if str(db_path.parent) not in {"", "."} else Path(".")
    return root / "runtime" / "scoreboard_memory_pack.json"


def load_scoreboard_memory_context(
    *,
    path: Path | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    memory_path = path or default_scoreboard_memory_path()
    context: dict[str, Any] = {
        "status": "missing",
        "available": False,
        "path": str(memory_path),
        "advisory_only": True,
        "order_authority": "RiskEngine",
        "max_age_seconds": int(SCOREBOARD_MEMORY_STALE_AFTER.total_seconds()),
    }
    try:
        if not memory_path.exists():
            return context
        size = memory_path.stat().st_size
        if size > MAX_SCOREBOARD_MEMORY_BYTES:
            return {
                **context,
                "status": "oversized",
                "error": "scoreboard_memory_oversized",
                "size_bytes": size,
                "max_bytes": MAX_SCOREBOARD_MEMORY_BYTES,
            }
        try:
            pack = json.loads(memory_path.read_text(encoding="utf-8"))
        except JSONDecodeError:
            return {**context, "status": "malformed", "error": "scoreboard_memory_malformed_json"}
        if not isinstance(pack, dict) or pack.get("kind") != "scoreboard_memory_pack":
            return {**context, "status": "invalid", "error": "invalid_scoreboard_memory_pack"}
        loaded = _compact_pack(pack, path=memory_path)
        generated_at = _parse_generated_at(loaded.get("generated_at"))
        if generated_at is None:
            return {**loaded, "status": "invalid", "available": False, "error": "invalid_generated_at"}
        age_seconds = max(0.0, ((now or datetime.now(UTC)) - generated_at).total_seconds())
        loaded["age_seconds"] = round(age_seconds, 3)
        loaded["max_age_seconds"] = int(SCOREBOARD_MEMORY_STALE_AFTER.total_seconds())
        if age_seconds > SCOREBOARD_MEMORY_STALE_AFTER.total_seconds():
            loaded["status"] = "stale"
            loaded["available"] = False
            loaded["error"] = "scoreboard_memory_stale"
            loaded["prompt_context"] = (
                "Scoreboard memory is stale; use only current candidate packet data and safety gates."
            )
        return loaded
    except Exception as exc:
        return {**context, "status": "error", "error": "scoreboard_memory_load_failed", "detail": str(exc)}


def _compact_pack(pack: dict[str, Any], *, path: Path) -> dict[str, Any]:
    prompt_context = str(pack.get("prompt_context") or "")
    if len(prompt_context) > MAX_PROMPT_CONTEXT_CHARS:
        prompt_context = prompt_context[:MAX_PROMPT_CONTEXT_CHARS].rstrip() + "\n[truncated]"
    return {
        "status": "loaded",
        "available": True,
        "path": str(path),
        "advisory_only": True,
        "order_authority": "RiskEngine",
        "generated_at": pack.get("generated_at"),
        "window_days": pack.get("window_days"),
        "closed_trade_count": pack.get("closed_trade_count"),
        "opportunity_count": pack.get("opportunity_count"),
        "sample_label": pack.get("sample_label"),
        "notes": _bounded_list(pack.get("notes"), limit=3),
        "performance": pack.get("performance") if isinstance(pack.get("performance"), dict) else {},
        "positive_observed_tags": _bounded_list(pack.get("positive_observed_tags"), limit=5),
        "negative_observed_tags": _bounded_list(pack.get("negative_observed_tags"), limit=5),
        "provider_vote_outcome_buckets": _bounded_list(pack.get("provider_vote_outcome_buckets"), limit=6),
        "blocked_pressure": _bounded_list(pack.get("blocked_pressure"), limit=5),
        "prompt_context": prompt_context,
        "max_age_seconds": int(SCOREBOARD_MEMORY_STALE_AFTER.total_seconds()),
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
