"""Deterministic brain review packs from observed trading evidence."""
from __future__ import annotations

import argparse
import asyncio
import json
import os
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any

from auto_trader.config.settings import get_settings
from auto_trader.edge_report import (
    EdgeReport,
    _bucket_counts,
    _build_closed_trades,
    _build_opportunities,
    _event_time,
    _fetch_rows,
    _money,
    _parse_dt,
    _short_reason,
    _trade_groups_by_values,
    _trade_stats,
)
from auto_trader.persistence.db import configure_db_path, get_configured_db_path, init_db
from auto_trader.utils.logging import setup_logging

BRAIN_REVIEW_KIND = "brain_review_pack"
BRAIN_GUIDANCE_KIND = "brain_guidance_pack"
BRAIN_REVIEW_WINDOWS = {"weekly": 5, "monthly": 21, "quarterly": 63}


def build_brain_review_pack(
    report: EdgeReport,
    *,
    label: str,
    session_target: int,
    observed_sessions: list[str],
    generated_at: datetime | None = None,
    limit: int = 5,
) -> dict[str, Any]:
    """Build one deterministic review pack for a window of observed sessions."""
    trades = report.closed_trades
    opportunities = report.opportunities
    stats = _trade_stats(trades)
    setup_groups = _trade_groups_by_values(trades, lambda trade: trade.setup_tags)
    provider_groups = _trade_groups_by_values(trades, lambda trade: trade.provider_votes)
    blocked = [opportunity for opportunity in opportunities if opportunity.outcome != "traded"]
    blocked_counts = _bucket_counts([f"{opportunity.outcome}: {_short_reason(opportunity.reason)}" for opportunity in blocked])
    ai_watch_or_reject = [
        opportunity
        for opportunity in opportunities
        if opportunity.outcome in {"ai_watch", "ai_reject", "prefilter_blocked"} and opportunity.setup_tags
    ]
    missed_counts = _bucket_counts([tag for opportunity in ai_watch_or_reject for tag in opportunity.setup_tags])

    positive_patterns = [
        _review_group(name, row_stats, action="prioritize")
        for name, _group, row_stats in sorted(
            setup_groups,
            key=lambda row: (row[2]["expectancy"], row[2]["realized_pnl"], row[0]),
            reverse=True,
        )
        if row_stats["realized_pnl"] > 0
    ][:limit]
    viable_patterns = [
        _review_group(name, row_stats, action="keep_viable")
        for name, _group, row_stats in sorted(
            setup_groups,
            key=lambda row: (row[2]["count"], row[2]["expectancy"], row[0]),
            reverse=True,
        )
        if row_stats["realized_pnl"] >= 0
    ][:limit]
    weak_patterns = [
        _review_group(name, row_stats, action="spend_less_budget")
        for name, _group, row_stats in sorted(
            setup_groups,
            key=lambda row: (row[2]["expectancy"], row[2]["realized_pnl"], row[0]),
        )
        if row_stats["realized_pnl"] < 0
    ][:limit]
    provider_rows = [
        _review_group(name, row_stats, action="provider_signal")
        for name, _group, row_stats in sorted(
            provider_groups,
            key=lambda row: (row[2]["realized_pnl"], row[2]["expectancy"], row[0]),
            reverse=True,
        )
    ][: limit * 2]

    sample = _sample_label(int(stats["count"]), len(opportunities))
    generated = generated_at or datetime.now(UTC)
    pack: dict[str, Any] = {
        "kind": BRAIN_REVIEW_KIND,
        "label": label,
        "generated_at": generated.astimezone(UTC).isoformat().replace("+00:00", "Z"),
        "window_basis": "observed_market_sessions",
        "session_target": session_target,
        "observed_session_count": len(observed_sessions),
        "observed_sessions": observed_sessions[-session_target:],
        "closed_trade_count": int(stats["count"]),
        "opportunity_count": len(opportunities),
        "sample_label": sample,
        "principles": [
            "Edge amplification, not default caution.",
            "Thin positive evidence is a hypothesis to test, not a reason to become passive.",
            "Operator recommendations are review-only and never automatic config changes.",
            "RiskEngine remains the sizing and order-flow authority.",
        ],
        "performance": {
            "realized_pnl": round(float(stats["realized_pnl"]), 4),
            "expectancy": round(float(stats["expectancy"]), 4),
            "win_rate": round(float(stats["win_rate"]), 2),
            "wins": int(stats["wins"]),
            "losses": int(stats["losses"]),
        },
        "observed_edge_amplifiers": positive_patterns,
        "viable_patterns": viable_patterns,
        "deprioritize": weak_patterns,
        "missed_edge_hypotheses": [
            {"key": key, "count": count, "note": "Blocked or watched setup; investigate, do not assume edge."}
            for key, count in missed_counts[:limit]
        ],
        "observed_leaks": [{"key": key, "count": count} for key, count in blocked_counts[:limit]],
        "provider_behavior": provider_rows,
        "operator_recommendations": _operator_recommendations(
            stats=stats,
            positive_patterns=positive_patterns,
            weak_patterns=weak_patterns,
            blocked_counts=blocked_counts,
        ),
    }
    pack["prompt_guidance"] = render_brain_review_prompt_guidance(pack)
    return pack


def build_brain_guidance_pack(
    review_packs: list[dict[str, Any]],
    *,
    postmortem_pack: dict[str, Any] | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    """Combine review packs into the tiny artifact loaded by AI packets."""
    generated = generated_at or datetime.now(UTC)
    pack: dict[str, Any] = {
        "kind": BRAIN_GUIDANCE_KIND,
        "generated_at": generated.astimezone(UTC).isoformat().replace("+00:00", "Z"),
        "advisory_only": True,
        "order_authority": "RiskEngine",
        "config_authority": "operator_only",
        "source_labels": [str(pack.get("label")) for pack in review_packs],
        "reviews": [_compact_review_for_guidance(pack) for pack in review_packs],
    }
    if postmortem_pack:
        pack["ai_postmortem"] = _compact_postmortem_for_guidance(postmortem_pack)
    pack["prompt_context"] = render_brain_guidance_prompt_context(pack)
    return pack


async def build_brain_review_bundle(
    *,
    db_path: Path | None = None,
    generated_at: datetime | None = None,
    postmortem_pack: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build weekly/monthly/quarterly review packs plus combined guidance."""
    await init_db()
    rows = await _fetch_rows(db_path or get_configured_db_path())
    observed_dates = _observed_market_dates(rows)
    generated = generated_at or datetime.now(UTC)
    reviews: dict[str, dict[str, Any]] = {}
    for label, session_target in BRAIN_REVIEW_WINDOWS.items():
        since, sessions = _window_since(observed_dates, session_target=session_target, now=generated)
        report = EdgeReport(
            window_days=session_target,
            closed_trades=_build_closed_trades(rows, since=since),
            opportunities=_build_opportunities(rows, since=since),
        )
        reviews[label] = build_brain_review_pack(
            report,
            label=label,
            session_target=session_target,
            observed_sessions=[value.isoformat() for value in sessions],
            generated_at=generated,
        )
    guidance = build_brain_guidance_pack(
        list(reviews.values()),
        postmortem_pack=postmortem_pack,
        generated_at=generated,
    )
    return {
        "kind": "brain_review_bundle",
        "generated_at": generated.astimezone(UTC).isoformat().replace("+00:00", "Z"),
        "reviews": reviews,
        "guidance": guidance,
    }


def default_brain_review_dir(settings: Any | None = None) -> Path:
    override = getattr(settings, "brain_review_dir", None) or os.getenv("AUTO_TRADER_BRAIN_REVIEW_DIR")
    if override:
        return Path(override)
    db_path = Path(str(getattr(settings, "db_path", None) or get_configured_db_path()))
    root = db_path.parent if str(db_path.parent) not in {"", "."} else Path(".")
    return root / "runtime" / "brain_reviews"


def default_brain_guidance_path(settings: Any | None = None) -> Path:
    override = getattr(settings, "brain_guidance_path", None) or os.getenv("AUTO_TRADER_BRAIN_GUIDANCE_PATH")
    if override:
        return Path(override)
    return default_brain_review_dir(settings) / "brain_guidance_pack.json"


def write_brain_review_bundle(
    bundle: dict[str, Any],
    directory: Path,
    *,
    guidance_path: Path | None = None,
) -> dict[str, Path]:
    directory.mkdir(parents=True, exist_ok=True)
    written: dict[str, Path] = {}
    reviews = bundle.get("reviews") if isinstance(bundle.get("reviews"), dict) else {}
    for label, pack in reviews.items():
        path = directory / f"{label}_review_pack.json"
        _write_json_atomic(pack, path)
        written[str(label)] = path
    guidance = bundle.get("guidance") if isinstance(bundle.get("guidance"), dict) else {}
    resolved_guidance_path = guidance_path or directory / "brain_guidance_pack.json"
    _write_json_atomic(guidance, resolved_guidance_path)
    written["guidance"] = resolved_guidance_path
    return written


async def run_brain_review_pack(
    *,
    write_cache: bool = False,
    cache_dir: Path | None = None,
    guidance_only: bool = False,
) -> str:
    settings = get_settings()
    configure_db_path(getattr(settings, "db_path", "auto_trader.db"))
    bundle = await build_brain_review_bundle()
    if write_cache:
        review_dir = cache_dir or default_brain_review_dir(settings)
        guidance_path = None if cache_dir else default_brain_guidance_path(settings)
        write_brain_review_bundle(bundle, review_dir, guidance_path=guidance_path)
    payload = bundle["guidance"] if guidance_only else bundle
    return json.dumps(payload, indent=2, sort_keys=True)


def render_brain_review_prompt_guidance(pack: dict[str, Any]) -> str:
    lines = [
        f"{str(pack.get('label') or 'review').upper()} BRAIN REVIEW",
        "Use as edge-amplification context only; do not use as order authority.",
        (
            f"Window: {pack.get('observed_session_count', 0)}/{pack.get('session_target')} observed sessions; "
            f"closed_trades={pack.get('closed_trade_count', 0)}; "
            f"opportunities={pack.get('opportunity_count', 0)}; sample={pack.get('sample_label')}"
        ),
        _performance_line(pack),
        "Prioritize when current candidate confirms:",
    ]
    lines.extend(_guidance_rows(pack.get("observed_edge_amplifiers"), limit=3) or ["- no proven amplifier yet; seek strong current evidence"])
    lines.append("Spend less attention on repeated low-EV drains:")
    lines.extend(_leak_rows(pack.get("observed_leaks"), limit=3) or ["- no repeated leak yet"])
    lines.append("Operator recommendations are review-only:")
    lines.extend(_recommendation_rows(pack.get("operator_recommendations"), limit=2))
    return "\n".join(lines)


def render_brain_guidance_prompt_context(pack: dict[str, Any]) -> str:
    lines = [
        "BRAIN GUIDANCE PACK",
        "Advisory edge-amplification context only. Current candidate data has priority.",
        "Use this to press better setups and reduce wasted research, not to default to passivity.",
        "Do not change config, sizing, orders, limits, or RiskEngine behavior.",
    ]
    for review in pack.get("reviews") if isinstance(pack.get("reviews"), list) else []:
        if not isinstance(review, dict):
            continue
        lines.append(
            f"{str(review.get('label') or 'review').upper()}: "
            f"sample={review.get('sample_label')}; "
            f"P/L={_money(float(review.get('realized_pnl') or 0.0))}; "
            f"exp={_money(float(review.get('expectancy') or 0.0))}; "
            f"win={float(review.get('win_rate') or 0.0):.1f}%"
        )
        for row in review.get("observed_edge_amplifiers") or []:
            if isinstance(row, dict):
                lines.append(f"- prioritize: {_compact_row_line(row)}")
        for row in review.get("observed_leaks") or []:
            if isinstance(row, dict):
                lines.append(f"- leak: {row.get('key')} count={row.get('count')}")
    postmortem = pack.get("ai_postmortem")
    if isinstance(postmortem, dict) and postmortem.get("status") == "completed":
        lines.append("AI POSTMORTEM:")
        for lesson in postmortem.get("distilled_lessons") or []:
            if isinstance(lesson, str) and lesson.strip():
                lines.append(f"- lesson: {lesson}")
        for hypothesis in postmortem.get("edge_hypotheses") or []:
            if isinstance(hypothesis, str) and hypothesis.strip():
                lines.append(f"- test: {hypothesis}")
        for leak in postmortem.get("budget_leaks") or []:
            if isinstance(leak, str) and leak.strip():
                lines.append(f"- paid-budget leak: {leak}")
        escalation = postmortem.get("escalation_review")
        if isinstance(escalation, dict) and escalation.get("status") == "completed":
            lines.append("AI POSTMORTEM ESCALATION REVIEW:")
            for lesson in escalation.get("highest_confidence_lessons") or []:
                if isinstance(lesson, str) and lesson.strip():
                    lines.append(f"- reviewer lesson: {lesson}")
            for note in escalation.get("provider_quality_notes") or []:
                if isinstance(note, str) and note.strip():
                    lines.append(f"- reviewer note: {note}")
    lines.append("If review context conflicts with the current packet, explain the conflict and judge the current packet.")
    return "\n".join(lines)


def _observed_market_dates(rows: dict[str, list[dict[str, Any]]]) -> list[date]:
    dates: set[date] = set()
    for signal in rows.get("signals", []):
        _add_weekday_date(dates, _parse_dt(signal.get("created_at")))
    for memo in rows.get("ai_memos", []):
        _add_weekday_date(dates, _parse_dt(memo.get("created_at")))
    for order in rows.get("orders", []):
        _add_weekday_date(dates, _event_time(order))
    return sorted(dates)


def _add_weekday_date(values: set[date], dt: datetime | None) -> None:
    if dt is not None and dt.weekday() < 5:
        values.add(dt.date())


def _window_since(observed_dates: list[date], *, session_target: int, now: datetime) -> tuple[datetime, list[date]]:
    if not observed_dates:
        return now - timedelta(days=max(7, session_target * 2)), []
    sessions = observed_dates[-session_target:]
    start = sessions[0]
    return datetime.combine(start, time.min, tzinfo=UTC), sessions


def _review_group(name: str, stats: dict[str, float], *, action: str) -> dict[str, Any]:
    count = int(stats["count"])
    return {
        "key": name,
        "action": action,
        "n": count,
        "sample": "thin" if count < 3 else "building",
        "realized_pnl": round(float(stats["realized_pnl"]), 4),
        "expectancy": round(float(stats["expectancy"]), 4),
        "win_rate": round(float(stats["win_rate"]), 2),
    }


def _operator_recommendations(
    *,
    stats: dict[str, float],
    positive_patterns: list[dict[str, Any]],
    weak_patterns: list[dict[str, Any]],
    blocked_counts: list[tuple[str, int]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if positive_patterns:
        rows.append(
            {
                "topic": "candidate_priority",
                "recommendation": f"Review whether `{positive_patterns[0]['key']}` deserves more candidate priority.",
                "review_only": True,
            }
        )
    else:
        rows.append(
            {
                "topic": "sample_building",
                "recommendation": "Keep seeking strong asymmetric candidates; no proven amplifier yet.",
                "review_only": True,
            }
        )
    if weak_patterns:
        rows.append(
            {
                "topic": "research_budget",
                "recommendation": f"Spend less repeat research on `{weak_patterns[0]['key']}` unless current evidence improves.",
                "review_only": True,
            }
        )
    if blocked_counts:
        rows.append(
            {
                "topic": "friction",
                "recommendation": f"Inspect top observed leak `{blocked_counts[0][0]}` for useful filtering vs wasted shots.",
                "review_only": True,
            }
        )
    if float(stats["expectancy"]) > 0:
        rows.append(
            {
                "topic": "aggression",
                "recommendation": "Positive expectancy observed; look for similar high-conviction candidates before conserving budget.",
                "review_only": True,
            }
        )
    return rows[:4]


def _sample_label(closed_trade_count: int, opportunity_count: int) -> str:
    if closed_trade_count <= 0:
        return "empty" if opportunity_count <= 0 else "opportunity_only"
    if closed_trade_count < 10:
        return "thin"
    if closed_trade_count < 30:
        return "building"
    return "mature"


def _compact_review_for_guidance(pack: dict[str, Any]) -> dict[str, Any]:
    performance = pack.get("performance") if isinstance(pack.get("performance"), dict) else {}
    return {
        "label": pack.get("label"),
        "window_basis": pack.get("window_basis"),
        "session_target": pack.get("session_target"),
        "observed_session_count": pack.get("observed_session_count"),
        "closed_trade_count": pack.get("closed_trade_count"),
        "opportunity_count": pack.get("opportunity_count"),
        "sample_label": pack.get("sample_label"),
        "realized_pnl": performance.get("realized_pnl"),
        "expectancy": performance.get("expectancy"),
        "win_rate": performance.get("win_rate"),
        "observed_edge_amplifiers": _bounded_dict_list(pack.get("observed_edge_amplifiers"), limit=3),
        "viable_patterns": _bounded_dict_list(pack.get("viable_patterns"), limit=2),
        "deprioritize": _bounded_dict_list(pack.get("deprioritize"), limit=2),
        "missed_edge_hypotheses": _bounded_dict_list(pack.get("missed_edge_hypotheses"), limit=3),
        "observed_leaks": _bounded_dict_list(pack.get("observed_leaks"), limit=3),
        "provider_behavior": _bounded_dict_list(pack.get("provider_behavior"), limit=4),
        "operator_recommendations": _bounded_dict_list(pack.get("operator_recommendations"), limit=3),
    }


def _compact_postmortem_for_guidance(pack: dict[str, Any]) -> dict[str, Any]:
    compact = {
        "kind": pack.get("kind"),
        "status": pack.get("status"),
        "generated_at": pack.get("generated_at"),
        "input_hash": pack.get("input_hash"),
        "paid_called": bool(pack.get("paid_called")),
        "distilled_lessons": _bounded_text_list(pack.get("distilled_lessons"), limit=4),
        "edge_hypotheses": _bounded_text_list(pack.get("edge_hypotheses"), limit=4),
        "budget_leaks": _bounded_text_list(pack.get("budget_leaks"), limit=3),
        "operator_recommendations": _bounded_dict_list(pack.get("operator_recommendations"), limit=3),
    }
    escalation = pack.get("escalation_review")
    if isinstance(escalation, dict) and escalation.get("status") == "completed":
        compact["escalation_review"] = {
            "status": escalation.get("status"),
            "trigger_reasons": _bounded_text_list(escalation.get("trigger_reasons"), limit=4),
            "highest_confidence_lessons": _bounded_text_list(escalation.get("highest_confidence_lessons"), limit=3),
            "provider_quality_notes": _bounded_text_list(escalation.get("provider_quality_notes"), limit=3),
            "operator_recommendations": _bounded_dict_list(escalation.get("operator_recommendations"), limit=2),
        }
    return compact


def _bounded_text_list(value: Any, *, limit: int) -> list[str]:
    if not isinstance(value, list):
        return []
    return [" ".join(str(item).split())[:500] for item in value[:limit] if str(item or "").strip()]


def _bounded_dict_list(value: Any, *, limit: int) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [row for row in value[:limit] if isinstance(row, dict)]


def _guidance_rows(value: Any, *, limit: int) -> list[str]:
    return [f"- {_compact_row_line(row)}" for row in _bounded_dict_list(value, limit=limit)]


def _leak_rows(value: Any, *, limit: int) -> list[str]:
    return [f"- {row.get('key')}: {row.get('count')}" for row in _bounded_dict_list(value, limit=limit)]


def _recommendation_rows(value: Any, *, limit: int) -> list[str]:
    rows = []
    for row in _bounded_dict_list(value, limit=limit):
        rows.append(f"- {row.get('recommendation')} (review-only)")
    return rows or ["- none"]


def _compact_row_line(row: dict[str, Any]) -> str:
    return (
        f"{row.get('key')} n={row.get('n')} sample={row.get('sample')} "
        f"pnl={_money(float(row.get('realized_pnl') or 0.0))} "
        f"exp={_money(float(row.get('expectancy') or 0.0))} "
        f"win={float(row.get('win_rate') or 0.0):.1f}%"
    )


def _performance_line(pack: dict[str, Any]) -> str:
    performance = pack.get("performance") if isinstance(pack.get("performance"), dict) else {}
    return (
        f"Observed P/L={_money(float(performance.get('realized_pnl') or 0.0))}; "
        f"expectancy={_money(float(performance.get('expectancy') or 0.0))}; "
        f"win={float(performance.get('win_rate') or 0.0):.1f}%"
    )


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


def main() -> None:
    parser = argparse.ArgumentParser(description="Build deterministic AUTO-TRADER brain review packs.")
    parser.add_argument("--write-cache", action="store_true", help="Write review and guidance packs to runtime cache.")
    parser.add_argument("--cache-dir", type=Path, default=None, help="Override brain review cache directory.")
    parser.add_argument("--guidance-only", action="store_true", help="Print only the compact AI guidance pack.")
    args = parser.parse_args()
    setup_logging("ERROR")
    print(
        asyncio.run(
            run_brain_review_pack(
                write_cache=args.write_cache,
                cache_dir=args.cache_dir,
                guidance_only=args.guidance_only,
            )
        )
    )


if __name__ == "__main__":
    main()
