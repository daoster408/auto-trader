"""Operator maintenance helpers for safe service restarts."""
import argparse
import asyncio

from auto_trader.config.settings import get_settings
from auto_trader.persistence.db import (
    configure_db_path,
    init_db,
    request_planned_maintenance_shutdown,
)
from auto_trader.utils.logging import get_logger, setup_logging

log = get_logger("auto_trader.maintenance")


async def _request_shutdown(args: argparse.Namespace) -> int:
    settings = get_settings()
    setup_logging(settings.log_level)
    configure_db_path(settings.db_path)
    await init_db()
    if not settings.alpaca_paper and not args.allow_live:
        raise RuntimeError(
            "Planned maintenance preserve-position restart in live mode requires --allow-live"
        )
    marker = await request_planned_maintenance_shutdown(
        reason=args.reason,
        ttl_seconds=args.ttl_seconds,
        allow_live=bool(args.allow_live),
    )
    log.warning(
        "planned_maintenance_shutdown_ready",
        reason=marker["planned_maintenance_reason"],
        expires_at=marker["planned_maintenance_expires_at"],
        allow_live=marker["planned_maintenance_allow_live"],
    )
    print(
        "Planned maintenance shutdown armed: "
        f"reason={marker['planned_maintenance_reason']}; "
        f"expires_at={marker['planned_maintenance_expires_at']}; "
        f"allow_live={marker['planned_maintenance_allow_live']}"
    )
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="AUTO-TRADER maintenance helpers")
    subparsers = parser.add_subparsers(dest="command", required=True)

    request = subparsers.add_parser(
        "request-shutdown",
        help="arm a short-lived marker for a normal planned service restart",
    )
    request.add_argument("--reason", default="planned deployment", help="audit reason")
    request.add_argument("--ttl-seconds", type=int, default=300, help="marker TTL, capped at 900 seconds")
    request.add_argument(
        "--allow-live",
        action="store_true",
        help="explicitly allow preserve-position maintenance restart in live mode",
    )
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    if args.command == "request-shutdown":
        raise SystemExit(asyncio.run(_request_shutdown(args)))
    parser.error(f"unknown command: {args.command}")


if __name__ == "__main__":
    main()
