"""Conservative repair for closed-trade exit reasons."""
from __future__ import annotations

import argparse
import asyncio
import json

from auto_trader.config.settings import get_settings
from auto_trader.persistence.db import backfill_exit_reasons_from_journal, configure_db_path
from auto_trader.utils.logging import setup_logging


async def run_exit_reason_repair(*, apply: bool = False, limit: int | None = None) -> dict[str, object]:
    settings = get_settings()
    configure_db_path(getattr(settings, "db_path", "auto_trader.db"))
    return await backfill_exit_reasons_from_journal(dry_run=not apply, limit=limit)


def main() -> None:
    parser = argparse.ArgumentParser(description="Repair administrative exit reasons from proven journal order-id matches.")
    parser.add_argument("--apply", action="store_true", help="Apply provable repairs. Default is dry-run.")
    parser.add_argument("--limit", type=int, default=None, help="Limit journal entries scanned.")
    args = parser.parse_args()
    setup_logging("ERROR")
    print(json.dumps(asyncio.run(run_exit_reason_repair(apply=args.apply, limit=args.limit)), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
