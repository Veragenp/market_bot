"""
Синхронизация ручных глобальных HVN из Google Sheets → price_levels.

  python trading_bot/entrypoints/sync_manual_global_hvn_from_sheets.py
  python trading_bot/entrypoints/sync_manual_global_hvn_from_sheets.py --dry-run
  python trading_bot/entrypoints/sync_manual_global_hvn_from_sheets.py --symbol BTC/USDT
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from trading_bot.data.manual_global_hvn_sheet_sync import sync_manual_global_hvn_from_sheets

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
log = logging.getLogger(__name__)


def main() -> int:
    p = argparse.ArgumentParser(description="Sync manual global HVN from Google Sheets to SQLite")
    p.add_argument("--dry-run", action="store_true", help="Parse sheets only, no DB writes")
    p.add_argument(
        "--no-ensure-tabs",
        action="store_true",
        help="Do not create missing worksheets (fail if tab missing)",
    )
    p.add_argument(
        "--symbol",
        action="append",
        help="Only this symbol (repeatable); default: TRADING_SYMBOLS + ANALYTIC_SYMBOLS[crypto_context,macro,indices]",
    )
    p.add_argument("--json", action="store_true", help="Print stats as JSON")
    args = p.parse_args()

    symbols = args.symbol if args.symbol else None
    stats = sync_manual_global_hvn_from_sheets(
        symbols=symbols,
        dry_run=args.dry_run,
        ensure_tabs=not args.no_ensure_tabs,
    )
    if args.json:
        print(json.dumps(stats, ensure_ascii=False, indent=2))
    else:
        log.info(
            "Done: inserted=%s updated=%s skipped_rows=%s errors=%s tabs_created=%s",
            stats.get("inserted"),
            stats.get("updated"),
            stats.get("skipped_rows"),
            len(stats.get("errors") or []),
            stats.get("tabs_created"),
        )
        for e in stats.get("errors") or []:
            log.error("%s", e)
    return 0 if not stats.get("errors") else 1


if __name__ == "__main__":
    raise SystemExit(main())
