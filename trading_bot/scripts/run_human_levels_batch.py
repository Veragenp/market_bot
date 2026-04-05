"""
Пакет человеческих уровней (D1 год + W1 три года по умолчанию) → price_levels + Google Sheets.

Из корня репозитория:
  PYTHONPATH=. python -m trading_bot.scripts.run_human_levels_batch
  PYTHONPATH=. python -m trading_bot.scripts.run_human_levels_batch --no-sheets
"""

from __future__ import annotations

import argparse
import logging

from trading_bot.analytics.human_levels_batch import run_human_levels_batch_job
from trading_bot.config.settings import (
    HUMAN_LEVELS_D1_LOOKBACK_DAYS,
    HUMAN_LEVELS_W1_LOOKBACK_DAYS,
)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    log = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(
        description="Human levels (D1/W1) → price_levels + optional Google Sheets"
    )
    parser.add_argument(
        "--no-sheets",
        action="store_true",
        help="Только SQLite, без выгрузки в Google Sheets",
    )
    args = parser.parse_args()

    log.info(
        "Human levels batch: d1_lookback_days=%s w1_lookback_days=%s",
        HUMAN_LEVELS_D1_LOOKBACK_DAYS,
        HUMAN_LEVELS_W1_LOOKBACK_DAYS,
    )
    n_sym, df = run_human_levels_batch_job(export_sheets=not args.no_sheets)
    log.info(
        "Human levels batch done: symbols_with_zones=%s export_rows=%s",
        n_sym,
        len(df),
    )


if __name__ == "__main__":
    main()
