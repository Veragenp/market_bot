"""
Человеческие уровни (D1/W1) по всем символам из `iter_config_symbols_with_source`:
пакетный job + CSV (без Google Sheets).

  python -m trading_bot.scripts.export_human_levels_to_csv
  python -m trading_bot.scripts.export_human_levels_to_csv -o D:/out/human.csv

Нужны 1d/1w OHLCV в SQLite (spot/macro/indices — см. load_all_data).
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone

import pandas as pd


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-o",
        "--output",
        default="",
        help="Путь к CSV (по умолчанию trading_bot/data/exports/human_levels_<ts>.csv)",
    )
    parser.add_argument(
        "--also-price-levels",
        action="store_true",
        help="Дополнительно выгрузить все строки price_levels с level_type=human",
    )
    args = parser.parse_args()

    os.environ.setdefault("HUMAN_LEVELS_DISABLE_SHEETS", "1")

    _here = os.path.dirname(os.path.abspath(__file__))
    _repo = os.path.dirname(os.path.dirname(_here))  # trading_bot/scripts -> REPO_ROOT
    if _repo not in sys.path:
        sys.path.insert(0, _repo)

    from trading_bot.analytics.human_levels_batch import run_human_levels_batch_job
    from trading_bot.config.settings import DATA_DIR
    from trading_bot.data.db import get_connection
    from trading_bot.data.schema import init_db, run_migrations

    init_db()
    run_migrations()

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_dir = os.path.join(DATA_DIR, "exports")
    os.makedirs(out_dir, exist_ok=True)
    out_batch = args.output or os.path.join(out_dir, f"human_levels_batch_{ts}.csv")

    print("Running run_human_levels_batch_job(export_sheets=False) …")
    n_sym, df = run_human_levels_batch_job(export_sheets=False)
    df.to_csv(out_batch, index=False, encoding="utf-8-sig")
    print(f"Batch rows: {len(df)}, symbols_with_zones: {n_sym}")
    print(f"Wrote: {out_batch}")

    if args.also_price_levels:
        conn = get_connection()
        df_pl = pd.read_sql_query(
            """
            SELECT * FROM price_levels
            WHERE level_type = 'human'
            ORDER BY symbol, created_at
            """,
            conn,
        )
        conn.close()
        out_pl = os.path.join(out_dir, f"price_levels_human_{ts}.csv")
        df_pl.to_csv(out_pl, index=False, encoding="utf-8-sig")
        print(f"price_levels (human): {len(df_pl)} rows -> {out_pl}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
