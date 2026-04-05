"""
Пакетный расчёт HTF-уровней объёма (`volume_profile_htf`) и опциональная выгрузка в Google Sheets.

Запуск из корня репозитория:
  PYTHONPATH=. python -m trading_bot.scripts.run_htf_volume_levels
  PYTHONPATH=. python -m trading_bot.scripts.run_htf_volume_levels --lookback-days 180 --timeframe 1M
"""

from __future__ import annotations

import argparse
import logging

from trading_bot.analytics.htf_volume_levels import (
    check_htf_ohlcv_coverage,
    export_htf_levels_to_sheets,
    run_htf_volume_levels_batch,
)
from trading_bot.config.settings import (
    HTF_LEVELS_LOOKBACK_DAYS,
    HTF_LEVELS_MIN_BARS,
    HTF_LEVELS_TIMEFRAME,
)
from trading_bot.data.volume_profile_peaks_db import LEVEL_TYPE_VOLUME_PROFILE_HTF_4H_90D


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    log = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(description="HTF volume profile levels → price_levels + Sheets")
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=None,
        help="Окно в днях (по умолчанию HTF_LEVELS_LOOKBACK_DAYS, обычно 365)",
    )
    parser.add_argument(
        "--timeframe",
        type=str,
        default=None,
        help="Таймфрейм ohlcv, например 1d, 1w или 1M (по умолчанию HTF_LEVELS_TIMEFRAME)",
    )
    parser.add_argument(
        "--no-sheets",
        action="store_true",
        help="Только SQLite, без Google Sheets",
    )
    parser.add_argument(
        "--check-data",
        action="store_true",
        help="Только проверка наличия OHLCV в окне HTF (без расчёта уровней)",
    )
    parser.add_argument(
        "--level-type",
        type=str,
        default=None,
        help="Тип в price_levels (по умолчанию volume_profile_htf). Эксперимент 4h/3мес: "
        + repr(LEVEL_TYPE_VOLUME_PROFILE_HTF_4H_90D),
    )
    parser.add_argument(
        "--worksheet",
        type=str,
        default=None,
        help="Имя листа Google Sheets (по умолчанию HTF_LEVELS_SHEET_WORKSHEET)",
    )
    args = parser.parse_args()

    batch_kw: dict = {}
    if args.lookback_days is not None:
        batch_kw["lookback_days"] = args.lookback_days
    if args.timeframe is not None:
        batch_kw["timeframe"] = args.timeframe
    if args.level_type is not None:
        batch_kw["level_type"] = args.level_type

    if args.check_data:
        rows = check_htf_ohlcv_coverage(**batch_kw)
        ok_n = sum(1 for r in rows if r["ok"])
        log.info(
            "HTF data check: symbols_ok=%s / %s (tf=%s, lookback_days=%s)",
            ok_n,
            len(rows),
            batch_kw.get("timeframe") or HTF_LEVELS_TIMEFRAME,
            batch_kw.get("lookback_days") or HTF_LEVELS_LOOKBACK_DAYS,
        )
        for r in rows:
            if not r["ok"]:
                extra = ""
                if r["rows_any_source"] > 0 and r["rows_with_source"] == 0:
                    extra = "  <-- есть строки без совпадения source (ожидали %r)" % (r["expected_source"],)
                log.warning(
                    "  %s: with_source=%s any_source=%s min_bars=%s%s",
                    r["symbol"],
                    r["rows_with_source"],
                    r["rows_any_source"],
                    HTF_LEVELS_MIN_BARS,
                    extra,
                )
            else:
                log.info(
                    "  %s: rows=%s source=%r OK",
                    r["symbol"],
                    r["rows_with_source"],
                    r["expected_source"],
                )
        return

    n_sym, df = run_htf_volume_levels_batch(**batch_kw)
    log.info("HTF batch: symbols_with_saved_levels=%s total_level_rows=%s", n_sym, len(df))

    if args.no_sheets:
        log.info("Skipping Sheets (--no-sheets)")
    else:
        export_htf_levels_to_sheets(df, worksheet_name=args.worksheet)


if __name__ == "__main__":
    main()
