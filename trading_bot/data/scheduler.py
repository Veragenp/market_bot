from __future__ import annotations

import logging
import os
import subprocess
import sys
import time
from datetime import datetime, timezone

from config import TRADING_SYMBOLS
from trading_bot.config.settings import (
    ENTRYPOINTS_DIR,
    LIQUIDATIONS_UPDATE_INTERVAL,
    OI_UPDATE_INTERVAL,
    REPO_ROOT,
    TIMEFRAMES_BY_CATEGORY,
)
from trading_bot.data.data_loader import DataLoaderManager
from trading_bot.data.collectors import (
    update_all_futures_data,
    update_binance_ohlcv,
    update_indices,
    update_yfinance_macro_all,
)
from trading_bot.analytics.level_events import build_level_events
from trading_bot.data.repositories import LevelEventsRepository

logger = logging.getLogger(__name__)


def _all_crypto_symbols() -> list[str]:
    """Спот Binance для планировщика: только `TRADING_SYMBOLS` (см. `trading_bot.config.symbols`)."""
    return list(TRADING_SYMBOLS)


def _run_macro_yfinance_batch() -> None:
    """Макро (Yahoo): все таймфреймы из `TIMEFRAMES_BY_CATEGORY['macro']` — раз в сутки."""
    tfs = list(TIMEFRAMES_BY_CATEGORY["macro"])  # type: ignore[index]
    logger.info("Running macro yfinance batch: timeframes=%s", tfs)
    update_yfinance_macro_all(timeframes=tfs, days_back=None)


def _run_binance_batch(timeframe: str) -> None:
    symbols = _all_crypto_symbols()
    logger.info("Running Binance batch: timeframe=%s symbols=%s", timeframe, len(symbols))
    for symbol in symbols:
        try:
            update_binance_ohlcv(symbol=symbol, timeframe=timeframe)
        except Exception:
            logger.exception("Batch update failed for %s %s", symbol, timeframe)
        time.sleep(0.2)


def _run_level_events_analytics_batch() -> None:
    events = build_level_events()
    n = LevelEventsRepository().save_batch(events)
    logger.info("Level events analytics batch: saved=%s", n)


def _run_rebuild_volume_profile_peaks_batch() -> None:
    """Пересчёт HVN в `price_levels` (нужен для Sheets и level_events)."""
    from trading_bot.scripts.rebuild_volume_profile_peaks_to_db import (
        main as rebuild_volume_profile_peaks_main,
    )

    logger.info("Running rebuild_volume_profile_peaks_to_db batch")
    try:
        rebuild_volume_profile_peaks_main()
    except Exception:
        logger.exception("rebuild_volume_profile_peaks_to_db batch failed")


def _run_human_levels_batch() -> None:
    """Человеческие уровни (D1/W1) → price_levels + лист human_levels в Sheets."""
    flag = os.getenv("SCHEDULER_DISABLE_HUMAN_LEVELS", "").strip().lower()
    if flag in ("1", "true", "yes", "on"):
        logger.info("Human levels batch skipped (SCHEDULER_DISABLE_HUMAN_LEVELS)")
        return
    from trading_bot.analytics.human_levels_batch import run_human_levels_batch_job

    skip_sheets = os.getenv("SCHEDULER_DISABLE_SHEETS_EXPORT", "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
    logger.info("Running human_levels batch")
    try:
        n_sym, df = run_human_levels_batch_job(export_sheets=not skip_sheets)
        logger.info(
            "Human levels batch: symbols_with_zones=%s sheet_rows=%s",
            n_sym,
            len(df),
        )
    except Exception:
        logger.exception("human_levels batch failed")


def _run_export_to_sheets_batch() -> None:
    """Выгрузка в Google Sheets (`trading_bot/entrypoints/export_to_sheets.py`)."""
    flag = os.getenv("SCHEDULER_DISABLE_SHEETS_EXPORT", "").strip().lower()
    if flag in ("1", "true", "yes", "on"):
        logger.info("Sheets export skipped (SCHEDULER_DISABLE_SHEETS_EXPORT is set)")
        return
    script = os.path.join(ENTRYPOINTS_DIR, "export_to_sheets.py")
    if not os.path.isfile(script):
        logger.error("export_to_sheets script not found: %s", script)
        return
    logger.info("Running export_to_sheets")
    try:
        proc = subprocess.run(
            [sys.executable, script],
            cwd=REPO_ROOT,
            check=False,
        )
        if proc.returncode != 0:
            logger.error("export_to_sheets exited with code %s", proc.returncode)
    except Exception:
        logger.exception("export_to_sheets failed")


def _is_sunday_23_utc() -> bool:
    now = datetime.now(timezone.utc)
    # Monday=0, Sunday=6
    return now.weekday() == 6 and now.hour == 23


def _is_month_start_00_utc() -> bool:
    now = datetime.now(timezone.utc)
    return now.day == 1 and now.hour == 0


def run_scheduler_forever() -> None:
    """
    Run update scheduler (основной долгоживущий процесс заливки в БД):
      - 1m: every 15 min
      - 1h: hourly (spot + индексы TradingView)
      - 4h: every 4 hours
      - 1d: daily at 01:00 UTC
      - instruments (Bybit + ATR по spot 1d): daily at 01:30 UTC
      - macro (yfinance): daily at 02:30 UTC
      - volume profile peaks → price_levels: daily at 02:45 UTC
      - human levels (D1/W1) → price_levels + Sheets: daily at 03:00 UTC
      - level events analytics (1m + ATR): daily at 03:10 UTC
      - Google Sheets export: daily at 03:25 UTC (отключить: SCHEDULER_DISABLE_SHEETS_EXPORT=1;
        тогда и human levels без Sheets)
      - 1w: Sunday 23:00 UTC
      - 1M: first day 00:00 UTC
      - Binance futures OI (история): every 4h (collectors; ликвидации — только Bybit WS)
      - Bybit OI / liquidations: по интервалам из settings
    """
    try:
        import schedule  # type: ignore
    except ImportError as exc:
        raise RuntimeError(
            "schedule is required for scheduler. Install with `pip install schedule`."
        ) from exc

    manager = DataLoaderManager()

    schedule.every(15).minutes.do(_run_binance_batch, timeframe="1m")
    schedule.every().hour.do(_run_binance_batch, timeframe="1h")
    # Индексы CRYPTOCAP — TradingView (coingecko_agg отключён).
    schedule.every().hour.do(update_indices)
    schedule.every(4).hours.do(_run_binance_batch, timeframe="4h")
    schedule.every().day.at("01:00").do(_run_binance_batch, timeframe="1d")
    schedule.every().day.at("01:30").do(manager.update_instruments_daily)
    schedule.every().day.at("02:30").do(_run_macro_yfinance_batch)
    schedule.every().day.at("02:45").do(_run_rebuild_volume_profile_peaks_batch)
    schedule.every().day.at("03:00").do(_run_human_levels_batch)
    schedule.every().day.at("03:10").do(_run_level_events_analytics_batch)
    schedule.every().day.at("03:25").do(_run_export_to_sheets_batch)
    schedule.every(4).hours.do(update_all_futures_data)

    # Weekly and monthly rules expressed as hourly guards in UTC.
    schedule.every().hour.do(lambda: _run_binance_batch("1w") if _is_sunday_23_utc() else None)
    schedule.every().hour.do(lambda: _run_binance_batch("1M") if _is_month_start_00_utc() else None)

    # Bybit incremental OI/liq updates (separate from Binance collectors).
    # `schedule` accepts seconds/minutes/hours; our settings are seconds.
    schedule.every(int(OI_UPDATE_INTERVAL / 60)).minutes.do(manager.update_incremental_oi)
    schedule.every(int(LIQUIDATIONS_UPDATE_INTERVAL / 60)).minutes.do(manager.update_liquidations)

    logger.info("Scheduler started")
    while True:
        schedule.run_pending()
        time.sleep(1)
