from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

from config import ANALYTIC_SYMBOLS, TRADING_SYMBOLS
from trading_bot.config.settings import LIQUIDATIONS_UPDATE_INTERVAL, OI_UPDATE_INTERVAL
from trading_bot.data.data_loader import DataLoaderManager
from trading_bot.data.collectors import (
    update_aggregated_indices,
    update_all_futures_data,
    update_binance_ohlcv,
)

logger = logging.getLogger(__name__)


def _all_crypto_symbols() -> list[str]:
    return sorted(set(TRADING_SYMBOLS + ANALYTIC_SYMBOLS.get("crypto", [])))


def _run_binance_batch(timeframe: str) -> None:
    symbols = _all_crypto_symbols()
    logger.info("Running Binance batch: timeframe=%s symbols=%s", timeframe, len(symbols))
    for symbol in symbols:
        try:
            update_binance_ohlcv(symbol=symbol, timeframe=timeframe)
        except Exception:
            logger.exception("Batch update failed for %s %s", symbol, timeframe)
        time.sleep(0.2)


def _is_sunday_23_utc() -> bool:
    now = datetime.now(timezone.utc)
    # Monday=0, Sunday=6
    return now.weekday() == 6 and now.hour == 23


def _is_month_start_00_utc() -> bool:
    now = datetime.now(timezone.utc)
    return now.day == 1 and now.hour == 0


def run_scheduler_forever() -> None:
    """
    Run update scheduler:
      - 1m: every 15 min
      - 1h: hourly
      - 4h: every 4 hours
      - 1d: daily at 01:00 UTC
      - 1w: Sunday 23:00 UTC
      - 1M: first day 00:00 UTC
    """
    try:
        import schedule  # type: ignore
    except ImportError as exc:
        raise RuntimeError(
            "schedule is required for scheduler. Install with `pip install schedule`."
        ) from exc

    schedule.every(15).minutes.do(_run_binance_batch, timeframe="1m")
    schedule.every().hour.do(_run_binance_batch, timeframe="1h")
    schedule.every().hour.do(update_aggregated_indices)
    schedule.every(4).hours.do(_run_binance_batch, timeframe="4h")
    schedule.every().day.at("01:00").do(_run_binance_batch, timeframe="1d")
    schedule.every(4).hours.do(update_all_futures_data)

    # Weekly and monthly rules expressed as hourly guards in UTC.
    schedule.every().hour.do(lambda: _run_binance_batch("1w") if _is_sunday_23_utc() else None)
    schedule.every().hour.do(lambda: _run_binance_batch("1M") if _is_month_start_00_utc() else None)

    # Bybit incremental OI/liq updates (separate from Binance collectors).
    manager = DataLoaderManager()
    # `schedule` accepts seconds/minutes/hours; our settings are seconds.
    schedule.every(int(OI_UPDATE_INTERVAL / 60)).minutes.do(manager.update_incremental_oi)
    schedule.every(int(LIQUIDATIONS_UPDATE_INTERVAL / 60)).minutes.do(manager.update_liquidations)

    logger.info("Scheduler started")
    while True:
        schedule.run_pending()
        time.sleep(1)
