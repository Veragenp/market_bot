from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

from config import ANALYTIC_SYMBOLS, TRADING_SYMBOLS
from trading_bot.config.settings import (
    LIQUIDATIONS_UPDATE_INTERVAL,
    OI_UPDATE_INTERVAL,
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
    return sorted(set(TRADING_SYMBOLS + ANALYTIC_SYMBOLS.get("crypto", [])))


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
      - level events analytics (1m + ATR): daily at 03:10 UTC
      - 1w: Sunday 23:00 UTC
      - 1M: first day 00:00 UTC
      - macro (yfinance): daily at 02:30 UTC
      - Binance futures OI/liq: every 4h (collectors)
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
    schedule.every().day.at("03:10").do(_run_level_events_analytics_batch)
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
