"""
Полная синхронизация данных в SQLite: spot Binance, макро Yahoo, индексы TradingView,
фьючерсы Bybit (OI, ликвидации), опционально инструменты.

  python scripts/load_all_data.py              # инкрементально (дозаполнить пробелы)
  python scripts/load_all_data.py --full       # история с 2017 + окно 1m (долго)
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from config import TRADING_SYMBOLS
from trading_bot.data.collectors import update_all_futures_data, update_indices
from trading_bot.data.data_loader import DataLoaderManager
from trading_bot.data.schema import init_db

logger = logging.getLogger(__name__)


def _safe(name: str, fn) -> None:
    try:
        fn()
    except Exception:
        logger.exception("%s failed", name)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--full",
        action="store_true",
        help="Полная перезагрузка spot/macro/indices с HISTORY_START (очень долго)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )

    init_db()
    mgr = DataLoaderManager()

    if args.full:
        logger.info("=== Full historical: spot (4h,1d,1w,1M) ===")
        mgr.load_historical_spot(force_full=True)
        logger.info("=== Spot 1m window ===")
        mgr.load_intraday_1m_spot(force_full=True)
        logger.info("=== Macro ===")
        mgr.load_historical_macro(force_full=True)
        logger.info("=== TradingView indices ===")
        mgr.load_historical_tradingview_indices(force_full=True)
    else:
        logger.info("=== Incremental spot ===")
        mgr.update_incremental_spot()
        logger.info("=== Incremental macro ===")
        mgr.update_incremental_macro()
        logger.info("=== TradingView indices (collector) ===")
        update_indices()

    logger.info("=== Bybit futures bundle ===")
    _safe("update_all_futures_data", lambda: update_all_futures_data(days_back=90))

    logger.info("=== Open interest incremental ===")
    _safe("update_incremental_oi", mgr.update_incremental_oi)

    logger.info("=== Liquidations ===")
    _safe("update_liquidations", mgr.update_liquidations)

    logger.info("=== Instruments (Bybit) ===")
    _safe(
        "update_instruments_for_symbols",
        lambda: mgr.update_instruments_for_symbols(TRADING_SYMBOLS),
    )

    logger.info("=== Done ===")


if __name__ == "__main__":
    main()
