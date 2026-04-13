"""
Инкрементальное обновление spot OHLCV 1m (Binance) для всех TRADING_SYMBOLS.

Тот же смысл, что батч 1m в планировщике (`schedule.every(15).minutes`, `update_binance_ohlcv`).
Нужно для свежего `STRUCTURAL_REF_PRICE_SOURCE=db_1m_close`, vp_local и прочих якорей от 1m.

  PYTHONPATH=. python -m trading_bot.scripts.sync_binance_1m_spot
  PYTHONPATH=. python -m trading_bot.scripts.sync_binance_1m_spot --symbol BTC/USDT
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from trading_bot.config.symbols import TRADING_SYMBOLS
from trading_bot.data.collectors import update_binance_ohlcv
from trading_bot.data.schema import init_db, run_migrations

logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--symbol",
        type=str,
        default="",
        help="Одна пара; по умолчанию все TRADING_SYMBOLS",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    init_db()
    run_migrations()
    symbols = [args.symbol.strip()] if args.symbol.strip() else list(TRADING_SYMBOLS)
    for sym in symbols:
        try:
            update_binance_ohlcv(symbol=sym, timeframe="1m")
        except Exception:
            logger.exception("1m update failed for %s", sym)
        time.sleep(0.2)
    logger.info("Done: %s symbols", len(symbols))


if __name__ == "__main__":
    main()
