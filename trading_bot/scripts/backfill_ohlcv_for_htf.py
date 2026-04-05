"""
Догрузка OHLCV в SQLite для расчёта HTF-уровней (`volume_profile_htf`).

По умолчанию:
  - спот Binance: указанные ТФ (дефолт только `1d`);
  - макро Yahoo: `1d`, `1w`, `1M` (без `4h`, чтобы не тянуть историю часовиков через Yahoo — лимит ~730 дней);
  - индексы CRYPTOCAP: `update_indices()` (все ТФ из конфига).

Запуск из корня репозитория:
  PYTHONPATH=. python -m trading_bot.scripts.backfill_ohlcv_for_htf
  PYTHONPATH=. python -m trading_bot.scripts.backfill_ohlcv_for_htf --spot-timeframes 1d,4h,1w
"""

from __future__ import annotations

import argparse
import logging
import time

from config import TRADING_SYMBOLS
from trading_bot.data.collectors import update_binance_ohlcv, update_indices, update_yfinance_macro_all
from trading_bot.data.schema import init_db

logger = logging.getLogger(__name__)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    p = argparse.ArgumentParser(description="Backfill ohlcv for HTF volume levels")
    p.add_argument(
        "--spot-timeframes",
        type=str,
        default="1d",
        help="Список ТФ для спота через запятую (например 1d или 1d,4h,1w,1M)",
    )
    p.add_argument(
        "--macro-timeframes",
        type=str,
        default="1d,1w,1M",
        help="Макро Yahoo через запятую (рекомендуется без 4h для длинной истории)",
    )
    p.add_argument("--skip-spot", action="store_true")
    p.add_argument("--skip-macro", action="store_true")
    p.add_argument("--skip-indices", action="store_true")
    args = p.parse_args()

    init_db()

    spot_tfs = [x.strip() for x in args.spot_timeframes.split(",") if x.strip()]

    if not args.skip_spot:
        logger.info("=== Binance spot: timeframes=%s symbols=%s ===", spot_tfs, len(TRADING_SYMBOLS))
        for symbol in TRADING_SYMBOLS:
            for tf in spot_tfs:
                try:
                    update_binance_ohlcv(symbol=symbol, timeframe=tf)
                except Exception:
                    logger.exception("Spot update failed %s %s", symbol, tf)
                time.sleep(0.2)

    if not args.skip_macro:
        macro_tfs = [x.strip() for x in args.macro_timeframes.split(",") if x.strip()]
        logger.info("=== Macro yfinance: timeframes=%s ===", macro_tfs)
        try:
            update_yfinance_macro_all(timeframes=macro_tfs, days_back=None)
        except Exception:
            logger.exception("Macro batch failed")

    if not args.skip_indices:
        logger.info("=== TradingView indices ===")
        try:
            update_indices(days_back=None)
        except Exception:
            logger.exception("Indices batch failed")

    logger.info("=== backfill_ohlcv_for_htf done ===")


if __name__ == "__main__":
    main()
