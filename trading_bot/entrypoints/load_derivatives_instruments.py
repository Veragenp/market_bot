"""
Только деривативы и инструменты: open_interest, liquidations, instruments.

  python trading_bot/entrypoints/load_derivatives_instruments.py
      Binance REST (только OI), Bybit REST (OI + instruments). Быстро.

  python trading_bot/entrypoints/load_derivatives_instruments.py --bybit-ws-liquidations
      Дополнительно Bybit WebSocket (~до минуты на символ; данные с source=bybit_futures).

Порядок:
  1) Binance futures: OI (публичный REST)
  2) Bybit: история OI за OI_HISTORY_DAYS (публичный REST)
  3) Bybit: instruments для TRADING_SYMBOLS (публичный REST)
  4) Опционально: Bybit ликвидации из WebSocket (исторического REST у Bybit в проекте нет)
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from config import TRADING_SYMBOLS
from trading_bot.data.collectors import update_all_futures_data
from trading_bot.data.data_loader import DataLoaderManager
from trading_bot.data.schema import init_db

logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--bybit-ws-liquidations",
        action="store_true",
        help="Слушать Bybit WebSocket для ликвидаций (долго; нужен pybit)",
    )
    parser.add_argument(
        "--days-back",
        type=int,
        default=90,
        help="Окно для Binance OI (дней)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )

    init_db()
    mgr = DataLoaderManager()

    logger.info("=== Binance futures: open interest ===")
    update_all_futures_data(days_back=args.days_back)
    if not args.bybit_ws_liquidations:
        logger.info(
            "Ликвидации в БД не собирались: для Bybit WS укажите флаг --bybit-ws-liquidations "
            "(как в load_all_data.py после OI)."
        )

    logger.info("=== Bybit: open interest history ===")
    try:
        mgr.load_historical_oi()
    except Exception:
        logger.exception("Bybit OI load failed")

    logger.info("=== Bybit: instruments ===")
    try:
        n = mgr.update_instruments_for_symbols(TRADING_SYMBOLS)
        logger.info("Instruments updated: %s rows", n)
    except Exception:
        logger.exception("Instruments update failed")

    if args.bybit_ws_liquidations:
        logger.info("=== Bybit: liquidations (WebSocket, short window per symbol) ===")
        try:
            mgr.update_liquidations()
        except Exception:
            logger.exception("Bybit liquidations WS failed")
    else:
        logger.info("Skipped Bybit WebSocket (use --bybit-ws-liquidations to enable)")

    logger.info("=== Done ===")


if __name__ == "__main__":
    main()
