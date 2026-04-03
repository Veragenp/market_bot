"""Daily update for Bybit `instruments` (tick size, min qty, turnover24h, fees) + ATR (spot 1d)."""

from __future__ import annotations

import logging

from trading_bot.data.data_loader import DataLoaderManager


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    log = logging.getLogger(__name__)
    manager = DataLoaderManager()
    updated = manager.update_instruments_full()
    atr_n = manager.update_instruments_atr_for_trading_symbols()
    log.info("Instruments update done. bybit_rows=%s, atr_updated=%s", updated, atr_n)


if __name__ == "__main__":
    main()

