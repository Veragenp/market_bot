"""Daily update for Bybit `instruments` (tick size, min qty, turnover24h, fees optional)."""

from __future__ import annotations

import logging

from trading_bot.data.data_loader import DataLoaderManager


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    manager = DataLoaderManager()
    updated = manager.update_instruments_full()
    logging.getLogger(__name__).info("Instruments update done. updated=%s", updated)


if __name__ == "__main__":
    main()

