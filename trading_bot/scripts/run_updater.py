from __future__ import annotations

import logging
import time

from trading_bot.config.settings import LIQUIDATIONS_UPDATE_INTERVAL, OI_UPDATE_INTERVAL
from trading_bot.data.data_loader import DataLoaderManager


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )

    manager = DataLoaderManager()

    next_oi_ts = time.time()
    next_liq_ts = time.time()

    while True:
        now = time.time()

        if now >= next_oi_ts:
            try:
                manager.update_incremental_oi()
            except Exception:
                logging.getLogger(__name__).exception("OI incremental update failed")
            next_oi_ts = now + float(OI_UPDATE_INTERVAL)

        if now >= next_liq_ts:
            try:
                manager.update_liquidations()
            except Exception:
                logging.getLogger(__name__).exception("Liquidations update failed")
            next_liq_ts = now + float(LIQUIDATIONS_UPDATE_INTERVAL)

        time.sleep(1)


if __name__ == "__main__":
    main()

