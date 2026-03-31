from __future__ import annotations

import logging

from trading_bot.data.data_loader import DataLoaderManager


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    manager = DataLoaderManager()
    manager.load_historical_oi()


if __name__ == "__main__":
    main()

