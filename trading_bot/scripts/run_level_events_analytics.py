from __future__ import annotations

import logging

from trading_bot.analytics.level_events import build_level_events
from trading_bot.data.repositories import LevelEventsRepository


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    log = logging.getLogger(__name__)
    events = build_level_events()
    n = LevelEventsRepository().save_batch(events)
    log.info("Level events analytics completed: events=%s", n)


if __name__ == "__main__":
    main()

