"""
Один тик level_cross_monitor (tutorial V3) + entry_gate — см. `run_level_cross_monitor`.

  PYTHONPATH=. python -m trading_bot.scripts.run_entry_detector
  PYTHONPATH=. python -m trading_bot.scripts.run_entry_detector --loop

Интервал цикла: LEVEL_CROSS_POLL_SEC (или legacy ENTRY_DETECTOR_POLL_SEC).
"""

from __future__ import annotations

import argparse
import logging
import sys
import time

_REPO = __import__("os").path.abspath(__import__("os").path.join(__import__("os").path.dirname(__file__), "..", ".."))
if _REPO not in sys.path:
    sys.path.insert(0, _REPO)

from trading_bot.analytics.entry_detector import run_entry_detector_tick
from trading_bot.config import settings as st

_POLL = max(float(st.LEVEL_CROSS_POLL_SEC), float(st.ENTRY_DETECTOR_POLL_SEC))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--loop", action="store_true", help="Крутить опрос (LEVEL_CROSS_POLL_SEC)")
    args = p.parse_args()

    if args.loop:
        while True:
            r = run_entry_detector_tick()
            logger.info("%s", r)
            time.sleep(_POLL)
    else:
        r = run_entry_detector_tick()
        print(r)


if __name__ == "__main__":
    main()
