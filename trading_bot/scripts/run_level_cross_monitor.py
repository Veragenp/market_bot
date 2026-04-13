"""
Монитор пересечений LONG/SHORT (tutorial_v3) по `cycle_levels` + entry gate.

  PYTHONPATH=. python -m trading_bot.scripts.run_level_cross_monitor
  PYTHONPATH=. python -m trading_bot.scripts.run_level_cross_monitor --loop

Env: LEVEL_CROSS_POLL_SEC, ALERT_TIMEOUT_MINUTES, MIN_ALERTS_COUNT, MAX_ADDITIONAL_ALERTS,
     LONG_ATR_THRESHOLD_PERCENT, SHORT_ATR_THRESHOLD_PERCENT, LEVEL_CROSS_TELEGRAM
"""

from __future__ import annotations

import argparse
import logging
import sys
import time

_REPO = __import__("os").path.abspath(__import__("os").path.join(__import__("os").path.dirname(__file__), "..", ".."))
if _REPO not in sys.path:
    sys.path.insert(0, _REPO)

from trading_bot.analytics.entry_gate import process_v3_signal
from trading_bot.analytics.level_cross_monitor import (
    get_level_cross_monitor,
    load_cycle_level_pairs,
    run_level_cross_tick,
)
from trading_bot.config import settings as st
from trading_bot.data.db import get_connection
from trading_bot.data.schema import init_db, run_migrations
from trading_bot.tools.price_feed import get_price_feed

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _fetch_prices(cur) -> dict:
    row = cur.execute("SELECT cycle_id, levels_frozen FROM trading_state WHERE id = 1").fetchone()
    if not row or not row["cycle_id"] or not int(row["levels_frozen"] or 0):
        return {}
    pairs = load_cycle_level_pairs(cur, str(row["cycle_id"]))
    if not pairs:
        return {}
    syms = list(pairs.keys())
    raw = get_price_feed().get_prices(syms)
    return {s: float(pp.price) for s, pp in raw.items()}


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--loop", action="store_true")
    args = p.parse_args()

    init_db()
    run_migrations()
    mon = get_level_cross_monitor()

    def _once() -> None:
        conn = get_connection()
        cur = conn.cursor()
        try:
            prices = _fetch_prices(cur)
            signals, summary = run_level_cross_tick(cur, prices=prices, monitor=mon)
            for sig in signals:
                r = process_v3_signal(cur, signal_type=sig, monitor=mon, prices=prices)
                logger.info("entry_gate %s -> %s", sig, r)
            conn.commit()
            logger.info("level_cross %s", summary)
        finally:
            conn.close()

    if args.loop:
        while True:
            _once()
            time.sleep(float(st.LEVEL_CROSS_POLL_SEC))
    else:
        _once()


if __name__ == "__main__":
    main()
