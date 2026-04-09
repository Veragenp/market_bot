"""Запуск structural-пайплайна: расчёт пула → при успехе freeze в cycle_levels + trading_state.

  PYTHONPATH=. python -m trading_bot.scripts.run_structural_cycle

Env: см. STRUCTURAL_* в trading_bot.config.settings; CYCLE_LEVELS_ALLOWED_LEVEL_TYPES.
"""

from __future__ import annotations

import json
import os
import sys
import argparse

_REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _REPO not in sys.path:
    sys.path.insert(0, _REPO)

from trading_bot.data.structural_cycle_db import run_structural_pipeline, run_structural_realtime_cycle


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--scan-only",
        action="store_true",
        help="Только structural scan/MAD без realtime touch-window и entry timer.",
    )
    args = parser.parse_args()
    if args.scan_only:
        r = run_structural_pipeline()
    else:
        r = run_structural_realtime_cycle()
    print(json.dumps(r, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
