"""Запуск structural-пайплайна: расчёт пула → immediate freeze в cycle_levels + trading_state.

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

from trading_bot.data.structural_cycle_db import run_structural_pipeline


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--no-freeze",
        action="store_true",
        help="Только structural scan, без записи freeze в cycle_levels/trading_state.",
    )
    args = parser.parse_args()
    r = run_structural_pipeline(auto_freeze=not args.no_freeze)
    print(json.dumps(r, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
