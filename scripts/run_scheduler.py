"""Совместимость: делегирует в trading_bot/entrypoints/run_scheduler.py."""
from __future__ import annotations

import os
import runpy
import sys

_REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO not in sys.path:
    sys.path.insert(0, _REPO)
runpy.run_path(
    os.path.join(_REPO, "trading_bot", "entrypoints", "run_scheduler.py"),
    run_name="__main__",
)
