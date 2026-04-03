"""
Entry-point для экспорта в Google Sheets.

Это тонкая обёртка над legacy `scripts/export_to_sheets.py`, чтобы код “Sheets-логики”
лежал в отдельной папке `trading_bot/google_sheets/`.

Запуск:
  python trading_bot/google_sheets/export_to_sheets.py
"""

from __future__ import annotations

import runpy
import sys
from pathlib import Path

MARKET_BOT_ROOT = Path(__file__).resolve().parents[2]
if str(MARKET_BOT_ROOT) not in sys.path:
    sys.path.insert(0, str(MARKET_BOT_ROOT))

_ORIG = MARKET_BOT_ROOT / "scripts" / "export_to_sheets.py"
_globals = runpy.run_path(str(_ORIG), run_name="export_to_sheets_orig")
main = _globals.get("main")

if __name__ == "__main__":
    if callable(main):
        main()
    else:
        raise RuntimeError("main() not found in original export_to_sheets.py")

