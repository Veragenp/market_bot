"""
Только уровни volume profile в Google Sheets.

Это тонкая обёртка над legacy `scripts/export_volume_peaks_to_sheets_only.py`,
чтобы Sheets-логика была доступна в `trading_bot/google_sheets/`.
"""

from __future__ import annotations

import runpy
import sys
from pathlib import Path

MARKET_BOT_ROOT = Path(__file__).resolve().parents[2]
if str(MARKET_BOT_ROOT) not in sys.path:
    sys.path.insert(0, str(MARKET_BOT_ROOT))

_ORIG = MARKET_BOT_ROOT / "scripts" / "export_volume_peaks_to_sheets_only.py"
_globals = runpy.run_path(str(_ORIG), run_name="export_volume_peaks_to_sheets_only_orig")
main = _globals.get("main")

if __name__ == "__main__":
    if callable(main):
        main()
    else:
        raise RuntimeError("main() not found in original export_volume_peaks_to_sheets_only.py")

