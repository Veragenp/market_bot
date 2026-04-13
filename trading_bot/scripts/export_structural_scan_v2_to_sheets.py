"""
Экспорт тестового structural v2 отчёта в Google Sheets (без запуска supervisor).

Запуск:
  PYTHONPATH=. python -m trading_bot.scripts.export_structural_scan_v2_to_sheets

Лист: trading_bot.config.settings.STRUCTURAL_LEVELS_REPORT_V2_WORKSHEET
Книга: MARKET_AUDIT_SHEET_* / export_to_sheets.py.
"""

from __future__ import annotations

import json
import os
import sys

_REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _REPO not in sys.path:
    sys.path.insert(0, _REPO)

from trading_bot.config import settings as st
from trading_bot.data.structural_ops_notify import export_levels_snapshot_v2


def main() -> None:
    if not st.STRUCTURAL_OPS_SHEETS_LEVELS:
        print(
            json.dumps(
                {
                    "ok": False,
                    "error": "STRUCTURAL_OPS_SHEETS_LEVELS is disabled",
                    "worksheet": st.STRUCTURAL_LEVELS_REPORT_V2_WORKSHEET,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return
    export_levels_snapshot_v2()
    print(
        json.dumps(
            {
                "ok": True,
                "worksheet": st.STRUCTURAL_LEVELS_REPORT_V2_WORKSHEET,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
