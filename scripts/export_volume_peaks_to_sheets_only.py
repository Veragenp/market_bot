"""
Только уровни volume profile + лист анализа (без остального аудита маркет-данных).

  python scripts/export_volume_peaks_to_sheets_only.py

Переменные окружения — как в export_to_sheets.py (credentials, MARKET_AUDIT_SHEET_*,
DYNAMIC_ZONES_SYMBOL, DYNAMIC_ZONES_YEAR/MONTH, PRO_LEVELS_*).
"""

from __future__ import annotations

import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
_SCRIPTS = os.path.join(PROJECT_ROOT, "scripts")
if _SCRIPTS not in sys.path:
    sys.path.insert(0, _SCRIPTS)

from trading_bot.data.schema import init_db
from trading_bot.tools.sheets_exporter import SheetsExporter

import export_to_sheets as es  # noqa: E402


def main() -> None:
    init_db()
    exporter = SheetsExporter(
        credentials_path=es.CREDENTIALS_PATH,
        spreadsheet_title=es.SHEET_TITLE,
        spreadsheet_url=es.SHEET_URL,
        spreadsheet_id=es.SHEET_ID,
    )
    peak_symbols = es.resolve_volume_peak_export_symbols()
    df_levels, df_audit = es._fetch_volume_peak_levels_for_sheet(peak_symbols)
    exporter.export_dataframe_to_sheet(df_levels, es.SHEET_TITLE, es.VOLUME_PEAK_LEVELS_WORKSHEET)
    df_analysis = es._build_volume_peaks_analysis_sheet(df_audit)
    exporter.export_dataframe_to_sheet(
        df_analysis, es.SHEET_TITLE, es.VOLUME_PEAK_ANALYSIS_WORKSHEET
    )
    print(
        f"OK: {es.VOLUME_PEAK_LEVELS_WORKSHEET} rows={len(df_levels)}, "
        f"{es.VOLUME_PEAK_ANALYSIS_WORKSHEET} rows={len(df_analysis)}"
    )


if __name__ == "__main__":
    main()
