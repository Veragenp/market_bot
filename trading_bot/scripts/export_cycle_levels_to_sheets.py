from __future__ import annotations

import os
from datetime import datetime, timezone

from trading_bot.config.settings import (
    CYCLE_LEVELS_CANDIDATES_WORKSHEET,
    CYCLE_LEVELS_DIAG_WORKSHEET,
    CYCLE_LEVELS_WORKSHEET,
)
from trading_bot.data.cycle_levels_db import (
    build_cycle_levels_candidates_df,
    build_cycle_levels_diagnostics,
    fetch_cycle_levels_df,
)
from trading_bot.tools.sheets_exporter import SheetsExporter


def main() -> None:
    credentials_path = os.getenv("GOOGLE_CREDENTIALS_PATH", "credentials.json")
    spreadsheet_title = os.getenv("MARKET_AUDIT_SHEET_TITLE", "Market Data Audit")
    spreadsheet_url = os.getenv("MARKET_AUDIT_SHEET_URL")
    spreadsheet_id = os.getenv("MARKET_AUDIT_SHEET_ID")

    exporter = SheetsExporter(
        credentials_path=credentials_path,
        spreadsheet_title=spreadsheet_title,
        spreadsheet_url=spreadsheet_url,
        spreadsheet_id=spreadsheet_id,
    )
    df = fetch_cycle_levels_df()
    if df.empty:
        df = df.assign(note="cycle_levels is empty")
    df["exported_at_utc"] = datetime.now(timezone.utc).isoformat()
    exporter.export_dataframe_to_sheet(df, spreadsheet_title, CYCLE_LEVELS_WORKSHEET)
    diag = build_cycle_levels_diagnostics()
    diag["exported_at_utc"] = datetime.now(timezone.utc).isoformat()
    exporter.export_dataframe_to_sheet(diag, spreadsheet_title, CYCLE_LEVELS_DIAG_WORKSHEET)
    cands = build_cycle_levels_candidates_df()
    cands["exported_at_utc"] = datetime.now(timezone.utc).isoformat()
    exporter.export_dataframe_to_sheet(cands, spreadsheet_title, CYCLE_LEVELS_CANDIDATES_WORKSHEET)
    print(
        f"OK exported cycle levels: worksheet={CYCLE_LEVELS_WORKSHEET} rows={len(df)}; "
        f"diag={CYCLE_LEVELS_DIAG_WORKSHEET} rows={len(diag)}; "
        f"candidates={CYCLE_LEVELS_CANDIDATES_WORKSHEET} rows={len(cands)}"
    )


if __name__ == "__main__":
    main()

