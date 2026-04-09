"""ATR (Gerchik) → instruments, опционально пересчёт vp_local, rebuild cycle_levels, выгрузка в Sheets.

Запуск из корня репозитория (родитель `trading_bot`):
  PYTHONPATH=. python -m trading_bot.scripts.refresh_atr_rebuild_cycle_export_vp_sheets

Пересчёт VP (долго, все TRADING_SYMBOLS): задайте env `RUN_VP_REBUILD=1`.
По умолчанию vp_local в БД не трогаем — только выгрузка текущих активных уровней.
"""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime, timezone

_REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _REPO not in sys.path:
    sys.path.insert(0, _REPO)

from trading_bot.analytics.atr import atr_gerchik_from_ohlcv_rows
from trading_bot.config.settings import (
    CYCLE_LEVELS_CANDIDATES_WORKSHEET,
    CYCLE_LEVELS_DIAG_WORKSHEET,
    CYCLE_LEVELS_WORKSHEET,
    DEFAULT_SOURCE_BINANCE,
    VOLUME_PEAK_LEVELS_WORKSHEET,
)
from trading_bot.config.symbols import TRADING_SYMBOLS
from trading_bot.data.cycle_levels_db import (
    build_cycle_levels_candidates_df,
    build_cycle_levels_diagnostics,
    fetch_cycle_levels_df,
)
from trading_bot.data.structural_cycle_db import run_structural_realtime_cycle
from trading_bot.data.repositories import get_ohlcv_tail, InstrumentsRepository
from trading_bot.data.schema import init_db
from trading_bot.tools.sheets_exporter import SheetsExporter


def _update_atr_only() -> int:
    init_db()
    repo = InstrumentsRepository()
    n = 0
    for symbol in TRADING_SYMBOLS:
        bybit = symbol.replace("/", "").upper()
        if not repo.get(bybit, "bybit_futures"):
            print("skip no instruments row:", symbol, flush=True)
            continue
        rows = get_ohlcv_tail(symbol, "1d", 400, source=DEFAULT_SOURCE_BINANCE)
        v = atr_gerchik_from_ohlcv_rows(rows)
        if v is None:
            print("skip atr None:", symbol, "bars=", len(rows), flush=True)
            continue
        repo.update_atr(bybit, "bybit_futures", float(v))
        n += 1
        print(symbol, "atr=", float(v), flush=True)
    return n


def main() -> None:
    print("=== 1) instruments.atr (Gerchik, last 10 of tail 1d) ===", flush=True)
    n = _update_atr_only()
    print(f"atr_updated={n}", flush=True)

    if os.getenv("RUN_VP_REBUILD", "").strip().lower() in ("1", "true", "yes", "on"):
        print("=== 2a) rebuild_volume_profile_peaks_to_db (vp_local) ===", flush=True)
        rc = subprocess.run(
            [sys.executable, "-m", "trading_bot.scripts.rebuild_volume_profile_peaks_to_db"],
            cwd=_REPO,
            env={**os.environ, "PYTHONPATH": _REPO},
        )
        print("vp rebuild exit=", rc.returncode, flush=True)

    print("=== 2) run_structural_realtime_cycle (touch_window -> entry_timer -> freeze) ===", flush=True)
    r = run_structural_realtime_cycle()
    print(r, flush=True)

    import trading_bot.entrypoints.export_to_sheets as ets

    exported_at = datetime.now(timezone.utc).isoformat()
    cred = os.getenv("GOOGLE_CREDENTIALS_PATH", ets.CREDENTIALS_PATH)
    title = os.getenv("MARKET_AUDIT_SHEET_TITLE", ets.SHEET_TITLE)
    exporter = SheetsExporter(
        credentials_path=cred,
        spreadsheet_title=title,
        spreadsheet_url=os.getenv("MARKET_AUDIT_SHEET_URL") or ets.SHEET_URL,
        spreadsheet_id=os.getenv("MARKET_AUDIT_SHEET_ID") or ets.SHEET_ID,
    )

    print("=== 3) Sheets: vp_local_levels ===", flush=True)
    peak_symbols = ets.resolve_volume_peak_export_symbols()
    df_vp, _ = ets._fetch_volume_peak_levels_for_sheet(peak_symbols)
    df_vp = df_vp.assign(exported_at_utc=exported_at)
    ws_vp = os.getenv("VOLUME_PEAK_LEVELS_WORKSHEET", VOLUME_PEAK_LEVELS_WORKSHEET)
    exporter.export_dataframe_to_sheet(df_vp, title, ws_vp)
    print(f"vp rows={len(df_vp)} worksheet={ws_vp}", flush=True)

    print("=== 4) Sheets: cycle_levels_v1 / diag / candidates ===", flush=True)
    df = fetch_cycle_levels_df()
    if df.empty:
        df = df.assign(note="cycle_levels is empty")
    df = df.assign(exported_at_utc=exported_at)
    exporter.export_dataframe_to_sheet(df, title, CYCLE_LEVELS_WORKSHEET)

    diag = build_cycle_levels_diagnostics()
    diag = diag.assign(exported_at_utc=exported_at)
    exporter.export_dataframe_to_sheet(diag, title, CYCLE_LEVELS_DIAG_WORKSHEET)

    cands = build_cycle_levels_candidates_df()
    cands = cands.assign(exported_at_utc=exported_at)
    exporter.export_dataframe_to_sheet(cands, title, CYCLE_LEVELS_CANDIDATES_WORKSHEET)

    print(
        f"cycle rows={len(df)} diag={len(diag)} cands={len(cands)} "
        f"worksheets={CYCLE_LEVELS_WORKSHEET},{CYCLE_LEVELS_DIAG_WORKSHEET},{CYCLE_LEVELS_CANDIDATES_WORKSHEET}",
        flush=True,
    )
    print("OK", flush=True)


if __name__ == "__main__":
    main()
