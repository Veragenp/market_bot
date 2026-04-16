"""
Fast supervisor entrypoint for quick structural/entry checks.

Goal:
- keep main supervisor untouched;
- run the same core modules in one pass;
- allow skipping heavy steps to avoid long waits.

Usage:
  PYTHONPATH=. python -m trading_bot.scripts.run_supervisor_fast

Env toggles (all optional):
  FAST_RUN_DATA_REFRESH=0/1      (default: 0)
  FAST_RUN_LEVELS_REBUILD=0/1    (default: 0)
  FAST_RUN_STRUCTURAL=0/1        (default: 1)
  FAST_RUN_ENTRY_TICK=0/1        (default: 1)
  FAST_USE_ALL_TRADING_SYMBOLS=0/1 (default: 1)
  FAST_SYMBOLS=BTC/USDT,ETH/USDT (used only when FAST_USE_ALL_TRADING_SYMBOLS=0)
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import datetime
from typing import Dict, List

_REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _REPO not in sys.path:
    sys.path.insert(0, _REPO)

from trading_bot.analytics.entry_detector import run_entry_detector_tick
from trading_bot.config import settings as st
from trading_bot.config.symbols import ANALYTIC_SYMBOLS, TRADING_SYMBOLS
from trading_bot.data.collectors import update_indices
from trading_bot.data.cycle_levels_db import export_cycle_levels_sheets_snapshot
from trading_bot.data.data_loader import DataLoaderManager
from trading_bot.data.db import get_connection
from trading_bot.data.ops_stage import record_stage_event
from trading_bot.data.schema import init_db, run_migrations
from trading_bot.data.structural_cycle_db import run_structural_pipeline
from trading_bot.entrypoints.export_volume_peaks_to_sheets_only import main as export_vp_to_sheets_main
from trading_bot.scripts.rebuild_volume_profile_peaks_to_db import main as rebuild_vp_local_main

logger = logging.getLogger(__name__)


def _env_bool(name: str, default: bool) -> bool:
    raw = (os.getenv(name, str(int(default))) or "").strip().lower()
    return raw in ("1", "true", "yes", "on")


def _parse_symbols() -> List[str]:
    if _env_bool("FAST_USE_ALL_TRADING_SYMBOLS", True):
        return list(TRADING_SYMBOLS)
    raw = (os.getenv("FAST_SYMBOLS", "") or "").strip()
    if not raw:
        return list(TRADING_SYMBOLS)
    parts = [x.strip() for x in raw.replace(";", ",").split(",")]
    out = [x for x in parts if x]
    return out or list(TRADING_SYMBOLS)


def _setup_logging() -> str:
    log_dir = os.path.join(_REPO, "trading_bot", "logs")
    os.makedirs(log_dir, exist_ok=True)
    ts = datetime.now().astimezone().strftime("%Y%m%d_%H%M%S")
    log_path = os.path.join(log_dir, f"supervisor_fast_{ts}_{os.getpid()}.log")
    fmt = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(logging.INFO)
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(logging.Formatter(fmt))
    sh = logging.StreamHandler(sys.stderr)
    sh.setFormatter(logging.Formatter(fmt))
    root.addHandler(fh)
    root.addHandler(sh)
    return log_path


def _cycle_id() -> str | None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        row = cur.execute("SELECT cycle_id FROM trading_state WHERE id = 1").fetchone()
        return str(row["cycle_id"]) if row and row["cycle_id"] else None
    finally:
        conn.close()


def _stage_event(stage: str, status: str, message: str, details: Dict[str, object] | None = None) -> None:
    cid = _cycle_id()
    conn = get_connection()
    try:
        cur = conn.cursor()
        now = int(time.time())
        record_stage_event(
            cur,
            stage=stage,
            status=status,
            cycle_id=cid,
            run_id=cid,
            message=message,
            details=details or {},
            started_at=now,
            finished_at=now,
        )
        conn.commit()
    finally:
        conn.close()


def _run_data_refresh(symbols: List[str]) -> Dict[str, object]:
    mgr = DataLoaderManager()
    steps: Dict[str, str] = {}
    if st.SUPERVISOR_DATA_REFRESH_SPOT_MAIN:
        mgr.update_incremental_spot(symbols=symbols)
        steps["spot_main"] = "ok"
    else:
        steps["spot_main"] = "skipped"
    if st.SUPERVISOR_DATA_REFRESH_MACRO:
        mgr.update_incremental_macro(symbols=list(ANALYTIC_SYMBOLS.get("macro", [])))
        steps["macro"] = "ok"
    else:
        steps["macro"] = "skipped"
    if st.SUPERVISOR_DATA_REFRESH_INDICES_TV:
        update_indices()
        steps["indices_tv"] = "ok"
    else:
        steps["indices_tv"] = "skipped"
    if st.SUPERVISOR_DATA_REFRESH_OI_BYBIT:
        mgr.update_incremental_oi(symbols=symbols)
        steps["oi_bybit"] = "ok"
    else:
        steps["oi_bybit"] = "skipped"
    if st.SUPERVISOR_DATA_REFRESH_INSTRUMENTS:
        mgr.update_instruments_for_symbols(symbols)
        steps["instruments"] = "ok"
    else:
        steps["instruments"] = "skipped"
    if st.SUPERVISOR_DATA_REFRESH_INSTRUMENTS_ATR:
        # In current project this updater works for tradable universe.
        mgr.update_instruments_atr_for_trading_symbols()
        steps["instruments_atr"] = "ok"
    else:
        steps["instruments_atr"] = "skipped"
    return {"steps": steps, "symbols": len(symbols)}


def run_fast_once() -> Dict[str, object]:
    init_db()
    run_migrations()

    symbols = _parse_symbols()
    out: Dict[str, object] = {"symbols": len(symbols)}
    logger.info("Supervisor FAST: symbols=%s sample=%s", len(symbols), symbols[:5])

    run_data = _env_bool("FAST_RUN_DATA_REFRESH", False)
    run_levels = _env_bool("FAST_RUN_LEVELS_REBUILD", False)
    run_structural = _env_bool("FAST_RUN_STRUCTURAL", True)
    run_entry = _env_bool("FAST_RUN_ENTRY_TICK", True)

    logger.info(
        "Supervisor FAST flags: data_refresh=%s levels_rebuild=%s structural=%s entry_tick=%s",
        run_data,
        run_levels,
        run_structural,
        run_entry,
    )

    if run_data:
        _stage_event("FAST_DATA_REFRESH", "started", "FAST data refresh started")
        data_out = _run_data_refresh(symbols)
        _stage_event("FAST_DATA_REFRESH", "ok", "FAST data refresh completed", data_out)
        out["data"] = data_out
    else:
        out["data"] = {"skipped": True}

    if run_levels:
        _stage_event("FAST_LEVELS_REBUILD", "started", "FAST levels rebuild started")
        rebuild_vp_local_main()
        if st.SUPERVISOR_EXPORT_VP_LOCAL_AFTER_LEVELS_REBUILD:
            try:
                export_vp_to_sheets_main()
            except Exception:
                logger.exception("Supervisor FAST: vp_local export to Sheets failed")
        _stage_event("FAST_LEVELS_REBUILD", "ok", "FAST levels rebuild completed")
        out["levels"] = {"ok": True}
    else:
        out["levels"] = {"skipped": True}

    if run_structural:
        _stage_event("FAST_STRUCTURAL_RUN", "started", "FAST structural started")
        st_out = run_structural_pipeline(auto_freeze=True)
        if st.OPS_STAGE_SHEETS:
            try:
                snap = export_cycle_levels_sheets_snapshot()
                logger.info(
                    "Supervisor FAST: cycle_levels snapshot exported rows=%s diag=%s candidates=%s",
                    snap.get("cycle_levels_rows"),
                    snap.get("diag_rows"),
                    snap.get("candidates_rows"),
                )
            except Exception:
                logger.exception("Supervisor FAST: cycle_levels snapshot export failed")
        _stage_event("FAST_STRUCTURAL_RUN", "ok", "FAST structural completed", st_out)
        out["structural"] = st_out
    else:
        out["structural"] = {"skipped": True}

    if run_entry:
        _stage_event("FAST_ENTRY_TICK", "started", "FAST entry tick started")
        en_out = run_entry_detector_tick()
        _stage_event("FAST_ENTRY_TICK", "ok", "FAST entry tick completed", en_out)
        out["entry"] = en_out
    else:
        out["entry"] = {"skipped": True}

    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.parse_args()
    log_path = _setup_logging()
    logger.info("Supervisor FAST log file: %s", log_path)
    print(run_fast_once())


if __name__ == "__main__":
    main()

