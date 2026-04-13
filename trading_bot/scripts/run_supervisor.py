"""
Единый supervisor-цикл: data refresh -> levels rebuild -> structural -> entry detector.

Перед structural (если SUPERVISOR_EXPORT_VP_LOCAL_BEFORE_STRUCTURAL): выгрузка vp_local из БД
в Google Sheet (те же листы, что export_volume_peaks_to_sheets_only), для сверки перед отбором.

Запуск:
  PYTHONPATH=. python -m trading_bot.scripts.run_supervisor --loop
  PYTHONPATH=. python -m trading_bot.scripts.run_supervisor

По умолчанию использует интервалы SUPERVISOR_* из settings/.env.

Логи: trading_bot/logs/supervisor_<локальное время>_<pid>.log (новый файл на каждый запуск) и stderr.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import datetime
from typing import Dict

_REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _REPO not in sys.path:
    sys.path.insert(0, _REPO)

from trading_bot.analytics.entry_detector import run_entry_detector_tick
from trading_bot.config import settings as st
from trading_bot.config.symbols import ANALYTIC_SYMBOLS, TRADING_SYMBOLS, crypto_context_binance_spot_not_in_trading
from trading_bot.data.collectors import update_indices
from trading_bot.data.data_loader import DataLoaderManager
from trading_bot.data.db import get_connection
from trading_bot.data.ops_stage import record_stage_event
from trading_bot.data.schema import init_db, run_migrations
from trading_bot.data.structural_cycle_db import run_structural_realtime_cycle
from trading_bot.entrypoints.export_volume_peaks_to_sheets_only import main as export_vp_to_sheets_main
from trading_bot.scripts.rebuild_volume_profile_peaks_to_db import main as rebuild_vp_local_main

logger = logging.getLogger(__name__)


def _should_skip_scheduled_structural() -> tuple[bool, str]:
    """
    Пока зафиксированы уровни и цикл не closed — полный structural не нужен (не сбрасывать V3-окно и freeze).
    """
    if os.getenv("PYTEST_CURRENT_TEST"):
        return False, ""
    if not st.SUPERVISOR_STRUCTURAL_SKIP_WHEN_CYCLE_ACTIVE:
        return False, ""
    conn = get_connection()
    try:
        cur = conn.cursor()
        row = cur.execute(
            "SELECT levels_frozen, cycle_phase FROM trading_state WHERE id = 1"
        ).fetchone()
        if not row:
            return False, ""
        frozen = int(row["levels_frozen"] or 0)
        phase = str(row["cycle_phase"] or "arming")
        if frozen and phase in ("arming", "in_position"):
            return True, f"active_trading_cycle phase={phase} levels_frozen=1"
        return False, ""
    finally:
        conn.close()


def _setup_supervisor_logging() -> str:
    """Новый UTF-8 лог-файл на каждый запуск + дублирование в stderr."""
    log_dir = os.path.join(_REPO, "trading_bot", "logs")
    os.makedirs(log_dir, exist_ok=True)
    ts = datetime.now().astimezone().strftime("%Y%m%d_%H%M%S")
    log_path = os.path.join(log_dir, f"supervisor_{ts}_{os.getpid()}.log")
    fmt = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
    formatter = logging.Formatter(fmt)
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(logging.INFO)
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(formatter)
    sh = logging.StreamHandler(sys.stderr)
    sh.setLevel(logging.INFO)
    sh.setFormatter(formatter)
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


def _stage_event(**kwargs) -> None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        record_stage_event(cur, **kwargs)
        conn.commit()
    finally:
        conn.close()


def _run_data_refresh() -> Dict[str, int]:
    run_id = _cycle_id()
    start = int(time.time())
    _stage_event(
        stage="DATA_REFRESH",
        status="started",
        cycle_id=run_id,
        run_id=run_id,
        message="Supervisor data refresh started",
        started_at=start,
    )
    mgr = DataLoaderManager()
    symbols_spot = list(TRADING_SYMBOLS)
    extra = crypto_context_binance_spot_not_in_trading()
    mgr.update_incremental_spot(symbols=symbols_spot)
    if extra:
        mgr.update_incremental_spot(symbols=extra)
    mgr.update_incremental_macro(symbols=list(ANALYTIC_SYMBOLS.get("macro", [])))
    update_indices()
    mgr.update_incremental_oi(symbols=symbols_spot)
    mgr.update_instruments_for_symbols(symbols_spot)
    mgr.update_instruments_atr_for_trading_symbols()
    end = int(time.time())
    out = {"spot_symbols": len(symbols_spot), "spot_extra_symbols": len(extra)}
    _stage_event(
        stage="DATA_REFRESH",
        status="ok",
        cycle_id=run_id,
        run_id=run_id,
        message="Supervisor data refresh completed",
        details=out,
        started_at=start,
        finished_at=end,
    )
    return out


def _run_levels_rebuild() -> Dict[str, int]:
    run_id = _cycle_id()
    start = int(time.time())
    _stage_event(
        stage="LEVELS_REBUILD",
        status="started",
        cycle_id=run_id,
        run_id=run_id,
        message="Supervisor levels rebuild started",
        started_at=start,
    )
    rebuild_vp_local_main()
    if st.OPS_STAGE_SHEETS and not os.getenv("PYTEST_CURRENT_TEST"):
        export_vp_to_sheets_main()
    end = int(time.time())
    out = {"vp_local_rebuild": 1}
    _stage_event(
        stage="LEVELS_REBUILD",
        status="ok",
        cycle_id=run_id,
        run_id=run_id,
        message="Supervisor levels rebuild completed",
        details=out,
        started_at=start,
        finished_at=end,
    )
    return out


def _run_structural() -> Dict[str, object]:
    run_id = _cycle_id()
    start = int(time.time())
    _stage_event(
        stage="STRUCTURAL_RUN",
        status="started",
        cycle_id=run_id,
        run_id=run_id,
        message="Supervisor structural cycle started",
        started_at=start,
    )
    if st.SUPERVISOR_EXPORT_VP_LOCAL_BEFORE_STRUCTURAL and not os.getenv("PYTEST_CURRENT_TEST"):
        try:
            export_vp_to_sheets_main()
        except Exception:
            logger.exception(
                "Supervisor: vp_local_levels → Sheets before structural failed (credentials / network?)"
            )
    out = run_structural_realtime_cycle(force_freeze=True)
    end = int(time.time())
    status = "ok" if out.get("phase") in ("armed", "completed") else "failed"
    sev = None if status == "ok" else "error"
    _stage_event(
        stage="STRUCTURAL_RUN",
        status=status,
        severity=sev,
        cycle_id=str(out.get("structural_cycle_id") or run_id or ""),
        run_id=str(out.get("structural_cycle_id") or run_id or ""),
        message="Supervisor structural cycle finished",
        details=out,
        started_at=start,
        finished_at=end,
    )
    return out


def _run_entry_tick() -> Dict[str, object]:
    run_id = _cycle_id()
    start = int(time.time())
    _stage_event(
        stage="ENTRY_TICK",
        status="started",
        cycle_id=run_id,
        run_id=run_id,
        message="Supervisor entry tick started",
        started_at=start,
    )
    out = run_entry_detector_tick()
    end = int(time.time())
    sev = "error" if any((not g.get("ok", True)) for g in out.get("gate", [])) else None
    status = "failed" if sev else "ok"
    _stage_event(
        stage="ENTRY_TICK",
        status=status,
        severity=sev,
        cycle_id=run_id,
        run_id=run_id,
        message="Supervisor entry tick completed",
        details=out,
        started_at=start,
        finished_at=end,
    )
    return out


def run_supervisor_once() -> Dict[str, object]:
    init_db()
    run_migrations()
    skip, reason = _should_skip_scheduled_structural()
    structural_out: Dict[str, object]
    if skip:
        logger.info("Supervisor once: structural skipped (%s)", reason)
        structural_out = {"skipped": True, "reason": reason}
    else:
        structural_out = _run_structural()
    return {
        "data": _run_data_refresh(),
        "levels": _run_levels_rebuild(),
        "structural": structural_out,
        "entry": _run_entry_tick(),
    }


def run_supervisor_loop() -> None:
    init_db()
    run_migrations()
    last_data = 0
    last_levels = 0
    last_structural = 0
    last_entry = 0
    logger.info(
        "Supervisor loop started: data=%ss levels=%ss structural=%ss entry=%ss "
        "(structural skip if cycle active=%s)",
        st.SUPERVISOR_DATA_REFRESH_SEC,
        st.SUPERVISOR_LEVELS_REBUILD_SEC,
        st.SUPERVISOR_STRUCTURAL_SEC,
        st.SUPERVISOR_ENTRY_TICK_SEC,
        st.SUPERVISOR_STRUCTURAL_SKIP_WHEN_CYCLE_ACTIVE,
    )
    while True:
        now = int(time.time())
        try:
            if now - last_data >= int(st.SUPERVISOR_DATA_REFRESH_SEC):
                _run_data_refresh()
                last_data = now
            if now - last_levels >= int(st.SUPERVISOR_LEVELS_REBUILD_SEC):
                _run_levels_rebuild()
                last_levels = now
            if now - last_structural >= int(st.SUPERVISOR_STRUCTURAL_SEC):
                skip, reason = _should_skip_scheduled_structural()
                if skip:
                    logger.info(
                        "Supervisor: structural skipped (%s); next check in %ss",
                        reason,
                        st.SUPERVISOR_STRUCTURAL_RETRY_WHEN_BLOCKED_SEC,
                    )
                    last_structural = now - int(st.SUPERVISOR_STRUCTURAL_SEC) + int(
                        st.SUPERVISOR_STRUCTURAL_RETRY_WHEN_BLOCKED_SEC
                    )
                else:
                    _run_structural()
                    last_structural = now
            if now - last_entry >= int(st.SUPERVISOR_ENTRY_TICK_SEC):
                _run_entry_tick()
                last_entry = now
        except Exception:
            logger.exception("Supervisor loop tick failed")
        time.sleep(max(0.5, float(st.SUPERVISOR_POLL_SEC)))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--loop", action="store_true", help="Run persistent supervisor loop")
    args = parser.parse_args()
    log_path = _setup_supervisor_logging()
    logging.getLogger(__name__).info("Supervisor log file: %s", log_path)
    if args.loop or st.SUPERVISOR_LOOP_ENABLED:
        run_supervisor_loop()
    else:
        print(run_supervisor_once())


if __name__ == "__main__":
    main()

