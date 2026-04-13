"""
Единый supervisor-цикл: data refresh → levels rebuild → structural → entry detector.

Однократный запуск без `--loop` выполняет те же шаги в этом порядке (`run_supervisor_once`).

Перед structural (если SUPERVISOR_EXPORT_VP_LOCAL_BEFORE_STRUCTURAL): выгрузка vp_local из БД
в Google Sheet (те же листы, что export_volume_peaks_to_sheets_only), для сверки перед отбором.

Если SUPERVISOR_EXPORT_STRUCTURAL_LEVELS_REPORT: при пропуске scheduled structural (активный цикл /
freeze) — выгрузка листа structural_levels_report из trading_state.structural_cycle_id
(нужно STRUCTURAL_OPS_SHEETS_LEVELS=1).

Запуск из корня репозитория (рядом с каталогом `trading_bot`):
  PYTHONPATH=. python -m trading_bot.scripts.run_supervisor --loop
  PYTHONPATH=. python -m trading_bot.scripts.run_supervisor
Без флага `--loop`: один проход, если в .env не задан SUPERVISOR_LOOP_ENABLED=1.

По умолчанию использует интервалы SUPERVISOR_* из settings/.env.

Шаги DATA_REFRESH (см. settings.py; по умолчанию макро/TV/OI/crypto_context spot выкл.):
  SUPERVISOR_DATA_REFRESH_SPOT_MAIN, _SPOT_CRYPTO_CONTEXT, _MACRO, _INDICES_TV,
  _OI_BYBIT, _INSTRUMENTS, _INSTRUMENTS_ATR — 0 / false / off отключает, 1 / true / on включает.

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
from trading_bot.data.structural_ops_notify import export_levels_snapshot, export_levels_snapshot_v2
from trading_bot.entrypoints.export_volume_peaks_to_sheets_only import main as export_vp_to_sheets_main
from trading_bot.scripts.rebuild_volume_profile_peaks_to_db import main as rebuild_vp_local_main

logger = logging.getLogger(__name__)


def _log_supervisor_config_banner() -> None:
    """Один раз при старте: интервалы + V3/Bybit/price feed (без секретов)."""
    if os.getenv("PYTEST_CURRENT_TEST"):
        return
    try:
        conn = get_connection()
        cur = conn.cursor()
        ts = cur.execute(
            "SELECT cycle_id, levels_frozen, cycle_phase, "
            "COALESCE(allow_long_entry,1) AS al, COALESCE(allow_short_entry,1) AS ashort "
            "FROM trading_state WHERE id=1"
        ).fetchone()
        conn.close()
    except Exception:
        ts = None
    extra = ""
    if ts:
        extra = (
            f" trading_state cycle_id={ts['cycle_id']!r} frozen={ts['levels_frozen']} "
            f"phase={ts['cycle_phase']} allow_L={ts['al']} allow_S={ts['ashort']}"
        )
    logger.info(
        "Supervisor config: DATA_REFRESH=%ss LEVELS_REBUILD=%ss STRUCTURAL=%ss ENTRY_TICK=%ss "
        "poll=%ss | structural_skip_cycle_active=%s structural_retry_blocked=%ss | "
        "export_structural_levels_when_skipped=%s | "
        "BYBIT_USE_DEMO=%s BYBIT_BASE_URL=%s | LEVEL_CROSS min=%s timeout_min=%s |%s",
        st.SUPERVISOR_DATA_REFRESH_SEC,
        st.SUPERVISOR_LEVELS_REBUILD_SEC,
        st.SUPERVISOR_STRUCTURAL_SEC,
        st.SUPERVISOR_ENTRY_TICK_SEC,
        st.SUPERVISOR_POLL_SEC,
        st.SUPERVISOR_STRUCTURAL_SKIP_WHEN_CYCLE_ACTIVE,
        st.SUPERVISOR_STRUCTURAL_RETRY_WHEN_BLOCKED_SEC,
        int(st.SUPERVISOR_EXPORT_STRUCTURAL_LEVELS_REPORT),
        st.BYBIT_USE_DEMO,
        st.BYBIT_BASE_URL,
        st.LEVEL_CROSS_MIN_ALERTS_COUNT,
        st.LEVEL_CROSS_ALERT_TIMEOUT_MINUTES,
        extra,
    )
    logger.info(
        "Supervisor data_refresh steps (1=on): spot_main=%s spot_ctx=%s macro=%s indices_tv=%s "
        "oi_bybit=%s instruments=%s atr=%s",
        int(st.SUPERVISOR_DATA_REFRESH_SPOT_MAIN),
        int(st.SUPERVISOR_DATA_REFRESH_SPOT_CRYPTO_CONTEXT),
        int(st.SUPERVISOR_DATA_REFRESH_MACRO),
        int(st.SUPERVISOR_DATA_REFRESH_INDICES_TV),
        int(st.SUPERVISOR_DATA_REFRESH_OI_BYBIT),
        int(st.SUPERVISOR_DATA_REFRESH_INSTRUMENTS),
        int(st.SUPERVISOR_DATA_REFRESH_INSTRUMENTS_ATR),
    )


def _export_structural_levels_report_from_state() -> None:
    """
    Выгрузка листа structural_levels_report по последнему structural_cycle_id из trading_state.

    Данные строк (L/U, ref, z_w) читаются из БД; pipeline_out пустой — pool/ref_source подтянутся из structural_cycles.
    """
    if os.getenv("PYTEST_CURRENT_TEST"):
        return
    if not st.SUPERVISOR_EXPORT_STRUCTURAL_LEVELS_REPORT:
        return
    conn = get_connection()
    try:
        cur = conn.cursor()
        row = cur.execute(
            "SELECT structural_cycle_id FROM trading_state WHERE id = 1"
        ).fetchone()
        scid = row["structural_cycle_id"] if row else None
    finally:
        conn.close()
    if not scid:
        logger.debug(
            "Supervisor: structural_levels_report export skipped (no structural_cycle_id in trading_state)"
        )
        return
    try:
        # В отчёт пишем консистентный structural snapshot:
        # ref_price/L/U должны оставаться из одного и того же расчёта structural-cycle.
        export_levels_snapshot(str(scid), {})
        logger.info("Supervisor: structural_levels_report exported cycle_id=%s", str(scid))
    except Exception:
        logger.exception(
            "Supervisor: structural_levels_report → Sheets failed (credentials / network?)"
        )
    if not st.SUPERVISOR_EXPORT_STRUCTURAL_LEVELS_REPORT_V2:
        return
    try:
        export_levels_snapshot_v2()
        logger.info("Supervisor: structural_levels_report_v2 exported")
    except Exception:
        logger.exception(
            "Supervisor: structural_levels_report_v2 → Sheets failed (credentials / network?)"
        )


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
    out: Dict[str, object] = {
        "spot_symbols": len(symbols_spot),
        "spot_extra_symbols": len(extra),
        "steps": {},
    }
    steps: Dict[str, str] = {}

    if st.SUPERVISOR_DATA_REFRESH_SPOT_MAIN:
        mgr.update_incremental_spot(symbols=symbols_spot)
        steps["spot_main"] = "ok"
    else:
        steps["spot_main"] = "skipped"

    if extra and st.SUPERVISOR_DATA_REFRESH_SPOT_CRYPTO_CONTEXT:
        mgr.update_incremental_spot(symbols=extra)
        steps["spot_crypto_context"] = "ok"
    else:
        steps["spot_crypto_context"] = "skipped" if extra else "n/a"

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
        mgr.update_incremental_oi(symbols=symbols_spot)
        steps["oi_bybit"] = "ok"
    else:
        steps["oi_bybit"] = "skipped"

    if st.SUPERVISOR_DATA_REFRESH_INSTRUMENTS:
        mgr.update_instruments_for_symbols(symbols_spot)
        steps["instruments"] = "ok"
    else:
        steps["instruments"] = "skipped"

    if st.SUPERVISOR_DATA_REFRESH_INSTRUMENTS_ATR:
        mgr.update_instruments_atr_for_trading_symbols()
        steps["instruments_atr"] = "ok"
    else:
        steps["instruments_atr"] = "skipped"

    out["steps"] = steps
    logger.info("DATA_REFRESH steps: %s", steps)
    end = int(time.time())
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
    if (
        st.SUPERVISOR_EXPORT_VP_LOCAL_AFTER_LEVELS_REBUILD
        and not os.getenv("PYTEST_CURRENT_TEST")
    ):
        try:
            export_vp_to_sheets_main()
        except Exception:
            logger.exception(
                "Supervisor: vp_local_levels → Sheets after levels rebuild failed (credentials / network?)"
            )
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
    if st.SUPERVISOR_EXPORT_STRUCTURAL_LEVELS_REPORT_V2 and not os.getenv("PYTEST_CURRENT_TEST"):
        try:
            export_levels_snapshot_v2()
            logger.info("Supervisor: structural_levels_report_v2 exported after structural run")
        except Exception:
            logger.exception(
                "Supervisor: structural_levels_report_v2 export after structural run failed"
            )
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
    """
    Один полный проход в том же порядке, что и в `--loop`: данные → уровни → structural → entry.
    (Раньше structural шёл первым и мог опираться на устаревшие instruments/VP в БД.)
    """
    init_db()
    run_migrations()
    data_out = _run_data_refresh()
    levels_out = _run_levels_rebuild()
    skip, reason = _should_skip_scheduled_structural()
    structural_out: Dict[str, object]
    if skip:
        logger.info("Supervisor once: structural skipped (%s)", reason)
        _export_structural_levels_report_from_state()
        structural_out = {"skipped": True, "reason": reason}
    else:
        structural_out = _run_structural()
    entry_out = _run_entry_tick()
    return {
        "data": data_out,
        "levels": levels_out,
        "structural": structural_out,
        "entry": entry_out,
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
                    _export_structural_levels_report_from_state()
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
    _log_supervisor_config_banner()
    if args.loop or st.SUPERVISOR_LOOP_ENABLED:
        run_supervisor_loop()
    else:
        print(run_supervisor_once())


if __name__ == "__main__":
    main()

