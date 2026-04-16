"""
Единый supervisor-цикл: data refresh → levels rebuild → structural → entry detector.

Однократный запуск без `--loop` выполняет те же шаги в этом порядке (`run_supervisor_once`).

Перед structural (если SUPERVISOR_EXPORT_VP_LOCAL_BEFORE_STRUCTURAL): выгрузка vp_local из БД
в Google Sheet (те же листы, что export_volume_peaks_to_sheets_only), для сверки перед отбором.

При пропуске scheduled structural (активный цикл / freeze) supervisor всё равно выгружает
cycle_levels snapshot (главный freeze-результат для торгового контура).

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
from typing import Any, Dict, Tuple

_REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _REPO not in sys.path:
    sys.path.insert(0, _REPO)

from trading_bot.analytics.entry_detector import run_entry_detector_tick
from trading_bot.analytics.entry_gate import maybe_transition_arming_after_package_all_flat
from trading_bot.config import settings as st
from trading_bot.config.symbols import ANALYTIC_SYMBOLS, TRADING_SYMBOLS, crypto_context_binance_spot_not_in_trading
from trading_bot.data.collectors import update_indices
from trading_bot.data.cycle_levels_db import export_cycle_levels_sheets_snapshot
from trading_bot.data.data_loader import DataLoaderManager
from trading_bot.data.db import get_connection
from trading_bot.data.ops_stage import record_stage_event
from trading_bot.data.schema import init_db, run_migrations
from trading_bot.data.state_manager import (
    determine_start_mode,
    handle_fresh_start,
    handle_recovery_add_missing,
    handle_clean_stale_positions,
    handle_recovery_continue,
    handle_recovery_sync_mismatch,
)
from trading_bot.data.structural_cycle_db import run_structural_pipeline

# Test mode import
if st.TEST_MODE:
    from trading_bot.analytics.test_level_generator import generate_test_levels, rebuild_opposite_test_levels

from trading_bot.entrypoints.export_volume_peaks_to_sheets_only import main as export_vp_to_sheets_main
from trading_bot.scripts.rebuild_volume_profile_peaks_to_db import main as rebuild_vp_local_main
from trading_bot.tools.bybit_trading import (
    get_linear_open_orders,
    get_linear_positions,
    linear_position_sizes_by_symbol,
    pool_symbols_flat_on_linear_exchange,
    to_bybit_symbol,
)

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
        "BYBIT_USE_DEMO=%s BYBIT_BASE_URL=%s | LEVEL_CROSS min=%s timeout_min=%s |%s",
        st.SUPERVISOR_DATA_REFRESH_SEC,
        st.SUPERVISOR_LEVELS_REBUILD_SEC,
        st.SUPERVISOR_STRUCTURAL_SEC,
        st.SUPERVISOR_ENTRY_TICK_SEC,
        st.SUPERVISOR_POLL_SEC,
        st.SUPERVISOR_STRUCTURAL_SKIP_WHEN_CYCLE_ACTIVE,
        st.SUPERVISOR_STRUCTURAL_RETRY_WHEN_BLOCKED_SEC,
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


def _export_cycle_levels_snapshot_from_state() -> None:
    """
    Выгрузка freeze-снимка cycle_levels/diag/candidates из текущего состояния БД.
    """
    if os.getenv("PYTEST_CURRENT_TEST"):
        return
    if not st.OPS_STAGE_SHEETS:
        return
    try:
        snap = export_cycle_levels_sheets_snapshot()
        logger.info(
            "Supervisor: cycle_levels snapshot exported rows=%s diag=%s candidates=%s",
            snap.get("cycle_levels_rows"),
            snap.get("diag_rows"),
            snap.get("candidates_rows"),
        )
    except Exception:
        logger.exception(
            "Supervisor: cycle_levels snapshot → Sheets failed (credentials / network?)"
        )


def _is_cycle_flat_on_exchange(symbols_trade: list[str]) -> Tuple[bool, str]:
    """
    Проверка flat на бирже по символам цикла:
    - все позиции size==0
    - нет открытых ордеров по символам цикла
    """
    if not symbols_trade:
        return True, "no_symbols"

    pos_resp = get_linear_positions()
    if pos_resp is None:
        return False, "exchange_positions_unavailable"
    sizes = linear_position_sizes_by_symbol(pos_resp)
    if not pool_symbols_flat_on_linear_exchange(symbols_trade, sizes):
        return False, "exchange_has_open_positions"

    oo_resp = get_linear_open_orders()
    if oo_resp is None:
        return False, "exchange_open_orders_unavailable"
    bybit_pool = {to_bybit_symbol(s) for s in symbols_trade}
    rows = ((oo_resp.get("result") or {}).get("list") or []) if isinstance(oo_resp, dict) else []
    for row in rows:
        sym = str(row.get("symbol") or "").upper()
        if sym in bybit_pool:
            return False, f"exchange_open_order:{sym}"
    return True, "exchange_flat"


def _safe_auto_reset_cycle() -> tuple[bool, str]:
    """
    Safe auto-reset залипшего цикла перед structural:
    - активная freeze-фаза (arming/in_position + levels_frozen=1)
    - нет open/pending позиций и pending/open ордеров в БД
    - flat на бирже (Bybit) по символам цикла
    """
    if os.getenv("PYTEST_CURRENT_TEST"):
        return False, "pytest_mode"

    now = int(time.time())
    conn = get_connection()
    try:
        cur = conn.cursor()
        row = cur.execute(
            """
            SELECT cycle_id, structural_cycle_id, levels_frozen, cycle_phase, last_transition_at
            FROM trading_state
            WHERE id = 1
            """
        ).fetchone()
        if not row:
            return False, "no_trading_state"

        cycle_id = str(row["cycle_id"]) if row["cycle_id"] else ""
        structural_cycle_id = str(row["structural_cycle_id"]) if row["structural_cycle_id"] else ""
        frozen = int(row["levels_frozen"] or 0)
        phase = str(row["cycle_phase"] or "")
        if not (frozen == 1 and phase in ("arming", "in_position")):
            return False, "not_active_frozen_cycle"
        if not cycle_id:
            return False, "no_cycle_id"

        last_transition_at = int(row["last_transition_at"] or 0)
        if last_transition_at and now - last_transition_at < 60:
            return False, "recent_transition_guard"

        open_pos_row = cur.execute(
            """
            SELECT COUNT(*) AS c
            FROM position_records
            WHERE cycle_id = ? AND status IN ('pending', 'open')
            """,
            (cycle_id,),
        ).fetchone()
        open_pos = int(open_pos_row["c"] if open_pos_row else 0)
        if open_pos > 0:
            return False, f"db_open_positions:{open_pos}"

        pending_exec_row = cur.execute(
            """
            SELECT COUNT(*) AS c
            FROM exec_orders
            WHERE cycle_id = ?
              AND lower(COALESCE(status, '')) NOT IN (
                'filled', 'cancelled', 'canceled', 'rejected', 'closed', 'failed', 'expired'
              )
            """,
            (cycle_id,),
        ).fetchone()
        pending_exec = int(pending_exec_row["c"] if pending_exec_row else 0)
        if pending_exec > 0:
            return False, f"db_pending_exec_orders:{pending_exec}"

        sym_cycle = structural_cycle_id or cycle_id
        sym_rows = cur.execute(
            """
            SELECT DISTINCT symbol
            FROM structural_cycle_symbols
            WHERE cycle_id = ?
            """,
            (sym_cycle,),
        ).fetchall()
        symbols_trade = [str(r["symbol"]) for r in sym_rows if r and r["symbol"]]
        if not symbols_trade:
            return False, "no_cycle_symbols"

        exch_flat, exch_reason = _is_cycle_flat_on_exchange(symbols_trade)
        if not exch_flat:
            return False, exch_reason

        cur.execute(
            """
            UPDATE trading_state
            SET cycle_phase = 'closed',
                levels_frozen = 0,
                cycle_id = NULL,
                structural_cycle_id = NULL,
                position_state = 'none',
                close_reason = 'safe_auto_reset_before_structural',
                channel_mode = 'two_sided',
                known_side = 'both',
                need_rebuild_opposite = 0,
                opposite_rebuild_deadline_ts = NULL,
                opposite_rebuild_attempts = 0,
                allow_long_entry = 1,
                allow_short_entry = 1,
                last_rebuild_reason = 'safe_auto_reset_before_structural',
                last_transition_at = ?,
                updated_at = ?
            WHERE id = 1
            """,
            (now, now),
        )
        cur.execute(
            """
            UPDATE structural_cycles
            SET phase = 'closed',
                cancel_reason = COALESCE(cancel_reason, 'safe_auto_reset_before_structural'),
                updated_at = ?
            WHERE id = ? AND phase != 'closed'
            """,
            (now, sym_cycle),
        )
        record_stage_event(
            cur,
            stage="SAFE_AUTO_RESET",
            status="ok",
            cycle_id=cycle_id,
            run_id=cycle_id,
            message="Safe auto-reset before structural",
            details={
                "reason": "stale_active_cycle_without_positions_or_orders",
                "phase": phase,
                "levels_frozen": frozen,
                "open_positions_db": open_pos,
                "pending_exec_orders_db": pending_exec,
                "symbols_checked": len(symbols_trade),
                "exchange_check": exch_reason,
            },
            started_at=now,
            finished_at=now,
        )
        conn.commit()
        logger.info(
            "Supervisor: safe auto-reset applied (cycle=%s symbols=%s reason=%s)",
            cycle_id[:8],
            len(symbols_trade),
            exch_reason,
        )
        return True, "reset_applied"
    except Exception:
        conn.rollback()
        logger.exception("Supervisor: safe auto-reset failed")
        return False, "exception"
    finally:
        conn.close()


def _should_skip_scheduled_structural() -> tuple[bool, str]:
    """
    Пока зафиксированы уровни и цикл не closed — полный structural не нужен (не сбрасывать V3-окно и freeze).
    Проверяем "залипший" цикл: если positions flat но цикл активен более N часов — авто-сброс.
    """
    if os.getenv("PYTEST_CURRENT_TEST"):
        return False, ""
    if not st.SUPERVISOR_STRUCTURAL_SKIP_WHEN_CYCLE_ACTIVE:
        return False, ""
    conn = get_connection()
    try:
        cur = conn.cursor()
        row = cur.execute(
            "SELECT levels_frozen, cycle_phase, last_transition_at, cycle_id FROM trading_state WHERE id = 1"
        ).fetchone()
        if not row:
            return False, ""
        frozen = int(row["levels_frozen"] or 0)
        phase = str(row["cycle_phase"] or "")
        if frozen and phase in ("arming", "in_position"):
            # Проверяем "залипший" цикл: нет позиций но цикл активен > 24ч
            cycle_id = str(row["cycle_id"]) if row["cycle_id"] else ""
            last_transition = int(row["last_transition_at"] or 0)
            now = int(time.time())
            
            # Проверка на залипший цикл (без позиций более 24 часов)
            if last_transition and now - last_transition > 86400:  # 24 часа
                # Проверяем есть ли позиции
                open_pos = cur.execute(
                    "SELECT COUNT(*) AS c FROM position_records WHERE cycle_id = ? AND status IN ('pending', 'open')",
                    (cycle_id,)
                ).fetchone()
                n_open = int(open_pos["c"] if open_pos else 0)
                
                if n_open == 0:
                    # Цикл залипший — авто-сброс
                    reset_done, reset_reason = _safe_auto_reset_cycle()
                    if reset_done:
                        logger.warning(
                            "Supervisor: auto-reset STUCK cycle (no positions for >24h, cycle=%s)",
                            cycle_id[:8]
                        )
                        return False, ""
                    logger.warning(
                        "Supervisor: stuck cycle detected but auto-reset failed (%s)",
                        reset_reason
                    )
            
            # После проверки залипания — выходим, structural будет пропущен
            # Auto-reset применяется только если цикл залип >24h
            logger.info(
                "Supervisor: safe auto-reset blocked, keep structural skip (phase=%s frozen=%s)",
                phase, frozen,
            )
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
    
    # Логирование TEST_MODE
    if st.TEST_MODE:
        logger = logging.getLogger(__name__)
        logger.info("TEST_MODE: Test level generator enabled")
    
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
    out = _structural_main()
    end = int(time.time())
    
    sev = "error" if not out.get("ok", True) else None
    status = "failed" if sev else "ok"
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
    
    # НОВЫЙ ШАГ: определить режим старта
    logger.info("Supervisor: determining start mode...")
    mode, session_id, details = determine_start_mode()
    logger.info("Supervisor: start mode=%s session=%s", mode, session_id[:8])
    
    # Выполняем обработчик режима
    if mode == "FRESH_START":
        result = handle_fresh_start()
        logger.info("Supervisor: fresh_start applied session=%s", session_id[:8])
    elif mode == "RECOVERY_ADD_MISSING":
        result = handle_recovery_add_missing(details)
        logger.info("Supervisor: recovery_add_missing applied positions=%s", 
                    len(details.get("exchange_positions", {})) if details else 0)
    elif mode == "CLEAN_STALE_POSITIONS":
        result = handle_clean_stale_positions(details)
        logger.info("Supervisor: clean_stale_positions applied positions_closed=%s",
                    len(details.get("db_positions", [])) if details else 0)
    elif mode == "RECOVERY_CONTINUE":
        result = handle_recovery_continue(details)
        logger.info("Supervisor: recovery_continue applied")
    elif mode == "RECOVERY_SYNC_MISMATCH":
        result = handle_recovery_sync_mismatch(details)
        logger.error("Supervisor: RECOVERY_SYNC_MISMATCH - manual reset required!")
        # Отправляем алерт
        logger.error("Supervisor: sync mismatch details=%s", details)
    else:
        logger.warning("Supervisor: unknown start mode=%s", mode)
    
    data_out = _run_data_refresh()
    levels_out = _run_levels_rebuild()
    skip, reason = _should_skip_scheduled_structural()
    structural_out: Dict[str, object]
    if skip:
        logger.info("Supervisor once: structural skipped (%s)", reason)
        _export_cycle_levels_snapshot_from_state()
        structural_out = {"skipped": True, "reason": reason}
    else:
        structural_out = _run_structural()
    entry_out = _run_entry_tick()
    return {
        "data": data_out,
        "levels": levels_out,
        "structural": structural_out,
        "entry": entry_out,
        "start_mode": mode,
        "session_id": session_id,
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
                    _export_cycle_levels_snapshot_from_state()
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

