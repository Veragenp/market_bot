"""Оркестрация одного тика: level_cross_monitor + entry_gate (v4)."""

from __future__ import annotations

import logging
import os
import time
from typing import Any, Dict

from trading_bot.config import settings as st
from trading_bot.analytics.entry_gate import (
    _load_cycle_member_symbols,
    maybe_transition_arming_after_package_all_flat,
    process_v3_signal,
)
from trading_bot.analytics.level_cross_monitor import (
    get_level_cross_monitor,
    run_level_cross_tick,
)
from trading_bot.data.db import get_connection
from trading_bot.data.ops_stage import record_stage_event
from trading_bot.data.schema import init_db, run_migrations
from trading_bot.data.trade_reconciliation import reconcile_recent_exec_orders
from trading_bot.tools.price_feed import get_price_feed
from trading_bot.tools.telegram_notify import escape_html_telegram, get_telegram_notifier

logger = logging.getLogger(__name__)

_V3_CONFIG_LOGGED = False


def _log_v3_config_once() -> None:
    global _V3_CONFIG_LOGGED
    if _V3_CONFIG_LOGGED or os.getenv("PYTEST_CURRENT_TEST"):
        return
    _V3_CONFIG_LOGGED = True
    logger.info(
        "EntryDetector V4 settings: LEVEL_CROSS min_alerts=%s timeout_min=%s max_add=%s "
        "tick_summary_log=%s | ENTRY_GATE long_atr%%=%s short_atr%%=%s | "
        "BYBIT_USE_DEMO=%s EXECUTION_ENABLED=%s ENTRY_AUTO_OPEN_AFTER_GATE=%s",
        st.LEVEL_CROSS_MIN_ALERTS_COUNT,
        st.LEVEL_CROSS_ALERT_TIMEOUT_MINUTES,
        st.LEVEL_CROSS_MAX_ADDITIONAL_ALERTS,
        st.LEVEL_CROSS_TICK_SUMMARY_LOG,
        st.ENTRY_GATE_LONG_ATR_THRESHOLD_PCT,
        st.ENTRY_GATE_SHORT_ATR_THRESHOLD_PCT,
        st.BYBIT_USE_DEMO,
        st.BYBIT_EXECUTION_ENABLED,
        st.ENTRY_AUTO_OPEN_AFTER_GATE,
    )


def _fetch_prices_for_cycle(cur) -> Dict[str, float]:
    row = cur.execute(
        "SELECT cycle_id, structural_cycle_id, levels_frozen FROM trading_state WHERE id = 1"
    ).fetchone()
    if not row or not row["cycle_id"]:
        logger.info("EntryDetector prices: skip (no cycle_id in trading_state)")
        return {}
    if not int(row["levels_frozen"] or 0):
        logger.info("EntryDetector prices: skip (levels_frozen=0 cycle_id=%s)", row["cycle_id"])
        return {}
    cycle_id = str(row["cycle_id"])
    structural_cycle_id = str(row["structural_cycle_id"]) if row["structural_cycle_id"] else cycle_id
    syms = _load_cycle_member_symbols(cur, cycle_id=structural_cycle_id)
    pool_src = "structural_cycle_id"
    if not syms:
        syms = _load_cycle_member_symbols(cur, cycle_id=cycle_id)
        pool_src = "cycle_id"
    if not syms:
        logger.info("EntryDetector prices: skip (no structural_cycle_symbols for cycle_id=%s)", cycle_id)
        return {}
    raw = get_price_feed().get_prices(syms)
    out = {s: float(pp.price) for s, pp in raw.items()}
    missing = [s for s in syms if s not in out]
    sample = ", ".join(f"{s}:{out[s]:.6g}" for s in list(out.keys())[:3])
    if len(out) > 3:
        sample += ",..."
    logger.info(
        "EntryDetector prices: pool=%s members=%s priced=%s/%s BYBIT_USE_DEMO=%s missing=%s sample=[%s]",
        pool_src, len(syms), len(out), len(syms), st.BYBIT_USE_DEMO,
        missing if len(missing) <= 8 else missing[:8] + ["..."], sample or "—",
    )
    return out


def run_entry_detector_tick() -> Dict[str, Any]:
    init_db()
    run_migrations()
    _log_v3_config_once()
    mon = get_level_cross_monitor()
    conn = get_connection()
    cur = conn.cursor()
    try:
        row = cur.execute("SELECT cycle_id FROM trading_state WHERE id = 1").fetchone()
        cycle_id = str(row["cycle_id"]) if row and row["cycle_id"] else None
        started = time.time()
        record_stage_event(
            cur,
            stage="ENTRY_SIGNAL",
            status="started",
            cycle_id=cycle_id,
            run_id=cycle_id,
            message="Entry detector tick started",
            started_at=int(started),
        )
        if st.ENTRY_DETECTOR_TELEGRAM_START and not os.getenv("PYTEST_CURRENT_TEST"):
            try:
                start_msg = f"Модуль entry detector: тик (level cross + entry gate + reconcile). cycle_id={cycle_id or 'n/a'}"
                get_telegram_notifier().send_message(f"<pre>{escape_html_telegram(start_msg)}</pre>", parse_mode="HTML")
            except Exception:
                logger.exception("entry_detector: telegram start notify failed")
        prices = _fetch_prices_for_cycle(cur)
        signals, summary = run_level_cross_tick(cur, prices=prices, monitor=mon)
        if summary.get("skipped"):
            logger.info("EntryDetector level_cross skipped=%s (signals would be empty)", summary.get("skipped"))
        elif signals:
            logger.info("EntryDetector level_cross signals=%s", signals)
        gate_results = []
        for sig in signals:
            gate_results.append(process_v3_signal(cur, signal_type=sig, monitor=mon, prices=prices))
        if gate_results:
            for gr in gate_results:
                sig = gr.get("signal", gr.get("cancel"))
                logger.info("EntryDetector gate signal=%s ok=%s entered=%s rejected_n=%s",
                            sig, gr.get("ok"), gr.get("entered"), len(gr.get("rejected") or []))
                if gr.get("auto_open"):
                    logger.info("EntryDetector auto_open results=%s", gr.get("auto_open"))
        rec_started = int(time.time())
        rec = reconcile_recent_exec_orders(cur, lookback_hours=24)
        sheets_trading: Dict[str, Any] = {}
        if not os.getenv("PYTEST_CURRENT_TEST"):
            try:
                from trading_bot.data.trading_cycle_sheets import sync_trading_positions_and_stats_to_sheets
                sheets_trading = sync_trading_positions_and_stats_to_sheets(cur)
            except Exception:
                logger.exception("entry_detector: trading_cycle_sheets sync failed")
                sheets_trading = {"ok": False, "error": "exception"}
        rec_status = "ok" if rec.get("ok", False) else "failed"
        rec_sev = None if rec_status == "ok" else "error"
        record_stage_event(
            cur,
            stage="ORDER_RECONCILE",
            status=rec_status,
            cycle_id=cycle_id,
            run_id=cycle_id,
            severity=rec_sev,
            message="Execution reconciliation tick",
            details=rec,
            started_at=rec_started,
            finished_at=int(time.time()),
        )
        flat_tr = maybe_transition_arming_after_package_all_flat(cur)
        from trading_bot.data.cycle_levels_db import export_cycle_levels_sheets_snapshot
        from trading_bot.data.trading_cycle_sheets import export_open_orders_to_sheets

        export_cycle_levels_sheets_snapshot()
        export_open_orders_to_sheets(cur)
        if flat_tr.get("transitioned"):
            logger.info("EntryDetector package flat → next leg arming: %s", flat_tr)
        finished = time.time()
        sev = "error" if any((not r.get("ok", True)) for r in gate_results) else None
        status = "failed" if sev else "ok"
        record_stage_event(
            cur,
            stage="ENTRY_SIGNAL",
            status=status,
            cycle_id=cycle_id,
            run_id=cycle_id,
            severity=sev,
            message="Entry detector tick completed",
            details={
                "signals": list(signals),
                "level_cross_summary": summary,
                "gate_results": gate_results,
                "reconcile": rec,
                "sheets_trading_cycle": sheets_trading,
                "package_flat_transition": flat_tr,
                "symbols_with_price": len(prices),
            },
            started_at=int(started),
            finished_at=int(finished),
        )
        conn.commit()
        out = dict(summary)
        out["signals"] = signals
        out["gate"] = gate_results
        out["package_flat_transition"] = flat_tr
        return out
    finally:
        conn.close()


__all__ = ["run_entry_detector_tick"]