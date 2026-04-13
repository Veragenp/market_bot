"""Оркестрация одного тика: level_cross_monitor (tutorial V3) + entry_gate.

Скрипт `run_entry_detector` вызывает `run_entry_detector_tick()` — полный цикл как
`run_level_cross_monitor` без --loop.
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any, Dict

from trading_bot.config import settings as st
from trading_bot.analytics.entry_gate import (
    _load_cycle_member_symbols,
    process_v3_signal,
    run_opposite_rebuild_maintenance_tick,
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


def _fetch_prices_for_cycle(cur) -> Dict[str, float]:
    row = cur.execute(
        "SELECT cycle_id, structural_cycle_id, levels_frozen FROM trading_state WHERE id = 1"
    ).fetchone()
    if not row or not row["cycle_id"] or not int(row["levels_frozen"] or 0):
        return {}
    cycle_id = str(row["cycle_id"])
    structural_cycle_id = str(row["structural_cycle_id"]) if row["structural_cycle_id"] else cycle_id
    syms = _load_cycle_member_symbols(cur, cycle_id=structural_cycle_id)
    if not syms:
        syms = _load_cycle_member_symbols(cur, cycle_id=cycle_id)
    if not syms:
        return {}
    raw = get_price_feed().get_prices(syms)
    return {s: float(pp.price) for s, pp in raw.items()}


def run_entry_detector_tick() -> Dict[str, Any]:
    init_db()
    run_migrations()
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
                start_msg = (
                    "Модуль entry detector: тик (level cross + entry gate + reconcile). "
                    f"cycle_id={cycle_id or 'n/a'}"
                )
                get_telegram_notifier().send_message(
                    f"<pre>{escape_html_telegram(start_msg)}</pre>",
                    parse_mode="HTML",
                )
            except Exception:
                logger.exception("entry_detector: telegram start notify failed")
        prices = _fetch_prices_for_cycle(cur)
        signals, summary = run_level_cross_tick(cur, prices=prices, monitor=mon)
        gate_results = []
        for sig in signals:
            gate_results.append(process_v3_signal(cur, signal_type=sig, monitor=mon, prices=prices))
        maintenance = run_opposite_rebuild_maintenance_tick(cur, prices=prices)
        rec_started = int(time.time())
        rec = reconcile_recent_exec_orders(cur, lookback_hours=24)
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
                "gate_results": gate_results,
                "maintenance": maintenance,
                "reconcile": rec,
                "symbols_with_price": len(prices),
            },
            started_at=int(started),
            finished_at=int(finished),
        )
        conn.commit()
        out = dict(summary)
        out["signals"] = signals
        out["gate"] = gate_results
        out["maintenance"] = maintenance
        return out
    finally:
        conn.close()


__all__ = ["run_entry_detector_tick"]
