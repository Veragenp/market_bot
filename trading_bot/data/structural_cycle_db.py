"""Structural cycle: запись structural_* и freeze → cycle_levels + trading_state (v4)."""

from __future__ import annotations

import json
import logging
import os
import time
import uuid
from typing import Any, Dict, List, Mapping, Optional

from trading_bot.analytics.structural_cycle import (
    StructuralParams,
    StructuralSymbolResult,
    compute_structural_symbol_results,
)
from trading_bot.config.settings import STRUCTURAL_ALLOWED_LEVEL_TYPES
from trading_bot.config.symbols import TRADING_SYMBOLS
from trading_bot.data.db import get_connection
from trading_bot.data.schema import init_db, run_migrations
from trading_bot.data.cycle_levels_db import export_cycle_levels_sheets_snapshot
from trading_bot.data.ops_stage import record_stage_event
from trading_bot.data.structural_ops_notify import (
    export_levels_snapshot,
    notify_no_valid_ref_prices,
    on_structural_event,
)
from trading_bot.tools.price_feed import PricePoint, get_price_feed

logger = logging.getLogger(__name__)


def _now_ts() -> int:
    return int(time.time())


def _default_structural_params() -> StructuralParams:
    from trading_bot.config import settings as st

    return StructuralParams(
        min_candidates_per_side=st.STRUCTURAL_MIN_CANDIDATES_PER_SIDE,
        top_k=st.STRUCTURAL_TOP_K,
        min_pool_symbols=st.STRUCTURAL_MIN_POOL_SYMBOLS,
        w_min=st.STRUCTURAL_W_MIN,
        w_max=st.STRUCTURAL_W_MAX,
        allowed_level_types=tuple(STRUCTURAL_ALLOWED_LEVEL_TYPES),
        strength_first_enabled=st.STRUCTURAL_STRENGTH_FIRST_ENABLED,
        mid_band_pct=st.STRUCTURAL_MID_BAND_PCT,
    )


def _params_dict(p: StructuralParams) -> Dict[str, Any]:
    return {
        "min_candidates_per_side": p.min_candidates_per_side,
        "top_k": p.top_k,
        "min_pool_symbols": p.min_pool_symbols,
        "w_min": p.w_min,
        "w_max": p.w_max,
        "allowed_level_types": list(p.allowed_level_types),
        "strength_first_enabled": p.strength_first_enabled,
        "mid_band_pct": p.mid_band_pct,
    }


def _freeze_side_symbol_sets(rows: List[StructuralSymbolResult]) -> Dict[str, Any]:
    long_syms = {r.symbol for r in rows if r.level_below_id is not None and r.L_price is not None}
    short_syms = {r.symbol for r in rows if r.level_above_id is not None and r.U_price is not None}
    return {
        "long_symbols": sorted(long_syms),
        "short_symbols": sorted(short_syms),
        "long_count": len(long_syms),
        "short_count": len(short_syms),
    }


def _insert_event(
    cur,
    cycle_id: str,
    event_type: str,
    ts: int,
    symbol: Optional[str] = None,
    price: Optional[float] = None,
    meta: Optional[Dict[str, Any]] = None,
) -> None:
    cur.execute(
        """
        INSERT INTO structural_events (cycle_id, symbol, event_type, price, ts, meta_json)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (cycle_id, symbol, event_type, price, ts, json.dumps(meta) if meta else None),
    )
    try:
        on_structural_event(cycle_id, event_type, ts, symbol=symbol, price=price, meta=meta)
    except Exception:
        logger.exception("structural_ops on_event failed")


def _persist_symbol_rows(cur, cycle_id: str, rows: List[StructuralSymbolResult], now_ts: int) -> None:
    cur.execute("DELETE FROM structural_cycle_symbols WHERE cycle_id = ?", (cycle_id,))
    for r in rows:
        cur.execute(
            """
            INSERT INTO structural_cycle_symbols (
                cycle_id, symbol, status,
                level_below_id, level_above_id,
                L_price, U_price, atr, W_atr,
                mid_price, mid_band_low, mid_band_high,
                ref_price_ws, evaluated_at,
                tier_below, tier_above, volume_peak_below, volume_peak_above
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                cycle_id, r.symbol, r.status,
                r.level_below_id, r.level_above_id,
                r.L_price, r.U_price, r.atr, r.W_atr,
                r.mid_price, r.mid_band_low, r.mid_band_high,
                r.ref_price, now_ts,
                r.tier_below, r.tier_above,
                r.volume_peak_below, r.volume_peak_above,
            ),
        )

    # === ПОДРОБНОЕ ЛОГИРОВАНИЕ ВСЕХ СОХРАНЁННЫХ СИМВОЛОВ ===
    logger.info("=" * 80)
    logger.info("STRUCTURAL CYCLE: PERSISTED SYMBOL RESULTS for cycle_id=%s", cycle_id)
    logger.info("=" * 80)
    
    # Разделяем по статусам
    ok_rows = [r for r in rows if r.status == "ok"]
    incomplete_rows = [r for r in rows if r.status != "ok"]
    
    # Считаем уровни
    long_syms = [r for r in ok_rows if r.L_price is not None]
    short_syms = [r for r in ok_rows if r.U_price is not None]
    both_syms = [r for r in ok_rows if r.L_price is not None and r.U_price is not None]
    long_only = [r for r in ok_rows if r.L_price is not None and r.U_price is None]
    short_only = [r for r in ok_rows if r.U_price is not None and r.L_price is None]
    
    logger.info("SUMMARY:")
    logger.info("  Total symbols: %d", len(rows))
    logger.info("  OK (with at least one level): %d", len(ok_rows))
    logger.info("  Incomplete (no ATR or no levels): %d", len(incomplete_rows))
    logger.info("  LONG levels available: %d symbols", len(long_syms))
    logger.info("  SHORT levels available: %d symbols", len(short_syms))
    logger.info("  BOTH sides (paired): %d symbols", len(both_syms))
    logger.info("  LONG only: %d symbols", len(long_only))
    logger.info("  SHORT only: %d symbols", len(short_only))
    
    # Детальная информация по каждому символу с LONG уровнем
    if long_syms:
        logger.info("")
        logger.info("LONG LEVELS (selected for potential entry):")
        logger.info("-" * 80)
        for r in sorted(long_syms, key=lambda x: x.symbol):
            logger.info(
                "  %-12s L=%.6f (tier=%-12s vol=%.2f) | ref=%.6f | ATR=%.6f | dist=%.2f ATR",
                r.symbol,
                r.L_price,
                r.tier_below or "N/A",
                r.volume_peak_below or 0,
                r.ref_price,
                r.atr or 0,
                abs(r.L_price - r.ref_price) / r.atr if r.atr and r.atr > 0 else 0,
            )
    
    # Детальная информация по каждому символу с SHORT уровнем
    if short_syms:
        logger.info("")
        logger.info("SHORT LEVELS (selected for potential entry):")
        logger.info("-" * 80)
        for r in sorted(short_syms, key=lambda x: x.symbol):
            logger.info(
                "  %-12s U=%.6f (tier=%-12s vol=%.2f) | ref=%.6f | ATR=%.6f | dist=%.2f ATR",
                r.symbol,
                r.U_price,
                r.tier_above or "N/A",
                r.volume_peak_above or 0,
                r.ref_price,
                r.atr or 0,
                abs(r.U_price - r.ref_price) / r.atr if r.atr and r.atr > 0 else 0,
            )
    
    # Если есть incomplete символы, логируем причину
    if incomplete_rows:
        logger.info("")
        logger.info("INCOMPLETE SYMBOLS (skipped):")
        logger.info("-" * 80)
        for r in sorted(incomplete_rows, key=lambda x: x.symbol):
            reason = f"status={r.status}"
            if r.atr is None or r.atr <= 0:
                reason += " (no ATR)"
            if r.L_price is None and r.U_price is None:
                reason += " (no levels in band)"
            logger.info("  %-12s %s", r.symbol, reason)
    
    logger.info("=" * 80)


def _freeze_cycle_levels(
    cur,
    cycle_id: str,
    ok_rows: List[StructuralSymbolResult],
    now_ts: int,
    ref_source: str,
) -> int:
    cur.execute("DELETE FROM cycle_levels")
    n = 0
    for r in ok_rows:
        if r.atr is None or r.atr <= 0:
            continue
        if r.L_price is not None and r.level_below_id is not None:
            dist_long = abs(r.L_price - r.ref_price) / r.atr
            cur.execute(
                """
                INSERT INTO cycle_levels (
                    cycle_id, symbol, direction, level_step, level_price, source_level_id,
                    tier, volume_peak, distance_atr, ref_price, ref_price_source, ref_price_ts,
                    is_primary, is_active, frozen_at, updated_at
                )
                VALUES (?, ?, 'long', 1, ?, ?, ?, ?, ?, ?, ?, ?, 1, 1, ?, ?)
                """,
                (
                    cycle_id, r.symbol, r.L_price, r.level_below_id,
                    r.tier_below, r.volume_peak_below, dist_long,
                    r.ref_price, ref_source, now_ts, now_ts, now_ts,
                ),
            )
            n += 1
        if r.U_price is not None and r.level_above_id is not None:
            dist_short = abs(r.U_price - r.ref_price) / r.atr
            cur.execute(
                """
                INSERT INTO cycle_levels (
                    cycle_id, symbol, direction, level_step, level_price, source_level_id,
                    tier, volume_peak, distance_atr, ref_price, ref_price_source, ref_price_ts,
                    is_primary, is_active, frozen_at, updated_at
                )
                VALUES (?, ?, 'short', 1, ?, ?, ?, ?, ?, ?, ?, ?, 1, 1, ?, ?)
                """,
                (
                    cycle_id, r.symbol, r.U_price, r.level_above_id,
                    r.tier_above, r.volume_peak_above, dist_short,
                    r.ref_price, ref_source, now_ts, now_ts, now_ts,
                ),
            )
            n += 1

    cur.execute(
        """
        UPDATE trading_state
        SET cycle_id = ?, structural_cycle_id = ?, position_state = 'none', cycle_phase = 'arming',
            levels_frozen = 1, cycle_version = cycle_version + 1,
            close_reason = NULL, last_package_exit_reason = NULL,
            last_transition_at = ?, updated_at = ?
        WHERE id = 1
        """,
        (cycle_id, cycle_id, now_ts, now_ts),
    )

    # === ПОДРОБНОЕ ЛОГИРОВАНИЕ ВСЕХ ЗАМОРОЖЕННЫХ УРОВНЕЙ ===
    if n > 0:
        cur.execute(
            """
            SELECT symbol, direction, level_price, tier, volume_peak, distance_atr
            FROM cycle_levels
            WHERE cycle_id = ?
            ORDER BY symbol, direction
            """,
            (cycle_id,),
        )
        rows_log = cur.fetchall()
        
        logger.info("=" * 80)
        logger.info("STRUCTURAL CYCLE: FROZEN LEVELS for cycle_id=%s", cycle_id)
        logger.info("=" * 80)
        logger.info("Total frozen levels: %d", n)
        
        # Разделяем по направлениям
        long_levels = [r for r in rows_log if r["direction"] == "long"]
        short_levels = [r for r in rows_log if r["direction"] == "short"]
        
        logger.info("LONG levels: %d", len(long_levels))
        logger.info("SHORT levels: %d", len(short_levels))
        
        # Детальная информация
        if long_levels:
            logger.info("")
            logger.info("FROZEN LONG LEVELS:")
            logger.info("-" * 80)
            for row in sorted(long_levels, key=lambda x: x["symbol"]):
                logger.info(
                    "  %-12s price=%.6f tier=%-12s vol_peak=%.2f dist=%.2f ATR",
                    row["symbol"],
                    row["level_price"],
                    row["tier"] or "N/A",
                    row["volume_peak"] or 0,
                    row["distance_atr"] or 0,
                )
        
        if short_levels:
            logger.info("")
            logger.info("FROZEN SHORT LEVELS:")
            logger.info("-" * 80)
            for row in sorted(short_levels, key=lambda x: x["symbol"]):
                logger.info(
                    "  %-12s price=%.6f tier=%-12s vol_peak=%.2f dist=%.2f ATR",
                    row["symbol"],
                    row["level_price"],
                    row["tier"] or "N/A",
                    row["volume_peak"] or 0,
                    row["distance_atr"] or 0,
                )
        
        logger.info("=" * 80)
    else:
        logger.warning("===========================================================")
        logger.warning("_freeze_cycle_levels: NO LEVELS INSERTED for cycle_id=%s", cycle_id)
        logger.warning("===========================================================")

    return n


def refresh_cycle_levels_from_structural(cur, structural_cycle_id: str) -> int:
    """
    Перезаписывает таблицу cycle_levels на основе текущих данных structural_cycle_symbols.
    Используется после rebuild opposite, чтобы входные уровни соответствовали перестроенным.
    """
    rows = cur.execute(
        """
        SELECT symbol, L_price, U_price, atr, ref_price_ws,
               tier_below, tier_above, volume_peak_below, volume_peak_above,
               level_below_id, level_above_id
        FROM structural_cycle_symbols
        WHERE cycle_id = ? AND status = 'ok'
        """,
        (structural_cycle_id,),
    ).fetchall()
    if not rows:
        logger.warning("refresh_cycle_levels_from_structural: no ok rows for cycle %s", structural_cycle_id)
        return 0

    now_ts = int(time.time())
    cur.execute("DELETE FROM cycle_levels")
    inserted = 0
    for r in rows:
        sym = str(r["symbol"])
        ref = float(r["ref_price_ws"]) if r["ref_price_ws"] is not None else None
        atr = float(r["atr"]) if r["atr"] is not None else None
        if atr is None or atr <= 0:
            continue
        # Long
        if r["L_price"] is not None and r["level_below_id"] is not None:
            lp = float(r["L_price"])
            dist = abs(lp - ref) / atr if ref is not None else None
            cur.execute(
                """
                INSERT INTO cycle_levels (
                    cycle_id, symbol, direction, level_step, level_price, source_level_id,
                    tier, volume_peak, distance_atr, ref_price, ref_price_source, ref_price_ts,
                    is_primary, is_active, frozen_at, updated_at
                )
                VALUES (?, ?, 'long', 1, ?, ?, ?, ?, ?, ?, ?, ?, 1, 1, ?, ?)
                """,
                (structural_cycle_id, sym, lp, r["level_below_id"], r["tier_below"],
                 r["volume_peak_below"], dist, ref, "structural_refresh", now_ts, now_ts, now_ts),
            )
            inserted += 1
        # Short
        if r["U_price"] is not None and r["level_above_id"] is not None:
            up = float(r["U_price"])
            dist = abs(up - ref) / atr if ref is not None else None
            cur.execute(
                """
                INSERT INTO cycle_levels (
                    cycle_id, symbol, direction, level_step, level_price, source_level_id,
                    tier, volume_peak, distance_atr, ref_price, ref_price_source, ref_price_ts,
                    is_primary, is_active, frozen_at, updated_at
                )
                VALUES (?, ?, 'short', 1, ?, ?, ?, ?, ?, ?, ?, ?, 1, 1, ?, ?)
                """,
                (structural_cycle_id, sym, up, r["level_above_id"], r["tier_above"],
                 r["volume_peak_above"], dist, ref, "structural_refresh", now_ts, now_ts, now_ts),
            )
            inserted += 1

    # === ЛОГИРОВАНИЕ ВСЕХ ОБНОВЛЁННЫХ УРОВНЕЙ ===
    if inserted > 0:
        cur.execute(
            """
            SELECT symbol, direction, level_price, tier, volume_peak, distance_atr
            FROM cycle_levels
            WHERE cycle_id = ?
            ORDER BY symbol, direction
            """,
            (structural_cycle_id,),
        )
        rows_log = cur.fetchall()
        logger.info("Refreshed %d cycle levels from structural cycle %s", inserted, structural_cycle_id)
        for row in rows_log:
            logger.info(
                "  level: symbol=%s dir=%s price=%.4f tier=%s vol_peak=%.2f dist_atr=%.2f",
                row["symbol"], row["direction"], row["level_price"],
                row["tier"], row["volume_peak"], row["distance_atr"]
            )
    else:
        logger.warning("refresh_cycle_levels_from_structural: no levels inserted for cycle_id=%s", structural_cycle_id)

    return inserted


def run_structural_pipeline(
    *,
    symbols: Optional[List[str]] = None,
    ref_prices_override: Optional[Mapping[str, PricePoint]] = None,
    auto_freeze: Optional[bool] = None,
    params: Optional[StructuralParams] = None,
) -> Dict[str, Any]:
    logger.info("=" * 80)
    logger.info("STRUCTURAL CYCLE PIPELINE STARTED")
    logger.info("=" * 80)
    init_db()
    run_migrations()
    syms_requested = list(symbols) if symbols is not None else list(TRADING_SYMBOLS)
    from trading_bot.config import settings as st

    do_freeze = st.STRUCTURAL_AUTO_FREEZE_ON_SCAN if auto_freeze is None else bool(auto_freeze)
    p = params or _default_structural_params()

    now_ts = _now_ts()
    cycle_id = str(uuid.uuid4())
    stage_started = now_ts
    conn = get_connection()
    cur = conn.cursor()

    logger.info("STRUCTURAL CYCLE ID: %s", cycle_id)
    logger.info("AUTO_FREEZE: %s", do_freeze)
    logger.info("SYMBOLS REQUESTED: %d", len(syms_requested))
    logger.info("PARAMS: min_candidates=%d top_k=%d min_pool=%d w_range=[%.2f, %.2f]",
                p.min_candidates_per_side, p.top_k, p.min_pool_symbols, p.w_min, p.w_max)
    logger.info("ALLOWED LEVEL TYPES: %s", p.allowed_level_types)

    if st.SUPERVISOR_STRUCTURAL_SKIP_WHEN_CYCLE_ACTIVE:
        tsrow = cur.execute(
            "SELECT levels_frozen, cycle_phase FROM trading_state WHERE id = 1"
        ).fetchone()
        if tsrow and int(tsrow["levels_frozen"] or 0):
            ph = str(tsrow["cycle_phase"] or "")
            if ph in ("arming", "in_position"):
                logger.warning("Structural pipeline refused because cycle active: phase=%s", ph)
                conn.close()
                return {
                    "ok": False,
                    "error": "refuse_structural_while_frozen_active",
                    "cycle_phase": ph,
                    "levels_frozen": 1,
                }

    record_stage_event(
        cur,
        stage="STRUCTURAL_SCAN",
        status="started",
        cycle_id=cycle_id,
        run_id=cycle_id,
        message="Structural scan started",
        details={"symbols_requested": len(syms_requested)},
        started_at=stage_started,
    )

    # Получение референсных цен
    ref_by_symbol: Dict[str, float] = {}
    ref_source = "override"
    if ref_prices_override is not None:
        for s in syms_requested:
            pp = ref_prices_override.get(s)
            if pp is None:
                conn.close()
                raise ValueError(f"ref_prices_override missing symbol {s}")
            ref_by_symbol[s] = float(pp.price)
        logger.info("Using override ref prices for %d symbols", len(ref_by_symbol))
    elif st.STRUCTURAL_REF_PRICE_SOURCE == "db_1m_close":
        ref_source = "db_1m_close"
        for s in syms_requested:
            row = cur.execute(
                "SELECT close FROM ohlcv WHERE symbol = ? AND timeframe = '1m' ORDER BY timestamp DESC LIMIT 1",
                (s,),
            ).fetchone()
            ref_by_symbol[s] = float(row["close"]) if row and row["close"] is not None else 0.0
        logger.info("Using db_1m_close ref prices")
    else:
        feed = get_price_feed()
        live = feed.get_prices(syms_requested)
        ref_source = "price_feed"
        any_db = False
        for s in syms_requested:
            pp = live.get(s)
            if pp is not None:
                ref_by_symbol[s] = float(pp.price)
            else:
                row = cur.execute(
                    "SELECT close FROM ohlcv WHERE symbol = ? AND timeframe = '1m' ORDER BY timestamp DESC LIMIT 1",
                    (s,),
                ).fetchone()
                ref_by_symbol[s] = float(row["close"]) if row and row["close"] is not None else 0.0
                any_db = True
        if any_db:
            ref_source = "mixed_price_feed_db_1m_close"
        logger.info("Ref prices source: %s", ref_source)

    syms = [s for s in syms_requested if ref_by_symbol.get(s, 0) > 0]
    if not syms:
        logger.error("No valid ref prices for any symbol")
        record_stage_event(
            cur,
            stage="STRUCTURAL_SCAN",
            status="failed",
            cycle_id=cycle_id,
            run_id=cycle_id,
            severity="error",
            message="No valid ref prices",
            details={"ref_source": ref_source, "symbols_requested": len(syms_requested)},
            started_at=stage_started,
            finished_at=_now_ts(),
        )
        conn.commit()
        conn.close()
        notify_no_valid_ref_prices(ref_source, len(syms_requested), 0)
        return {
            "error": "no_valid_ref_prices",
            "structural_cycle_id": None,
            "phase": None,
            "frozen": False,
            "ref_price_source": ref_source,
        }

    cur.execute(
        """
        INSERT INTO structural_cycles (
            id, phase, created_at, updated_at, params_json,
            pool_median_w, pool_mad, pool_k, symbols_valid_count
        )
        VALUES (?, 'scanning', ?, ?, ?, NULL, NULL, NULL, NULL)
        """,
        (cycle_id, now_ts, now_ts, json.dumps(_params_dict(p))),
    )
    _insert_event(cur, cycle_id, "phase_change", now_ts, meta={"to": "scanning"})

    logger.info("Computing structural symbol results for %d symbols", len(syms))
    logger.info("PASS require_pair=False (independent long/short selection)")
    results, _ = compute_structural_symbol_results(cur, syms, ref_by_symbol, p, require_pair=False)
    freeze_rows = [r for r in results if (r.level_below_id is not None or r.level_above_id is not None)]
    ok_rows = freeze_rows
    logger.info("Computed: total results=%d, ok_rows (with at least one level)=%d", len(results), len(ok_rows))

    cur.execute(
        """
        UPDATE structural_cycles
        SET updated_at = ?, symbols_valid_count = ?
        WHERE id = ?
        """,
        (now_ts, len(ok_rows), cycle_id),
    )
    _persist_symbol_rows(cur, cycle_id, results, now_ts)

    side_sets = _freeze_side_symbol_sets(ok_rows)
    out: Dict[str, Any] = {
        "structural_cycle_id": cycle_id,
        "symbols_ok": len(ok_rows),
        "symbols_with_levels": len(freeze_rows),
        "min_pool_required": p.min_pool_symbols,
        "ref_price_source": ref_source,
        **side_sets,
    }

    if side_sets["long_count"] < p.min_candidates_per_side and side_sets["short_count"] < p.min_candidates_per_side:
        logger.warning("Insufficient pool: long_count=%d short_count=%d min_candidates=%d",
                       side_sets["long_count"], side_sets["short_count"], p.min_candidates_per_side)
        cur.execute(
            "UPDATE structural_cycles SET phase = 'cancelled', updated_at = ?, cancel_reason = ? WHERE id = ?",
            (now_ts, "insufficient_pool_after_selection", cycle_id),
        )
        _insert_event(cur, cycle_id, "phase_change", now_ts, meta={"to": "cancelled", "reason": "insufficient_pool"})
        record_stage_event(
            cur,
            stage="STRUCTURAL_SCAN",
            status="failed",
            cycle_id=cycle_id,
            run_id=cycle_id,
            severity="error",
            message="Insufficient pool after level selection",
            details={"long_count": side_sets["long_count"], "short_count": side_sets["short_count"],
                     "min_candidates": p.min_candidates_per_side},
            started_at=stage_started,
            finished_at=now_ts,
        )
        conn.commit()
        conn.close()
        out["phase"] = "cancelled"
        out["frozen"] = False
        return out

    cur.execute(
        "UPDATE structural_cycles SET phase = 'armed', updated_at = ?, cancel_reason = NULL WHERE id = ?",
        (now_ts, cycle_id),
    )
    _insert_event(cur, cycle_id, "phase_change", now_ts, meta={"to": "armed"})

    if do_freeze:
        logger.info("Freezing cycle levels...")
        n_ins = _freeze_cycle_levels(cur, cycle_id, ok_rows, now_ts, ref_source)
        _insert_event(cur, cycle_id, "freeze", now_ts, meta={"cycle_levels_rows": n_ins, **side_sets})
        out["phase"] = "armed"
        out["cycle_levels_rows"] = n_ins
        out["frozen"] = True
        record_stage_event(
            cur,
            stage="STRUCTURAL_FREEZE",
            status="ok",
            cycle_id=cycle_id,
            run_id=cycle_id,
            message="Cycle levels frozen after scan",
            details={"cycle_levels_rows": n_ins},
            started_at=now_ts,
            finished_at=now_ts,
        )
        if st.OPS_STAGE_SHEETS and not os.getenv("PYTEST_CURRENT_TEST"):
            try:
                logger.info("Exporting cycle_levels sheets snapshot...")
                export_cycle_levels_sheets_snapshot()
                logger.info("cycle_levels sheets snapshot exported successfully")
            except Exception:
                logger.exception("cycle levels sheets snapshot export failed after structural freeze")
    else:
        out["phase"] = "armed"
        out["cycle_levels_rows"] = 0
        out["frozen"] = False
        logger.info("Skipped freeze (auto_freeze=False)")

    record_stage_event(
        cur,
        stage="STRUCTURAL_SCAN",
        status="ok",
        cycle_id=cycle_id,
        run_id=cycle_id,
        message="Structural scan completed",
        details={"symbols_ok": len(ok_rows), "frozen": bool(do_freeze)},
        started_at=stage_started,
        finished_at=now_ts,
    )
    conn.commit()
    conn.close()
    if not os.getenv("PYTEST_CURRENT_TEST"):
        try:
            logger.info("Exporting levels snapshot to Sheets...")
            export_levels_snapshot(cycle_id, out)
            logger.info("Levels snapshot exported")
            
            # Экспорт упрощённой таблицы торговых уровней
            from trading_bot.data.structural_ops_notify import export_structural_trading_levels
            export_structural_trading_levels(cycle_id)
            logger.info("Trading levels exported")
        except Exception:
            logger.exception("structural_ops export_levels_snapshot failed")
    logger.info("Structural pipeline finished successfully, cycle_id=%s", cycle_id)
    return out