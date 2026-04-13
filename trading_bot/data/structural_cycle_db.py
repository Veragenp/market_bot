"""Structural cycle: запись structural_* и freeze → cycle_levels + trading_state."""

from __future__ import annotations

import json
import logging
import os
import time
import uuid
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from trading_bot.analytics.entry_gate import signal_structural_ready
from trading_bot.analytics.structural_cycle import (
    StrongLevel,
    StructuralCycle,
    StructuralParams,
    StructuralSymbolResult,
    SymbolPair,
    check_breakout,
    compute_initial_zones,
    compute_structural_symbol_results,
    fire_if_enough_in_mid,
    symbols_past_breakout_threshold,
    update_trigger_counts,
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


def _rows_to_symbol_pairs(rows: List[Dict[str, Any]]) -> Dict[str, SymbolPair]:
    """Снимок structural_cycle_symbols / Row → SymbolPair для realtime."""
    out: Dict[str, SymbolPair] = {}
    for r in rows:
        if r.get("L_price") is None or r.get("U_price") is None:
            continue
        lb = r.get("level_below_id")
        la = r.get("level_above_id")
        if lb is None or la is None:
            continue
        sym = str(r["symbol"])
        atr = float(r["atr"])
        out[sym] = SymbolPair(
            symbol=sym,
            level_below=StrongLevel(
                id=int(lb),
                price=float(r["L_price"]),
                volume_peak=float(r.get("volume_peak_below") or 0),
                strength=0.0,
                tier=str(r.get("tier_below") or ""),
                level_type="",
            ),
            level_above=StrongLevel(
                id=int(la),
                price=float(r["U_price"]),
                volume_peak=float(r.get("volume_peak_above") or 0),
                strength=0.0,
                tier=str(r.get("tier_above") or ""),
                level_type="",
            ),
            W=float(r["W_atr"])
            if r.get("W_atr") is not None
            else (float(r["U_price"]) - float(r["L_price"])) / atr,
            atr=atr,
            ref_price=float(r["ref_price_ws"])
            if r.get("ref_price_ws") is not None
            else float((float(r["L_price"]) + float(r["U_price"])) / 2.0),
        )
    return out


def _now_ts() -> int:
    return int(time.time())


def _default_structural_params() -> StructuralParams:
    from trading_bot.config import settings as st

    w_slack_frac = float(st.STRUCTURAL_W_SLACK_PCT) / 100.0
    w_slack_abs_min = float(st.STRUCTURAL_W_SLACK_ABS_MIN)
    return StructuralParams(
        min_candidates_per_side=st.STRUCTURAL_MIN_CANDIDATES_PER_SIDE,
        top_k=st.STRUCTURAL_TOP_K,
        min_pool_symbols=st.STRUCTURAL_MIN_POOL_SYMBOLS,
        n_etalon=st.STRUCTURAL_N_ETALON,
        w_min=st.STRUCTURAL_W_MIN,
        w_max=st.STRUCTURAL_W_MAX,
        w_slack=w_slack_frac,
        w_slack_abs_min=w_slack_abs_min,
        mid_band_pct=st.STRUCTURAL_MID_BAND_PCT,
        edge_atr_frac=st.STRUCTURAL_EDGE_ATR_FRAC,
        allowed_level_types=tuple(STRUCTURAL_ALLOWED_LEVEL_TYPES),
        strength_first_enabled=st.STRUCTURAL_STRENGTH_FIRST_ENABLED,
        z_w_ok_threshold=st.STRUCTURAL_Z_W_OK_THRESHOLD,
    )


def _params_dict(p: StructuralParams) -> Dict[str, Any]:
    return {
        "min_candidates_per_side": p.min_candidates_per_side,
        "top_k": p.top_k,
        "min_pool_symbols": p.min_pool_symbols,
        "n_etalon": p.n_etalon,
        "w_min": p.w_min,
        "w_max": p.w_max,
        "w_slack": p.w_slack,
        "w_slack_abs_min": p.w_slack_abs_min,
        "mid_band_pct": p.mid_band_pct,
        "edge_atr_frac": p.edge_atr_frac,
        "allowed_level_types": list(p.allowed_level_types),
        "strength_first_enabled": p.strength_first_enabled,
        "z_w_ok_threshold": p.z_w_ok_threshold,
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
        (
            cycle_id,
            symbol,
            event_type,
            price,
            ts,
            json.dumps(meta, ensure_ascii=False) if meta else None,
        ),
    )
    try:
        on_structural_event(cycle_id, event_type, ts, symbol=symbol, price=price, meta=meta)
    except Exception:
        logger.exception("structural_ops on_event failed (event still in DB transaction)")


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
                cycle_id,
                r.symbol,
                r.status,
                r.level_below_id,
                r.level_above_id,
                r.L_price,
                r.U_price,
                r.atr,
                r.W_atr,
                r.mid_price,
                r.mid_band_low,
                r.mid_band_high,
                r.ref_price,
                now_ts,
                r.tier_below,
                r.tier_above,
                r.volume_peak_below,
                r.volume_peak_above,
            ),
        )


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
        if r.L_price is None or r.U_price is None or r.level_below_id is None or r.level_above_id is None:
            continue
        dist_long = abs(r.L_price - r.ref_price) / r.atr
        dist_short = abs(r.U_price - r.ref_price) / r.atr
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
                cycle_id,
                r.symbol,
                r.L_price,
                r.level_below_id,
                r.tier_below,
                r.volume_peak_below,
                dist_long,
                r.ref_price,
                ref_source,
                now_ts,
                now_ts,
                now_ts,
            ),
        )
        n += 1
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
                cycle_id,
                r.symbol,
                r.U_price,
                r.level_above_id,
                r.tier_above,
                r.volume_peak_above,
                dist_short,
                r.ref_price,
                ref_source,
                now_ts,
                now_ts,
                now_ts,
            ),
        )
        n += 1

    cur.execute(
        """
        UPDATE trading_state
        SET cycle_id = ?, structural_cycle_id = ?, position_state = 'none', cycle_phase = 'arming',
            levels_frozen = 1, cycle_version = cycle_version + 1,
            close_reason = NULL, last_transition_at = ?, updated_at = ?
        WHERE id = 1
        """,
        (cycle_id, cycle_id, now_ts, now_ts),
    )
    return n


def run_structural_pipeline(
    *,
    symbols: Optional[List[str]] = None,
    ref_prices_override: Optional[Mapping[str, PricePoint]] = None,
    auto_freeze: Optional[bool] = None,
    params: Optional[StructuralParams] = None,
) -> Dict[str, Any]:
    """
    Один проход: structural_cycles (scanning) → расчёт пула → при успехе armed + freeze в cycle_levels.

    ``cycle_id`` торгового контура = ``structural_cycles.id`` (единый writer freeze).
    """
    init_db()
    run_migrations()
    syms_requested = list(symbols) if symbols is not None else list(TRADING_SYMBOLS)
    syms = syms_requested
    from trading_bot.config import settings as st

    do_freeze = st.STRUCTURAL_AUTO_FREEZE_ON_SCAN if auto_freeze is None else bool(auto_freeze)
    p = params or _default_structural_params()

    now_ts = _now_ts()
    cycle_id = str(uuid.uuid4())
    stage_started = now_ts
    conn = get_connection()
    cur = conn.cursor()
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

    ref_by_symbol: Dict[str, float] = {}
    ref_source = "override"
    if ref_prices_override is not None:
        for s in syms:
            pp = ref_prices_override.get(s)
            if pp is None:
                conn.close()
                raise ValueError(f"ref_prices_override missing symbol {s}")
            ref_by_symbol[s] = float(pp.price)
    elif st.STRUCTURAL_REF_PRICE_SOURCE == "db_1m_close":
        ref_source = "db_1m_close"
        for s in syms:
            row = cur.execute(
                """
                SELECT close FROM ohlcv
                WHERE symbol = ? AND timeframe = '1m'
                ORDER BY timestamp DESC LIMIT 1
                """,
                (s,),
            ).fetchone()
            if row is None or row["close"] is None:
                ref_by_symbol[s] = 0.0
            else:
                ref_by_symbol[s] = float(row["close"])
    else:
        feed = get_price_feed()
        live = feed.get_prices(syms)
        ref_source = "price_feed"
        any_db = False
        for s in syms:
            pp = live.get(s)
            if pp is not None:
                ref_by_symbol[s] = float(pp.price)
            else:
                row = cur.execute(
                    """
                    SELECT close FROM ohlcv
                    WHERE symbol = ? AND timeframe = '1m'
                    ORDER BY timestamp DESC LIMIT 1
                    """,
                    (s,),
                ).fetchone()
                if row is None or row["close"] is None:
                    ref_by_symbol[s] = 0.0
                else:
                    ref_by_symbol[s] = float(row["close"])
                    any_db = True
        if any_db:
            ref_source = "mixed_price_feed_db_1m_close"

    syms = [s for s in syms if ref_by_symbol.get(s, 0) > 0]
    if p.min_pool_symbols > len(syms):
        logger.warning(
            "STRUCTURAL_MIN_POOL_SYMBOLS (%s) больше числа символов с валидной ref-ценой (%s): "
            "успешный пул формально недостижим.",
            p.min_pool_symbols,
            len(syms),
        )
    if not syms:
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
        notify_no_valid_ref_prices(
            ref_source=ref_source,
            symbols_requested=len(syms_requested),
            symbols_with_ref=0,
        )
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
            pool_median_w, pool_mad, pool_k, symbols_valid_count,
            touch_started_at, entry_timer_until, cancel_reason
        )
        VALUES (?, 'scanning', ?, ?, ?, NULL, NULL, ?, NULL, NULL, NULL, NULL)
        """,
        (cycle_id, now_ts, now_ts, json.dumps(_params_dict(p), ensure_ascii=False), p.z_w_ok_threshold),
    )
    _insert_event(cur, cycle_id, "phase_change", now_ts, meta={"to": "scanning"})

    results, pool_stats = compute_structural_symbol_results(cur, syms, ref_by_symbol, p)
    ok_rows = [r for r in results if r.status == "ok"]
    valid_n = len(ok_rows)

    cur.execute(
        """
        UPDATE structural_cycles
        SET updated_at = ?, pool_median_w = ?, pool_mad = ?, pool_k = ?, symbols_valid_count = ?
        WHERE id = ?
        """,
        (
            now_ts,
            pool_stats["pool_median_w"],
            pool_stats["pool_mad"],
            p.z_w_ok_threshold,
            valid_n,
            cycle_id,
        ),
    )
    _persist_symbol_rows(cur, cycle_id, results, now_ts)

    out: Dict[str, Any] = {
        "structural_cycle_id": cycle_id,
        "symbols_ok": valid_n,
        "min_pool_required": p.min_pool_symbols,
        "pool_median_w": pool_stats["pool_median_w"],
        "pool_mad": pool_stats["pool_mad"],
        "pool_median_r": pool_stats.get("pool_median_r", 0.0),
        "pool_mad_r": pool_stats.get("pool_mad_r", 0.0),
        "w_star": pool_stats.get("w_star", 0.0),
        "ref_price_source": ref_source,
    }

    etalon_failed = bool(pool_stats.get("etalon_failed"))
    if etalon_failed:
        cur.execute(
            """
            UPDATE structural_cycles
            SET phase = 'cancelled', updated_at = ?, cancel_reason = ?
            WHERE id = ?
            """,
            (now_ts, "insufficient_etalon", cycle_id),
        )
        _insert_event(
            cur,
            cycle_id,
            "phase_change",
            now_ts,
            meta={"to": "cancelled", "reason": "insufficient_etalon"},
        )
        record_stage_event(
            cur,
            stage="STRUCTURAL_SCAN",
            status="failed",
            cycle_id=cycle_id,
            run_id=cycle_id,
            severity="error",
            message="Insufficient etalon voters (W in band)",
            details={"n_etalon": p.n_etalon},
            started_at=stage_started,
            finished_at=now_ts,
        )
        conn.commit()
        conn.close()
        out["phase"] = "cancelled"
        out["cycle_levels_rows"] = 0
        out["frozen"] = False
        if not os.getenv("PYTEST_CURRENT_TEST"):
            try:
                export_levels_snapshot(cycle_id, out)
            except Exception:
                logger.exception("structural_ops export_levels_snapshot failed")
        return out

    if valid_n < p.min_pool_symbols:
        cur.execute(
            """
            UPDATE structural_cycles
            SET phase = 'cancelled', updated_at = ?, cancel_reason = ?
            WHERE id = ?
            """,
            (now_ts, "insufficient_pool_after_fit", cycle_id),
        )
        _insert_event(
            cur,
            cycle_id,
            "phase_change",
            now_ts,
            meta={"to": "cancelled", "reason": "insufficient_pool_after_fit"},
        )
        record_stage_event(
            cur,
            stage="STRUCTURAL_SCAN",
            status="failed",
            cycle_id=cycle_id,
            run_id=cycle_id,
            severity="error",
            message="Insufficient pool after W* fit",
            details={"symbols_ok": valid_n, "min_pool": p.min_pool_symbols},
            started_at=stage_started,
            finished_at=now_ts,
        )
        conn.commit()
        conn.close()
        out["phase"] = "cancelled"
        out["cycle_levels_rows"] = 0
        out["frozen"] = False
        if not os.getenv("PYTEST_CURRENT_TEST"):
            try:
                export_levels_snapshot(cycle_id, out)
            except Exception:
                logger.exception("structural_ops export_levels_snapshot failed")
        return out

    cur.execute(
        "UPDATE structural_cycles SET phase = 'armed', updated_at = ?, cancel_reason = NULL WHERE id = ?",
        (now_ts, cycle_id),
    )
    _insert_event(cur, cycle_id, "phase_change", now_ts, meta={"to": "armed"})

    if do_freeze:
        n_ins = _freeze_cycle_levels(cur, cycle_id, ok_rows, now_ts, ref_source)
        _insert_event(
            cur,
            cycle_id,
            "phase_change",
            now_ts,
            meta={"action": "freeze", "cycle_levels_rows": n_ins},
        )
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
            details={"cycle_levels_rows": n_ins, "symbols_ok": valid_n},
            started_at=now_ts,
            finished_at=now_ts,
        )
        if st.OPS_STAGE_SHEETS and not os.getenv("PYTEST_CURRENT_TEST"):
            try:
                export_cycle_levels_sheets_snapshot()
            except Exception:
                logger.exception("cycle levels sheets snapshot export failed after structural freeze")
    else:
        out["phase"] = "armed"
        out["cycle_levels_rows"] = 0
        out["frozen"] = False

    record_stage_event(
        cur,
        stage="STRUCTURAL_SCAN",
        status="ok",
        cycle_id=cycle_id,
        run_id=cycle_id,
        message="Structural scan completed",
        details={"symbols_ok": valid_n, "frozen": bool(do_freeze)},
        started_at=stage_started,
        finished_at=now_ts,
    )
    conn.commit()
    conn.close()
    if not os.getenv("PYTEST_CURRENT_TEST"):
        try:
            export_levels_snapshot(cycle_id, out)
        except Exception:
            logger.exception("structural_ops export_levels_snapshot failed")
    return out


def _load_ok_symbol_rows(cur, cycle_id: str) -> List[Dict[str, Any]]:
    rows = cur.execute(
        """
        SELECT
            symbol, level_below_id, level_above_id,
            L_price, U_price, atr, W_atr, mid_price, mid_band_low, mid_band_high,
            ref_price_ws, tier_below, tier_above, volume_peak_below, volume_peak_above
        FROM structural_cycle_symbols
        WHERE cycle_id = ? AND status = 'ok'
        """,
        (cycle_id,),
    ).fetchall()
    out: List[Dict[str, Any]] = []
    for r in rows:
        if (
            r["L_price"] is None
            or r["U_price"] is None
            or r["atr"] is None
            or r["mid_band_low"] is None
            or r["mid_band_high"] is None
        ):
            continue
        out.append(
            {
                "symbol": str(r["symbol"]),
                "level_below_id": int(r["level_below_id"]) if r["level_below_id"] is not None else None,
                "level_above_id": int(r["level_above_id"]) if r["level_above_id"] is not None else None,
                "L_price": float(r["L_price"]),
                "U_price": float(r["U_price"]),
                "atr": float(r["atr"]),
                "W_atr": float(r["W_atr"]) if r["W_atr"] is not None else None,
                "mid_price": float(r["mid_price"]) if r["mid_price"] is not None else None,
                "mid_band_low": float(r["mid_band_low"]),
                "mid_band_high": float(r["mid_band_high"]),
                "ref_price_ws": float(r["ref_price_ws"]) if r["ref_price_ws"] is not None else None,
                "tier_below": r["tier_below"],
                "tier_above": r["tier_above"],
                "volume_peak_below": float(r["volume_peak_below"]) if r["volume_peak_below"] is not None else None,
                "volume_peak_above": float(r["volume_peak_above"]) if r["volume_peak_above"] is not None else None,
            }
        )
    return out


def _record_group_touch_event_state(
    cur,
    *,
    cycle_id: str,
    now_ts: int,
    source: str,
    symbols: Sequence[str],
) -> None:
    cur.execute(
        """
        UPDATE trading_state
        SET last_group_touch_event_ts = ?,
            last_group_touch_cycle_id = ?,
            last_group_touch_source = ?,
            last_group_touch_symbols_json = ?,
            updated_at = ?
        WHERE id = 1
        """,
        (now_ts, cycle_id, source, json.dumps(sorted(set(symbols)), ensure_ascii=False), now_ts),
    )


def _has_recent_group_touch_in_cycle(cur, *, cycle_id: str, now_ts: int) -> bool:
    from trading_bot.config import settings as st

    row = cur.execute(
        """
        SELECT last_group_touch_event_ts, last_group_touch_cycle_id
        FROM trading_state
        WHERE id = 1
        """
    ).fetchone()
    if not row:
        return False
    last_ts = row["last_group_touch_event_ts"]
    last_cycle = row["last_group_touch_cycle_id"]
    if not last_ts or not last_cycle:
        return False
    if str(last_cycle) != cycle_id:
        return False
    return (now_ts - int(last_ts)) <= int(st.STRUCTURAL_GROUP_TOUCH_DEDUP_SEC)


def _collect_recent_mid_touch_symbols(cur, *, cycle_id: str, now_ts: int) -> List[str]:
    from trading_bot.config import settings as st

    lookback = int(st.STRUCTURAL_TOUCH_HISTORY_LOOKBACK_SEC)
    rows = cur.execute(
        """
        SELECT DISTINCT symbol
        FROM structural_events
        WHERE cycle_id = ?
          AND event_type = 'mid_touch'
          AND ts BETWEEN ? AND ?
          AND symbol IS NOT NULL
          AND symbol != ''
        """,
        (cycle_id, now_ts - lookback, now_ts),
    ).fetchall()
    return [str(r["symbol"]) for r in rows]


def _next_prices_from_override(
    tick_idx: int, price_ticks_override: Sequence[Mapping[str, PricePoint]]
) -> Tuple[Dict[str, PricePoint], int]:
    if tick_idx >= len(price_ticks_override):
        return {}, tick_idx
    snap = price_ticks_override[tick_idx]
    out = {str(k): v for k, v in snap.items()}
    return out, tick_idx + 1


def _latest_1m_close(cur, symbol: str) -> Optional[float]:
    row = cur.execute(
        """
        SELECT close FROM ohlcv
        WHERE symbol = ? AND timeframe = '1m'
        ORDER BY timestamp DESC LIMIT 1
        """,
        (symbol,),
    ).fetchone()
    if row is None or row["close"] is None:
        return None
    v = float(row["close"])
    return v if v > 0 else None


def _price_map_for_symbols(
    cur,
    prices_pp: Mapping[str, PricePoint],
    symbols: Sequence[str],
) -> Dict[str, float]:
    """WS/override цена; если нет — последний 1m close из БД."""
    out: Dict[str, float] = {}
    for s in symbols:
        pp = prices_pp.get(s)
        if pp is not None and float(pp.price) > 0:
            out[s] = float(pp.price)
            continue
        fb = _latest_1m_close(cur, s)
        if fb is not None:
            out[s] = fb
    return out


def _collective_breakout_cancel(
    cur,
    *,
    cycle_id: str,
    now_ts: int,
    started_at: int,
    abort_count: int,
) -> None:
    cur.execute(
        """
        UPDATE structural_cycles
        SET phase = 'cancelled', updated_at = ?, cancel_reason = ?
        WHERE id = ?
        """,
        (now_ts, "collective_breakout", cycle_id),
    )
    _insert_event(
        cur,
        cycle_id,
        "phase_change",
        now_ts,
        meta={"to": "cancelled", "reason": "collective_breakout", "abort_count": abort_count},
    )
    record_stage_event(
        cur,
        stage="MID_TOUCH_MONITOR",
        status="failed",
        cycle_id=cycle_id,
        run_id=cycle_id,
        severity="error",
        message="Collective breakout cancellation",
        details={"abort_count": abort_count},
        started_at=started_at,
        finished_at=now_ts,
    )


def run_structural_realtime_cycle(
    *,
    symbols: Optional[List[str]] = None,
    ref_prices_override: Optional[Mapping[str, PricePoint]] = None,
    price_ticks_override: Optional[Sequence[Mapping[str, PricePoint]]] = None,
    params: Optional[StructuralParams] = None,
    force_freeze: bool = True,
) -> Dict[str, Any]:
    """
    Полный structural-контур:
      scanning -> touch_window -> entry_timer -> armed -> freeze
      либо cancelled по abort/timeout.
    """
    from trading_bot.config import settings as st

    base = run_structural_pipeline(
        symbols=symbols,
        ref_prices_override=ref_prices_override,
        auto_freeze=False,
        params=params,
    )
    cycle_id = base.get("structural_cycle_id")
    if not cycle_id or base.get("phase") != "armed":
        base["frozen"] = False
        base["mode"] = "realtime"
        if cycle_id:
            if not os.getenv("PYTEST_CURRENT_TEST"):
                try:
                    export_levels_snapshot(cycle_id, base)
                except Exception:
                    logger.exception("structural_ops export_levels_snapshot failed")
        return base

    init_db()
    run_migrations()
    conn = get_connection()
    cur = conn.cursor()
    now_ts = _now_ts()
    cur.execute(
        "UPDATE structural_cycles SET phase = 'touch_window', updated_at = ? WHERE id = ?",
        (now_ts, cycle_id),
    )
    record_stage_event(
        cur,
        stage="MID_TOUCH_MONITOR",
        status="started",
        cycle_id=cycle_id,
        run_id=cycle_id,
        message="Realtime touch window started",
        details={"symbols": len(base.get("symbols_ok") or []) if isinstance(base.get("symbols_ok"), list) else base.get("symbols_ok")},
        started_at=now_ts,
    )
    _insert_event(cur, cycle_id, "phase_change", now_ts, meta={"to": "touch_window"})
    conn.commit()

    rows = _load_ok_symbol_rows(cur, cycle_id)
    if not rows:
        cur.execute(
            "UPDATE structural_cycles SET phase = 'cancelled', updated_at = ?, cancel_reason = ? WHERE id = ?",
            (_now_ts(), "no_ok_rows_for_touch_window", cycle_id),
        )
        _insert_event(
            cur,
            cycle_id,
            "phase_change",
            _now_ts(),
            meta={"to": "cancelled", "reason": "no_ok_rows_for_touch_window"},
        )
        record_stage_event(
            cur,
            stage="MID_TOUCH_MONITOR",
            status="failed",
            cycle_id=cycle_id,
            run_id=cycle_id,
            severity="error",
            message="No ok rows for touch window",
            started_at=now_ts,
            finished_at=_now_ts(),
        )
        conn.commit()
        conn.close()
        base.update({"phase": "cancelled", "frozen": False, "mode": "realtime"})
        if not os.getenv("PYTEST_CURRENT_TEST"):
            try:
                export_levels_snapshot(cycle_id, base)
            except Exception:
                logger.exception("structural_ops export_levels_snapshot failed")
        return base

    feed = get_price_feed()
    syms = [r["symbol"] for r in rows]
    if price_ticks_override is None:
        feed.start_ws(syms)
    pairs_map = _rows_to_symbol_pairs([dict(r) for r in rows])
    touch_started_at: Optional[int] = None
    entry_timer_until: Optional[int] = None
    tick_idx = 0
    started_at = _now_ts()
    timed_out = False
    w_star_b = base.get("w_star")
    scycle = StructuralCycle(
        cycle_id=str(cycle_id),
        start_time=float(started_at),
        phase="touch_window",
        w_star=float(w_star_b) if w_star_b is not None else None,
        symbols_map=pairs_map,
        trigger_state={},
        trigger_count={},
        last_trigger_time=None,
        current_direction=None,
        trigger_fired=False,
        last_change_time=None,
    )
    bootstrapped = False

    while True:
        now_ts = _now_ts()
        if now_ts - started_at > int(st.STRUCTURAL_MAX_RUNTIME_SEC):
            timed_out = True
            break

        if price_ticks_override is not None:
            prices, tick_idx = _next_prices_from_override(tick_idx, price_ticks_override)
            if not prices:
                timed_out = True
                break
            now_ts = max(int(pp.ts) for pp in prices.values())
        else:
            prices = feed.get_prices(syms)

        price_map = _price_map_for_symbols(cur, prices, syms)

        if not bootstrapped:
            compute_initial_zones(
                scycle,
                price_map,
                edge_atr_frac=float(st.STRUCTURAL_EDGE_ATR_FRAC),
                now_ts=float(now_ts),
            )
            bootstrapped = True
            if not scycle.trigger_fired and fire_if_enough_in_mid(
                scycle, price_map, now_ts=float(now_ts)
            ):
                signal_structural_ready(cur, structural_cycle_id=str(cycle_id), direction="both")
                scycle.phase = "ready_to_enter"
                scycle.trigger_fired = True
                scycle.last_trigger_time = float(now_ts)
                touch_syms = [s for s in pairs_map if scycle.trigger_state.get(s) == "mid"]
                touch_started_at = now_ts
                entry_timer_until = now_ts + int(st.STRUCTURAL_ENTRY_TIMER_SEC)
                _record_group_touch_event_state(
                    cur,
                    cycle_id=cycle_id,
                    now_ts=now_ts,
                    source="initial_mid_cluster",
                    symbols=touch_syms,
                )
                cur.execute(
                    """
                    UPDATE structural_cycles
                    SET phase = 'entry_timer', touch_started_at = ?, entry_timer_until = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (touch_started_at, entry_timer_until, now_ts, cycle_id),
                )
                _insert_event(
                    cur,
                    cycle_id,
                    "phase_change",
                    now_ts,
                    meta={
                        "to": "entry_timer",
                        "source": "initial_mid_cluster",
                        "touch_count": len(touch_syms),
                        "touch_symbols": sorted(touch_syms),
                    },
                )
                for s in touch_syms:
                    px = price_map.get(s)
                    if px is not None:
                        _insert_event(cur, cycle_id, "mid_touch", now_ts, symbol=s, price=float(px))
                conn.commit()
                if price_ticks_override is None:
                    time.sleep(max(0.1, float(st.STRUCTURAL_POLL_SEC)))
                continue

        bdist = float(st.STRUCTURAL_ABORT_DIST_ATR)
        broken_syms = symbols_past_breakout_threshold(
            pairs_map, price_map, breakout_atr_frac=bdist
        )
        for s in broken_syms:
            px = price_map.get(s)
            if px is None:
                continue
            pair = pairs_map.get(s)
            if pair is None:
                continue
            lp = pair.level_below.price
            up = pair.level_above.price
            dist = bdist * pair.atr
            if float(px) <= lp - dist:
                _insert_event(cur, cycle_id, "breakout_lower", now_ts, symbol=s, price=px)
            else:
                _insert_event(cur, cycle_id, "breakout_upper", now_ts, symbol=s, price=px)

        if check_breakout(pairs_map, price_map, breakout_atr_frac=bdist):
            logger.info(
                "Структурный пробой: %s монет за пределами канала (порог отмены %s), цикл отменён",
                len(broken_syms),
                int(st.STRUCTURAL_N_ABORT),
            )
            _collective_breakout_cancel(
                cur,
                cycle_id=cycle_id,
                now_ts=now_ts,
                started_at=started_at,
                abort_count=len(broken_syms),
            )
            conn.commit()
            conn.close()
            base.update({"phase": "cancelled", "frozen": False, "mode": "realtime"})
            if not os.getenv("PYTEST_CURRENT_TEST"):
                try:
                    export_levels_snapshot(cycle_id, base)
                except Exception:
                    logger.exception("structural_ops export_levels_snapshot failed")
            return base

        if touch_started_at is None:
            if not _has_recent_group_touch_in_cycle(cur, cycle_id=cycle_id, now_ts=now_ts):
                touched_syms = _collect_recent_mid_touch_symbols(cur, cycle_id=cycle_id, now_ts=now_ts)
                history_min_n = int(st.STRUCTURAL_TOUCH_HISTORY_MIN_SYMBOLS)
                if len(touched_syms) >= history_min_n:
                    signal_structural_ready(cur, structural_cycle_id=str(cycle_id), direction="both")
                    scycle.phase = "ready_to_enter"
                    scycle.trigger_fired = True
                    scycle.last_trigger_time = float(now_ts)
                    touch_started_at = now_ts
                    entry_timer_until = now_ts + int(st.STRUCTURAL_ENTRY_TIMER_SEC)
                    _record_group_touch_event_state(
                        cur,
                        cycle_id=cycle_id,
                        now_ts=now_ts,
                        source="history_recovered",
                        symbols=touched_syms,
                    )
                    cur.execute(
                        """
                        UPDATE structural_cycles
                        SET phase = 'entry_timer', touch_started_at = ?, entry_timer_until = ?, updated_at = ?
                        WHERE id = ?
                        """,
                        (touch_started_at, entry_timer_until, now_ts, cycle_id),
                    )
                    _insert_event(
                        cur,
                        cycle_id,
                        "phase_change",
                        now_ts,
                        meta={
                            "to": "entry_timer",
                            "source": "history_recovered",
                            "touch_count": len(touched_syms),
                            "touch_symbols": sorted(touched_syms),
                        },
                    )
                    conn.commit()
                    continue

            triggered, _zone, transitioned = update_trigger_counts(
                scycle,
                price_map,
                now_ts=float(now_ts),
                edge_atr_frac=float(st.STRUCTURAL_EDGE_ATR_FRAC),
            )
            cluster_mid = fire_if_enough_in_mid(scycle, price_map, now_ts=float(now_ts))
            group_ready = bool(triggered or cluster_mid)

            for s in transitioned:
                px = price_map.get(s)
                _insert_event(cur, cycle_id, "mid_touch", now_ts, symbol=s, price=px)
            if cluster_mid and not triggered:
                for s in pairs_map:
                    if scycle.trigger_state.get(s) != "mid":
                        continue
                    px = price_map.get(s)
                    if px is None:
                        continue
                    _insert_event(cur, cycle_id, "mid_touch", now_ts, symbol=s, price=float(px))

            if group_ready:
                signal_structural_ready(cur, structural_cycle_id=str(cycle_id), direction="both")
                scycle.phase = "ready_to_enter"
                touch_syms = [s for s in pairs_map if scycle.trigger_state.get(s) == "mid"]
                touch_started_at = now_ts
                entry_timer_until = now_ts + int(st.STRUCTURAL_ENTRY_TIMER_SEC)
                _record_group_touch_event_state(
                    cur,
                    cycle_id=cycle_id,
                    now_ts=now_ts,
                    source="online",
                    symbols=touch_syms,
                )
                cur.execute(
                    """
                    UPDATE structural_cycles
                    SET phase = 'entry_timer', touch_started_at = ?, entry_timer_until = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (touch_started_at, entry_timer_until, now_ts, cycle_id),
                )
                _insert_event(
                    cur,
                    cycle_id,
                    "phase_change",
                    now_ts,
                    meta={"to": "entry_timer", "touch_count": len(touch_syms)},
                )
                conn.commit()
        else:
            if entry_timer_until is not None and now_ts >= entry_timer_until:
                break

        if price_ticks_override is None:
            time.sleep(max(0.1, float(st.STRUCTURAL_POLL_SEC)))

    if timed_out:
        cur.execute(
            "UPDATE structural_cycles SET phase = 'cancelled', updated_at = ?, cancel_reason = ? WHERE id = ?",
            (_now_ts(), "touch_window_timeout", cycle_id),
        )
        _insert_event(
            cur,
            cycle_id,
            "phase_change",
            _now_ts(),
            meta={"to": "cancelled", "reason": "touch_window_timeout"},
        )
        record_stage_event(
            cur,
            stage="MID_TOUCH_MONITOR",
            status="failed",
            cycle_id=cycle_id,
            run_id=cycle_id,
            severity="error",
            message="Touch window timeout",
            started_at=started_at,
            finished_at=_now_ts(),
        )
        conn.commit()
        conn.close()
        base.update({"phase": "cancelled", "frozen": False, "mode": "realtime"})
        if not os.getenv("PYTEST_CURRENT_TEST"):
            try:
                export_levels_snapshot(cycle_id, base)
            except Exception:
                logger.exception("structural_ops export_levels_snapshot failed")
        return base

    now_ts = _now_ts()
    cur.execute(
        "UPDATE structural_cycles SET phase = 'armed', updated_at = ?, cancel_reason = NULL WHERE id = ?",
        (now_ts, cycle_id),
    )
    _insert_event(cur, cycle_id, "phase_change", now_ts, meta={"to": "armed"})

    frozen_rows = 0
    if force_freeze:
        persisted: List[StructuralSymbolResult] = []
        for rr in rows:
            ref = rr["ref_price_ws"] if rr["ref_price_ws"] is not None else rr["mid_price"]
            if ref is None:
                ref = (rr["L_price"] + rr["U_price"]) / 2.0
            persisted.append(
                StructuralSymbolResult(
                    symbol=rr["symbol"],
                    status="ok",
                    level_below_id=rr["level_below_id"],
                    level_above_id=rr["level_above_id"],
                    L_price=rr["L_price"],
                    U_price=rr["U_price"],
                    atr=rr["atr"],
                    W_atr=rr["W_atr"],
                    mid_price=rr["mid_price"],
                    mid_band_low=rr["mid_band_low"],
                    mid_band_high=rr["mid_band_high"],
                    ref_price=float(ref),
                    tier_below=rr["tier_below"],
                    tier_above=rr["tier_above"],
                    volume_peak_below=rr["volume_peak_below"],
                    volume_peak_above=rr["volume_peak_above"],
                )
            )
        frozen_rows = _freeze_cycle_levels(cur, cycle_id, persisted, now_ts, "structural_snapshot")
        _insert_event(cur, cycle_id, "phase_change", now_ts, meta={"action": "freeze", "cycle_levels_rows": frozen_rows})
        record_stage_event(
            cur,
            stage="STRUCTURAL_FREEZE",
            status="ok",
            cycle_id=cycle_id,
            run_id=cycle_id,
            message="Realtime freeze completed",
            details={"cycle_levels_rows": frozen_rows},
            started_at=now_ts,
            finished_at=now_ts,
        )
        if st.OPS_STAGE_SHEETS and not os.getenv("PYTEST_CURRENT_TEST"):
            try:
                export_cycle_levels_sheets_snapshot()
            except Exception:
                logger.exception("cycle levels sheets snapshot export failed after realtime freeze")

    record_stage_event(
        cur,
        stage="MID_TOUCH_MONITOR",
        status="ok",
        cycle_id=cycle_id,
        run_id=cycle_id,
        message="Touch window completed",
        started_at=started_at,
        finished_at=now_ts,
    )
    conn.commit()
    conn.close()
    base.update({"phase": "armed", "frozen": bool(force_freeze), "cycle_levels_rows": frozen_rows, "mode": "realtime"})
    if not os.getenv("PYTEST_CURRENT_TEST"):
        try:
            export_levels_snapshot(cycle_id, base)
        except Exception:
            logger.exception("structural_ops export_levels_snapshot failed")
    return base
