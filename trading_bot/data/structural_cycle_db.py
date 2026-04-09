"""Structural cycle: запись structural_* и freeze → cycle_levels + trading_state."""

from __future__ import annotations

import json
import time
import uuid
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from trading_bot.analytics.structural_cycle import (
    StructuralParams,
    StructuralSymbolResult,
    compute_structural_symbol_results,
)
from trading_bot.config.settings import STRUCTURAL_ALLOWED_LEVEL_TYPES
from trading_bot.config.symbols import TRADING_SYMBOLS
from trading_bot.data.db import get_connection
from trading_bot.data.schema import init_db, run_migrations
from trading_bot.tools.price_feed import PricePoint, get_price_feed


def _now_ts() -> int:
    return int(time.time())


def _default_structural_params() -> StructuralParams:
    from trading_bot.config import settings as st

    return StructuralParams(
        min_candidates_per_side=st.STRUCTURAL_MIN_CANDIDATES_PER_SIDE,
        top_k=st.STRUCTURAL_TOP_K,
        mad_k=st.STRUCTURAL_MAD_K,
        center_filter_enabled=st.STRUCTURAL_CENTER_FILTER_ENABLED,
        center_mad_k=st.STRUCTURAL_CENTER_MAD_K,
        target_align_enabled=st.STRUCTURAL_TARGET_ALIGN_ENABLED,
        anchor_symbols=tuple(st.STRUCTURAL_ANCHOR_SYMBOLS),
        target_w_band_k=st.STRUCTURAL_TARGET_W_BAND_K,
        target_center_weight=st.STRUCTURAL_TARGET_CENTER_WEIGHT,
        target_width_weight=st.STRUCTURAL_TARGET_WIDTH_WEIGHT,
        min_pool_symbols=st.STRUCTURAL_MIN_POOL_SYMBOLS,
        mid_band_pct=st.STRUCTURAL_MID_BAND_PCT,
        refine_max_rounds=st.STRUCTURAL_REFINE_MAX_ROUNDS,
        allowed_level_types=tuple(STRUCTURAL_ALLOWED_LEVEL_TYPES),
    )


def _params_dict(p: StructuralParams) -> Dict[str, Any]:
    return {
        "min_candidates_per_side": p.min_candidates_per_side,
        "top_k": p.top_k,
        "mad_k": p.mad_k,
        "center_filter_enabled": p.center_filter_enabled,
        "center_mad_k": p.center_mad_k,
        "target_align_enabled": p.target_align_enabled,
        "anchor_symbols": list(p.anchor_symbols),
        "target_w_band_k": p.target_w_band_k,
        "target_center_weight": p.target_center_weight,
        "target_width_weight": p.target_width_weight,
        "min_pool_symbols": p.min_pool_symbols,
        "mid_band_pct": p.mid_band_pct,
        "refine_max_rounds": p.refine_max_rounds,
        "allowed_level_types": list(p.allowed_level_types),
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
    syms = list(symbols) if symbols is not None else list(TRADING_SYMBOLS)
    from trading_bot.config import settings as st

    do_freeze = st.STRUCTURAL_AUTO_FREEZE_ON_SCAN if auto_freeze is None else bool(auto_freeze)
    p = params or _default_structural_params()

    now_ts = _now_ts()
    cycle_id = str(uuid.uuid4())
    conn = get_connection()
    cur = conn.cursor()

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
    if not syms:
        conn.close()
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
        (cycle_id, now_ts, now_ts, json.dumps(_params_dict(p), ensure_ascii=False), p.mad_k),
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
            p.mad_k,
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
        "ref_price_source": ref_source,
    }

    if valid_n < p.min_pool_symbols:
        cur.execute(
            """
            UPDATE structural_cycles
            SET phase = 'cancelled', updated_at = ?, cancel_reason = ?
            WHERE id = ?
            """,
            (now_ts, "insufficient_pool_after_mad", cycle_id),
        )
        _insert_event(
            cur,
            cycle_id,
            "phase_change",
            now_ts,
            meta={"to": "cancelled", "reason": "insufficient_pool_after_mad"},
        )
        conn.commit()
        conn.close()
        out["phase"] = "cancelled"
        out["cycle_levels_rows"] = 0
        out["frozen"] = False
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
    else:
        out["phase"] = "armed"
        out["cycle_levels_rows"] = 0
        out["frozen"] = False

    conn.commit()
    conn.close()
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


def _next_prices_from_override(
    tick_idx: int, price_ticks_override: Sequence[Mapping[str, PricePoint]]
) -> Tuple[Dict[str, PricePoint], int]:
    if tick_idx >= len(price_ticks_override):
        return {}, tick_idx
    snap = price_ticks_override[tick_idx]
    out = {str(k): v for k, v in snap.items()}
    return out, tick_idx + 1


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
        conn.commit()
        conn.close()
        base.update({"phase": "cancelled", "frozen": False, "mode": "realtime"})
        return base

    feed = get_price_feed()
    syms = [r["symbol"] for r in rows]
    touch_times: Dict[str, int] = {}
    last_touch_emit_ts: Dict[str, int] = {}
    touch_started_at: Optional[int] = None
    entry_timer_until: Optional[int] = None
    tick_idx = 0
    started_at = _now_ts()
    timed_out = False

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

        abort_syms: set[str] = set()
        for r in rows:
            s = r["symbol"]
            pp = prices.get(s)
            if pp is None:
                continue
            px = float(pp.price)
            if r["mid_band_low"] <= px <= r["mid_band_high"]:
                prev_emit = last_touch_emit_ts.get(s, 0)
                if now_ts - prev_emit >= int(st.STRUCTURAL_TOUCH_DEBOUNCE_SEC):
                    touch_times[s] = now_ts
                    last_touch_emit_ts[s] = now_ts
                    _insert_event(cur, cycle_id, "mid_touch", now_ts, symbol=s, price=px)
            lower_abort = r["L_price"] - float(st.STRUCTURAL_ABORT_DIST_ATR) * r["atr"]
            upper_abort = r["U_price"] + float(st.STRUCTURAL_ABORT_DIST_ATR) * r["atr"]
            if px <= lower_abort:
                abort_syms.add(s)
                _insert_event(cur, cycle_id, "breakout_lower", now_ts, symbol=s, price=px)
            elif px >= upper_abort:
                abort_syms.add(s)
                _insert_event(cur, cycle_id, "breakout_upper", now_ts, symbol=s, price=px)

        if touch_started_at is None:
            touch_times = {
                s: ts
                for s, ts in touch_times.items()
                if now_ts - ts <= int(st.STRUCTURAL_TOUCH_WINDOW_SEC)
            }
            if len(touch_times) >= int(st.STRUCTURAL_N_TOUCH):
                touch_started_at = now_ts
                entry_timer_until = now_ts + int(st.STRUCTURAL_ENTRY_TIMER_SEC)
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
                    meta={"to": "entry_timer", "touch_count": len(touch_times)},
                )
                conn.commit()
        else:
            if len(abort_syms) >= int(st.STRUCTURAL_N_ABORT):
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
                    meta={"to": "cancelled", "reason": "collective_breakout", "abort_count": len(abort_syms)},
                )
                conn.commit()
                conn.close()
                base.update({"phase": "cancelled", "frozen": False, "mode": "realtime"})
                return base
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
        conn.commit()
        conn.close()
        base.update({"phase": "cancelled", "frozen": False, "mode": "realtime"})
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

    conn.commit()
    conn.close()
    base.update({"phase": "armed", "frozen": bool(force_freeze), "cycle_levels_rows": frozen_rows, "mode": "realtime"})
    return base
