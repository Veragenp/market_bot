"""
Structural cycle – независимые наборы long/short уровней (v4).
Без эталонов, без парности long+short на одном тикере.
"""

from __future__ import annotations

import logging
import statistics
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

from trading_bot.config import settings as settings_pkg
from trading_bot.data.db import get_connection
from trading_bot.data.repositories import get_instruments_atr_bybit_futures_cur

logger = logging.getLogger(__name__)


def _symbol_list_preview(symbols: Sequence[str], limit: int = 14) -> str:
    lst = [s for s in symbols if s]
    if not lst:
        return ""
    head = ", ".join(lst[:limit])
    return f" ({head}{' …' if len(lst) > limit else ''})"


# ---------------------------------------------------------------------------
# Параметры скана (v4)
# ---------------------------------------------------------------------------
@dataclass
class StructuralParams:
    min_candidates_per_side: int
    top_k: int
    min_pool_symbols: int
    w_min: float
    w_max: float
    allowed_level_types: Tuple[str, ...]
    strength_first_enabled: bool
    # Ниже – только для диагностики, не влияет на торговый контур
    mid_band_pct: float


@dataclass
class StructuralSymbolResult:
    symbol: str
    status: str
    level_below_id: Optional[int]   # для long
    level_above_id: Optional[int]   # для short
    L_price: Optional[float]
    U_price: Optional[float]
    atr: Optional[float]
    W_atr: Optional[float]          # ширина, если есть оба уровня (только для отчёта)
    mid_price: Optional[float]
    mid_band_low: Optional[float]
    mid_band_high: Optional[float]
    ref_price: float
    tier_below: Optional[str]
    tier_above: Optional[str]
    volume_peak_below: Optional[float]
    volume_peak_above: Optional[float]


@dataclass
class StrongLevel:
    id: int
    price: float
    volume_peak: float
    strength: float
    tier: str
    level_type: str


# ---------------------------------------------------------------------------
# Вспомогательные функции
# ---------------------------------------------------------------------------
def _level_rank_key(lvl: StrongLevel) -> Tuple[float, int, int]:
    """Ранжирование: volume_peak, manual_global_hvn бонус, id."""
    manual_bonus = 1 if str(lvl.level_type or "") == "manual_global_hvn" else 0
    return (float(lvl.volume_peak or 0.0), manual_bonus, int(lvl.id))


def _band_bounds(ref_price: float, atr: float, direction: str) -> Tuple[float, float]:
    """Границы ATR-полосы для поиска уровней."""
    d_min = float(settings_pkg.STRUCTURAL_V4_BAND_MIN_ATR)
    d_max = float(settings_pkg.STRUCTURAL_V4_BAND_MAX_ATR)
    if direction == "long":   # ищем уровни ниже ref
        return ref_price - d_max * atr, ref_price - d_min * atr
    else:                     # ищем уровни выше ref
        return ref_price + d_min * atr, ref_price + d_max * atr


def _fetch_top_levels(
    cur,
    symbol: str,
    ref_price: float,
    atr: float,
    direction: str,
    types: Sequence[str],
    k: int,
) -> List[StrongLevel]:
    """Топ-K сильнейших уровней в ATR-полосе."""
    if not types or k <= 0 or atr <= 0:
        return []
    lo, hi = _band_bounds(ref_price, atr, direction)
    ph = ",".join("?" * len(types))
    logger.debug(
        "Fetching %s levels for %s: ref=%.4f atr=%.4f range=[%.4f, %.4f]",
        direction, symbol, ref_price, atr, lo, hi
    )
    rows = cur.execute(
        f"""
        SELECT id, price, volume_peak, strength, tier, level_type
        FROM price_levels
        WHERE symbol = ?
          AND is_active = 1
          AND status = 'active'
          AND level_type IN ({ph})
          AND price BETWEEN ? AND ?
        ORDER BY COALESCE(volume_peak, strength, 0) DESC, id DESC
        LIMIT ?
        """,
        (symbol, *types, lo, hi, k),
    ).fetchall()
    out = [
        StrongLevel(
            id=int(r[0]),
            price=float(r[1]),
            volume_peak=float(r[2] or 0),
            strength=float(r[3] or 0),
            tier=r[4] or "",
            level_type=r[5],
        )
        for r in rows
    ]
    out.sort(key=_level_rank_key, reverse=True)
    out = out[:k]
    if out:
        logger.debug(
            "Found %d %s levels for %s: best price=%.4f vol=%.2f",
            len(out), direction, symbol, out[0].price, out[0].volume_peak
        )
    else:
        logger.debug("No %s levels found for %s in range", direction, symbol)
    return out


def _pick_best_opposite_level(
    candidates: Sequence[StrongLevel],
    anchor_price: float,
    atr: float,
    lo: float,
    hi: float,
    opposite_side: str,
) -> Optional[StrongLevel]:
    """Выбрать уровень противоположной стороны в расширенном диапазоне (по объёму)."""
    if atr <= 0:
        return None
    best: Optional[StrongLevel] = None
    best_vol = float("-inf")
    for lvl in candidates:
        if opposite_side == "above":
            w = (lvl.price - anchor_price) / atr
        else:
            w = (anchor_price - lvl.price) / atr
        if lo <= w <= hi:
            vol = float(lvl.volume_peak)
            if vol > best_vol:
                best = lvl
                best_vol = vol
    if best:
        logger.debug("Picked opposite level: price=%.4f vol=%.2f w=%.2f", best.price, best.volume_peak, 
                     (best.price - anchor_price)/atr if opposite_side=="above" else (anchor_price - best.price)/atr)
    return best


def _build_row(
    symbol: str,
    status: str,
    ref_price: float,
    atr: Optional[float],
    mid_pct: float,
    level_below: Optional[StrongLevel] = None,
    level_above: Optional[StrongLevel] = None,
) -> StructuralSymbolResult:
    """Создать результат для одного символа (независимо по сторонам)."""
    if atr is None or atr <= 0:
        return StructuralSymbolResult(
            symbol=symbol,
            status="incomplete_structure" if status in ("ok", "partial") else status,
            level_below_id=None,
            level_above_id=None,
            L_price=None,
            U_price=None,
            atr=atr,
            W_atr=None,
            mid_price=None,
            mid_band_low=None,
            mid_band_high=None,
            ref_price=ref_price,
            tier_below=None,
            tier_above=None,
            volume_peak_below=None,
            volume_peak_above=None,
        )
    lp = level_below.price if level_below else None
    up = level_above.price if level_above else None
    w_atr = None
    mid = None
    mlo = None
    mhi = None
    if lp is not None and up is not None:
        w_atr = (up - lp) / atr
        mid = (lp + up) / 2.0
        half = (mid_pct / 100.0) * (up - lp) / 2.0
        mlo = mid - half
        mhi = mid + half
    return StructuralSymbolResult(
        symbol=symbol,
        status=status,
        level_below_id=level_below.id if level_below else None,
        level_above_id=level_above.id if level_above else None,
        L_price=lp,
        U_price=up,
        atr=atr,
        W_atr=w_atr,
        mid_price=mid,
        mid_band_low=mlo,
        mid_band_high=mhi,
        ref_price=ref_price,
        tier_below=level_below.tier if level_below else None,
        tier_above=level_above.tier if level_above else None,
        volume_peak_below=level_below.volume_peak if level_below else None,
        volume_peak_above=level_above.volume_peak if level_above else None,
    )


# ---------------------------------------------------------------------------
# Основной отбор (независимые long/short)
# ---------------------------------------------------------------------------
def compute_structural_symbol_results(
    cur,
    symbols: Sequence[str],
    ref_by_symbol: Dict[str, float],
    params: StructuralParams,
    require_pair: bool = False,
) -> Tuple[List[StructuralSymbolResult], Dict[str, float]]:
    """
    Отбор уровней для структурного цикла.
    """
    types = tuple(params.allowed_level_types) or tuple(settings_pkg.STRUCTURAL_ALLOWED_LEVEL_TYPES)
    k = params.top_k

    logger.info("=" * 80)
    logger.info("STRUCTURAL CYCLE: SYMBOL-LEVEL SELECTION STARTED")
    logger.info("=" * 80)
    logger.info("PARAMS: require_pair=%s, top_k=%d, allowed_types=%s", require_pair, k, types)
    logger.info("BAND: min=%.2f ATR, max=%.2f ATR", 
                float(settings_pkg.STRUCTURAL_V4_BAND_MIN_ATR),
                float(settings_pkg.STRUCTURAL_V4_BAND_MAX_ATR))

    if not require_pair:
        # НЕЗАВИСИМЫЙ ОТБОР
        results: List[StructuralSymbolResult] = []
        no_atr_symbols: List[str] = []
        no_level_symbols: List[str] = []

        for symbol in symbols:
            ref = float(ref_by_symbol[symbol])
            atr = get_instruments_atr_bybit_futures_cur(cur, symbol)
            if atr is None or atr <= 0:
                no_atr_symbols.append(symbol)
                logger.warning("Symbol %s: NO ATR AVAILABLE (ATR is None or <= 0)", symbol)
                results.append(_build_row(symbol, "incomplete_structure", ref, atr, params.mid_band_pct))
                continue

            below = _fetch_top_levels(cur, symbol, ref, atr, "long", types, k)
            above = _fetch_top_levels(cur, symbol, ref, atr, "short", types, k)

            logger.info(
                "Symbol %s: ref=%.4f atr=%.4f | below=%d above=%d",
                symbol, ref, atr, len(below), len(above)
            )

            if not below and not above:
                no_level_symbols.append(symbol)
                logger.warning(
                    "Symbol %s: NO LEVELS IN BAND | below_range=[%.4f, %.4f] above_range=[%.4f, %.4f]",
                    symbol,
                    ref - float(settings_pkg.STRUCTURAL_V4_BAND_MAX_ATR) * atr,
                    ref - float(settings_pkg.STRUCTURAL_V4_BAND_MIN_ATR) * atr,
                    ref + float(settings_pkg.STRUCTURAL_V4_BAND_MIN_ATR) * atr,
                    ref + float(settings_pkg.STRUCTURAL_V4_BAND_MAX_ATR) * atr,
                )
                results.append(_build_row(symbol, "incomplete_structure", ref, atr, params.mid_band_pct))
                continue

            row = _build_row(
                symbol,
                "ok",
                ref,
                atr,
                params.mid_band_pct,
                level_below=below[0] if below else None,
                level_above=above[0] if above else None,
            )
            results.append(row)
            logger.info(
                ">>> Symbol %s SELECTED: L=%.4f (tier=%s vol=%.2f) U=%.4f (tier=%s vol=%.2f) atr=%.4f",
                symbol,
                row.L_price if row.L_price else 0,
                row.tier_below or "None",
                row.volume_peak_below or 0,
                row.U_price if row.U_price else 0,
                row.tier_above or "None",
                row.volume_peak_above or 0,
            )

        logger.info("=" * 80)
        logger.info(
            "STRUCTURAL CYCLE: SELECTION SUMMARY | total=%d no_atr=%d no_level_in_band=%d selected=%d",
            len(symbols), len(no_atr_symbols), len(no_level_symbols),
            len([r for r in results if r.status == "ok"])
        )
        if no_atr_symbols:
            logger.warning("NO ATR symbols (%d): %s", len(no_atr_symbols), ", ".join(no_atr_symbols[:10]))
        if no_level_symbols:
            logger.warning("NO LEVELS symbols (%d): %s", len(no_level_symbols), ", ".join(no_level_symbols[:10]))
        logger.info("=" * 80)

        # Разделение на LONG и SHORT
        long_syms = [r for r in results if r.L_price is not None]
        short_syms = [r for r in results if r.U_price is not None]
        both_syms = [r for r in results if r.L_price is not None and r.U_price is not None]
        long_only = [r for r in results if r.L_price is not None and r.U_price is None]
        short_only = [r for r in results if r.L_price is None and r.U_price is not None]

        logger.info("LONG levels: %d symbols", len(long_syms))
        for r in sorted(long_syms, key=lambda x: float(x.volume_peak_below or 0), reverse=True)[:10]:
            logger.info("  LONG: %s price=%.4f tier=%s vol=%.2f dist=%.2f ATR",
                       r.symbol, r.L_price, r.tier_below, r.volume_peak_below,
                       (r.ref_price - r.L_price) / r.atr if r.atr else 0)

        logger.info("SHORT levels: %d symbols", len(short_syms))
        for r in sorted(short_syms, key=lambda x: float(x.volume_peak_above or 0), reverse=True)[:10]:
            logger.info("  SHORT: %s price=%.4f tier=%s vol=%.2f dist=%.2f ATR",
                       r.symbol, r.U_price, r.tier_above, r.volume_peak_above,
                       (r.U_price - r.ref_price) / r.atr if r.atr else 0)

        logger.info("BOTH sides (paired): %d symbols", len(both_syms))
        logger.info("LONG only: %d symbols", len(long_only))
        logger.info("SHORT only: %d symbols", len(short_only))

        logger.info(
            "Structural v4 selection (independent): symbols=%s no_atr=%s%s no_level_in_band=%s%s",
            len(symbols),
            len(no_atr_symbols),
            _symbol_list_preview(no_atr_symbols),
            len(no_level_symbols),
            _symbol_list_preview(no_level_symbols),
        )

        pool_stats = {
            "pool_median_w": 0.0,
            "pool_mad": 0.0,
            "pool_median_r": 0.0,
            "pool_mad_r": 0.0,
            "w_star": 0.0,
            "etalon_failed": 0,
        }
        return results, pool_stats

    # ПАРНЫЙ ОТБОР (не используется в основном пайплайне, но оставлен)
    results: List[StructuralSymbolResult] = []
    no_atr_symbols: List[str] = []
    no_level_symbols: List[str] = []
    all_widths: List[float] = []

    for symbol in symbols:
        ref = float(ref_by_symbol[symbol])
        atr = get_instruments_atr_bybit_futures_cur(cur, symbol)
        if atr is None or atr <= 0:
            no_atr_symbols.append(symbol)
            results.append(_build_row(symbol, "incomplete_structure", ref, atr, params.mid_band_pct))
            continue

        below = _fetch_top_levels(cur, symbol, ref, atr, "long", types, k)
        above = _fetch_top_levels(cur, symbol, ref, atr, "short", types, k)

        if not below or not above:
            no_level_symbols.append(symbol)
            results.append(_build_row(symbol, "incomplete_structure", ref, atr, params.mid_band_pct))
            continue

        row = _build_row(
            symbol,
            "ok",
            ref,
            atr,
            params.mid_band_pct,
            level_below=below[0],
            level_above=above[0],
        )
        results.append(row)
        all_widths.append(float(row.W_atr) if row.W_atr is not None else (row.U_price - row.L_price) / atr)

    logger.info(
        "Structural v4 selection (pair required): symbols=%s no_atr=%s%s no_level_in_band=%s%s two_sided=%s",
        len(symbols),
        len(no_atr_symbols),
        _symbol_list_preview(no_atr_symbols),
        len(no_level_symbols),
        _symbol_list_preview(no_level_symbols),
        len([r for r in results if r.L_price is not None and r.U_price is not None]),
    )

    pool_stats = {
        "pool_median_w": 0.0,
        "pool_mad": 0.0,
        "pool_median_r": 0.0,
        "pool_mad_r": 0.0,
        "w_star": 0.0,
        "etalon_failed": 0,
    }
    return results, pool_stats


# ---------------------------------------------------------------------------
# Rebuild противоположной стороны (используется только при флипе)
# ---------------------------------------------------------------------------
def _rebuild_side_test_mode(
    cur,
    cycle_id: str,
    target_direction: str,
    prices: Optional[Dict[str, float]] = None,
) -> bool:
    """
    TEST MODE: упрощённый rebuild противоположной стороны.
    Использует смещение от текущей цены: offset * ATR.
    """
    from trading_bot.analytics.test_level_generator import _get_tick_size, _round_to_tick
    
    if target_direction not in ("long", "short"):
        return False

    try:
        # Получить offset из настроек
        offset = settings_pkg.TEST_OPPOSITE_OFFSET_ATR
        
        # Получить символы цикла
        srows = cur.execute(
            """
            SELECT symbol, atr
            FROM structural_cycle_symbols
            WHERE cycle_id = ? AND status = 'ok'
            """,
            (cycle_id,),
        ).fetchall()
        
        if not srows:
            logger.warning("TEST rebuild: no symbols for cycle %s", cycle_id)
            return False

        now_ts = int(time.time())
        updated = 0

        for r in srows:
            sym = str(r["symbol"])
            atr = float(r["atr"])
            if atr <= 0:
                continue

            # Получить текущую цену
            current_price = (prices or {}).get(sym)
            if current_price is None:
                # Fallback: последняя цена из БД
                px_row = cur.execute(
                    "SELECT close FROM ohlcv WHERE symbol = ? AND timeframe = '1m' ORDER BY timestamp DESC LIMIT 1",
                    (sym,),
                ).fetchone()
                if px_row and px_row["close"]:
                    current_price = float(px_row["close"])
                else:
                    continue
            
            # Расчёт уровня
            if target_direction == "long":
                level_price = current_price - offset * atr
            else:
                level_price = current_price + offset * atr
            
            # ROUND to tick size
            tick = _get_tick_size(sym)
            if tick:
                level_price = _round_to_tick(level_price, tick)
            
            # Обновить structural_cycle_symbols
            if target_direction == "long":
                cur.execute(
                    """
                    UPDATE structural_cycle_symbols
                    SET L_price = ?, W_atr = ?, evaluated_at = ?
                    WHERE cycle_id = ? AND symbol = ?
                    """,
                    (level_price, offset, now_ts, cycle_id, sym),
                )
            else:
                cur.execute(
                    """
                    UPDATE structural_cycle_symbols
                    SET U_price = ?, W_atr = ?, evaluated_at = ?
                    WHERE cycle_id = ? AND symbol = ?
                    """,
                    (level_price, offset, now_ts, cycle_id, sym),
                )
            
            # Обновить cycle_levels
            cur.execute(
                """
                UPDATE cycle_levels
                SET level_price = ?, updated_at = ?, is_active = 1, status = 'active'
                WHERE cycle_id = ? AND symbol = ? AND direction = ?
                """,
                (level_price, now_ts, cycle_id, sym, target_direction),
            )
            
            updated += 1
            logger.info(
                "TEST rebuild: %s side for %s: %.2f (offset=%.2f*ATR=%.2f)",
                target_direction.upper(), sym, level_price, offset, atr
            )
        
        if updated == 0:
            logger.warning("TEST rebuild: no symbols updated")
            return False

        logger.info("TEST rebuild: successfully updated %d symbols for %s", updated, target_direction)
        return True
        
    except Exception as e:
        logger.exception("TEST rebuild failed: %s", e)
        return False


def rebuild_side_on_cursor(
    cur,
    cycle_id: str,
    target_direction: str,
    prices: Optional[Dict[str, float]] = None,
) -> bool:
    """
    Перестраивает уровни для заданной стороны (target_direction) на основе текущих цен.
    """
    if target_direction not in ("long", "short"):
        logger.warning("rebuild_side_on_cursor: invalid direction %s", target_direction)
        return False

    # TEST MODE: использовать упрощённый алгоритм
    if settings_pkg.TEST_MODE:
        return _rebuild_side_test_mode(cur, cycle_id, target_direction, prices)

    cur.execute("SAVEPOINT sp_rebuild_side")
    try:
        srows = cur.execute(
            """
            SELECT symbol, level_below_id, level_above_id,
                   L_price, U_price, atr, ref_price_ws
            FROM structural_cycle_symbols
            WHERE cycle_id = ? AND status = 'ok'
            """,
            (cycle_id,),
        ).fetchall()
        if not srows:
            logger.warning("rebuild_side_on_cursor: no ok rows for cycle %s", cycle_id)
            cur.execute("ROLLBACK TO SAVEPOINT sp_rebuild_side")
            cur.execute("RELEASE SAVEPOINT sp_rebuild_side")
            return False

        types = list(settings_pkg.STRUCTURAL_SETTINGS.ALLOWED_LEVEL_TYPES)
        top_k = settings_pkg.STRUCTURAL_SETTINGS.TOP_K_PER_SIDE
        band_mult = float(getattr(settings_pkg, "STRUCTURAL_OPPOSITE_REBUILD_BAND_MULT", 1.0))
        lo = float(settings_pkg.STRUCTURAL_W_MIN) * band_mult
        hi = float(settings_pkg.STRUCTURAL_W_MAX) * band_mult
        now_ts = int(time.time())
        updated = 0

        def _anchor_with_fallback(sym: str, anchor_value: Optional[float]) -> Optional[float]:
            if anchor_value is not None and float(anchor_value) > 0:
                return float(anchor_value)
            px = (prices or {}).get(sym)
            if px is not None and float(px) > 0:
                return float(px)
            row_px = cur.execute(
                "SELECT close FROM ohlcv WHERE symbol = ? AND timeframe = '1m' ORDER BY timestamp DESC LIMIT 1",
                (sym,),
            ).fetchone()
            if not row_px or row_px["close"] is None:
                return None
            v = float(row_px["close"])
            return v if v > 0 else None

        for r in srows:
            sym = str(r["symbol"])
            atr = float(r["atr"])
            if atr <= 0:
                continue

            if target_direction == "long":
                anchor = _anchor_with_fallback(sym, r["U_price"])
                if anchor is None:
                    continue
                ph = ",".join("?" * len(types))
                rows = cur.execute(
                    f"""
                    SELECT id, price, volume_peak, strength, tier, level_type
                    FROM price_levels
                    WHERE symbol = ?
                      AND is_active = 1
                      AND status = 'active'
                      AND level_type IN ({ph})
                      AND price < ?
                    ORDER BY COALESCE(volume_peak, strength, 0) DESC, id DESC
                    LIMIT ?
                    """,
                    (sym, *types, anchor, max(top_k * 5, top_k)),
                ).fetchall()
                cands = [
                    StrongLevel(
                        id=int(rr[0]), price=float(rr[1]), volume_peak=float(rr[2] or 0),
                        strength=float(rr[3] or 0), tier=rr[4] or "", level_type=rr[5] or "",
                    )
                    for rr in rows
                ]
                best = _pick_best_opposite_level(cands, anchor, atr, lo, hi, "below")
                if not best:
                    continue
                cur.execute(
                    """
                    UPDATE structural_cycle_symbols
                    SET level_below_id = ?, L_price = ?, W_atr = ?, tier_below = ?,
                        volume_peak_below = ?, evaluated_at = ?
                    WHERE cycle_id = ? AND symbol = ?
                    """,
                    (best.id, best.price, (anchor - best.price) / atr, best.tier,
                     best.volume_peak, now_ts, cycle_id, sym),
                )
                updated += 1
                logger.info("Rebuilt long side for %s: new L=%.4f (tier=%s vol=%.2f)", sym, best.price, best.tier, best.volume_peak)
            else:  # short
                anchor = _anchor_with_fallback(sym, r["L_price"])
                if anchor is None:
                    continue
                ph = ",".join("?" * len(types))
                rows = cur.execute(
                    f"""
                    SELECT id, price, volume_peak, strength, tier, level_type
                    FROM price_levels
                    WHERE symbol = ?
                      AND is_active = 1
                      AND status = 'active'
                      AND level_type IN ({ph})
                      AND price > ?
                    ORDER BY COALESCE(volume_peak, strength, 0) DESC, id DESC
                    LIMIT ?
                    """,
                    (sym, *types, anchor, max(top_k * 5, top_k)),
                ).fetchall()
                cands = [
                    StrongLevel(
                        id=int(rr[0]), price=float(rr[1]), volume_peak=float(rr[2] or 0),
                        strength=float(rr[3] or 0), tier=rr[4] or "", level_type=rr[5] or "",
                    )
                    for rr in rows
                ]
                best = _pick_best_opposite_level(cands, anchor, atr, lo, hi, "above")
                if not best:
                    continue
                cur.execute(
                    """
                    UPDATE structural_cycle_symbols
                    SET level_above_id = ?, U_price = ?, W_atr = ?, tier_above = ?,
                        volume_peak_above = ?, evaluated_at = ?
                    WHERE cycle_id = ? AND symbol = ?
                    """,
                    (best.id, best.price, (best.price - anchor) / atr, best.tier,
                     best.volume_peak, now_ts, cycle_id, sym),
                )
                updated += 1
                logger.info("Rebuilt short side for %s: new U=%.4f (tier=%s vol=%.2f)", sym, best.price, best.tier, best.volume_peak)

        if updated < settings_pkg.STRUCTURAL_SETTINGS.N_TRIGGER:
            logger.error("rebuild_side_on_cursor: only %s symbols updated, need %s", updated, settings_pkg.STRUCTURAL_SETTINGS.N_TRIGGER)
            cur.execute("ROLLBACK TO SAVEPOINT sp_rebuild_side")
            cur.execute("RELEASE SAVEPOINT sp_rebuild_side")
            return False

        cur.execute("RELEASE SAVEPOINT sp_rebuild_side")
        logger.info("rebuild_side_on_cursor: successfully updated %d symbols for %s", updated, target_direction)
        return True
    except Exception:
        logger.exception("rebuild_side_on_cursor failed")
        cur.execute("ROLLBACK TO SAVEPOINT sp_rebuild_side")
        cur.execute("RELEASE SAVEPOINT sp_rebuild_side")
        return False