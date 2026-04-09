"""Расчёт структурного пула: пара (L,U) из price_levels, W_i/ATR, MAD, mid-полоса (спека §3)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

from trading_bot.config.settings import STRUCTURAL_ALLOWED_LEVEL_TYPES
from trading_bot.data.repositories import get_instruments_atr_bybit_futures_cur


@dataclass
class StructuralParams:
    min_candidates_per_side: int
    top_k: int
    mad_k: float
    center_filter_enabled: bool
    center_mad_k: float
    target_align_enabled: bool
    anchor_symbols: Tuple[str, ...]
    target_w_band_k: float
    target_center_weight: float
    target_width_weight: float
    min_pool_symbols: int
    mid_band_pct: float
    refine_max_rounds: int
    allowed_level_types: Tuple[str, ...]


@dataclass
class StructuralSymbolResult:
    symbol: str
    status: str
    level_below_id: Optional[int]
    level_above_id: Optional[int]
    L_price: Optional[float]
    U_price: Optional[float]
    atr: Optional[float]
    W_atr: Optional[float]
    mid_price: Optional[float]
    mid_band_low: Optional[float]
    mid_band_high: Optional[float]
    ref_price: float
    tier_below: Optional[str]
    tier_above: Optional[str]
    volume_peak_below: Optional[float]
    volume_peak_above: Optional[float]


def _median_sorted(vals: List[float]) -> float:
    if not vals:
        return 0.0
    s = sorted(vals)
    n = len(s)
    mid = n // 2
    if n % 2 == 1:
        return float(s[mid])
    return float((s[mid - 1] + s[mid]) / 2.0)


def _mad(vals: List[float], m: float) -> float:
    if not vals:
        return 0.0
    return _median_sorted([abs(v - m) for v in vals])


def _fetch_side_topk(
    cur,
    symbol: str,
    ref_price: float,
    direction: str,
    types: Sequence[str],
    k: int,
) -> List[Dict[str, Any]]:
    if not types or k <= 0:
        return []
    op = "<" if direction == "long" else ">"
    ph = ",".join("?" * len(types))
    rows = cur.execute(
        f"""
        SELECT id, symbol, price, volume_peak, strength, tier,
               COALESCE(updated_at, created_at) AS upd
        FROM price_levels
        WHERE symbol = ?
          AND is_active = 1
          AND status = 'active'
          AND level_type IN ({ph})
          AND price {op} ?
        ORDER BY COALESCE(volume_peak, 0) DESC, COALESCE(strength, 0) DESC,
                 COALESCE(updated_at, created_at) DESC
        LIMIT ?
        """,
        (symbol, *types, ref_price, k),
    ).fetchall()
    return [dict(r) for r in rows]


def _pair_w(
    below_row: Dict[str, Any], above_row: Dict[str, Any], atr: float
) -> Tuple[float, float, float]:
    lp = float(below_row["price"])
    up = float(above_row["price"])
    return lp, up, (up - lp) / atr


def _pair_center_ratio(lp: float, up: float, ref_price: float) -> Optional[float]:
    den = up - lp
    if den <= 0:
        return None
    return (ref_price - lp) / den


def _best_pair_for_pool(
    below: List[Dict[str, Any]],
    above: List[Dict[str, Any]],
    atr: float,
    ref_price: float,
    m: float,
    mad_val: float,
    mad_k: float,
    center_enabled: bool,
    r_m: float,
    r_mad: float,
    r_mad_k: float,
    target_align_enabled: bool,
    target_w_band_k: float,
    target_center_weight: float,
    target_width_weight: float,
    k: int,
) -> Tuple[Optional[int], Optional[int], Optional[float], bool]:
    """
    Возвращает (i, j, W, is_outlier) для лучшей пары в сетке top_k.
    Сначала ищем не-выброс относительно (m, mad_val); иначе — пару с минимальным |W-m|.
    """
    kb = min(k, len(below))
    ka = min(k, len(above))
    best_tier0: Optional[Tuple[float, int, int, float]] = None  # ok_W and ok_r
    best_tier1: Optional[Tuple[float, int, int, float]] = None  # ok_W
    best_tier2: Optional[Tuple[float, int, int, float]] = None  # fallback any
    for i in range(kb):
        for j in range(ka):
            lp, up, w = _pair_w(below[i], above[j], atr)
            if lp >= ref_price or up <= ref_price:
                continue
            dist_m = abs(w - m)
            if center_enabled:
                r = _pair_center_ratio(lp, up, ref_price)
                if r is None:
                    continue
                dist_r = abs(r - r_m)
            else:
                dist_r = 0.0
            ok_w = not (mad_val >= 1e-12 and dist_m > mad_k * mad_val)
            if target_align_enabled and mad_val >= 1e-12:
                ok_w = ok_w and (dist_m <= target_w_band_k * mad_val)
            if center_enabled:
                ok_r = not (r_mad >= 1e-12 and dist_r > r_mad_k * r_mad)
            else:
                ok_r = True

            # Иерархия выбора:
            #   tier0: ok_W + ok_r (лучший баланс)
            #   tier1: ok_W (W важнее center)
            #   tier2: fallback любой
            score = (target_width_weight * dist_m) + (
                target_center_weight * dist_r if center_enabled else 0.0
            )
            cand = (score, i, j, w)
            if ok_w and ok_r:
                if best_tier0 is None or cand[0] < best_tier0[0]:
                    best_tier0 = cand
            if ok_w:
                if best_tier1 is None or cand[0] < best_tier1[0]:
                    best_tier1 = cand
            if best_tier2 is None or cand[0] < best_tier2[0]:
                best_tier2 = cand
    chosen = best_tier0 or best_tier1 or best_tier2
    if chosen is None:
        return None, None, None, True
    _score, i, j, w = chosen
    out_f = mad_val >= 1e-12 and abs(w - m) > mad_k * mad_val
    return i, j, w, out_f


def _build_row(
    symbol: str,
    status: str,
    ref_price: float,
    below_row: Optional[Dict[str, Any]],
    above_row: Optional[Dict[str, Any]],
    atr: Optional[float],
    mid_pct: float,
) -> StructuralSymbolResult:
    if status == "incomplete_structure" or below_row is None or above_row is None or atr is None:
        return StructuralSymbolResult(
            symbol=symbol,
            status="incomplete_structure",
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
    lp = float(below_row["price"])
    up = float(above_row["price"])
    w_atr = (up - lp) / atr
    mid = (lp + up) / 2.0
    half = (mid_pct / 100.0) * (up - lp) / 2.0
    return StructuralSymbolResult(
        symbol=symbol,
        status=status,
        level_below_id=int(below_row["id"]),
        level_above_id=int(above_row["id"]),
        L_price=lp,
        U_price=up,
        atr=float(atr),
        W_atr=w_atr,
        mid_price=mid,
        mid_band_low=mid - half,
        mid_band_high=mid + half,
        ref_price=ref_price,
        tier_below=str(below_row.get("tier") or "") or None,
        tier_above=str(above_row.get("tier") or "") or None,
        volume_peak_below=float(below_row.get("volume_peak") or 0.0),
        volume_peak_above=float(above_row.get("volume_peak") or 0.0),
    )


def compute_structural_symbol_results(
    cur,
    symbols: Sequence[str],
    ref_by_symbol: Dict[str, float],
    params: StructuralParams,
) -> Tuple[List[StructuralSymbolResult], Dict[str, float]]:
    """
    Для каждого символа — топ-K снизу/сверху от ref, подбор пары в сетке с уточнением под MAD пула.
    Возвращает (строки по символам, агрегат pool_median_w / pool_mad для финального ок-пула).
    """
    types = tuple(params.allowed_level_types) or tuple(STRUCTURAL_ALLOWED_LEVEL_TYPES)
    k = params.top_k
    min_side = params.min_candidates_per_side

    below_cache: Dict[str, List[Dict[str, Any]]] = {}
    above_cache: Dict[str, List[Dict[str, Any]]] = {}
    atr_cache: Dict[str, Optional[float]] = {}

    working: Dict[str, Tuple[int, int]] = {}

    for symbol in symbols:
        ref = float(ref_by_symbol[symbol])
        atr = get_instruments_atr_bybit_futures_cur(cur, symbol)
        atr_cache[symbol] = atr
        below_cache[symbol] = _fetch_side_topk(cur, symbol, ref, "long", types, k)
        above_cache[symbol] = _fetch_side_topk(cur, symbol, ref, "short", types, k)
        if (
            atr is None
            or len(below_cache[symbol]) < min_side
            or len(above_cache[symbol]) < min_side
        ):
            continue
        working[symbol] = (0, 0)

    def current_ws() -> Dict[str, float]:
        out: Dict[str, float] = {}
        for symbol, (i, j) in working.items():
            atr = atr_cache[symbol]
            if atr is None or atr <= 0:
                continue
            b = below_cache[symbol]
            a = above_cache[symbol]
            if i >= len(b) or j >= len(a):
                continue
            _lp, _up, w = _pair_w(b[i], a[j], atr)
            out[symbol] = w
        return out

    for _ in range(params.refine_max_rounds):
        ws_map = current_ws()
        if len(ws_map) < 2:
            break
        ws_list = list(ws_map.values())
        m = _median_sorted(ws_list)
        mad_val = _mad(ws_list, m)
        rs_map: Dict[str, float] = {}
        if params.center_filter_enabled:
            for symbol, (i, j) in working.items():
                atr = atr_cache[symbol]
                if atr is None or atr <= 0:
                    continue
                b = below_cache[symbol]
                a = above_cache[symbol]
                if i >= len(b) or j >= len(a):
                    continue
                lp, up, _w = _pair_w(b[i], a[j], atr)
                rv = _pair_center_ratio(lp, up, float(ref_by_symbol[symbol]))
                if rv is not None:
                    rs_map[symbol] = rv
        anchor_ws = [
            ws_map[s]
            for s in params.anchor_symbols
            if s in ws_map
        ]
        if params.target_align_enabled and len(anchor_ws) >= 2:
            m = _median_sorted(anchor_ws)
            mad_val = _mad(anchor_ws, m)
        r_m = _median_sorted(list(rs_map.values())) if rs_map else 0.0
        r_mad = _mad(list(rs_map.values()), r_m) if rs_map else 0.0
        if params.target_align_enabled and params.center_filter_enabled:
            anchor_rs = [rs_map[s] for s in params.anchor_symbols if s in rs_map]
            if len(anchor_rs) >= 2:
                r_m = _median_sorted(anchor_rs)
                r_mad = _mad(anchor_rs, r_m)
        changed = False
        for symbol in list(working.keys()):
            atr = atr_cache[symbol]
            if atr is None or atr <= 0:
                continue
            b = below_cache[symbol]
            a = above_cache[symbol]
            w_self = ws_map.get(symbol)
            if w_self is None:
                continue
            out_now = mad_val >= 1e-12 and abs(w_self - m) > params.mad_k * mad_val
            if params.center_filter_enabled:
                r_self = rs_map.get(symbol)
                if r_self is not None and r_mad >= 1e-12 and abs(r_self - r_m) > params.center_mad_k * r_mad:
                    out_now = True
            if not out_now:
                continue
            bi, aj, new_w, _still = _best_pair_for_pool(
                b,
                a,
                atr,
                float(ref_by_symbol[symbol]),
                m,
                mad_val,
                params.mad_k,
                params.center_filter_enabled,
                r_m,
                r_mad,
                params.center_mad_k,
                params.target_align_enabled,
                params.target_w_band_k,
                params.target_center_weight,
                params.target_width_weight,
                k,
            )
            if bi is None or aj is None:
                continue
            if working[symbol] != (bi, aj):
                working[symbol] = (bi, aj)
                changed = True
        if not changed:
            break

    results: List[StructuralSymbolResult] = []
    ok_ws: List[float] = []

    ws_map_final = current_ws()
    ws_list_all = list(ws_map_final.values()) if len(ws_map_final) >= 2 else []
    m_fin = _median_sorted(ws_list_all)
    mad_fin = _mad(ws_list_all, m_fin) if ws_list_all else 0.0
    rs_map_final: Dict[str, float] = {}
    if params.center_filter_enabled:
        for symbol, (i, j) in working.items():
            atr = atr_cache[symbol]
            if atr is None or atr <= 0:
                continue
            b = below_cache[symbol]
            a = above_cache[symbol]
            if i >= len(b) or j >= len(a):
                continue
            lp, up, _w = _pair_w(b[i], a[j], atr)
            rv = _pair_center_ratio(lp, up, float(ref_by_symbol[symbol]))
            if rv is not None:
                rs_map_final[symbol] = rv
    r_m_fin = _median_sorted(list(rs_map_final.values())) if rs_map_final else 0.0
    r_mad_fin = _mad(list(rs_map_final.values()), r_m_fin) if rs_map_final else 0.0

    for symbol in symbols:
        ref = float(ref_by_symbol[symbol])
        atr = atr_cache[symbol]
        below = below_cache[symbol]
        above = above_cache[symbol]
        if (
            atr is None
            or len(below) < min_side
            or len(above) < min_side
            or symbol not in working
        ):
            results.append(_build_row(symbol, "incomplete_structure", ref, None, None, atr, params.mid_band_pct))
            continue
        i, j = working[symbol]
        if i >= len(below) or j >= len(above):
            results.append(_build_row(symbol, "incomplete_structure", ref, None, None, atr, params.mid_band_pct))
            continue
        _lp, _up, w = _pair_w(below[i], above[j], atr)
        is_out = (
            len(ws_list_all) >= 2
            and mad_fin >= 1e-12
            and abs(w - m_fin) > params.mad_k * mad_fin
        )
        if params.center_filter_enabled:
            rv = _pair_center_ratio(_lp, _up, ref)
            if rv is not None and r_mad_fin >= 1e-12 and abs(rv - r_m_fin) > params.center_mad_k * r_mad_fin:
                is_out = True
        status = "outlier" if is_out else "ok"
        row = _build_row(
            symbol,
            status,
            ref,
            below[i],
            above[j],
            atr,
            params.mid_band_pct,
        )
        results.append(row)
        if status == "ok":
            ok_ws.append(w)

    pool_m = _median_sorted(ok_ws)
    pool_mad = _mad(ok_ws, pool_m) if ok_ws else 0.0
    ok_rs: List[float] = []
    for r in results:
        if r.status != "ok" or r.L_price is None or r.U_price is None:
            continue
        rv = _pair_center_ratio(float(r.L_price), float(r.U_price), float(r.ref_price))
        if rv is not None:
            ok_rs.append(rv)
    pool_r_m = _median_sorted(ok_rs)
    pool_r_mad = _mad(ok_rs, pool_r_m) if ok_rs else 0.0
    pool_stats = {
        "pool_median_w": pool_m,
        "pool_mad": pool_mad,
        "pool_median_r": pool_r_m,
        "pool_mad_r": pool_r_mad,
    }
    return results, pool_stats
