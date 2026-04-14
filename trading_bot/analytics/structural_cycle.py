"""
Structural cycle – эталон ширины W*, фазы цены (low / mid / high), групповые триггеры.

Ядро выбора пары и эталона; запись в БД и supervisor остаются в structural_cycle_db.
"""

from __future__ import annotations

import logging
import statistics
import time
from dataclasses import dataclass, replace
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


def _trigger_timeout_sec() -> int:
    v = getattr(settings_pkg.STRUCTURAL_SETTINGS, "TRIGGER_TIMEOUT_SEC", 0)
    try:
        return int(v)
    except (TypeError, ValueError):
        return 0


# ---------------------------------------------------------------------------
# Параметры скана (совместимость с structural_cycle_db / тестами)
# ---------------------------------------------------------------------------


@dataclass
class StructuralParams:
    min_candidates_per_side: int
    top_k: int
    min_pool_symbols: int
    n_etalon: int
    w_min: float
    w_max: float
    # Доля от W* для люфта подгонки: эффективный slack = max(w_slack_abs_min, w_star * w_slack).
    w_slack: float
    w_slack_abs_min: float
    mid_band_pct: float
    edge_atr_frac: float
    allowed_level_types: Tuple[str, ...]
    strength_first_enabled: bool
    z_w_ok_threshold: float


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


# ---------------------------------------------------------------------------
# Вспомогательные структуры (пары уровней / runtime)
# ---------------------------------------------------------------------------


@dataclass
class StrongLevel:
    id: int
    price: float
    volume_peak: float
    strength: float
    tier: str
    level_type: str


@dataclass
class SymbolPair:
    symbol: str
    level_below: StrongLevel
    level_above: StrongLevel
    W: float
    atr: float
    ref_price: float


@dataclass
class StructuralCycle:
    cycle_id: str
    start_time: float
    phase: str
    w_star: Optional[float]
    symbols_map: Dict[str, SymbolPair]
    trigger_state: Dict[str, str]
    trigger_count: Dict[str, int]
    last_trigger_time: Optional[float]
    current_direction: Optional[str]
    trigger_fired: bool = False
    last_change_time: Optional[float] = None


# ---------------------------------------------------------------------------
# SQL: топ-K уровней в ATR-полосе от ref (v4-style)
# ---------------------------------------------------------------------------


def _level_rank_key(lvl: StrongLevel) -> Tuple[float, int, int]:
    """Ранжирование как в v4: сила, при равенстве manual_global_hvn, затем id."""
    manual_bonus = 1 if str(lvl.level_type or "") == "manual_global_hvn" else 0
    return (float(lvl.volume_peak or 0.0), manual_bonus, int(lvl.id))


def _band_bounds(ref_price: float, atr: float, direction: str) -> Tuple[float, float]:
    """Цена в полосе [min_atr, max_atr] от ref (ниже/выше)."""
    d_min = float(settings_pkg.STRUCTURAL_V4_BAND_MIN_ATR)
    d_max = float(settings_pkg.STRUCTURAL_V4_BAND_MAX_ATR)
    if direction == "long":
        # Ниже ref: ref - dist, dist in [d_min*atr, d_max*atr]
        return ref_price - d_max * atr, ref_price - d_min * atr
    # Выше ref: ref + dist, dist in [d_min*atr, d_max*atr]
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
    if not types or k <= 0 or atr <= 0:
        return []
    lo, hi = _band_bounds(float(ref_price), float(atr), direction)
    ph = ",".join("?" * len(types))
    op = "<" if direction == "long" else ">"
    # Берем расширенную выборку по стороне и приоритизируем уровни внутри v4-полосы.
    rows: Sequence[Any] = cur.execute(
        f"""
        SELECT id, price, volume_peak, strength, tier, level_type
        FROM price_levels
        WHERE symbol = ?
          AND is_active = 1
          AND status = 'active'
          AND level_type IN ({ph})
          AND price {op} ?
        ORDER BY COALESCE(volume_peak, strength, 0) DESC, id DESC
        LIMIT ?
        """,
        (symbol, *types, ref_price, int(max(k * 4, k))),
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
    def _score(lvl: StrongLevel) -> Tuple[float, int, int, int]:
        in_band = 1 if (lo < hi and lo <= lvl.price <= hi) else 0
        st, manual, lid = _level_rank_key(lvl)
        # Совместимость: сила — первичный критерий; полоса v4 используется как доп. приоритет.
        return (st, manual, in_band, lid)

    out.sort(key=_score, reverse=True)
    return out[: int(k)]


def select_best_pair_from_sides(
    symbol: str,
    below: Sequence[StrongLevel],
    above: Sequence[StrongLevel],
    atr: float,
    ref_price: float,
    w_lo: float,
    w_hi: float,
) -> Optional[SymbolPair]:
    if not below or not above or atr <= 0:
        return None
    for b in below:
        for a in above:
            if b.price >= ref_price or a.price <= ref_price:
                continue
            w = (a.price - b.price) / atr
            if w_lo <= w <= w_hi:
                return SymbolPair(
                    symbol=symbol,
                    level_below=b,
                    level_above=a,
                    W=w,
                    atr=atr,
                    ref_price=ref_price,
                )
    return None


def select_best_pair_for_symbol(
    cur,
    symbol: str,
    ref_price: float,
    atr: float,
    types: Sequence[str],
    top_k: int,
    w_lo: float,
    w_hi: float,
) -> Optional[SymbolPair]:
    below = _fetch_top_levels(cur, symbol, ref_price, atr, "long", types, top_k)
    above = _fetch_top_levels(cur, symbol, ref_price, atr, "short", types, top_k)
    return select_best_pair_from_sides(symbol, below, above, atr, ref_price, w_lo, w_hi)


def build_etalon(
    symbols_pairs: List[SymbolPair],
    w_lo: float,
    w_hi: float,
    n_etalon_min: int,
) -> Tuple[Optional[float], List[SymbolPair]]:
    valid = [p for p in symbols_pairs if w_lo <= p.W <= w_hi]
    if len(valid) < n_etalon_min:
        logger.warning(
            "Недостаточно монет для эталона: %s < %s",
            len(valid),
            n_etalon_min,
        )
        return None, []
    w_vals = sorted(p.W for p in valid)
    n = len(w_vals)
    if n % 2 == 1:
        median = w_vals[n // 2]
    else:
        median = (w_vals[n // 2 - 1] + w_vals[n // 2]) / 2.0
    return float(median), valid


def fit_pair_to_etalon(
    cur,
    symbol: str,
    ref_price: float,
    atr: float,
    w_star: float,
    types: Sequence[str],
    top_k: int,
    w_lo_global: float,
    w_hi_global: float,
    slack: float,
) -> Optional[SymbolPair]:
    below = _fetch_top_levels(cur, symbol, ref_price, atr, "long", types, top_k)
    above = _fetch_top_levels(cur, symbol, ref_price, atr, "short", types, top_k)
    if not below or not above or atr <= 0:
        return None
    lo, hi = _w_fit_bounds(w_star, w_lo_global, w_hi_global, slack)
    for b in below:
        for a in above:
            if b.price >= ref_price or a.price <= ref_price:
                continue
            w = (a.price - b.price) / atr
            if lo <= w <= hi:
                return SymbolPair(
                    symbol=symbol,
                    level_below=b,
                    level_above=a,
                    W=w,
                    atr=atr,
                    ref_price=ref_price,
                )
    return None


def _w_fit_bounds(w_star: float, w_lo_global: float, w_hi_global: float, slack: float) -> Tuple[float, float]:
    lo = max(float(w_lo_global), float(w_star) - float(slack))
    hi = min(float(w_hi_global), float(w_star) + float(slack))
    return lo, hi


def _pick_best_opposite_level(
    candidates: Sequence[StrongLevel],
    anchor_price: float,
    atr: float,
    lo: float,
    hi: float,
    opposite_side: str,
) -> Optional[StrongLevel]:
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
    return best


def symbol_pair_to_zone_bounds(pair: SymbolPair, mid_band_pct: float) -> Tuple[float, float, float]:
    lp = pair.level_below.price
    up = pair.level_above.price
    mid = (lp + up) / 2.0
    half = (mid_band_pct / 100.0) * (up - lp) / 2.0
    return mid, mid - half, mid + half


def _build_row(
    symbol: str,
    status: str,
    ref_price: float,
    pair: Optional[SymbolPair],
    atr: Optional[float],
    mid_pct: float,
    level_below: Optional[StrongLevel] = None,
    level_above: Optional[StrongLevel] = None,
) -> StructuralSymbolResult:
    if pair is not None:
        level_below = pair.level_below
        level_above = pair.level_above
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
    lp = float(level_below.price) if level_below is not None else None
    up = float(level_above.price) if level_above is not None else None
    w_atr: Optional[float] = None
    mid: Optional[float] = None
    mlo: Optional[float] = None
    mhi: Optional[float] = None
    if lp is not None and up is not None:
        if pair is not None:
            w_atr = float(pair.W)
        else:
            w_atr = (up - lp) / float(atr)
        mid = (lp + up) / 2.0
        half = (mid_pct / 100.0) * (up - lp) / 2.0
        mlo = mid - half
        mhi = mid + half
    return StructuralSymbolResult(
        symbol=symbol,
        status=status,
        level_below_id=(level_below.id if level_below is not None else None),
        level_above_id=(level_above.id if level_above is not None else None),
        L_price=lp,
        U_price=up,
        atr=float(atr),
        W_atr=w_atr,
        mid_price=mid,
        mid_band_low=mlo,
        mid_band_high=mhi,
        ref_price=ref_price,
        tier_below=(level_below.tier or None) if level_below is not None else None,
        tier_above=(level_above.tier or None) if level_above is not None else None,
        volume_peak_below=(float(level_below.volume_peak) if level_below is not None else None),
        volume_peak_above=(float(level_above.volume_peak) if level_above is not None else None),
    )


def compute_structural_symbol_results(
    cur,
    symbols: Sequence[str],
    ref_by_symbol: Dict[str, float],
    params: StructuralParams,
) -> Tuple[List[StructuralSymbolResult], Dict[str, float]]:
    """Пулы пар → W*; подгонка: эталон с L<ref<U сохраняется, иначе fit в [W*±eff]∩[w_min,w_max]."""
    types = tuple(params.allowed_level_types) or tuple(settings_pkg.STRUCTURAL_ALLOWED_LEVEL_TYPES)
    k = params.top_k
    min_side = params.min_candidates_per_side
    w_lo_band = params.w_min
    w_hi_band = params.w_max

    below_cache: Dict[str, List[StrongLevel]] = {}
    above_cache: Dict[str, List[StrongLevel]] = {}
    atr_cache: Dict[str, Optional[float]] = {}

    initial_pairs: List[SymbolPair] = []
    no_atr_symbols: List[str] = []
    thin_side_symbols: List[str] = []
    no_w_band_symbols: List[str] = []

    for symbol in symbols:
        ref = float(ref_by_symbol[symbol])
        atr = get_instruments_atr_bybit_futures_cur(cur, symbol)
        atr_cache[symbol] = atr
        if atr is None or atr <= 0:
            below_cache[symbol] = []
            above_cache[symbol] = []
        else:
            below_cache[symbol] = _fetch_top_levels(cur, symbol, ref, float(atr), "long", types, k)
            above_cache[symbol] = _fetch_top_levels(cur, symbol, ref, float(atr), "short", types, k)
        if atr is None or atr <= 0:
            no_atr_symbols.append(symbol)
            continue
        if len(below_cache[symbol]) < min_side or len(above_cache[symbol]) < min_side:
            thin_side_symbols.append(symbol)
            continue
        pair = select_best_pair_from_sides(
            symbol, below_cache[symbol], above_cache[symbol], float(atr), ref, w_lo_band, w_hi_band
        )
        if pair is None:
            no_w_band_symbols.append(symbol)
        else:
            initial_pairs.append(pair)

    logger.info(
        "Structural (этап эталона): всего символов=%s; нет/некорректный ATR=%s%s; "
        "мало уровней снизу/сверху (<%s с стороны)=%s%s; есть уровни, но нет пары W∈[%.2f,%.2f]=%s%s; "
        "голосов для медианы W*=%s (top_k=%s)",
        len(symbols),
        len(no_atr_symbols),
        _symbol_list_preview(no_atr_symbols),
        min_side,
        len(thin_side_symbols),
        _symbol_list_preview(thin_side_symbols),
        w_lo_band,
        w_hi_band,
        len(no_w_band_symbols),
        _symbol_list_preview(no_w_band_symbols),
        len(initial_pairs),
        k,
    )

    results: List[StructuralSymbolResult] = []

    w_star, etalon_pairs = build_etalon(initial_pairs, w_lo_band, w_hi_band, params.n_etalon)
    etalon_failed = w_star is None
    if etalon_failed:
        logger.warning(
            "Эталон W* не сформирован: продолжаем с односторонними/сырьевыми уровнями по symbol."
        )
        w_star = 0.0
        etalon_pairs = []
    else:
        logger.info(
            "Эталон W* = %s на основе %s монет",
            w_star,
            len(etalon_pairs),
        )

    w_slack_frac = float(params.w_slack)
    slack_abs_min = float(params.w_slack_abs_min)
    effective_slack = max(slack_abs_min, float(w_star) * w_slack_frac if not etalon_failed else 0.0)
    etalon_by_symbol = {p.symbol: p for p in etalon_pairs}
    ok_ws: List[float] = []

    for symbol in symbols:
        ref = float(ref_by_symbol[symbol])
        atr = atr_cache.get(symbol)
        below = below_cache.get(symbol, ())
        above = above_cache.get(symbol, ())
        if atr is None or atr <= 0:
            results.append(
                _build_row(symbol, "incomplete_structure", ref, None, atr, params.mid_band_pct)
            )
            continue
        if not below and not above:
            results.append(
                _build_row(symbol, "incomplete_structure", ref, None, atr, params.mid_band_pct)
            )
            continue
        pair: Optional[SymbolPair] = None
        if len(below) >= min_side and len(above) >= min_side:
            orig = etalon_by_symbol.get(symbol)
            if orig is not None and orig.level_below.price < ref < orig.level_above.price:
                pair = replace(orig, ref_price=ref)
            elif not etalon_failed:
                pair = fit_pair_to_etalon(
                    cur,
                    symbol,
                    ref,
                    float(atr),
                    w_star,
                    types,
                    k,
                    w_lo_band,
                    w_hi_band,
                    effective_slack,
                )
            if pair is None and below and above:
                # fallback: берем сильнейшие стороны без W*-fit, чтобы передать уровни в торговый контур
                b0 = below[0]
                a0 = above[0]
                if b0.price < ref < a0.price:
                    pair = SymbolPair(
                        symbol=symbol,
                        level_below=b0,
                        level_above=a0,
                        W=(a0.price - b0.price) / float(atr),
                        atr=float(atr),
                        ref_price=ref,
                    )
        if pair is None:
            results.append(
                _build_row(
                    symbol,
                    "partial",
                    ref,
                    None,
                    atr,
                    params.mid_band_pct,
                    level_below=(below[0] if below else None),
                    level_above=(above[0] if above else None),
                )
            )
            continue
        row = _build_row(symbol, "ok", ref, pair, float(atr), params.mid_band_pct)
        results.append(row)
        if row.W_atr is not None:
            ok_ws.append(float(row.W_atr))

    if ok_ws:
        ws_sorted = sorted(ok_ws)
        w_preview = ", ".join(f"{w:.3f}" for w in ws_sorted[:30])
        if len(ws_sorted) > 30:
            w_preview += ", …"
        logger.info(
            "После подгонки к W*: ok=%s/%s; W min/med/max=%.3f/%.3f/%.3f; все W: [%s]",
            len(ok_ws),
            len(symbols),
            ws_sorted[0],
            statistics.median(ws_sorted),
            ws_sorted[-1],
            w_preview,
        )
    else:
        logger.info("После подгонки к W*: ok=0/%s (нет пар в допуске)", len(symbols))

    pool_stats = {
        "pool_median_w": float(w_star),
        "pool_mad": float(effective_slack),
        "pool_median_r": 0.0,
        "pool_mad_r": 0.0,
        "w_star": float(w_star),
        "etalon_failed": (1 if etalon_failed else 0),
    }
    return results, pool_stats


# ---------------------------------------------------------------------------
# Фазы цены и групповые триггеры (realtime)
# ---------------------------------------------------------------------------


def price_zone(pair: SymbolPair, current_price: float, edge_atr_frac: Optional[float] = None) -> str:
    edge_frac = (
        float(settings_pkg.STRUCTURAL_SETTINGS.EDGE_TOLERANCE_ATR_FRAC)
        if edge_atr_frac is None
        else edge_atr_frac
    )
    lp = pair.level_below.price
    up = pair.level_above.price
    atr = pair.atr
    edge = edge_frac * atr
    low_zone_high = lp + edge
    high_zone_low = up - edge
    if current_price <= low_zone_high:
        return "low"
    if current_price >= high_zone_low:
        return "high"
    return "mid"


def symbols_past_breakout_threshold(
    symbols_pairs: Dict[str, SymbolPair],
    current_prices: Dict[str, float],
    breakout_atr_frac: Optional[float] = None,
) -> List[str]:
    """Символы, у которых цена за пределами канала более чем на breakout_atr_frac * ATR."""
    bfrac = (
        float(settings_pkg.STRUCTURAL_SETTINGS.BREAKOUT_ATR_FRAC)
        if breakout_atr_frac is None
        else breakout_atr_frac
    )
    out: List[str] = []
    for sym, pair in symbols_pairs.items():
        price = current_prices.get(sym)
        if price is None:
            continue
        lp = pair.level_below.price
        up = pair.level_above.price
        atr = pair.atr
        dist = bfrac * atr
        if float(price) < lp - dist or float(price) > up + dist:
            out.append(sym)
    return out


def check_breakout(
    symbols_pairs: Dict[str, SymbolPair],
    current_prices: Dict[str, float],
    n_need: Optional[int] = None,
    breakout_atr_frac: Optional[float] = None,
) -> bool:
    n = int(
        settings_pkg.STRUCTURAL_SETTINGS.N_BREAKOUT if n_need is None else n_need
    )
    broken = symbols_past_breakout_threshold(
        symbols_pairs, current_prices, breakout_atr_frac=breakout_atr_frac
    )
    return len(broken) >= n


def compute_initial_zones(
    cycle: StructuralCycle,
    current_prices: Dict[str, float],
    *,
    edge_atr_frac: Optional[float] = None,
    now_ts: Optional[float] = None,
) -> int:
    """
    Заполняет trigger_state текущими зонами по ценам; счётчики переходов не меняет.
    Возвращает число символов с известной ценой, у которых зона 'mid'.
    """
    now = float(now_ts) if now_ts is not None else time.time()
    n_mid = 0
    for sym, pair in cycle.symbols_map.items():
        px = current_prices.get(sym)
        if px is None:
            continue
        z = price_zone(pair, float(px), edge_atr_frac=edge_atr_frac)
        cycle.trigger_state[sym] = z
        if z == "mid":
            n_mid += 1
    cycle.last_change_time = now
    return n_mid


def update_trigger_counts(
    cycle: StructuralCycle,
    current_prices: Dict[str, float],
    now_ts: Optional[float] = None,
    *,
    edge_atr_frac: Optional[float] = None,
) -> Tuple[bool, Optional[str], List[str]]:
    """
    Переходы low/high→mid за один тик (по символам, ещё не отмеченным key sym_to_mid).
    Возвращает (triggered, zone, symbols_transitioned_this_tick).
    trigger_fired + TRIGGER_TIMEOUT_SEC блокируют повторный True до истечения таймаута
    (счётчики trigger_count при этом не сбрасываются). После таймаута trigger_fired снимается.
    TRIGGER_TIMEOUT_SEC <= 0: только снятие trigger_fired по времени не применяется, cooldown повтора нет.
    """
    now = float(now_ts) if now_ts is not None else time.time()
    tsec = _trigger_timeout_sec()

    if cycle.trigger_fired and cycle.last_trigger_time is not None and tsec > 0:
        if now - float(cycle.last_trigger_time) >= tsec:
            cycle.trigger_fired = False

    low_to_mid_count = 0
    high_to_mid_count = 0
    transitioned_syms: List[str] = []

    for sym, pair in cycle.symbols_map.items():
        price = current_prices.get(sym)
        if price is None:
            continue
        zone = price_zone(pair, float(price), edge_atr_frac=edge_atr_frac)
        prev_zone = cycle.trigger_state.get(sym)
        if prev_zone is None:
            cycle.trigger_state[sym] = zone
            continue

        if prev_zone in ("low", "high") and zone == "mid":
            key = f"{sym}_to_mid"
            if cycle.trigger_count.get(key, 0) == 0:
                cycle.trigger_count[key] = 1
                transitioned_syms.append(sym)
                if prev_zone == "low":
                    low_to_mid_count += 1
                else:
                    high_to_mid_count += 1
        elif prev_zone == "mid" and zone in ("low", "high"):
            key = f"{sym}_to_mid"
            cycle.trigger_count[key] = 0

        cycle.trigger_state[sym] = zone

    cycle.last_change_time = now

    n_tr = int(settings_pkg.STRUCTURAL_SETTINGS.N_TRIGGER)
    in_cooldown = (
        cycle.trigger_fired
        and cycle.last_trigger_time is not None
        and tsec > 0
        and (now - float(cycle.last_trigger_time) < tsec)
    )
    if in_cooldown:
        return False, None, transitioned_syms

    if not cycle.trigger_fired:
        if low_to_mid_count >= n_tr or high_to_mid_count >= n_tr:
            cycle.trigger_fired = True
            cycle.last_trigger_time = now
            logger.info(
                "Триггер structural по переходам: low→mid=%s high→mid=%s (порог %s), вход запущен",
                low_to_mid_count,
                high_to_mid_count,
                n_tr,
            )
            return True, "mid", transitioned_syms

    return False, None, transitioned_syms


def fire_if_enough_in_mid(
    cycle: StructuralCycle,
    current_prices: Dict[str, float],
    now_ts: Optional[float] = None,
) -> bool:
    """
    Достаточно символов уже в mid (после обновления trigger_state) — групповой триггер
    без ожидания перехода в этом тике (нарастающее «уже в середине»).
    """
    now = float(now_ts) if now_ts is not None else time.time()
    tsec = _trigger_timeout_sec()
    if cycle.trigger_fired and cycle.last_trigger_time is not None and tsec > 0:
        if now - float(cycle.last_trigger_time) >= tsec:
            cycle.trigger_fired = False
    if cycle.trigger_fired and cycle.last_trigger_time is not None and tsec > 0:
        if now - float(cycle.last_trigger_time) < tsec:
            return False
    if cycle.trigger_fired:
        return False
    n_mid = sum(
        1
        for s in cycle.symbols_map
        if cycle.trigger_state.get(s) == "mid" and current_prices.get(s) is not None
    )
    n_tr = int(settings_pkg.STRUCTURAL_SETTINGS.N_TRIGGER)
    if n_mid >= n_tr:
        cycle.trigger_fired = True
        cycle.last_trigger_time = now
        logger.info(
            "Триггер structural: %s монет в mid (порог %s), вход запущен",
            n_mid,
            n_tr,
        )
        return True
    return False


def rebuild_opposite_zone_on_cursor(cur, cycle_id: str, entered_direction: str) -> Optional[str]:
    """
    То же, что rebuild_opposite_zone, но на переданном cursor (без commit).
    При недостаточном числе обновлений откатывает изменения внутри SAVEPOINT.
    """
    if entered_direction not in ("long", "short"):
        logger.warning("rebuild_opposite_zone_on_cursor: invalid direction %s", entered_direction)
        return None

    cur.execute("SAVEPOINT sp_rebuild_opposite")
    try:
        row = cur.execute(
            "SELECT pool_median_w FROM structural_cycles WHERE id = ?",
            (cycle_id,),
        ).fetchone()
        if not row or row["pool_median_w"] is None:
            cur.execute("ROLLBACK TO SAVEPOINT sp_rebuild_opposite")
            cur.execute("RELEASE SAVEPOINT sp_rebuild_opposite")
            return None
        w_star = float(row["pool_median_w"])

        srows = cur.execute(
            """
            SELECT symbol, level_below_id, level_above_id,
                   L_price, U_price, atr, W_atr, ref_price_ws,
                   tier_below, tier_above, volume_peak_below, volume_peak_above
            FROM structural_cycle_symbols
            WHERE cycle_id = ? AND status = 'ok'
            """,
            (cycle_id,),
        ).fetchall()
        if not srows:
            cur.execute("ROLLBACK TO SAVEPOINT sp_rebuild_opposite")
            cur.execute("RELEASE SAVEPOINT sp_rebuild_opposite")
            return None

        types = list(settings_pkg.STRUCTURAL_SETTINGS.ALLOWED_LEVEL_TYPES)
        top_k = settings_pkg.STRUCTURAL_SETTINGS.TOP_K_PER_SIDE
        w_slack_frac = float(settings_pkg.STRUCTURAL_SETTINGS.W_SLACK_FRAC)
        slack_abs_min = float(settings_pkg.STRUCTURAL_SETTINGS.W_SLACK_ABS_MIN)
        eff_slack = max(slack_abs_min, w_star * w_slack_frac)
        w_lo = settings_pkg.STRUCTURAL_SETTINGS.W_GLOBAL_MIN
        w_hi = settings_pkg.STRUCTURAL_SETTINGS.W_GLOBAL_MAX
        lo, hi = _w_fit_bounds(w_star, w_lo, w_hi, eff_slack)
        now_ts = int(time.time())
        updated = 0

        for r in srows:
            sym = str(r["symbol"])
            atr = float(r["atr"])
            if atr <= 0:
                continue

            if entered_direction == "long":
                anchor = float(r["L_price"])
                cands = _fetch_top_levels(cur, sym, anchor, atr, "short", types, top_k)
                best = _pick_best_opposite_level(
                    candidates=cands,
                    anchor_price=anchor,
                    atr=atr,
                    lo=lo,
                    hi=hi,
                    opposite_side="above",
                )
                if not best:
                    continue
                cur.execute(
                    """
                    UPDATE structural_cycle_symbols
                    SET level_above_id = ?, U_price = ?, W_atr = ?, tier_above = ?,
                        volume_peak_above = ?, evaluated_at = ?
                    WHERE cycle_id = ? AND symbol = ?
                    """,
                    (
                        best.id,
                        best.price,
                        (best.price - anchor) / atr,
                        best.tier or None,
                        float(best.volume_peak),
                        now_ts,
                        cycle_id,
                        sym,
                    ),
                )
                updated += 1
            else:
                anchor = float(r["U_price"])
                cands = _fetch_top_levels(cur, sym, anchor, atr, "long", types, top_k)
                best = _pick_best_opposite_level(
                    candidates=cands,
                    anchor_price=anchor,
                    atr=atr,
                    lo=lo,
                    hi=hi,
                    opposite_side="below",
                )
                if not best:
                    continue
                cur.execute(
                    """
                    UPDATE structural_cycle_symbols
                    SET level_below_id = ?, L_price = ?, W_atr = ?, tier_below = ?,
                        volume_peak_below = ?, evaluated_at = ?
                    WHERE cycle_id = ? AND symbol = ?
                    """,
                    (
                        best.id,
                        best.price,
                        (anchor - best.price) / atr,
                        best.tier or None,
                        float(best.volume_peak),
                        now_ts,
                        cycle_id,
                        sym,
                    ),
                )
                updated += 1

        if updated < settings_pkg.STRUCTURAL_SETTINGS.N_TRIGGER:
            logger.error(
                "rebuild_opposite_zone_on_cursor: only %s symbols updated, need %s",
                updated,
                settings_pkg.STRUCTURAL_SETTINGS.N_TRIGGER,
            )
            cur.execute("ROLLBACK TO SAVEPOINT sp_rebuild_opposite")
            cur.execute("RELEASE SAVEPOINT sp_rebuild_opposite")
            return None

        mid_pct = float(settings_pkg.STRUCTURAL_SETTINGS.MID_BAND_PCT)
        srows2 = cur.execute(
            """
            SELECT symbol, L_price, U_price FROM structural_cycle_symbols
            WHERE cycle_id = ? AND status = 'ok'
            """,
            (cycle_id,),
        ).fetchall()
        for rr in srows2:
            lp = float(rr["L_price"])
            up = float(rr["U_price"])
            mid = (lp + up) / 2.0
            half = (mid_pct / 100.0) * (up - lp) / 2.0
            cur.execute(
                """
                UPDATE structural_cycle_symbols
                SET mid_price = ?, mid_band_low = ?, mid_band_high = ?
                WHERE cycle_id = ? AND symbol = ?
                """,
                (mid, mid - half, mid + half, cycle_id, str(rr["symbol"])),
            )

        cur.execute(
            "UPDATE structural_cycles SET updated_at = ?, phase = 'armed' WHERE id = ?",
            (now_ts, cycle_id),
        )
        cur.execute("RELEASE SAVEPOINT sp_rebuild_opposite")
        return cycle_id
    except Exception:
        logger.exception("rebuild_opposite_zone_on_cursor failed")
        cur.execute("ROLLBACK TO SAVEPOINT sp_rebuild_opposite")
        cur.execute("RELEASE SAVEPOINT sp_rebuild_opposite")
        return None


def rebuild_opposite_zone(cycle_id: str, entered_direction: str) -> Optional[str]:
    """
    После входа пересчитывает противоположный уровень с тем же W* (из pool_median_w), якорь — L или U.

    Обновляет строки structural_cycle_symbols для того же cycle_id (без нового UUID).
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        out = rebuild_opposite_zone_on_cursor(cur, cycle_id, entered_direction)
        if out:
            conn.commit()
        else:
            conn.rollback()
        return out
    except Exception:
        logger.exception("rebuild_opposite_zone failed")
        conn.rollback()
        return None
    finally:
        conn.close()
