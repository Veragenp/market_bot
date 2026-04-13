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
# SQL: топ-K уровней с одной стороны от ref
# ---------------------------------------------------------------------------


def _fetch_top_levels(
    cur,
    symbol: str,
    ref_price: float,
    direction: str,
    types: Sequence[str],
    k: int,
) -> List[StrongLevel]:
    if not types or k <= 0:
        return []
    op = "<" if direction == "long" else ">"
    ph = ",".join("?" * len(types))
    rows = cur.execute(
        f"""
        SELECT id, price, volume_peak, strength, tier, level_type
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
    return [
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
    below = _fetch_top_levels(cur, symbol, ref_price, "long", types, top_k)
    above = _fetch_top_levels(cur, symbol, ref_price, "short", types, top_k)
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
    below = _fetch_top_levels(cur, symbol, ref_price, "long", types, top_k)
    above = _fetch_top_levels(cur, symbol, ref_price, "short", types, top_k)
    if not below or not above or atr <= 0:
        return None
    delta = slack
    lo = max(w_lo_global, w_star - delta)
    hi = min(w_hi_global, w_star + delta)
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
) -> StructuralSymbolResult:
    if status != "ok" or pair is None or atr is None or atr <= 0:
        return StructuralSymbolResult(
            symbol=symbol,
            status="incomplete_structure" if status == "ok" else status,
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
    mid, mlo, mhi = symbol_pair_to_zone_bounds(pair, mid_pct)
    return StructuralSymbolResult(
        symbol=symbol,
        status="ok",
        level_below_id=pair.level_below.id,
        level_above_id=pair.level_above.id,
        L_price=pair.level_below.price,
        U_price=pair.level_above.price,
        atr=float(atr),
        W_atr=pair.W,
        mid_price=mid,
        mid_band_low=mlo,
        mid_band_high=mhi,
        ref_price=ref_price,
        tier_below=pair.level_below.tier or None,
        tier_above=pair.level_above.tier or None,
        volume_peak_below=float(pair.level_below.volume_peak),
        volume_peak_above=float(pair.level_above.volume_peak),
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
        below_cache[symbol] = _fetch_top_levels(cur, symbol, ref, "long", types, k)
        above_cache[symbol] = _fetch_top_levels(cur, symbol, ref, "short", types, k)
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
    if w_star is None:
        for symbol in symbols:
            ref = float(ref_by_symbol[symbol])
            results.append(
                _build_row(
                    symbol,
                    "incomplete_structure",
                    ref,
                    None,
                    atr_cache.get(symbol),
                    params.mid_band_pct,
                )
            )
        return results, {
            "pool_median_w": 0.0,
            "pool_mad": 0.0,
            "pool_median_r": 0.0,
            "pool_mad_r": 0.0,
            "w_star": 0.0,
            "etalon_failed": 1,
        }

    logger.info(
        "Эталон W* = %s на основе %s монет",
        w_star,
        len(etalon_pairs),
    )

    w_slack_frac = float(params.w_slack)
    slack_abs_min = float(params.w_slack_abs_min)
    effective_slack = max(slack_abs_min, float(w_star) * w_slack_frac)
    etalon_by_symbol = {p.symbol: p for p in etalon_pairs}
    ok_ws: List[float] = []

    for symbol in symbols:
        ref = float(ref_by_symbol[symbol])
        atr = atr_cache.get(symbol)
        if (
            atr is None
            or atr <= 0
            or len(below_cache.get(symbol, ())) < min_side
            or len(above_cache.get(symbol, ())) < min_side
        ):
            results.append(
                _build_row(symbol, "incomplete_structure", ref, None, atr, params.mid_band_pct)
            )
            continue
        orig = etalon_by_symbol.get(symbol)
        if orig is not None and orig.level_below.price < ref < orig.level_above.price:
            pair: Optional[SymbolPair] = replace(orig, ref_price=ref)
        else:
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
        if pair is None:
            results.append(
                _build_row(symbol, "incomplete_structure", ref, None, atr, params.mid_band_pct)
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
        "etalon_failed": 0,
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
        lo = max(w_lo, w_star - eff_slack)
        hi = min(w_hi, w_star + eff_slack)
        now_ts = int(time.time())
        updated = 0

        for r in srows:
            sym = str(r["symbol"])
            atr = float(r["atr"])
            if atr <= 0:
                continue

            if entered_direction == "long":
                anchor = float(r["L_price"])
                cands = _fetch_top_levels(cur, sym, anchor, "short", types, top_k)
                best = None
                for a in cands:
                    w = (a.price - anchor) / atr
                    if lo <= w <= hi:
                        best = a
                        break
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
                cands = _fetch_top_levels(cur, sym, anchor, "long", types, top_k)
                best = None
                for b in cands:
                    w = (anchor - b.price) / atr
                    if lo <= w <= hi:
                        best = b
                        break
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
