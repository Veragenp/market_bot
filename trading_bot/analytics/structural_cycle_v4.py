"""
Structural levels v4: ref price + ATR band [min,max] — strongest level below / above ref.

Источники уровней: `vp_local`, `manual_global_hvn` (ручные при равной числовой силе предпочтительнее).
Полоса по умолчанию: 0.8–2.0 ATR от ref (ниже / выше), настраивается в settings.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd

from trading_bot.config import settings as settings_pkg
from trading_bot.config.symbols import TRADING_SYMBOLS
from trading_bot.data.repositories import get_instruments_atr_bybit_futures_cur

# Только эти типы участвуют в v4 (спецификация: VP local + ручные HVN).
V4_LEVEL_TYPES: Tuple[str, ...] = ("vp_local", "manual_global_hvn")
_MANUAL = "manual_global_hvn"


def _fetch_ref_price(cur, symbol: str) -> Optional[float]:
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
    ref = float(row["close"])
    return ref if ref > 0 else None


def _fetch_v4_levels(cur, symbol: str, limit: int) -> List[Any]:
    ph = ",".join("?" for _ in V4_LEVEL_TYPES)
    rows = cur.execute(
        f"""
        SELECT id, price, level_type, tier,
               COALESCE(volume_peak, strength, 0) AS lvl_strength
        FROM price_levels
        WHERE symbol = ?
          AND is_active = 1
          AND status = 'active'
          AND level_type IN ({ph})
        ORDER BY COALESCE(volume_peak, strength, 0) DESC, id DESC
        LIMIT ?
        """,
        (symbol, *V4_LEVEL_TYPES, int(limit)),
    ).fetchall()
    return list(rows)


def _level_rank_key(row: Any) -> Tuple[float, int, int]:
    st = float(row["lvl_strength"] or 0.0)
    manual_bonus = 1 if (row["level_type"] or "") == _MANUAL else 0
    lid = int(row["id"] or 0)
    return (st, manual_bonus, lid)


def _pick_strongest(candidates: Sequence[Any]) -> Optional[Any]:
    if not candidates:
        return None
    return max(candidates, key=_level_rank_key)


def _band_below(ref: float, atr: float, d_min_atr: float, d_max_atr: float) -> Tuple[float, float]:
    """Цены уровней ниже ref: ref − dist ∈ [d_min*atr, d_max*atr] → P ∈ [ref − d_max*atr, ref − d_min*atr]."""
    lo = ref - d_max_atr * atr
    hi = ref - d_min_atr * atr
    return lo, hi


def _band_above(ref: float, atr: float, d_min_atr: float, d_max_atr: float) -> Tuple[float, float]:
    """Выше ref: dist ∈ [d_min*atr, d_max*atr] → P ∈ [ref + d_min*atr, ref + d_max*atr]."""
    lo = ref + d_min_atr * atr
    hi = ref + d_max_atr * atr
    return lo, hi


def build_structural_v4_report_df(cur, symbols: Optional[Sequence[str]] = None) -> pd.DataFrame:
    syms = list(symbols) if symbols is not None else list(TRADING_SYMBOLS)
    d_min = float(settings_pkg.STRUCTURAL_V4_BAND_MIN_ATR)
    d_max = float(settings_pkg.STRUCTURAL_V4_BAND_MAX_ATR)
    fetch_limit = int(settings_pkg.STRUCTURAL_V4_LEVELS_FETCH_LIMIT)

    rows_out: List[Dict[str, Any]] = []
    exported_at = datetime.now(timezone.utc).isoformat()

    for sym in syms:
        ref_price = _fetch_ref_price(cur, sym)
        atr = get_instruments_atr_bybit_futures_cur(cur, sym)
        levels = _fetch_v4_levels(cur, sym, fetch_limit) if atr else []

        rec: Dict[str, Any] = {
            "exported_at_utc": exported_at,
            "symbol": sym,
            "ref_price": ref_price,
            "atr_daily": float(atr) if atr is not None else None,
            "band_min_atr": d_min,
            "band_max_atr": d_max,
            "levels_n": len(levels),
            "lower_band_low": None,
            "lower_band_high": None,
            "upper_band_low": None,
            "upper_band_high": None,
            "lower_level_price": None,
            "lower_level_strength": None,
            "lower_level_type": None,
            "lower_level_id": None,
            "lower_level_tier": None,
            "lower_dist_ref_atr": None,
            "lower_candidates_n": 0,
            "upper_level_price": None,
            "upper_level_strength": None,
            "upper_level_type": None,
            "upper_level_id": None,
            "upper_level_tier": None,
            "upper_dist_ref_atr": None,
            "upper_candidates_n": 0,
            "v4_status": "",
        }

        if ref_price is None or atr is None or float(atr) <= 0:
            rec["v4_status"] = "no_ref_or_atr"
            rows_out.append(rec)
            continue

        ref = float(ref_price)
        a = float(atr)
        lb_lo, lb_hi = _band_below(ref, a, d_min, d_max)
        ub_lo, ub_hi = _band_above(ref, a, d_min, d_max)
        rec["lower_band_low"] = lb_lo
        rec["lower_band_high"] = lb_hi
        rec["upper_band_low"] = ub_lo
        rec["upper_band_high"] = ub_hi

        below_c: List[Any] = []
        above_c: List[Any] = []
        for r in levels:
            p = float(r["price"])
            if lb_lo <= p <= lb_hi:
                below_c.append(r)
            if ub_lo <= p <= ub_hi:
                above_c.append(r)

        rec["lower_candidates_n"] = len(below_c)
        rec["upper_candidates_n"] = len(above_c)

        best_lo = _pick_strongest(below_c)
        best_hi = _pick_strongest(above_c)

        if best_lo is not None:
            lp = float(best_lo["price"])
            rec["lower_level_price"] = lp
            rec["lower_level_strength"] = float(best_lo["lvl_strength"] or 0.0)
            rec["lower_level_type"] = best_lo["level_type"]
            rec["lower_level_id"] = int(best_lo["id"]) if best_lo["id"] is not None else None
            rec["lower_level_tier"] = best_lo["tier"]
            rec["lower_dist_ref_atr"] = (ref - lp) / a

        if best_hi is not None:
            up = float(best_hi["price"])
            rec["upper_level_price"] = up
            rec["upper_level_strength"] = float(best_hi["lvl_strength"] or 0.0)
            rec["upper_level_type"] = best_hi["level_type"]
            rec["upper_level_id"] = int(best_hi["id"]) if best_hi["id"] is not None else None
            rec["upper_level_tier"] = best_hi["tier"]
            rec["upper_dist_ref_atr"] = (up - ref) / a

        if best_lo is None and best_hi is None:
            rec["v4_status"] = "no_level_in_bands"
        elif best_lo is None:
            rec["v4_status"] = "missing_lower"
        elif best_hi is None:
            rec["v4_status"] = "missing_upper"
        else:
            rec["v4_status"] = "ok"

        rows_out.append(rec)

    return pd.DataFrame(rows_out)


__all__ = [
    "V4_LEVEL_TYPES",
    "build_structural_v4_report_df",
]
