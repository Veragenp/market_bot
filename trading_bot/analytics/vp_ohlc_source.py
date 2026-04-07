"""
Автовыбор OHLC для расчёта VP (HVN): только метрики по данным, без списков символов.

Если минутные свечи «плоские» (high≈low на большей части баров) или медианный
относительный размах слишком мал — строим 5m-бары ресемплом из тех же 1m в памяти
(отдельная загрузка таймфрейма 5m в SQLite не обязательна).
"""

from __future__ import annotations

import math
from typing import Any, Dict, Tuple

import numpy as np
import pandas as pd

from trading_bot.config.settings import (
    VP_OHLC_FLAT_BAR_MAX_FRAC,
    VP_OHLC_MEDIAN_RANGE_MIN,
    VP_OHLC_RESAMPLE_MIN_1M_BARS,
)


def vp_ohlc_quality_metrics(df: pd.DataFrame) -> Dict[str, Any]:
    """
    flat_frac: доля баров, где относительный размах (H-L)/|close| <= rel_eps.
    median_rel_range: медиана (H-L)/|close| по барам.
    """
    if df is None or df.empty:
        return {"n": 0, "flat_frac": 1.0, "median_rel_range": 0.0}

    h = df["high"].to_numpy(dtype=np.float64)
    lo = df["low"].to_numpy(dtype=np.float64)
    cl = df["close"].to_numpy(dtype=np.float64)
    abs_c = np.maximum(np.abs(cl), 1e-12)
    rel_rng = (h - lo) / abs_c
    rel_eps = 1e-8
    flat = rel_rng <= rel_eps
    flat_frac = float(np.mean(flat)) if len(flat) else 1.0
    med = float(np.nanmedian(rel_rng))
    if not math.isfinite(med):
        med = 0.0
    return {"n": int(len(df)), "flat_frac": flat_frac, "median_rel_range": med}


def resample_1m_ohlcv_to_5m(df: pd.DataFrame) -> pd.DataFrame:
    """5m OHLCV из 1m: open=first, high=max, low=min, close=last, volume=sum."""
    if df is None or df.empty:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])
    work = df.sort_values("timestamp").copy()
    ix = pd.to_datetime(work["timestamp"].astype(np.int64), unit="s", utc=True)
    work = work.set_index(ix)
    o = work["open"].resample("5min", label="right", closed="right").first()
    hi = work["high"].resample("5min", label="right", closed="right").max()
    lo = work["low"].resample("5min", label="right", closed="right").min()
    cl = work["close"].resample("5min", label="right", closed="right").last()
    vol = work["volume"].resample("5min", label="right", closed="right").sum()
    agg = pd.DataFrame({"open": o, "high": hi, "low": lo, "close": cl, "volume": vol})
    agg = agg.dropna(how="all").dropna(subset=["open", "high", "low", "close"], how="any")
    tidx = pd.DatetimeIndex(agg.index, tz="UTC") if agg.index.tz is None else agg.index
    agg = agg.reset_index(drop=True)
    agg.insert(0, "timestamp", (tidx.astype("int64") // 10**9).astype(np.int64))
    return agg[["timestamp", "open", "high", "low", "close", "volume"]]


def select_vp_ohlcv_dataframe(
    df_1m: pd.DataFrame,
    *,
    flat_frac_max: float | None = None,
    median_range_min: float | None = None,
    min_1m_bars_for_resample: int | None = None,
) -> Tuple[pd.DataFrame, str, Dict[str, Any]]:
    """
    Возвращает (df_for_find_pro_levels, timeframe_tag, diagnostics).

    timeframe_tag: '1m' или '5m' (5m из ресемпла 1m в памяти).
    """
    fmax = float(VP_OHLC_FLAT_BAR_MAX_FRAC if flat_frac_max is None else flat_frac_max)
    rmin = float(VP_OHLC_MEDIAN_RANGE_MIN if median_range_min is None else median_range_min)
    min_bars = int(VP_OHLC_RESAMPLE_MIN_1M_BARS if min_1m_bars_for_resample is None else min_1m_bars_for_resample)

    m1 = vp_ohlc_quality_metrics(df_1m)
    poor = m1["n"] > 0 and (m1["flat_frac"] > fmax or m1["median_rel_range"] < rmin)
    diag: Dict[str, Any] = {
        "quality_1m": m1,
        "thresholds": {"flat_frac_max": fmax, "median_range_min": rmin, "min_1m_bars": min_bars},
    }

    if not poor:
        diag["vp_source"] = "1m"
        return df_1m.copy(), "1m", diag

    if m1["n"] < min_bars:
        diag["vp_source"] = "1m_short_window_no_resample"
        return df_1m.copy(), "1m", diag

    df5 = resample_1m_ohlcv_to_5m(df_1m)
    m5 = vp_ohlc_quality_metrics(df5)
    diag["quality_5m_rs"] = m5

    min_5m = 8
    if df5.empty or len(df5) < min_5m:
        diag["vp_source"] = "1m_fallback_bad_5m"
        return df_1m.copy(), "1m", diag

    diag["vp_source"] = "5m_rs"
    return df5, "5m", diag


__all__ = [
    "vp_ohlc_quality_metrics",
    "resample_1m_ohlcv_to_5m",
    "select_vp_ohlcv_dataframe",
]
