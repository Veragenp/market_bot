from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict, List, Sequence, Tuple

import numpy as np
import pandas as pd

from trading_bot.data.db import get_connection


@dataclass
class PoolSelectorParams:
    enabled: bool
    min_avg_volume_24h: float
    target_pool_size: int
    min_pool_size: int
    timeframe: str
    corr_lookback_bars: int
    corr_velocity_window_bars: int
    benchmark_symbols: Tuple[str, ...]
    corr_weight: float
    velocity_weight: float


def _to_bybit_symbol(symbol: str) -> str:
    return symbol.replace("/", "").upper()


def _fetch_avg_volume_map(symbols: Sequence[str]) -> Dict[str, float]:
    if not symbols:
        return {}
    conn = get_connection()
    cur = conn.cursor()
    out: Dict[str, float] = {}
    for s in symbols:
        row = cur.execute(
            """
            SELECT avg_volume_24h
            FROM instruments
            WHERE symbol = ? AND exchange = 'bybit_futures'
            """,
            (_to_bybit_symbol(s),),
        ).fetchone()
        out[s] = float(row["avg_volume_24h"]) if row and row["avg_volume_24h"] is not None else 0.0
    conn.close()
    return out


def _fetch_close_series(
    symbol: str,
    timeframe: str,
    bars: int,
) -> pd.Series:
    conn = get_connection()
    q = """
    SELECT timestamp, close
    FROM ohlcv
    WHERE symbol = ? AND timeframe = ?
    ORDER BY timestamp DESC
    LIMIT ?
    """
    df = pd.read_sql_query(q, conn, params=(symbol, timeframe, int(bars)))
    conn.close()
    if df.empty:
        return pd.Series(dtype=float)
    df = df.sort_values("timestamp")
    s = pd.to_numeric(df["close"], errors="coerce")
    s.index = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    return s.dropna()


def _corr(a: pd.Series, b: pd.Series) -> float:
    if a.empty or b.empty:
        return 0.0
    m = pd.concat([a, b], axis=1, join="inner").dropna()
    if len(m) < 10:
        return 0.0
    v = float(m.iloc[:, 0].corr(m.iloc[:, 1]))
    return 0.0 if math.isnan(v) else v


def select_symbol_pool(
    candidate_symbols: Sequence[str],
    params: PoolSelectorParams,
) -> Tuple[List[str], Dict[str, Dict[str, float]]]:
    """
    Возвращает (selected_symbols, diagnostics).
    Алгоритм:
      1) фильтр по ликвидности (avg_volume_24h >= threshold),
      2) score по корреляции к benchmark + "скорости корреляции",
      3) top-N, но не меньше min_pool_size при наличии данных.
    """
    symbols = list(candidate_symbols)
    if not params.enabled or not symbols:
        return symbols, {}

    avg_vol = _fetch_avg_volume_map(symbols)
    liquid = [s for s in symbols if avg_vol.get(s, 0.0) >= params.min_avg_volume_24h]
    if len(liquid) < params.min_pool_size:
        # fallback: не сужаем пул, чтобы не сломать торговый контур.
        liquid = list(symbols)

    lookback = max(params.corr_lookback_bars, params.corr_velocity_window_bars * 2 + 10)
    ret_map: Dict[str, pd.Series] = {}
    for s in set(liquid).union(set(params.benchmark_symbols)):
        close = _fetch_close_series(s, params.timeframe, lookback + 5)
        if close.empty:
            ret_map[s] = pd.Series(dtype=float)
            continue
        ret_map[s] = np.log(close).diff().dropna()

    diagnostics: Dict[str, Dict[str, float]] = {}
    win = max(6, params.corr_velocity_window_bars)
    for s in liquid:
        rs = ret_map.get(s, pd.Series(dtype=float))
        corr_vals: List[float] = []
        vel_vals: List[float] = []
        for b in params.benchmark_symbols:
            rb = ret_map.get(b, pd.Series(dtype=float))
            if rs.empty or rb.empty:
                corr_vals.append(0.0)
                vel_vals.append(0.0)
                continue
            c_full = abs(_corr(rs.tail(params.corr_lookback_bars), rb.tail(params.corr_lookback_bars)))
            corr_vals.append(c_full)

            a_now = rs.tail(win)
            b_now = rb.tail(win)
            a_prev = rs.tail(win * 2).head(win)
            b_prev = rb.tail(win * 2).head(win)
            c_now = abs(_corr(a_now, b_now))
            c_prev = abs(_corr(a_prev, b_prev))
            vel_vals.append(c_now - c_prev)

        corr_score = float(np.mean(corr_vals)) if corr_vals else 0.0
        vel_score = float(np.mean(vel_vals)) if vel_vals else 0.0
        total = (params.corr_weight * corr_score) + (params.velocity_weight * vel_score)
        diagnostics[s] = {
            "avg_volume_24h": float(avg_vol.get(s, 0.0)),
            "corr_score": corr_score,
            "corr_velocity": vel_score,
            "score": total,
        }

    ranked = sorted(
        liquid,
        key=lambda s: (
            diagnostics.get(s, {}).get("score", 0.0),
            diagnostics.get(s, {}).get("avg_volume_24h", 0.0),
        ),
        reverse=True,
    )
    target_n = max(params.min_pool_size, params.target_pool_size)
    selected = ranked[:target_n]
    if len(selected) < params.min_pool_size:
        selected = ranked
    return selected, diagnostics

