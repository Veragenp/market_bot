from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict, List, Sequence, Tuple

import numpy as np
import pandas as pd
import requests


@dataclass
class TradableSelectorParams:
    volume_threshold: float = 10_000_000.0
    top_n: int = 30
    timeframe: str = "15"
    lookback_bars: int = 672
    velocity_window_bars: int = 96
    benchmark_symbols: Tuple[str, ...] = ("BTCUSDT", "ETHUSDT")
    weight_corr: float = 0.55
    weight_velocity: float = 0.05
    weight_liquidity: float = 0.40
    exclude_bases: Tuple[str, ...] = (
        "XAU",
        "XAG",
        "XAUT",
        "PAXG",
        "USDC",
        "USDE",
        "USD0",
        "FDUSD",
        "TUSD",
        "USDP",
    )
    exclude_numeric_bases: bool = True
    symbol_suffix: str = "USDT"


def _is_clean_symbol(symbol: str, p: TradableSelectorParams) -> bool:
    if not symbol.endswith(p.symbol_suffix):
        return False
    base = symbol[: -len(p.symbol_suffix)]
    if not base:
        return False
    if len(base) < 3 or len(base) > 12:
        return False
    if not base.isalnum():
        return False
    if p.exclude_numeric_bases and any(ch.isdigit() for ch in base):
        return False
    if base in set(p.exclude_bases):
        return False
    return True


def _corr(a: pd.Series, b: pd.Series) -> float:
    merged = pd.concat([a, b], axis=1, join="inner").dropna()
    if len(merged) < 20:
        return 0.0
    v = float(merged.iloc[:, 0].corr(merged.iloc[:, 1]))
    return 0.0 if math.isnan(v) else v


def _fetch_tickers(session: requests.Session) -> List[dict]:
    resp = session.get(
        "https://api.bybit.com/v5/market/tickers",
        params={"category": "linear"},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json().get("result", {}).get("list", [])


def _fetch_close_series(
    session: requests.Session,
    symbol: str,
    interval: str,
    limit: int,
) -> pd.Series:
    resp = session.get(
        "https://api.bybit.com/v5/market/kline",
        params={
            "category": "linear",
            "symbol": symbol,
            "interval": interval,
            "limit": limit,
        },
        timeout=30,
    )
    resp.raise_for_status()
    rows = resp.json().get("result", {}).get("list", [])
    if not rows:
        return pd.Series(dtype=float)
    rows = sorted(rows, key=lambda x: int(x[0]))
    close = pd.Series([float(x[4]) for x in rows])
    return np.log(close).diff().dropna()


def select_tradable_symbols(
    params: TradableSelectorParams,
) -> Tuple[List[str], List[Dict[str, float]]]:
    """
    Возвращает (top_symbols, diagnostics_rows).
    symbols в формате Bybit: BTCUSDT, ETHUSDT, ...
    """
    session = requests.Session()
    tickers = _fetch_tickers(session)

    candidates: List[Tuple[str, float]] = []
    for row in tickers:
        symbol = str(row.get("symbol", ""))
        if not _is_clean_symbol(symbol, params):
            continue
        turnover = float(row.get("turnover24h") or 0.0)
        if turnover >= params.volume_threshold:
            candidates.append((symbol, turnover))

    candidates.sort(key=lambda x: x[1], reverse=True)
    if not candidates:
        return [], []

    need = set([s for s, _ in candidates]).union(set(params.benchmark_symbols))
    returns_map: Dict[str, pd.Series] = {}
    for symbol in need:
        returns_map[symbol] = _fetch_close_series(
            session,
            symbol=symbol,
            interval=params.timeframe,
            limit=params.lookback_bars + 20,
        ).tail(params.lookback_bars)

    max_turn = max(t for _, t in candidates) if candidates else 1.0
    diagnostics: List[Dict[str, float]] = []
    for symbol, turnover in candidates:
        rs = returns_map.get(symbol, pd.Series(dtype=float))
        corr_vals: List[float] = []
        vel_vals: List[float] = []
        for bench in params.benchmark_symbols:
            rb = returns_map.get(bench, pd.Series(dtype=float))
            if rs.empty or rb.empty:
                corr_vals.append(0.0)
                vel_vals.append(0.0)
                continue
            c_full = abs(_corr(rs.tail(params.lookback_bars), rb.tail(params.lookback_bars)))
            corr_vals.append(c_full)
            c_now = abs(_corr(rs.tail(params.velocity_window_bars), rb.tail(params.velocity_window_bars)))
            c_prev = abs(
                _corr(
                    rs.tail(params.velocity_window_bars * 2).head(params.velocity_window_bars),
                    rb.tail(params.velocity_window_bars * 2).head(params.velocity_window_bars),
                )
            )
            vel_vals.append(c_now - c_prev)

        corr_score = float(np.mean(corr_vals)) if corr_vals else 0.0
        vel_score = float(np.mean(vel_vals)) if vel_vals else 0.0
        liq_score = math.log1p(turnover) / math.log1p(max_turn)
        score = (
            (params.weight_corr * corr_score)
            + (params.weight_velocity * vel_score)
            + (params.weight_liquidity * liq_score)
        )
        diagnostics.append(
            {
                "symbol": symbol,
                "turnover24h": float(turnover),
                "corr_score": corr_score,
                "corr_velocity": vel_score,
                "liq_score": liq_score,
                "score": score,
            }
        )

    diagnostics.sort(key=lambda x: x["score"], reverse=True)
    top = [d["symbol"] for d in diagnostics[: params.top_n]]
    return top, diagnostics

