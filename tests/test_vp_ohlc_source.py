"""Метрики OHLC и автопереключение 1m -> 5m ресемпл для VP."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from trading_bot.analytics.vp_ohlc_source import (
    resample_1m_ohlcv_to_5m,
    select_vp_ohlcv_dataframe,
    vp_ohlc_quality_metrics,
)


def test_vp_ohlc_quality_all_flat():
    n = 100
    ts = np.arange(1_700_000_000, 1_700_000_000 + n * 60, 60, dtype=np.int64)
    c = np.full(n, 0.5)
    df = pd.DataFrame(
        {"timestamp": ts, "open": c, "high": c, "low": c, "close": c, "volume": np.ones(n)}
    )
    m = vp_ohlc_quality_metrics(df)
    assert m["n"] == n
    assert m["flat_frac"] == 1.0
    assert m["median_rel_range"] == 0.0


def test_vp_ohlc_quality_with_range():
    n = 50
    ts = np.arange(1_700_100_000, 1_700_100_000 + n * 60, 60, dtype=np.int64)
    close = np.linspace(100.0, 101.0, n)
    high = close * 1.002
    low = close * 0.998
    df = pd.DataFrame(
        {"timestamp": ts, "open": close, "high": high, "low": low, "close": close, "volume": np.ones(n)}
    )
    m = vp_ohlc_quality_metrics(df)
    assert m["flat_frac"] < 0.05
    assert m["median_rel_range"] > 1e-4


def test_resample_1m_to_5m_shape():
    # 10 минут -> 2 пятиминутки (последняя может быть неполной в зависимости от выравнивания)
    ts = np.array([1_700_200_000 + i * 60 for i in range(10)], dtype=np.int64)
    c = np.linspace(50.0, 50.9, 10)
    df = pd.DataFrame(
        {
            "timestamp": ts,
            "open": c,
            "high": c + 0.01,
            "low": c - 0.01,
            "close": c,
            "volume": np.arange(10, dtype=float),
        }
    )
    out = resample_1m_ohlcv_to_5m(df)
    assert len(out) >= 1
    assert set(out.columns) == {"timestamp", "open", "high", "low", "close", "volume"}
    assert (out["high"] >= out["low"]).all()


def test_select_vp_switches_on_flat():
    n = 200
    ts = np.arange(1_700_300_000, 1_700_300_000 + n * 60, 60, dtype=np.int64)
    c = np.full(n, 0.2)
    df = pd.DataFrame(
        {"timestamp": ts, "open": c, "high": c, "low": c, "close": c, "volume": np.random.rand(n) * 1e6}
    )
    d_use, tf, diag = select_vp_ohlcv_dataframe(
        df,
        flat_frac_max=0.2,
        median_range_min=1e-6,
        min_1m_bars_for_resample=50,
    )
    assert tf == "5m"
    assert diag["vp_source"] == "5m_rs"
    assert len(d_use) < len(df)


def test_select_vp_keeps_good_1m():
    n = 200
    ts = np.arange(1_700_400_000, 1_700_400_000 + n * 60, 60, dtype=np.int64)
    close = np.linspace(100.0, 110.0, n)
    high = close * 1.001
    low = close * 0.999
    df = pd.DataFrame(
        {
            "timestamp": ts,
            "open": close,
            "high": high,
            "low": low,
            "close": close,
            "volume": np.ones(n),
        }
    )
    d_use, tf, diag = select_vp_ohlcv_dataframe(df)
    assert tf == "1m"
    assert len(d_use) == len(df)
    assert diag["vp_source"] == "1m"
