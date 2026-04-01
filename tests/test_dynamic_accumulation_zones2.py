"""
Пики объёмного профиля (find_pro_levels) — см. trading_bot.analytics.volume_profile_peaks.
Не трогаем test_dynamic_accumulation_zones.py.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

pytest.importorskip("scipy")

from trading_bot.analytics.volume_profile_peaks import (
    analyze_coin_zones,
    find_pro_levels,
    get_adaptive_params,
)


def test_find_pro_levels_detects_central_hvn():
    """Сглаженный профиль с максимумом в центре диапазона цен — ожидаем хотя бы один пик."""
    n = 500
    ts = np.arange(1_700_000_000, 1_700_000_000 + n * 60, 60, dtype=np.int64)
    prices = np.linspace(49_900.0, 50_100.0, n)
    center = 250
    vol = 2.0 + 800.0 * np.exp(-0.5 * ((np.arange(n) - center) / 45.0) ** 2)
    df = pd.DataFrame(
        {"timestamp": ts, "close": prices, "volume": vol},
    )
    out = find_pro_levels(df, height_mult=1.2, distance_pct=0.002)
    assert not out.empty
    assert (out["Price"] >= 49_950).all() and (out["Price"] <= 50_050).all()


def test_find_pro_levels_requires_columns():
    with pytest.raises(ValueError, match="close"):
        find_pro_levels(pd.DataFrame({"volume": [1.0]}))


def test_find_pro_levels_empty_input():
    out = find_pro_levels(pd.DataFrame({"close": [], "volume": []}))
    assert out.empty


def test_find_pro_levels_valley_merge_reduces_nearby_peaks():
    n = 600
    ts = np.arange(1_700_100_000, 1_700_100_000 + n * 60, 60, dtype=np.int64)
    prices = np.linspace(49_900.0, 50_100.0, n)
    i = np.arange(n)
    g1 = np.exp(-0.5 * ((i - 200) / 25.0) ** 2)
    g2 = np.exp(-0.5 * ((i - 260) / 25.0) ** 2)
    vol = 2.0 + 700.0 * (g1 + g2)
    df = pd.DataFrame({"timestamp": ts, "close": prices, "volume": vol})

    out_no_merge = find_pro_levels(
        df, height_mult=1.1, distance_pct=0.001, valley_threshold=0.95
    )
    out_merge = find_pro_levels(
        df, height_mult=1.1, distance_pct=0.001, valley_threshold=0.5
    )
    assert len(out_merge) <= len(out_no_merge)


def test_get_adaptive_params_has_expected_keys():
    n = 300
    ts = np.arange(1_700_200_000, 1_700_200_000 + n * 60, 60, dtype=np.int64)
    close = np.linspace(60_000.0, 60_500.0, n)
    high = close * 1.001
    low = close * 0.999
    volume = np.full(n, 50.0)
    df = pd.DataFrame(
        {"timestamp": ts, "close": close, "high": high, "low": low, "volume": volume}
    )
    p = get_adaptive_params(df)
    assert "tick_size" in p and p["tick_size"] > 0
    assert "distance_pct" in p and p["distance_pct"] >= 0.005
    assert p["valley_threshold"] in (0.55, 0.70)


def test_analyze_coin_zones_runs_with_adaptive_params():
    n = 300
    ts = np.arange(1_700_300_000, 1_700_300_000 + n * 60, 60, dtype=np.int64)
    close = np.linspace(70_000.0, 70_200.0, n)
    high = close * 1.001
    low = close * 0.999
    volume = np.full(n, 100.0)
    df = pd.DataFrame(
        {"timestamp": ts, "close": close, "high": high, "low": low, "volume": volume}
    )
    out = analyze_coin_zones(df, symbol="BTC/USDT")
    assert isinstance(out, pd.DataFrame)
