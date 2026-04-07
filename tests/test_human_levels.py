import numpy as np
import pandas as pd

from trading_bot.analytics.human_levels import (
    HumanZone,
    W1_ATR_EQUIV_MULT,
    atr_w1_equiv_from_daily,
    bill_williams_fractal_mask,
    cluster_fractal_prices_to_zones,
    deduplicate_zones_by_vertical_gap,
    detect_flip_events,
    extract_fractals,
    filter_human_zones,
    human_levels_from_ohlcv_rows,
    last_valid_atr_d1,
    run_human_levels_pipeline,
    wilder_atr,
)


def test_atr_w1_equiv():
    assert abs(atr_w1_equiv_from_daily(100.0) - 100.0 * W1_ATR_EQUIV_MULT) < 1e-9


def test_fractal_high_center():
    high = np.array([1.0, 1.0, 5.0, 1.0, 1.0])
    low = np.array([0.5, 0.5, 4.0, 0.5, 0.5])
    up, dn = bill_williams_fractal_mask(high, low)
    assert up[2]
    assert not dn[2]


def test_wilder_atr_smoke():
    n = 20
    h = np.ones(n) * 2.0
    l = np.ones(n) * 1.0
    c = np.ones(n) * 1.5
    atr = wilder_atr(h, l, c, period=14)
    assert np.isnan(atr[12])
    assert np.isfinite(atr[13])


def test_flip_resistance_to_support():
    zl, zh = 100.0, 102.0
    # два закрытия выше, ретест снизу (prev < zl), подтверждение вверх
    close = np.array([103.0, 104.0, 99.0, 101.0, 103.0], dtype=float)
    ts = np.array([1, 2, 3, 4, 5], dtype=np.int64)
    ev = detect_flip_events(close, zl, zh, timestamps=ts)
    assert len(ev) == 1
    assert ev[0].direction == "resistance_to_support"
    assert ev[0].bar_index_confirm == 4
    assert ev[0].timestamp == 5


def test_flip_no_sideways_retest():
    """Ретест без входа снизу (prev уже в зоне) — не flip."""
    zl, zh = 100.0, 102.0
    close = np.array([103.0, 104.0, 101.0, 101.5, 103.0], dtype=float)
    ev = detect_flip_events(close, zl, zh)
    assert ev == []


def test_cluster_merges_close_prices():
    from trading_bot.analytics.human_levels import HumanFractal

    fr = [
        HumanFractal(0, 1, 100.0, "low", "1d", 1.0),
        HumanFractal(1, 2, 100.05, "high", "1d", 1.0),
    ]
    zones = cluster_fractal_prices_to_zones(fr, eps_price=1.0, timeframe="1d")
    assert len(zones) == 1
    assert zones[0].zone_low == 100.0
    assert zones[0].zone_high == 100.05
    assert zones[0].strength == 2.0


def test_pipeline_from_trivial_daily():
    # Достаточно дневных баров для ATR(14) + пара фракталов
    rows_d1 = []
    base = 1000
    for i in range(20):
        rows_d1.append(
            {
                "timestamp": base + i * 86400,
                "open": 100.0 + i * 0.1,
                "high": 101.0 + i * 0.1,
                "low": 99.0 + i * 0.1,
                "close": 100.5 + i * 0.1,
                "volume": 1.0,
            }
        )
    rows_w1 = [
        {
            "timestamp": base,
            "open": 100.0,
            "high": 110.0,
            "low": 95.0,
            "close": 105.0,
            "volume": 1.0,
        },
        {
            "timestamp": base + 7 * 86400,
            "open": 105.0,
            "high": 108.0,
            "low": 100.0,
            "close": 102.0,
            "volume": 1.0,
        },
    ]
    res = human_levels_from_ohlcv_rows(rows_d1, rows_w1, cluster_atr_mult=0.5)
    assert res.atr_d1_last > 0
    assert res.atr_w1_equiv > 0


def test_human_zone_center():
    z = HumanZone(100.0, 104.0, "1d", 1.0, 1)
    assert z.center == 102.0


def test_deduplicate_zones_by_vertical_gap_keeps_stronger_and_drops_close():
    atr = 10.0
    gap = 0.5
    # центры 100 и 103 → разница 3 < 5? min_gap_price = 5, so 3 < 5 → too close
    strong = HumanZone(99.0, 101.0, "1d", 10.0, 3)
    weak = HumanZone(102.0, 104.0, "1d", 2.0, 1)
    out = deduplicate_zones_by_vertical_gap([weak, strong], atr, gap)
    assert len(out) == 1
    assert out[0].strength == 10.0

    far = HumanZone(200.0, 202.0, "1d", 1.0, 1)
    out2 = deduplicate_zones_by_vertical_gap([strong, far], atr, gap)
    assert len(out2) == 2


def test_deduplicate_zones_zero_gap_noop():
    z = HumanZone(1.0, 2.0, "1d", 1.0, 1)
    assert deduplicate_zones_by_vertical_gap([z], 10.0, 0.0) == [z]


def test_filter_human_zones_by_count_and_strength():
    z = [
        HumanZone(1.0, 2.0, "1d", 2.0, 1),
        HumanZone(3.0, 4.0, "1d", 5.0, 2),
        HumanZone(5.0, 6.0, "1d", 10.0, 2),
    ]
    assert len(filter_human_zones(z, min_fractal_count=2)) == 2
    assert len(filter_human_zones(z, min_fractal_count=1, min_strength=6.0)) == 1
    assert filter_human_zones(z, min_fractal_count=1, min_strength=0.0) == z


def test_last_valid_atr_uses_last_bar_not_max():
    df = pd.DataFrame(
        {
            "high": [10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 20],
            "low": [9] * 15,
            "close": [9.5] * 15,
        }
    )
    atr = last_valid_atr_d1(df, atr_period=14)
    full = wilder_atr(
        df["high"].to_numpy(),
        df["low"].to_numpy(),
        df["close"].to_numpy(),
        14,
    )
    assert atr == float(full[14])
