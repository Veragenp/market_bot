"""Synthetic checks for dynamic accumulation zones (original spec)."""

from __future__ import annotations

import numpy as np
import pandas as pd

from trading_bot.analytics.dynamic_accumulation_zones import (
    AccumulationZone,
    apply_tick_step_to_close,
    assign_tiers_by_original_spec,
    find_accumulation_zones,
    hour_volume_profile,
    merge_close_zones_weighted,
    poc_from_profile,
    rescan_to_master_levels,
    take_top_n_per_price_band,
    take_top_n_per_price_band_detailed,
)


def _minute_rows(
    start_ts: int,
    n_minutes: int,
    close: float,
    vol_per_min: float = 1.0,
) -> pd.DataFrame:
    ts = np.arange(start_ts, start_ts + n_minutes * 60, 60, dtype=np.int64)
    return pd.DataFrame(
        {
            "timestamp": ts,
            "open": close,
            "high": close,
            "low": close,
            "close": close,
            "volume": vol_per_min,
        }
    )


def test_hour_volume_profile_poc():
    step = 10.0
    df = _minute_rows(1_700_000_000, 60, close=64_205.0, vol_per_min=2.0)
    prof = hour_volume_profile(df, step)
    poc = poc_from_profile(prof, step)
    assert poc == 64_200.0


def test_apply_tick_step_floor_10_usdt():
    tick = 10.0
    df = pd.DataFrame(
        {
            "timestamp": [100, 160],
            "open": [70031.0, 70039.0],
            "high": [70031.0, 70039.0],
            "low": [70031.0, 70039.0],
            "close": [70031.0, 70039.0],
            "volume": [1.0, 1.0],
        }
    )
    out = apply_tick_step_to_close(df, tick)
    assert list(out["close"]) == [70030.0, 70030.0]


def test_merge_adjacent_hours_same_poc_bin():
    step = 10.0
    t0 = 1_700_000_000
    h2 = pd.concat(
        [
            _minute_rows(t0, 60, 64_000.0, 1.0),
            _minute_rows(t0 + 3600, 60, 64_005.0, 1.0),
        ],
        ignore_index=True,
    )
    zones = find_accumulation_zones(
        h2,
        bin_step=step,
        poc_merge_threshold_pct=0.001,
        min_zone_hours=1.0,
    )
    assert len(zones) == 1
    assert zones[0].duration_hours == 2.0


def test_merge_compares_candidate_poc_to_mean_of_zone_hours():
    """
    Цепочка POC (от нового к старому): 100000 → 99980 → 100090 → 100000.
    Попарно: после 100000/99980 следующая пара 99980/100090 даёт разрыв >0.1%%.
    Среднее по уже включённым часам тянет «мост» — все 4 часа остаются в одной зоне.
    """
    step = 10.0
    t0 = 1_710_000_000
    df = pd.concat(
        [
            _minute_rows(t0, 60, 100_000.0, 1.0),
            _minute_rows(t0 + 3600, 60, 100_090.0, 1.0),
            _minute_rows(t0 + 7200, 60, 99_980.0, 1.0),
            _minute_rows(t0 + 10_800, 60, 100_000.0, 1.0),
        ],
        ignore_index=True,
    )
    zones = find_accumulation_zones(
        df,
        bin_step=step,
        poc_merge_threshold_pct=0.001,
        min_zone_hours=1.0,
    )
    assert len(zones) == 1
    assert zones[0].duration_hours == 4.0


def test_tiers_by_original_spec():
    # Tier 1: vol > 3 * mean(all volumes) и длительность > 48 ч
    z = [
        AccumulationZone(1.0, 1000.0, 50.0, 0, 1),
        AccumulationZone(1.0, 10.0, 20.0, 0, 1),
        AccumulationZone(1.0, 10.0, 8.0, 0, 1),
        AccumulationZone(1.0, 5.0, 12.0, 0, 1),
    ]
    assign_tiers_by_original_spec(z)
    assert z[0].tier == "Tier 1 (Бетон)"
    assert z[1].tier == "Tier 2 (Сильный)"
    assert z[2].tier == "Tier 3 (Локальный)"
    assert z[3].tier == "Tier 3 (Локальный)"


def test_take_top_n_per_price_band():
    z = [
        AccumulationZone(70_000.0, 100.0, 1.0, 0, 1),
        AccumulationZone(70_050.0, 500.0, 1.0, 0, 1),
        AccumulationZone(70_080.0, 300.0, 1.0, 0, 1),
        AccumulationZone(72_000.0, 999.0, 1.0, 0, 1),
        AccumulationZone(72_100.0, 1.0, 1.0, 0, 1),
    ]
    out = take_top_n_per_price_band(z, band_width_usdt=500.0, top_n=2)
    assert len(out) == 4
    prices_70k = [x.poc_price for x in out if 69_500 <= x.poc_price < 70_500]
    assert set(prices_70k) == {70_050.0, 70_080.0}
    prices_72k = [x.poc_price for x in out if 71_500 <= x.poc_price < 72_500]
    assert set(prices_72k) == {72_000.0, 72_100.0}


def test_merge_close_zones_weighted_example():
    """66689/66891 ~0.3%% < 0.5%%; взвешенная цена и суммарный объём."""
    z = [
        AccumulationZone(66_689.0, 1580.0, 10.0, 100, 200),
        AccumulationZone(66_891.0, 1084.0, 8.0, 150, 250),
    ]
    out = merge_close_zones_weighted(z, merge_threshold_pct=0.005)
    assert len(out) == 1
    exp_p = (66_689.0 * 1580.0 + 66_891.0 * 1084.0) / (1580.0 + 1084.0)
    assert abs(out[0].poc_price - round(exp_p, 2)) < 0.02
    assert out[0].total_volume == 1580.0 + 1084.0
    assert out[0].duration_hours == 18.0
    assert out[0].t_start == 100
    assert out[0].t_end == 250


def test_merge_close_zones_weighted_no_merge_when_far():
    z = [
        AccumulationZone(100.0, 10.0, 1.0, 0, 1),
        AccumulationZone(110.0, 10.0, 1.0, 0, 1),
    ]
    out = merge_close_zones_weighted(z, merge_threshold_pct=0.005)
    assert len(out) == 2


def test_take_top_n_per_price_band_detailed_explains_drop():
    """Зона 70_000 с объёмом 100 отсекается: в той же полосе 500 и 300 при top_n=2."""
    z = [
        AccumulationZone(70_000.0, 100.0, 1.0, 0, 1),
        AccumulationZone(70_050.0, 500.0, 1.0, 0, 1),
        AccumulationZone(70_080.0, 300.0, 1.0, 0, 1),
    ]
    kept, dropped = take_top_n_per_price_band_detailed(z, band_width_usdt=500.0, top_n=2)
    assert len(kept) == 2
    assert len(dropped) == 1
    assert dropped[0]["poc_price"] == 70_000.0
    assert dropped[0]["rank_by_volume_in_band"] == 3
    assert dropped[0]["kept_volumes_in_band"] == [500.0, 300.0]
    assert "топ-2" in dropped[0]["reason"]


def test_rescan_clusters_nearby_levels():
    tick = 100.0
    t0 = 1_800_000_000
    df1 = _minute_rows(t0, 60, 70_100.0, 2.0)
    df2 = _minute_rows(t0 + 3600, 60, 70_500.0, 2.0)
    df = pd.concat([df1, df2], ignore_index=True)
    df_b = apply_tick_step_to_close(df, tick)
    primary = [
        AccumulationZone(70_000.0, 120.0, 1.0, int(df1["timestamp"].min()), int(df1["timestamp"].max())),
        AccumulationZone(70_500.0, 120.0, 1.0, int(df2["timestamp"].min()), int(df2["timestamp"].max())),
    ]
    masters = rescan_to_master_levels(primary, df_b, tick, cluster_threshold_pct=0.01)
    assert len(masters) == 1
    assert masters[0].total_volume == float(df_b["volume"].sum())
