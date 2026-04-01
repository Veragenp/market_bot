"""
Уровни по пикам сглаженного объёмного профиля (HVN) — scipy.signal.find_peaks.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

__all__ = [
    "find_pro_levels",
    "merge_by_valley",
    "get_adaptive_params",
    "analyze_coin_zones",
]


def merge_by_valley(
    profile: pd.Series, peaks: np.ndarray, threshold: float = 0.5
) -> list[dict]:
    """
    Склеивает соседние пики, если "долина" между ними неглубокая:
    valley_min > min(peak1, peak2) * threshold.
    """
    if len(peaks) == 0:
        return []

    peaks_sorted = np.array(sorted(int(p) for p in peaks), dtype=int)
    merged_peaks: list[list[int]] = []
    current_cluster = [int(peaks_sorted[0])]

    for i in range(1, len(peaks_sorted)):
        p1, p2 = int(peaks_sorted[i - 1]), int(peaks_sorted[i])
        valley_min = float(profile.iloc[p1 : p2 + 1].min())
        peak_min = float(min(profile.iloc[p1], profile.iloc[p2]))
        if valley_min > peak_min * float(threshold):
            current_cluster.append(p2)
        else:
            merged_peaks.append(current_cluster)
            current_cluster = [p2]
    merged_peaks.append(current_cluster)

    final_levels: list[dict] = []
    for cluster in merged_peaks:
        left = int(cluster[0])
        right = int(cluster[-1])
        prices = profile.index[left : right + 1].to_numpy(dtype=np.float64)
        volumes = profile.iloc[left : right + 1].to_numpy(dtype=np.float64)
        v_sum = float(volumes.sum())
        if v_sum <= 0:
            continue
        poc_price = float((prices * volumes).sum() / v_sum)
        final_levels.append({"Price": round(poc_price, 2), "Volume": round(v_sum, 2)})
    return final_levels


def find_pro_levels(
    df: pd.DataFrame,
    height_mult: float = 2.0,
    distance_pct: float = 0.002,
    valley_threshold: float = 0.9,
    tick_size: float | None = None,
) -> pd.DataFrame:
    """
    height_mult: минимальная высота пика относительно среднего по сглаженному профилю.
    distance_pct: минимальное расстояние между уровнями по цене (0.005 = 0.5%).
    """
    try:
        from scipy.signal import find_peaks
    except ImportError as exc:
        raise RuntimeError(
            "Для find_pro_levels нужен scipy: pip install scipy"
        ) from exc

    work = df.copy()
    if "close" not in work.columns or "volume" not in work.columns:
        raise ValueError("Нужны колонки close и volume")
    work = work.dropna(subset=["close", "volume"])
    work = work[work["volume"] >= 0]
    if work.empty:
        return pd.DataFrame(
            columns=[
                "Price",
                "Volume",
                "Duration_Hrs",
                "Tier",
                "start_utc",
                "end_utc",
            ]
        )

    current_price = float(work["close"].iloc[-1])
    if tick_size is None:
        tick_size_eff = max(current_price * 0.0005, 1e-8)
    else:
        tick_size_eff = max(float(tick_size), 1e-8)

    work["price_bin"] = (work["close"] / tick_size_eff).round() * tick_size_eff
    profile = work.groupby("price_bin", sort=True)["volume"].sum()
    volume_sm = profile.rolling(window=3, center=True).mean().fillna(0.0)

    if len(volume_sm) < 1:
        return pd.DataFrame(
            columns=[
                "Price",
                "Volume",
                "Duration_Hrs",
                "Tier",
                "start_utc",
                "end_utc",
            ]
        )

    mean_sm = float(volume_sm.mean())
    if mean_sm <= 0:
        mean_sm = 1e-18

    min_dist = int((current_price * distance_pct) / tick_size_eff)
    peaks, _ = find_peaks(
        volume_sm.values,
        height=mean_sm * height_mult,
        distance=max(1, min_dist),
    )

    levels: list[dict] = []
    for lvl in merge_by_valley(volume_sm, peaks, threshold=valley_threshold):
        price_level = float(lvl["Price"])
        vol_at_level = float(lvl["Volume"])

        mask = (work["close"] >= price_level * 0.998) & (
            work["close"] <= price_level * 1.002
        )
        duration_hrs = float(work.loc[mask].shape[0]) / 60.0

        if duration_hrs > 48:
            tier = "Tier 1 (Бетон)"
        elif duration_hrs > 12:
            tier = "Tier 2 (Сильный)"
        else:
            tier = "Tier 3 (Локальный)"

        start_utc = ""
        end_utc = ""
        if "timestamp" in work.columns:
            sub = work.loc[mask]
            if not sub.empty:
                t0 = int(sub["timestamp"].min())
                t1 = int(sub["timestamp"].max())
                start_utc = pd.Timestamp(t0, unit="s", tz="UTC").isoformat()
                end_utc = pd.Timestamp(t1, unit="s", tz="UTC").isoformat()

        levels.append(
            {
                "Price": round(price_level, 2),
                "Volume": round(vol_at_level, 2),
                "Duration_Hrs": round(duration_hrs, 1),
                "Tier": tier,
                "start_utc": start_utc,
                "end_utc": end_utc,
            }
        )

    if not levels:
        return pd.DataFrame(
            columns=[
                "Price",
                "Volume",
                "Duration_Hrs",
                "Tier",
                "start_utc",
                "end_utc",
            ]
        )
    return (
        pd.DataFrame(levels).sort_values("Volume", ascending=False).reset_index(drop=True)
    )


def get_adaptive_params(df: pd.DataFrame) -> dict:
    """
    Универсальная адаптация параметров под волатильность и структуру объема монеты.
    """
    work = df.copy()
    required = {"close", "high", "low", "volume"}
    missing = required.difference(work.columns)
    if missing:
        raise ValueError(f"Нужны колонки: {', '.join(sorted(missing))}")
    work = work.dropna(subset=["close", "high", "low", "volume"])
    if work.empty:
        raise ValueError("Пустой DataFrame после очистки")

    current_price = float(work["close"].iloc[-1])
    tick_size = max(current_price * 0.0005, 1e-8)

    work["price_bin"] = (work["close"] / tick_size).round() * tick_size
    profile_vol = work.groupby("price_bin")["volume"].sum()
    mean_v = float(profile_vol.mean()) if not profile_vol.empty else 0.0
    std_v = float(profile_vol.std()) if not profile_vol.empty else 0.0

    volume_cv = float(work["volume"].std() / (work["volume"].mean() + 1e-9))
    sigma_factor = 1.5 if volume_cv < 2.0 else 1.0
    height_threshold = mean_v + (sigma_factor * std_v)
    if mean_v > 1e-12:
        height_mult = float(height_threshold / mean_v)
    else:
        height_mult = 1.2

    valley_threshold = 0.65 if volume_cv < 2.0 else 0.55
    distance_pct = max(0.003, float((work["high"] - work["low"]).mean()) / max(current_price, 1e-9) * 1.5)

    return {
        "tick_size": float(tick_size),
        "distance_pct": float(distance_pct),
        "height_mult": round(float(height_mult), 2),
        "valley_threshold": float(valley_threshold),
        "volume_cv": round(float(volume_cv), 2),
        "avg_hourly_volatility": round(float(distance_pct / 1.5), 5),
    }


def analyze_coin_zones(df: pd.DataFrame, symbol: str = "BTC/USDT") -> pd.DataFrame:
    params = get_adaptive_params(df)
    print(
        f"--- Анализ {symbol} --- "
        f"Dist: {params['distance_pct']:.4f}, Valley: {params['valley_threshold']}"
    )
    return find_pro_levels(
        df,
        height_mult=float(params["height_mult"]),
        distance_pct=float(params["distance_pct"]),
        valley_threshold=float(params["valley_threshold"]),
        tick_size=float(params["tick_size"]),
    )
