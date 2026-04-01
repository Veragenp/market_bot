"""
Уровни по пикам сглаженного объёмного профиля (HVN) — scipy.signal.find_peaks.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from typing import Optional, Tuple

__all__ = [
    "find_pro_levels",
    "merge_by_valley",
    "greedy_level_selection",
    "get_adaptive_params",
    "analyze_coin_zones",
]


def _merge_close_level_rows(
    rows: list[dict],
    *,
    merge_pct: float,
    tier1_h: float,
    tier2_h: float,
    df_original: pd.DataFrame,
    distance_pct: float,
    profile: Optional[pd.Series] = None,
    final_merge_valley_threshold: Optional[float] = None,
) -> list[dict]:
    if len(rows) <= 1 or merge_pct <= 0:
        return rows
    rows_sorted = sorted(rows, key=lambda r: float(r["Price"]))
    groups: list[list[dict]] = [[rows_sorted[0]]]
    for r in rows_sorted[1:]:
        prev = groups[-1][-1]
        p1 = float(prev["Price"])
        p2 = float(r["Price"])
        ref = max(min(abs(p1), abs(p2)), 1e-9)
        can_merge = abs(p2 - p1) / ref <= merge_pct
        if can_merge and profile is not None and final_merge_valley_threshold is not None:
            i1 = int(profile.index.get_indexer([p1], method="nearest")[0])
            i2 = int(profile.index.get_indexer([p2], method="nearest")[0])
            lo, hi = (i1, i2) if i1 <= i2 else (i2, i1)
            if hi > lo:
                valley_min = float(profile.iloc[lo : hi + 1].min())
                peak_min = float(min(profile.iloc[i1], profile.iloc[i2]))
                can_merge = valley_min >= peak_min * float(final_merge_valley_threshold)
        if can_merge:
            groups[-1].append(r)
        else:
            groups.append([r])

    out: list[dict] = []
    for g in groups:
        p_min = min(float(x["Price"]) for x in g)
        p_max = max(float(x["Price"]) for x in g)
        lower = p_min * (1.0 - float(distance_pct))
        upper = p_max * (1.0 + float(distance_pct))
        sub = df_original[(df_original["close"] >= lower) & (df_original["close"] <= upper)]
        if sub.empty:
            continue
        poc_idx = sub["volume"].idxmax()
        poc_price = float(sub.loc[poc_idx, "close"])
        vol_sum = float(sub["volume"].sum())
        if "timestamp" in sub.columns:
            ts = np.sort(sub["timestamp"].to_numpy(dtype=np.int64))
            step_seconds = float(np.median(np.diff(ts))) if ts.size >= 2 else 60.0
            dur_h = float(sub.shape[0]) * max(step_seconds, 1.0) / 3600.0
            start_utc = pd.Timestamp(int(sub["timestamp"].min()), unit="s", tz="UTC").isoformat()
            end_utc = pd.Timestamp(int(sub["timestamp"].max()), unit="s", tz="UTC").isoformat()
        else:
            dur_h = float(sub.shape[0]) / 60.0
            start_utc = ""
            end_utc = ""
        if dur_h > tier1_h:
            tier = "Tier 1 (Бетон)"
        elif dur_h > tier2_h:
            tier = "Tier 2 (Сильный)"
        else:
            tier = "Tier 3 (Локальный)"
        out.append(
            {
                "Price": round(float(poc_price), 2),
                "Volume": round(vol_sum, 2),
                "Duration_Hrs": round(dur_h, 1),
                "Tier": tier,
                "start_utc": start_utc,
                "end_utc": end_utc,
            }
        )
    return sorted(out, key=lambda r: float(r["Volume"]), reverse=True)


def merge_by_valley(
    profile: pd.Series,
    peaks: np.ndarray,
    threshold: float = 0.5,
    merge_distance_pct: float = 0.0,
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
        price_1 = float(profile.index[p1])
        price_2 = float(profile.index[p2])
        p_ref = max(min(abs(price_1), abs(price_2)), 1e-9)
        near_by_distance = abs(price_1 - price_2) / p_ref <= max(float(merge_distance_pct), 0.0)
        if valley_min > peak_min * float(threshold) or near_by_distance:
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
        poc_price = float(prices[int(np.argmax(volumes))])
        final_levels.append({"Price": round(poc_price, 2), "Volume": round(v_sum, 2)})
    return final_levels


def _dedup_level_rows(
    rows: list[dict],
    *,
    tick_size: float,
    current_price: float,
    dedup_round_pct: float,
) -> list[dict]:
    if not rows:
        return []
    dedup_round = max(float(tick_size), float(current_price) * max(float(dedup_round_pct), 1e-6))
    buckets: dict[float, dict] = {}
    for r in rows:
        price = float(r["Price"])
        key = round(price / dedup_round) * dedup_round
        prev = buckets.get(key)
        if prev is None:
            buckets[key] = r
            continue
        if float(r["Volume"]) > float(prev["Volume"]) or (
            float(r["Volume"]) == float(prev["Volume"])
            and float(r["Duration_Hrs"]) > float(prev["Duration_Hrs"])
        ):
            buckets[key] = r
    return sorted(buckets.values(), key=lambda x: float(x["Volume"]), reverse=True)


def greedy_level_selection(
    profile: pd.Series,
    df_original: pd.DataFrame,
    *,
    distance_pct: float = 0.01,
    top_n: int = 5,
    min_duration_hours: float = 1.0,
    use_normalized_score: bool = True,
) -> pd.DataFrame:
    if "timestamp" in df_original.columns:
        ts = np.sort(df_original["timestamp"].to_numpy(dtype=np.int64))
        step_seconds = float(np.median(np.diff(ts))) if ts.size >= 2 else 60.0
    else:
        step_seconds = 60.0
    step_seconds = max(step_seconds, 1.0)

    candidates = []
    for price_bin, volume in profile.items():
        lower = float(price_bin) * (1.0 - float(distance_pct))
        upper = float(price_bin) * (1.0 + float(distance_pct))
        mask = (df_original["close"] >= lower) & (df_original["close"] <= upper)
        sub = df_original.loc[mask]
        if sub.empty:
            continue
        duration = float(sub.shape[0]) * step_seconds / 3600.0
        if duration >= float(min_duration_hours):
            candidates.append(
                {
                    "price": float(price_bin),
                    "volume": float(volume),
                    "duration": float(duration),
                    "score": float(volume) * float(duration),
                }
            )

    if candidates and use_normalized_score:
        vols = np.array([float(c["volume"]) for c in candidates], dtype=np.float64)
        durs = np.array([float(c["duration"]) for c in candidates], dtype=np.float64)
        med_v = float(np.median(vols)) if np.isfinite(np.median(vols)) and np.median(vols) > 1e-12 else 1.0
        med_d = float(np.median(durs)) if np.isfinite(np.median(durs)) and np.median(durs) > 1e-12 else 1.0
        for c in candidates:
            c["score"] = (float(c["volume"]) / med_v) * (float(c["duration"]) / med_d)

    candidates_sorted = sorted(candidates, key=lambda x: x["score"], reverse=True)
    selected = []
    for cand in candidates_sorted:
        if len(selected) >= int(top_n):
            break
        overlapping = any(
            abs(float(cand["price"]) - float(s["price"])) / max(abs(float(s["price"])), 1e-9)
            < float(distance_pct)
            for s in selected
        )
        if not overlapping:
            selected.append(cand)

    result = []
    for s in selected:
        lower = float(s["price"]) * (1.0 - float(distance_pct))
        upper = float(s["price"]) * (1.0 + float(distance_pct))
        mask = (df_original["close"] >= lower) & (df_original["close"] <= upper)
        sub = df_original.loc[mask]
        if sub.empty:
            continue
        poc_idx = sub["volume"].idxmax()
        poc_price = float(sub.loc[poc_idx, "close"])
        total_volume = float(sub["volume"].sum())
        if "timestamp" in sub.columns:
            t0 = int(sub["timestamp"].min())
            t1 = int(sub["timestamp"].max())
        else:
            t0, t1 = None, None
        duration = float(sub.shape[0]) * step_seconds / 3600.0
        result.append(
            {
                "Price": round(poc_price, 2),
                "Volume": round(total_volume, 2),
                "Duration_Hrs": round(duration, 2),
                "Score": round(float(s["score"]), 6),
                "start_ts": t0,
                "end_ts": t1,
            }
        )
    return pd.DataFrame(result)


def find_pro_levels(
    df: pd.DataFrame,
    smoothing_window: int = 5,
    height_percentile: float = 0.8,
    distance_pct: float = 0.002,
    valley_threshold: float = 0.9,
    merge_distance_pct: float = 0.001,
    tick_size: float | None = None,
    duration_thresholds: Optional[Tuple[float, float]] = None,
    height_mult: Optional[float] = None,
    top_n: int = 10,
    min_duration_hours: float = 1.0,
    final_merge_pct: Optional[float] = None,
    max_levels: Optional[int] = None,
    include_all_tiers: bool = True,
    valley_merge_threshold: Optional[float] = None,
    enable_valley_merge: bool = True,
    return_raw: bool = False,
    return_dedup: bool = False,
    height_percentile_strong: float = 0.85,
    height_percentile_weak: float = 0.65,
    allow_stage_b_overlap: bool = True,
    dedup_round_pct: float = 0.001,
    final_merge_valley_threshold: Optional[float] = None,
) -> pd.DataFrame:
    """
    smoothing_window: окно сглаживания профиля (рекомендуемо 5-7).
    height_percentile: порог высоты как перцентиль сглаженного профиля.
    distance_pct: минимальное расстояние между уровнями по цене.
    merge_distance_pct: склейка близких пиков по расстоянию.
    duration_thresholds: пороги Tier как (tier1_hours, tier2_hours), по умолчанию (48, 12).
    height_mult: обратная совместимость; если задан, перекрывает percentile-порог.
    """
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
    # Адаптивный шаг сетки: процент от текущей цены (без fixed "10" fallback).
    if tick_size is None:
        tick_size_eff = max(current_price * 0.0005, 1e-8)
    else:
        tick_size_eff = max(float(tick_size), 1e-8)

    work["price_bin"] = (work["close"] / tick_size_eff).round() * tick_size_eff
    profile = work.groupby("price_bin", sort=True)["volume"].sum()
    win = max(3, int(smoothing_window))
    if win % 2 == 0:
        win += 1
    volume_sm = profile.rolling(window=win, center=True, min_periods=1).mean().fillna(0.0)

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

    mean_sm = float(volume_sm.mean()) if float(volume_sm.mean()) > 0 else 1e-18
    # Base threshold by percentile (less sensitive to outliers).
    q = min(max(float(height_percentile), 0.5), 0.99)
    height_thr = float(volume_sm.quantile(q))
    if not np.isfinite(height_thr) or height_thr <= 0:
        height_thr = mean_sm

    # Stage A/B: strong and weak candidate pools.
    q_strong = min(max(float(height_percentile_strong), 0.55), 0.99)
    q_weak = min(max(float(height_percentile_weak), 0.5), q_strong)
    strong_thr = max(height_thr, float(volume_sm.quantile(q_strong)))
    if height_mult is not None:
        # Explicit override for strong stage only.
        strong_thr = max(strong_thr, mean_sm * max(float(height_mult), 1.0))
    weak_thr = float(volume_sm.quantile(q_weak))
    strong_profile = volume_sm[volume_sm >= strong_thr].sort_values(ascending=False)
    weak_profile = volume_sm[volume_sm >= weak_thr].sort_values(ascending=False)
    if strong_profile.empty and weak_profile.empty:
        return pd.DataFrame(
            columns=["Price", "Volume", "Duration_Hrs", "Tier", "start_utc", "end_utc"]
        )
    # Доп. этап: склеиваем пики с неглубокой долиной до жадного отбора.
    vm_thr = float(valley_merge_threshold) if valley_merge_threshold is not None else float(valley_threshold)
    vm_dist = min(max(float(merge_distance_pct), 0.0), 0.005)
    if enable_valley_merge:
        def _valley_merge_series(s: pd.Series) -> pd.Series:
            if s.empty:
                return s
            peak_positions = np.array([int(volume_sm.index.get_loc(p)) for p in s.index], dtype=int)
            valley_merged = merge_by_valley(
                volume_sm, peak_positions, threshold=vm_thr, merge_distance_pct=vm_dist
            )
            if not valley_merged:
                return s
            return pd.Series({float(x["Price"]): float(x["Volume"]) for x in valley_merged}).sort_values(ascending=False)

        strong_profile = _valley_merge_series(strong_profile)
        weak_profile = _valley_merge_series(weak_profile)

    eff_top_n = int(max_levels) if max_levels is not None else (len(strong_profile) + len(weak_profile))
    if eff_top_n <= 0:
        eff_top_n = max(len(strong_profile), len(weak_profile))

    select_distance_pct = max(float(distance_pct) * 0.5, 0.001)
    selected_strong = greedy_level_selection(
        strong_profile,
        work,
        distance_pct=select_distance_pct,
        top_n=eff_top_n,
        min_duration_hours=float(min_duration_hours),
        use_normalized_score=True,
    )
    selected_df = selected_strong.copy()
    if not weak_profile.empty:
        selected_weak = greedy_level_selection(
            weak_profile,
            work,
            distance_pct=select_distance_pct,
            top_n=eff_top_n * 2,
            min_duration_hours=float(min_duration_hours),
            use_normalized_score=True,
        )
        if not allow_stage_b_overlap and not selected_weak.empty and not selected_strong.empty:
            strong_prices = selected_strong["Price"].to_numpy(dtype=np.float64)
            keep_mask = []
            for p in selected_weak["Price"].to_numpy(dtype=np.float64):
                ref = np.maximum(np.minimum(np.abs(strong_prices), np.abs(p)), 1e-9)
                near = np.any(np.abs(strong_prices - p) / ref <= float(distance_pct))
                keep_mask.append(not near)
            selected_weak = selected_weak.loc[keep_mask]
        selected_df = pd.concat([selected_strong, selected_weak], ignore_index=True)
        if not selected_df.empty:
            selected_df = selected_df.sort_values("Score", ascending=False).head(eff_top_n).reset_index(drop=True)

    if selected_df.empty:
        return pd.DataFrame(
            columns=["Price", "Volume", "Duration_Hrs", "Tier", "start_utc", "end_utc"]
        )
    if duration_thresholds is None:
        tier1_h, tier2_h = 48.0, 12.0
    else:
        tier1_h = float(duration_thresholds[0])
        tier2_h = float(duration_thresholds[1])

    if "timestamp" in work.columns:
        ts_sorted = np.sort(work["timestamp"].to_numpy(dtype=np.int64))
        step_seconds = max(1.0, float(np.median(np.diff(ts_sorted)))) if ts_sorted.size >= 2 else 60.0
    else:
        step_seconds = 60.0

    levels: list[dict] = []
    for _, row_match in selected_df.iterrows():
        price_level = float(row_match["Price"])
        duration_hrs = float(row_match["Duration_Hrs"])

        if duration_hrs > tier1_h:
            tier = "Tier 1 (Бетон)"
        elif duration_hrs > tier2_h:
            tier = "Tier 2 (Сильный)"
        else:
            tier = "Tier 3 (Локальный)"
        if not include_all_tiers and tier == "Tier 3 (Локальный)":
            continue

        start_utc = ""
        end_utc = ""
        if "timestamp" in work.columns:
            t0 = row_match.get("start_ts")
            t1 = row_match.get("end_ts")
            if pd.notnull(t0) and pd.notnull(t1):
                t0 = int(t0)
                t1 = int(t1)
                start_utc = pd.Timestamp(t0, unit="s", tz="UTC").isoformat()
                end_utc = pd.Timestamp(t1, unit="s", tz="UTC").isoformat()

        levels.append(
            {
                "Price": round(price_level, 2),
                "Volume": round(float(row_match["Volume"]), 2),
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
    if return_raw:
        return pd.DataFrame(levels).sort_values("Volume", ascending=False).reset_index(drop=True)
    levels = _dedup_level_rows(
        levels,
        tick_size=tick_size_eff,
        current_price=current_price,
        dedup_round_pct=float(dedup_round_pct),
    )
    if return_dedup:
        return pd.DataFrame(levels).sort_values("Volume", ascending=False).reset_index(drop=True)
    if final_merge_pct is not None:
        merge_pct = float(final_merge_pct)
    else:
        if len(levels) >= 2:
            ps = sorted(float(x["Price"]) for x in levels)
            gaps = [
                abs(ps[i + 1] - ps[i]) / max(min(abs(ps[i + 1]), abs(ps[i])), 1e-9)
                for i in range(len(ps) - 1)
            ]
            med_gap = float(np.median(gaps)) if gaps else 0.0
            merge_pct = max(0.01, med_gap * 1.5)
        else:
            merge_pct = max(0.01, float(distance_pct) * 2.0)
    merge_pct = min(max(float(merge_pct), 0.003), 0.01)
    levels = _merge_close_level_rows(
        levels,
        merge_pct=merge_pct,
        tier1_h=tier1_h,
        tier2_h=tier2_h,
        df_original=work,
        distance_pct=float(distance_pct),
        profile=volume_sm,
        final_merge_valley_threshold=final_merge_valley_threshold,
    )
    return pd.DataFrame(levels).sort_values("Volume", ascending=False).reset_index(drop=True)


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
    q80 = float(profile_vol.quantile(0.80)) if not profile_vol.empty else 0.0
    if mean_v > 1e-12:
        height_mult = max(1.2, q80 / mean_v)
    else:
        height_mult = 1.2

    volume_cv = float(work["volume"].std() / (work["volume"].mean() + 1e-9))
    valley_threshold = 0.40
    distance_pct = 0.003

    return {
        "tick_size": float(tick_size),
        "distance_pct": float(distance_pct),
        "height_mult": round(float(height_mult), 2),
        "height_percentile": 0.8,
        "height_percentile_strong": 0.85,
        "height_percentile_weak": 0.65,
        "smoothing_window": 5,
        "merge_distance_pct": 0.001,
        "dedup_round_pct": 0.001,
        "duration_thresholds": (48.0, 12.0),
        "min_duration_hours": 6.0,
        "top_n": 10,
        "max_levels": None,
        "dynamic_merge_pct": min(
            max(0.003, float((work["high"] - work["low"]).mean()) / max(current_price, 1e-9) * 1.5),
            0.01,
        ),
        "final_merge_valley_threshold": 0.5,
        "valley_merge_threshold": 0.4,
        "enable_valley_merge": True,
        "valley_threshold": float(valley_threshold),
        "volume_cv": round(float(volume_cv), 2),
        "avg_hourly_volatility": round(
            float((work["high"] - work["low"]).mean()) / max(current_price, 1e-9), 5
        ),
    }


def analyze_coin_zones(df: pd.DataFrame, symbol: str = "BTC/USDT") -> pd.DataFrame:
    params = get_adaptive_params(df)
    print(
        f"--- Анализ {symbol} --- "
        f"Dist: {params['distance_pct']:.4f}, Valley: {params['valley_threshold']}"
    )
    hm = params.get("height_mult")
    return find_pro_levels(
        df,
        smoothing_window=int(params.get("smoothing_window", 5)),
        height_percentile=float(params.get("height_percentile", 0.8)),
        height_percentile_strong=float(params.get("height_percentile_strong", 0.85)),
        height_percentile_weak=float(params.get("height_percentile_weak", 0.65)),
        distance_pct=float(params["distance_pct"]),
        valley_threshold=float(params["valley_threshold"]),
        merge_distance_pct=float(params.get("merge_distance_pct", 0.001)),
        duration_thresholds=params.get("duration_thresholds", (48.0, 12.0)),
        tick_size=float(params["tick_size"]),
        height_mult=float(hm) if hm is not None else None,
        top_n=int(params.get("top_n", 10)),
        min_duration_hours=float(params.get("min_duration_hours", 6.0)),
        max_levels=params.get("max_levels"),
        final_merge_pct=float(params.get("dynamic_merge_pct")) if params.get("dynamic_merge_pct") is not None else None,
        valley_merge_threshold=float(params.get("valley_merge_threshold", 0.5)),
        enable_valley_merge=bool(params.get("enable_valley_merge", True)),
        allow_stage_b_overlap=True,
        dedup_round_pct=float(params.get("dedup_round_pct", 0.001)),
        final_merge_valley_threshold=(
            float(params.get("final_merge_valley_threshold"))
            if params.get("final_merge_valley_threshold") is not None
            else None
        ),
    )
