"""
Уровни по пикам сглаженного объёмного профиля (HVN): жадный отбор по сглаженному профилю,
склейка по долинам и финальный merge близких уровней.

Единая «умная» метрика уровня (жёсткий и мягкий проходы): коридор цен
[min·(1−distance_pct), max·(1+distance_pct)]; в зону входят свечи, **пересекающие** диапазон
по high/low (не только close внутри — иначе теряется «пила»). Суммарный объём по всем таким
свечам; POC — **price_bin(close)** с максимальной суммой объёма (тот же шаг tick_size, что
у глобального профиля), а не одна свеча с max volume. Длительность — по шагу времени в поднаборе.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from typing import Optional, Tuple

TIER1_LABEL = "Tier 1 (Бетон)"

# Финальная склейка соседних уровней (доля |Δцена|/цена). Доли процента дают «лесенку»;
# целевой разнос итоговых зон — порядка нескольких процентов (см. комментарий у _cluster_level_row_groups).
FINAL_MERGE_PCT_CLAMP_MIN = 0.03
FINAL_MERGE_PCT_CLAMP_MAX = 0.05


def _resolve_final_merge_pct(
    levels_dedup: list[dict],
    final_merge_pct: Optional[float],
    distance_pct: float,
    *,
    skip_clamp: bool,
) -> float:
    """Порог слияния соседних уровней по цене (относительный)."""
    if final_merge_pct is not None:
        merge_pct = float(final_merge_pct)
    else:
        if len(levels_dedup) >= 2:
            ps = sorted(float(x["Price"]) for x in levels_dedup)
            gaps = [
                abs(ps[i + 1] - ps[i]) / max(min(abs(ps[i + 1]), abs(ps[i])), 1e-9)
                for i in range(len(ps) - 1)
            ]
            med_gap = float(np.median(gaps)) if gaps else 0.0
            merge_pct = max(FINAL_MERGE_PCT_CLAMP_MIN, med_gap * 1.5)
        else:
            merge_pct = max(FINAL_MERGE_PCT_CLAMP_MIN, float(distance_pct) * 2.0)
    if not skip_clamp:
        merge_pct = min(
            max(float(merge_pct), float(FINAL_MERGE_PCT_CLAMP_MIN)),
            float(FINAL_MERGE_PCT_CLAMP_MAX),
        )
    return merge_pct


def _cluster_level_row_groups(
    rows_sorted: list[dict],
    merge_pct: float,
    profile: Optional[pd.Series],
    final_merge_valley_threshold: Optional[float],
) -> list[list[dict]]:
    """
    Группы уровней для финального merge. Важно: привязка к **якорю** группы (мин. цена в кластере),
    а не цепочка «каждый к предыдущему» — иначе при merge_pct≈3% весь диапазон 66k–71k схлопывается
    транзитивно в один уровень.
    """
    if not rows_sorted:
        return []

    def _valley_allows(p1: float, p2: float) -> bool:
        if profile is None or final_merge_valley_threshold is None:
            return True
        i1 = int(profile.index.get_indexer([p1], method="nearest")[0])
        i2 = int(profile.index.get_indexer([p2], method="nearest")[0])
        lo, hi = (i1, i2) if i1 <= i2 else (i2, i1)
        if hi <= lo:
            return True
        valley_min = float(profile.iloc[lo : hi + 1].min())
        peak_min = float(min(profile.iloc[i1], profile.iloc[i2]))
        return valley_min >= peak_min * float(final_merge_valley_threshold)

    groups: list[list[dict]] = [[rows_sorted[0]]]
    anchor = float(rows_sorted[0]["Price"])
    for r in rows_sorted[1:]:
        p = float(r["Price"])
        ref = max(min(abs(anchor), abs(p)), 1e-9)
        can_merge = abs(p - anchor) / ref <= float(merge_pct) and _valley_allows(anchor, p)
        if can_merge:
            groups[-1].append(r)
        else:
            groups.append([r])
            anchor = p
    return groups


def _snapshot_band_volume(
    df_original: pd.DataFrame,
    lower: float,
    upper: float,
    tick_size: float,
) -> Optional[tuple[float, float, float, Optional[int], Optional[int]]]:
    """
    Свечи, пересекающие [lower, upper] по high/low.
    Суммарный объём — сумма volume по всем свечам в коридоре.
    POC — бин с максимальной суммой объёма (bin по close round(tick)).

    Returns (poc_price, vol_sum, dur_h, t0, t1) или None.
    """
    lo, hi = float(lower), float(upper)
    if lo > hi:
        lo, hi = hi, lo
    if "high" in df_original.columns and "low" in df_original.columns:
        sub = df_original[(df_original["high"] >= lo) & (df_original["low"] <= hi)]
    else:
        sub = df_original[(df_original["close"] >= lo) & (df_original["close"] <= hi)]
    if sub.empty:
        return None
    ts_bin = max(float(tick_size), 1e-12)
    # Бинирование по close round (чтобы совпадало с профилем в других модулях).
    binned = (sub["close"] / ts_bin).round() * ts_bin
    vol_by_bin = sub.groupby(binned, sort=False)["volume"].sum()
    if vol_by_bin.empty:
        return None

    poc_price = float(vol_by_bin.idxmax())
    vol_sum = float(sub["volume"].sum())
    if "timestamp" in sub.columns:
        ts = np.sort(sub["timestamp"].to_numpy(dtype=np.int64))
        step_seconds = float(np.median(np.diff(ts))) if ts.size >= 2 else 60.0
        dur_h = float(sub.shape[0]) * max(step_seconds, 1.0) / 3600.0
        t0 = int(sub["timestamp"].min())
        t1 = int(sub["timestamp"].max())
    else:
        dur_h = float(sub.shape[0]) / 60.0
        t0, t1 = None, None
    return poc_price, vol_sum, dur_h, t0, t1


def _level_row_from_ohlcv_band(
    df_original: pd.DataFrame,
    lower: float,
    upper: float,
    tier1_h: float,
    tier2_h: float,
    *,
    tick_size: float,
    tier_override: Optional[str] = None,
) -> Optional[dict]:
    """Один уровень по полосе цен: суммарный объём, POC по бинам, Tier по длительности."""
    snap = _snapshot_band_volume(df_original, lower, upper, tick_size)
    if snap is None:
        return None
    poc_price, vol_sum, dur_h, t0, t1 = snap
    if "timestamp" in df_original.columns and t0 is not None and t1 is not None:
        start_utc = pd.Timestamp(t0, unit="s", tz="UTC").isoformat()
        end_utc = pd.Timestamp(t1, unit="s", tz="UTC").isoformat()
    else:
        start_utc = ""
        end_utc = ""
    if tier_override is not None:
        tier = tier_override
    elif dur_h > tier1_h:
        tier = TIER1_LABEL
    elif dur_h > tier2_h:
        tier = "Tier 2 (Сильный)"
    else:
        tier = "Tier 3 (Локальный)"
    return {
        "Price": round(float(poc_price), 2),
        "Volume": round(vol_sum, 2),
        "Duration_Hrs": round(dur_h, 1),
        "Tier": tier,
        "start_utc": start_utc,
        "end_utc": end_utc,
    }


def _refine_level_row_smart_band(
    row: dict,
    df_original: pd.DataFrame,
    distance_pct: float,
    tier1_h: float,
    tier2_h: float,
    tick_size: float,
    *,
    tier_override: Optional[str] = None,
) -> dict:
    """Пересчёт одной строки уровня тем же коридором ±distance_pct вокруг её цены."""
    p = float(row["Price"])
    lower = p * (1.0 - float(distance_pct))
    upper = p * (1.0 + float(distance_pct))
    got = _level_row_from_ohlcv_band(
        df_original,
        lower,
        upper,
        tier1_h,
        tier2_h,
        tick_size=float(tick_size),
        tier_override=tier_override,
    )
    return got if got is not None else dict(row)


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
    tick_size_eff: float,
    profile: Optional[pd.Series] = None,
    final_merge_valley_threshold: Optional[float] = None,
    preserve_tier1_from_group: bool = False,
) -> list[dict]:
    if merge_pct <= 0:
        refined = [
            _refine_level_row_smart_band(
                r, df_original, distance_pct, tier1_h, tier2_h, float(tick_size_eff)
            )
            for r in rows
        ]
        return sorted(refined, key=lambda r: float(r["Volume"]), reverse=True)

    rows_sorted = sorted(rows, key=lambda r: float(r["Price"]))
    groups = _cluster_level_row_groups(
        rows_sorted,
        float(merge_pct),
        profile,
        final_merge_valley_threshold,
    )

    out: list[dict] = []
    for g in groups:
        p_min = min(float(x["Price"]) for x in g)
        p_max = max(float(x["Price"]) for x in g)
        lower = p_min * (1.0 - float(distance_pct))
        upper = p_max * (1.0 + float(distance_pct))
        any_t1 = preserve_tier1_from_group and any(x.get("Tier") == TIER1_LABEL for x in g)
        row = _level_row_from_ohlcv_band(
            df_original,
            lower,
            upper,
            tier1_h,
            tier2_h,
            tick_size=float(tick_size_eff),
            tier_override=TIER1_LABEL if any_t1 else None,
        )
        if row is not None:
            out.append(row)
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


def _price_excluded_by_reserved(
    price: float,
    reserved_prices: list[float],
    exclude_reserved_pct: float,
) -> bool:
    if not reserved_prices or exclude_reserved_pct <= 0:
        return False
    pb = float(price)
    pct = float(exclude_reserved_pct)
    for rp in reserved_prices:
        ref = max(min(abs(pb), abs(float(rp))), 1e-9)
        if abs(pb - float(rp)) / ref <= pct:
            return True
    return False


def greedy_level_selection(
    profile: pd.Series,
    df_original: pd.DataFrame,
    *,
    tick_size: float,
    distance_pct: float = 0.01,
    top_n: int = 5,
    min_duration_hours: float = 1.0,
    use_normalized_score: bool = True,
    reserved_prices: Optional[list[float]] = None,
    exclude_reserved_pct: Optional[float] = None,
) -> pd.DataFrame:
    if "timestamp" in df_original.columns:
        ts = np.sort(df_original["timestamp"].to_numpy(dtype=np.int64))
        step_seconds = float(np.median(np.diff(ts))) if ts.size >= 2 else 60.0
    else:
        step_seconds = 60.0
    step_seconds = max(step_seconds, 1.0)

    ex_pct = float(exclude_reserved_pct) if exclude_reserved_pct is not None else 0.0
    res = list(reserved_prices) if reserved_prices else []

    candidates = []
    for price_bin, volume in profile.items():
        if _price_excluded_by_reserved(float(price_bin), res, ex_pct):
            continue
        lower = float(price_bin) * (1.0 - float(distance_pct))
        upper = float(price_bin) * (1.0 + float(distance_pct))
        if "high" in df_original.columns and "low" in df_original.columns:
            mask = (df_original["high"] >= lower) & (df_original["low"] <= upper)
        else:
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
        snap = _snapshot_band_volume(df_original, lower, upper, float(tick_size))
        if snap is None:
            continue
        poc_price, total_volume, duration, t0, t1 = snap
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


def _combine_main_weak(
    selected_strong: pd.DataFrame,
    selected_weak: pd.DataFrame,
    *,
    eff_top_n: int,
    distance_pct: float,
    allow_stage_b_overlap: bool,
) -> pd.DataFrame:
    """Склейка strong + weak в один датафрейм (legacy / single-pass)."""
    if selected_weak.empty:
        return selected_strong.copy()
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
    return selected_df


def _find_pro_levels_single_pass(
    work: pd.DataFrame,
    volume_sm: pd.Series,
    *,
    tick_size_eff: float,
    current_price: float,
    height_percentile: float,
    height_percentile_strong: float,
    height_percentile_weak: float,
    height_mult: Optional[float],
    distance_pct: float,
    merge_distance_pct: float,
    valley_threshold: float,
    valley_merge_threshold: Optional[float],
    enable_valley_merge: bool,
    min_duration_hours: float,
    top_n: int,
    max_levels: Optional[int],
    include_all_tiers: bool,
    allow_stage_b_overlap: bool,
    include_weak: bool,
    duration_thresholds: Optional[Tuple[float, float]],
    dedup_round_pct: float,
    final_merge_pct: Optional[float],
    final_merge_valley_threshold: Optional[float],
    reserved_prices: Optional[list[float]] = None,
    exclude_reserved_pct: Optional[float] = None,
    skip_final_merge_clamp: bool = False,
) -> tuple[list[dict], list[dict], list[dict]]:
    """
    Один полный проход: raw → dedup → merge.
    Возвращает (levels_raw, levels_dedup, levels_final).
    """
    mean_sm = float(volume_sm.mean()) if float(volume_sm.mean()) > 0 else 1e-18
    q = min(max(float(height_percentile), 0.5), 0.99)
    height_thr = float(volume_sm.quantile(q))
    if not np.isfinite(height_thr) or height_thr <= 0:
        height_thr = mean_sm

    q_strong = min(max(float(height_percentile_strong), 0.55), 0.99)
    q_weak = min(max(float(height_percentile_weak), 0.5), q_strong)
    strong_thr = max(height_thr, float(volume_sm.quantile(q_strong)))
    if height_mult is not None:
        strong_thr = max(strong_thr, mean_sm * max(float(height_mult), 1.0))
    weak_thr = float(volume_sm.quantile(q_weak))
    strong_profile = volume_sm[volume_sm >= strong_thr].sort_values(ascending=False)
    weak_profile = volume_sm[volume_sm >= weak_thr].sort_values(ascending=False)
    if strong_profile.empty and weak_profile.empty:
        return [], [], []

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
            return pd.Series({float(x["Price"]): float(x["Volume"]) for x in valley_merged}).sort_values(
                ascending=False
            )

        strong_profile = _valley_merge_series(strong_profile)
        weak_profile = _valley_merge_series(weak_profile)

    eff_top_n = int(max_levels) if max_levels is not None else (len(strong_profile) + len(weak_profile))
    if eff_top_n <= 0:
        eff_top_n = max(len(strong_profile), len(weak_profile))

    select_distance_pct = max(float(distance_pct) * 0.5, 0.001)
    res = reserved_prices
    ex_r = exclude_reserved_pct

    selected_strong = greedy_level_selection(
        strong_profile,
        work,
        tick_size=float(tick_size_eff),
        distance_pct=select_distance_pct,
        top_n=eff_top_n,
        min_duration_hours=float(min_duration_hours),
        use_normalized_score=True,
        reserved_prices=res,
        exclude_reserved_pct=ex_r,
    )
    if include_weak and not weak_profile.empty:
        selected_weak = greedy_level_selection(
            weak_profile,
            work,
            tick_size=float(tick_size_eff),
            distance_pct=select_distance_pct,
            top_n=eff_top_n * 2,
            min_duration_hours=float(min_duration_hours),
            use_normalized_score=True,
            reserved_prices=res,
            exclude_reserved_pct=ex_r,
        )
        selected_df = _combine_main_weak(
            selected_strong,
            selected_weak,
            eff_top_n=eff_top_n,
            distance_pct=float(distance_pct),
            allow_stage_b_overlap=allow_stage_b_overlap,
        )
    else:
        selected_df = selected_strong.copy()

    if selected_df.empty:
        return [], [], []

    if duration_thresholds is None:
        tier1_h, tier2_h = 48.0, 12.0
    else:
        tier1_h = float(duration_thresholds[0])
        tier2_h = float(duration_thresholds[1])

    levels: list[dict] = []
    for _, row_match in selected_df.iterrows():
        price_level = float(row_match["Price"])
        duration_hrs = float(row_match["Duration_Hrs"])

        if duration_hrs > tier1_h:
            tier = TIER1_LABEL
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
        return [], [], []

    raw_levels = list(levels)
    levels_dedup = _dedup_level_rows(
        list(levels),
        tick_size=tick_size_eff,
        current_price=current_price,
        dedup_round_pct=float(dedup_round_pct),
    )

    merge_pct = _resolve_final_merge_pct(
        list(levels_dedup),
        final_merge_pct,
        float(distance_pct),
        skip_clamp=skip_final_merge_clamp,
    )

    levels_final = _merge_close_level_rows(
        list(levels_dedup),
        merge_pct=merge_pct,
        tier1_h=tier1_h,
        tier2_h=tier2_h,
        df_original=work,
        distance_pct=float(distance_pct),
        tick_size_eff=float(tick_size_eff),
        profile=volume_sm,
        final_merge_valley_threshold=final_merge_valley_threshold,
    )
    return raw_levels, levels_dedup, levels_final


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
    two_pass_mode: bool = True,
    legacy_weak_merge: bool = False,
    run_soft_pass: bool = True,
    strict_height_percentile_weak: Optional[float] = None,
    strict_height_mult: Optional[float] = None,
    exclude_reserved_pct: Optional[float] = None,
    soft_height_percentile_strong: float = 0.6,
    soft_height_percentile_weak: float = 0.55,
    soft_height_mult: Optional[float] = None,
    soft_min_duration_hours: float = 4.0,
    soft_final_merge_pct: Optional[float] = None,
    include_weak: bool = True,
    weak_fallback_threshold: Optional[float] = None,
) -> pd.DataFrame:
    """
    two_pass_mode=True (по умолчанию): жёсткий проход фиксирует резерв (strict_full),
    в итог — Tier 1 только из жёсткого финала (после merge); мягкий остаток без пересечения
    с ценами резерва, при этом уровни мягкого прохода не получают Tier 1 (макс. Tier 2).
    soft_final_merge_pct: None — та же финальная склейка, что и у жёсткого; 0.0 — без склейки на мягком;
    число > 0 — свой порог. После strict+soft — общая финальная склейка (~3–5% от цены, см. константы
    FINAL_MERGE_PCT_CLAMP_*), чтобы итоговые зоны не шли «лесенкой» в доли процента.
    legacy_weak_merge=True: один проход как раньше (игнорируется two_pass).
    two_pass_mode=False: один проход без второго.
    return_raw / return_dedup при two_pass относятся к жёсткому проходу.
    weak_fallback_threshold: зарезервировано под совместимость (пока не используется).
    """
    if return_raw and return_dedup:
        raise ValueError("Задайте только один из return_raw или return_dedup")

    if legacy_weak_merge:
        two_pass_mode = False

    _ = weak_fallback_threshold  # API hook for future legacy behavior

    work = df.copy()
    if "close" not in work.columns or "volume" not in work.columns:
        raise ValueError("Нужны колонки close и volume")
    work = work.dropna(subset=["close", "volume"])
    work = work[work["volume"] >= 0]
    empty = pd.DataFrame(columns=["Price", "Volume", "Duration_Hrs", "Tier", "start_utc", "end_utc"])
    if work.empty:
        return empty

    current_price = float(work["close"].iloc[-1])
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
        return empty

    # --- Двухпроходный режим ---
    if two_pass_mode and not legacy_weak_merge:
        strict_weak_pct = (
            float(strict_height_percentile_weak)
            if strict_height_percentile_weak is not None
            else float(height_percentile_weak)
        )
        strict_hm = strict_height_mult if strict_height_mult is not None else height_mult
        raw_s, dedup_s, strict_full = _find_pro_levels_single_pass(
            work,
            volume_sm,
            tick_size_eff=tick_size_eff,
            current_price=current_price,
            height_percentile=float(height_percentile),
            height_percentile_strong=float(height_percentile_strong),
            height_percentile_weak=strict_weak_pct,
            height_mult=strict_hm,
            distance_pct=float(distance_pct),
            merge_distance_pct=float(merge_distance_pct),
            valley_threshold=float(valley_threshold),
            valley_merge_threshold=valley_merge_threshold,
            enable_valley_merge=bool(enable_valley_merge),
            min_duration_hours=float(min_duration_hours),
            top_n=int(top_n),
            max_levels=max_levels,
            include_all_tiers=bool(include_all_tiers),
            allow_stage_b_overlap=bool(allow_stage_b_overlap),
            include_weak=True,
            duration_thresholds=duration_thresholds,
            dedup_round_pct=float(dedup_round_pct),
            final_merge_pct=final_merge_pct,
            final_merge_valley_threshold=final_merge_valley_threshold,
            reserved_prices=None,
            exclude_reserved_pct=None,
        )
        if return_raw:
            return pd.DataFrame(raw_s).sort_values("Volume", ascending=False).reset_index(drop=True)
        if return_dedup:
            return pd.DataFrame(dedup_s).sort_values("Volume", ascending=False).reset_index(drop=True)

        if duration_thresholds is None:
            tier1_h, tier2_h = 48.0, 12.0
        else:
            tier1_h, tier2_h = float(duration_thresholds[0]), float(duration_thresholds[1])

        # Tier 1 только из жёсткого финала (strict_full), а не из dedup: иначе соседние пики,
        # которые уже склеил final merge, снова попадают в вывод как несколько «бетонов».
        # Пересчёт ±distance_pct + tier_override — якорь «бетона» даже если длительность по узкому коридору ниже порога.
        tier1_strict = [
            _refine_level_row_smart_band(
                {**r},
                work,
                float(distance_pct),
                tier1_h,
                tier2_h,
                float(tick_size_eff),
                tier_override=TIER1_LABEL,
            )
            for r in strict_full
            if r.get("Tier") == TIER1_LABEL
        ]
        reserved_prices = [float(r["Price"]) for r in strict_full]
        ex_pct = (
            float(exclude_reserved_pct)
            if exclude_reserved_pct is not None
            else max(float(dedup_round_pct), float(distance_pct))
        )

        if not run_soft_pass:
            out = list(tier1_strict)
            out.sort(key=lambda r: float(r["Volume"]), reverse=True)
            return pd.DataFrame(out)

        if soft_final_merge_pct is None:
            eff_soft_merge: Optional[float] = final_merge_pct
            skip_soft_clamp = False
        else:
            eff_soft_merge = float(soft_final_merge_pct)
            skip_soft_clamp = eff_soft_merge <= 0.0

        _, _, soft_final = _find_pro_levels_single_pass(
            work,
            volume_sm,
            tick_size_eff=tick_size_eff,
            current_price=current_price,
            height_percentile=float(height_percentile),
            height_percentile_strong=float(soft_height_percentile_strong),
            height_percentile_weak=float(soft_height_percentile_weak),
            height_mult=soft_height_mult,
            distance_pct=float(distance_pct),
            merge_distance_pct=float(merge_distance_pct),
            valley_threshold=float(valley_threshold),
            valley_merge_threshold=valley_merge_threshold,
            enable_valley_merge=False,
            min_duration_hours=float(soft_min_duration_hours),
            top_n=int(top_n),
            max_levels=max_levels,
            include_all_tiers=bool(include_all_tiers),
            allow_stage_b_overlap=bool(allow_stage_b_overlap),
            include_weak=True,
            duration_thresholds=duration_thresholds,
            dedup_round_pct=float(dedup_round_pct),
            final_merge_pct=eff_soft_merge,
            final_merge_valley_threshold=(
                final_merge_valley_threshold if not skip_soft_clamp else None
            ),
            reserved_prices=reserved_prices,
            exclude_reserved_pct=ex_pct,
            skip_final_merge_clamp=skip_soft_clamp,
        )
        tier1_prices = [float(r["Price"]) for r in tier1_strict]
        soft_kept: list[dict] = []
        for r in soft_final:
            if _price_excluded_by_reserved(float(r["Price"]), tier1_prices, ex_pct):
                continue
            rc = dict(r)
            # Мягкий проход не должен дублировать «бетон»: Tier1 только из жёсткого резерва.
            if rc.get("Tier") == TIER1_LABEL:
                rc["Tier"] = "Tier 2 (Сильный)"
            soft_kept.append(rc)
        combined = list(tier1_strict) + soft_kept
        merge_out = _resolve_final_merge_pct(
            combined,
            final_merge_pct,
            float(distance_pct),
            skip_clamp=False,
        )
        merged = _merge_close_level_rows(
            combined,
            merge_pct=merge_out,
            tier1_h=tier1_h,
            tier2_h=tier2_h,
            df_original=work,
            distance_pct=float(distance_pct),
            tick_size_eff=float(tick_size_eff),
            profile=volume_sm,
            final_merge_valley_threshold=final_merge_valley_threshold,
            preserve_tier1_from_group=True,
        )
        merged.sort(key=lambda r: float(r["Volume"]), reverse=True)
        return pd.DataFrame(merged)

    # --- Один проход (legacy / two_pass_mode=False) ---
    raw_l, dedup_l, final_l = _find_pro_levels_single_pass(
        work,
        volume_sm,
        tick_size_eff=tick_size_eff,
        current_price=current_price,
        height_percentile=float(height_percentile),
        height_percentile_strong=float(height_percentile_strong),
        height_percentile_weak=float(height_percentile_weak),
        height_mult=height_mult,
        distance_pct=float(distance_pct),
        merge_distance_pct=float(merge_distance_pct),
        valley_threshold=float(valley_threshold),
        valley_merge_threshold=valley_merge_threshold,
        enable_valley_merge=bool(enable_valley_merge),
        min_duration_hours=float(min_duration_hours),
        top_n=int(top_n),
        max_levels=max_levels,
        include_all_tiers=bool(include_all_tiers),
        allow_stage_b_overlap=bool(allow_stage_b_overlap),
        include_weak=bool(include_weak),
        duration_thresholds=duration_thresholds,
        dedup_round_pct=float(dedup_round_pct),
        final_merge_pct=final_merge_pct,
        final_merge_valley_threshold=final_merge_valley_threshold,
    )
    if return_raw:
        return pd.DataFrame(raw_l).sort_values("Volume", ascending=False).reset_index(drop=True)
    if return_dedup:
        return pd.DataFrame(dedup_l).sort_values("Volume", ascending=False).reset_index(drop=True)
    return pd.DataFrame(final_l).sort_values("Volume", ascending=False).reset_index(drop=True)


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
            max(
                FINAL_MERGE_PCT_CLAMP_MIN,
                float((work["high"] - work["low"]).mean()) / max(current_price, 1e-9) * 2.0,
            ),
            FINAL_MERGE_PCT_CLAMP_MAX,
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
