"""
Динамические зоны накопления по минутным OHLCV (BTC/USDT, календарный месяц).

Исходное ТЗ:
1) Close → корзины: шаг max(10 USDT, 0.02% от опорной цены — последний Close в выборке).
2) С конца истории: блоки по 60 минут; POC часа; час присоединяют к зоне, пока
   |POC_этого_часа − среднее_POC_уже_включённых_часов| ≤ 0.1% × ref.
3) Зона: общий POC по суммарному профилю, суммарный объём, длительность (ч),
   время начала/конца.
4) Сила: Tier 1 — длительность > 48 ч и объём > 3× среднего объёма по всем зонам месяца;
   Tier 2 — длительность > 12 ч; Tier 3 — от 4 до 12 ч (включительно по верхней границе).

После первичного списка зон (и опционально rescan / top-N): слияние близких по цене уровней
взвешенно по объёму — см. merge_close_zones_weighted (порог по умолчанию 0.5%% между соседями
по отсортированной цене).

Дополнительно (не по умолчанию): кластеризация первичных зон, Master POC, отбор top-N
по ценовым полосам — см. run_pipeline(rescan=True, ...).
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# Порог слияния соседних часов: 0.1% от цены (ТЗ)
DEFAULT_POC_MERGE_THRESHOLD_PCT = 0.001
# Расширенный конвейер (опционально)
DEFAULT_CLUSTER_THRESHOLD_PCT = 0.01
DEFAULT_TOP_N_PER_BAND = 3
DEFAULT_PRICE_BAND_TICK_MULTIPLIER = 50
# Слияние соседних по цене зон: |p2−p1|/p_lower ≤ порога (по умолчанию 0.5%%)
DEFAULT_WEIGHTED_MERGE_THRESHOLD_PCT = 0.005
# Устаревшее имя: явный фиксированный шаг USDT, если не задавать — берётся max(10, 0.02%%)
DEFAULT_ZONE_BIN_STEP_USDT: Optional[float] = None

__all__ = [
    "AccumulationZone",
    "DEFAULT_ZONE_BIN_STEP_USDT",
    "DEFAULT_POC_MERGE_THRESHOLD_PCT",
    "DEFAULT_CLUSTER_THRESHOLD_PCT",
    "DEFAULT_TOP_N_PER_BAND",
    "DEFAULT_PRICE_BAND_TICK_MULTIPLIER",
    "DEFAULT_WEIGHTED_MERGE_THRESHOLD_PCT",
    "merge_close_zones_weighted",
    "take_top_n_per_price_band",
    "take_top_n_per_price_band_detailed",
    "default_bin_step_usdt",
    "default_bin_step_from_last_close",
    "default_zone_bin_step",
    "apply_tick_step_to_close",
    "slice_calendar_month_utc",
    "build_hourly_blocks_from_end",
    "hour_volume_profile",
    "poc_from_profile",
    "find_accumulation_zones",
    "rescan_to_master_levels",
    "assign_tiers_by_original_spec",
    "golden_levels_to_dataframe",
    "run_pipeline",
]


@dataclass
class AccumulationZone:
    poc_price: float
    total_volume: float
    duration_hours: float
    t_start: int
    t_end: int
    tier: str = ""


def default_bin_step_usdt(ref_close: float, min_step: float = 10.0, pct_step: float = 0.0002) -> float:
    """Шаг сетки ТЗ: max(10 USDT, 0.02% от опорной цены)."""
    return float(max(min_step, pct_step * float(ref_close)))


def default_bin_step_from_last_close(df: pd.DataFrame) -> float:
    if df.empty or "close" not in df.columns:
        return 10.0
    ref = float(df["close"].iloc[-1])
    return default_bin_step_usdt(ref)


def default_zone_bin_step(step_usdt: Optional[float] = None) -> float:
    """Фиксированный шаг, если задан; иначе только fallback 10 (лучше передавать из df)."""
    if step_usdt is not None:
        return float(step_usdt)
    return 10.0


def apply_tick_step_to_close(df: pd.DataFrame, tick_step: float) -> pd.DataFrame:
    """Close_i := floor(close / tick) * tick."""
    out = df.copy()
    c = out["close"].to_numpy(dtype=np.float64)
    out["close"] = np.floor(c / float(tick_step)) * float(tick_step)
    return out


def slice_calendar_month_utc(df: pd.DataFrame, year: int, month: int) -> pd.DataFrame:
    import calendar

    start = int(pd.Timestamp(year=year, month=month, day=1, tz="UTC").timestamp())
    last_day = calendar.monthrange(year, month)[1]
    end = int(pd.Timestamp(year=year, month=month, day=last_day, hour=23, minute=59, second=59, tz="UTC").timestamp())
    out = df[(df["timestamp"] >= start) & (df["timestamp"] <= end)].copy()
    return out.sort_values("timestamp").reset_index(drop=True)


def hour_volume_profile(df_hour: pd.DataFrame, bin_step: float) -> pd.Series:
    if df_hour.empty:
        return pd.Series(dtype=np.float64)
    close = df_hour["close"].to_numpy(dtype=np.float64)
    bin_ids = np.floor(close / bin_step).astype(np.int64)
    vol = df_hour["volume"].to_numpy(dtype=np.float64)
    s = pd.Series(vol, index=bin_ids)
    return s.groupby(level=0).sum()


def interval_volume_profile(df: pd.DataFrame, tick_step: float) -> pd.Series:
    return hour_volume_profile(df, tick_step)


def poc_from_profile(profile: pd.Series, bin_step: float) -> Optional[float]:
    if profile.empty:
        return None
    bid = int(profile.idxmax())
    price = float(bid) * float(bin_step)
    if bin_step >= 1.0 and bin_step == round(bin_step):
        return float(int(round(price)))
    return round(price, 2)


def build_hourly_blocks_from_end(df: pd.DataFrame) -> List[pd.DataFrame]:
    if df.empty:
        return []
    df = df.sort_values("timestamp").reset_index(drop=True)
    end_ts = int(df["timestamp"].iloc[-1])
    start_ts = int(df["timestamp"].iloc[0])
    blocks: List[pd.DataFrame] = []
    k = 0
    while True:
        w_end = end_ts - k * 3600
        w_start = w_end - 3600
        if w_end <= start_ts:
            break
        part = df[(df["timestamp"] > w_start) & (df["timestamp"] <= w_end)]
        if not part.empty:
            blocks.append(part)
        k += 1
        if w_start < start_ts:
            break
    return blocks


def find_accumulation_zones(
    df: pd.DataFrame,
    *,
    bin_step: Optional[float] = None,
    poc_merge_threshold_pct: float = DEFAULT_POC_MERGE_THRESHOLD_PCT,
    min_zone_hours: float = 4.0,
    preprocess_tick: bool = True,
) -> List[AccumulationZone]:
    """
    Первичные зоны с конца времени: идём от новых часов к старым; каждый следующий час
    включаем в зону, пока |POC(час) − mean(POC уже объединённых часов)| ≤ threshold × ref.
    Итоговая цена уровня — POC суммарного объёмного профиля зоны (как раньше).
    """
    required = {"timestamp", "close", "volume"}
    if not required.issubset(df.columns):
        raise ValueError(f"df must contain columns {required}")

    df = df.sort_values("timestamp").reset_index(drop=True)
    if df.empty:
        return []

    if bin_step is None:
        bin_step = default_zone_bin_step()

    if preprocess_tick:
        df = apply_tick_step_to_close(df, bin_step)

    blocks = build_hourly_blocks_from_end(df)
    if not blocks:
        return []

    block_profiles: List[pd.Series] = []
    block_pocs: List[Optional[float]] = []
    for blk in blocks:
        pr = hour_volume_profile(blk, bin_step)
        block_profiles.append(pr)
        block_pocs.append(poc_from_profile(pr, bin_step))

    zones: List[AccumulationZone] = []
    idx = 0
    n = len(blocks)

    while idx < n:
        if block_pocs[idx] is None:
            idx += 1
            continue

        prof0 = block_profiles[idx]
        combined = prof0.astype(np.float64).copy()
        t_end = int(blocks[idx]["timestamp"].max())
        t_start = int(blocks[idx]["timestamp"].min())
        merged_blocks = 1
        b = 1
        zone_hourly_pocs: List[float] = [float(block_pocs[idx])]

        while idx + b < n:
            po = block_pocs[idx + b]
            if po is None:
                break
            zone_avg = float(np.mean(zone_hourly_pocs))
            ref = max(abs(zone_avg), abs(float(po)), 1.0)
            if abs(float(po) - zone_avg) > poc_merge_threshold_pct * ref:
                break
            combined = combined.add(block_profiles[idx + b].astype(np.float64), fill_value=0.0)
            t_start = min(t_start, int(blocks[idx + b]["timestamp"].min()))
            merged_blocks += 1
            zone_hourly_pocs.append(float(po))
            b += 1

        final_poc = poc_from_profile(combined, bin_step)
        if final_poc is None:
            idx += max(merged_blocks, 1)
            continue

        total_vol = float(combined.sum())
        dur_h = float(merged_blocks)

        if dur_h >= min_zone_hours:
            zones.append(
                AccumulationZone(
                    poc_price=final_poc,
                    total_volume=total_vol,
                    duration_hours=dur_h,
                    t_start=t_start,
                    t_end=t_end,
                )
            )

        idx += max(merged_blocks, 1)

    return zones


def _uf_find(parent: List[int], x: int) -> int:
    while parent[x] != x:
        parent[x] = parent[parent[x]]
        x = parent[x]
    return x


def _uf_union(parent: List[int], a: int, b: int) -> None:
    ra, rb = _uf_find(parent, a), _uf_find(parent, b)
    if ra != rb:
        parent[rb] = ra


def _cluster_primary_indices(zones: List[AccumulationZone], cluster_threshold_pct: float) -> List[List[int]]:
    n = len(zones)
    if n == 0:
        return []
    parent = list(range(n))
    prices = [float(z.poc_price) for z in zones]
    for i in range(n):
        for j in range(i + 1, n):
            ref = max(abs(prices[i]), abs(prices[j]), 1.0)
            if abs(prices[i] - prices[j]) <= cluster_threshold_pct * ref:
                _uf_union(parent, i, j)
    roots: dict[int, List[int]] = {}
    for i in range(n):
        r = _uf_find(parent, i)
        roots.setdefault(r, []).append(i)
    return list(roots.values())


def rescan_to_master_levels(
    primary: List[AccumulationZone],
    df_1m_binned: pd.DataFrame,
    tick_step: float,
    cluster_threshold_pct: float = DEFAULT_CLUSTER_THRESHOLD_PCT,
) -> List[AccumulationZone]:
    """
    Группировка первичных уровней в пределах cluster_threshold_pct, затем Master POC
    и суммарный объём по всем минуткам объединённого интервала.
    """
    if not primary:
        return []

    df_1m_binned = df_1m_binned.sort_values("timestamp").reset_index(drop=True)
    ts_arr = df_1m_binned["timestamp"].to_numpy(dtype=np.int64)
    clusters = _cluster_primary_indices(primary, cluster_threshold_pct)
    masters: List[AccumulationZone] = []

    for memb in clusters:
        t0 = min(primary[i].t_start for i in memb)
        t1 = max(primary[i].t_end for i in memb)
        mask = (ts_arr >= t0) & (ts_arr <= t1)
        sl = df_1m_binned.loc[mask]
        if sl.empty:
            continue
        prof = interval_volume_profile(sl, tick_step)
        m_poc = poc_from_profile(prof, tick_step)
        if m_poc is None:
            continue
        total_v = float(sl["volume"].sum())
        dur_h = (t1 - t0) / 3600.0
        masters.append(
            AccumulationZone(
                poc_price=m_poc,
                total_volume=total_v,
                duration_hours=float(dur_h),
                t_start=int(t0),
                t_end=int(t1),
            )
        )

    return masters


def take_top_n_per_price_band_detailed(
    zones: List[AccumulationZone],
    *,
    band_width_usdt: float,
    top_n: int,
) -> Tuple[List[AccumulationZone], List[Dict[str, Any]]]:
    """
    Как take_top_n_per_price_band, плюс список отброшенных зон с причиной.

    Полоса цены k: [k * W, (k + 1) * W) USDT, где k = floor(POC / W).
    Внутри полосы зоны сортируются по убыванию total_volume; остаются только первые top_n.
    Зона с ненулевым объёмом всё равно отбрасывается, если в той же полосе есть top_n зон
    с ещё большим объёмом — это не баг, а осознанное ограничение плотности уровней.
    """
    dropped: List[Dict[str, Any]] = []
    if top_n <= 0 or not zones:
        return list(zones), dropped
    w = float(band_width_usdt)
    if w <= 0:
        return list(zones), dropped
    bands: dict[int, List[AccumulationZone]] = defaultdict(list)
    for z in zones:
        bid = int(np.floor(float(z.poc_price) / w))
        bands[bid].append(z)
    out: List[AccumulationZone] = []
    for bid in sorted(bands.keys()):
        grp = bands[bid]
        grp.sort(key=lambda z: float(z.total_volume), reverse=True)
        kept_slice = grp[:top_n]
        out.extend(kept_slice)
        kept_vols = [float(x.total_volume) for x in kept_slice]
        for j, z in enumerate(grp):
            if j < top_n:
                continue
            dropped.append(
                {
                    "poc_price": z.poc_price,
                    "total_volume": z.total_volume,
                    "duration_hours": z.duration_hours,
                    "t_start": z.t_start,
                    "t_end": z.t_end,
                    "band_index": bid,
                    "band_low_usdt": float(bid) * w,
                    "band_high_usdt": float(bid + 1) * w,
                    "rank_by_volume_in_band": j + 1,
                    "zones_in_band": len(grp),
                    "top_n": top_n,
                    "kept_volumes_in_band": list(kept_vols),
                    "reason": (
                        f"POC {z.poc_price:g} попадает в полосу [{bid * w:g}, {(bid + 1) * w:g}) USDT; "
                        f"в полосе {len(grp)} зон, сортировка по объёму — эта на месте {j + 1} "
                        f"(остаются только топ-{top_n}). Объёмы оставшихся: {kept_vols}."
                    ),
                }
            )
    out.sort(key=lambda z: float(z.poc_price))
    return out, dropped


def take_top_n_per_price_band(
    zones: List[AccumulationZone],
    *,
    band_width_usdt: float,
    top_n: int,
) -> List[AccumulationZone]:
    """
    Оставляет в каждой ценовой полосе ширины W не более top_n зон с наибольшим total_volume.

    Разбиение по полосам: индекс полосы = floor(POC / W), полоса [index·W, (index+1)·W).
    Длительность зоны и Tier не участвуют — только POC (для привязки к полосе) и суммарный объём.

    Важно: «объёмная» зона может исчезнуть, если в её полосе уже отобраны top_n зон с ещё
    большим объёмом; соседняя полоса считается отдельно (граница по цене — жёсткая).
    """
    kept, _ = take_top_n_per_price_band_detailed(
        zones, band_width_usdt=band_width_usdt, top_n=top_n
    )
    return kept


def merge_close_zones_weighted(
    zones: List[AccumulationZone],
    *,
    merge_threshold_pct: float = DEFAULT_WEIGHTED_MERGE_THRESHOLD_PCT,
) -> List[AccumulationZone]:
    """
    Сортируем зоны по цене POC; пока за проход находится пара соседей с
    |p2 − p1| / p_lower ≤ merge_threshold_pct — объединяем:

    - новая цена = (p1*V1 + p2*V2) / (V1+V2);
    - объём = V1+V2;
    - длительность = сумма часов;
    - t_start = min, t_end = max.

    Повторяем проходы, пока есть слияния (цепочки из нескольких уровней схлопываются за несколько итераций).
    """
    if merge_threshold_pct <= 0.0 or len(zones) <= 1:
        return list(zones)

    rows: List[Dict[str, Any]] = [
        {
            "poc_price": float(z.poc_price),
            "total_volume": float(z.total_volume),
            "duration_hours": float(z.duration_hours),
            "t_start": int(z.t_start),
            "t_end": int(z.t_end),
        }
        for z in zones
    ]

    changed = True
    while changed and len(rows) > 1:
        changed = False
        rows.sort(key=lambda r: r["poc_price"])
        new_rows: List[Dict[str, Any]] = []
        i = 0
        while i < len(rows):
            if i + 1 < len(rows):
                p1 = float(rows[i]["poc_price"])
                p2 = float(rows[i + 1]["poc_price"])
                v1 = float(rows[i]["total_volume"])
                v2 = float(rows[i + 1]["total_volume"])
                p_lo, p_hi = (p1, p2) if p1 <= p2 else (p2, p1)
                denom = abs(p_lo) if abs(p_lo) > 1e-12 else 1.0
                rel = (p_hi - p_lo) / denom
                if rel <= merge_threshold_pct:
                    wsum = v1 + v2
                    if wsum > 0.0:
                        new_p = (p1 * v1 + p2 * v2) / wsum
                    else:
                        new_p = (p1 + p2) / 2.0
                    new_rows.append(
                        {
                            "poc_price": new_p,
                            "total_volume": v1 + v2,
                            "duration_hours": rows[i]["duration_hours"] + rows[i + 1]["duration_hours"],
                            "t_start": min(rows[i]["t_start"], rows[i + 1]["t_start"]),
                            "t_end": max(rows[i]["t_end"], rows[i + 1]["t_end"]),
                        }
                    )
                    changed = True
                    i += 2
                    continue
            new_rows.append(dict(rows[i]))
            i += 1
        rows = new_rows

    out: List[AccumulationZone] = []
    for r in rows:
        out.append(
            AccumulationZone(
                poc_price=round(float(r["poc_price"]), 2),
                total_volume=float(r["total_volume"]),
                duration_hours=float(r["duration_hours"]),
                t_start=int(r["t_start"]),
                t_end=int(r["t_end"]),
                tier="",
            )
        )
    return out


def assign_tiers_by_original_spec(zones: List[AccumulationZone]) -> None:
    """
    Tier 1 (Бетон): длительность > 48 ч и объём > 3× среднего объёма по зонам месяца.
    Tier 2 (Сильный): длительность > 12 ч.
    Tier 3 (Локальный): 4 ч ≤ длительность ≤ 12 ч.
    """
    if not zones:
        return
    avg_vol = float(np.mean([float(z.total_volume) for z in zones]))
    for z in zones:
        d = float(z.duration_hours)
        v = float(z.total_volume)
        if d > 48.0 and avg_vol > 0.0 and v > 3.0 * avg_vol:
            z.tier = "Tier 1 (Бетон)"
        elif d > 12.0:
            z.tier = "Tier 2 (Сильный)"
        elif d >= 4.0:
            z.tier = "Tier 3 (Локальный)"
        else:
            z.tier = ""


def golden_levels_to_dataframe(zones: List[AccumulationZone]) -> pd.DataFrame:
    rows = []
    for z in zones:
        rows.append(
            {
                "Цена уровня": z.poc_price,
                "Суммарный объем": z.total_volume,
                "Время жизни (ч)": round(z.duration_hours, 2),
                "Сила (Tier)": z.tier,
                "t_start_unix": z.t_start,
                "t_end_unix": z.t_end,
            }
        )
    return pd.DataFrame(rows)


def run_pipeline(
    df_1m: pd.DataFrame,
    *,
    year: Optional[int] = None,
    month: Optional[int] = None,
    bin_step: Optional[float] = None,
    zone_bin_step_usdt: Optional[float] = None,
    tick_step: Optional[float] = None,
    poc_merge_threshold_pct: float = DEFAULT_POC_MERGE_THRESHOLD_PCT,
    cluster_threshold_pct: float = DEFAULT_CLUSTER_THRESHOLD_PCT,
    min_zone_hours: float = 4.0,
    rescan: bool = False,
    top_n_per_band: int = 0,
    price_band_usdt: Optional[float] = None,
    price_band_tick_multiplier: float = DEFAULT_PRICE_BAND_TICK_MULTIPLIER,
    month_slice: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None,
    weighted_merge_threshold_pct: Optional[float] = DEFAULT_WEIGHTED_MERGE_THRESHOLD_PCT,
) -> Tuple[pd.DataFrame, float]:
    """
    По умолчанию — первичный скан, взвешенное слияние близких уровней (0.5%%), Tier по ТЗ.
    rescan=True: кластер + Master POC; top_n_per_band>0 — отбор по ценовым полосам.
    weighted_merge_threshold_pct=None или ≤0 — пропустить merge_close_zones_weighted.
    """
    work = df_1m.copy()
    if year is not None and month is not None:
        work = slice_calendar_month_utc(work, year, month)
    elif month_slice is not None:
        work = month_slice(work)

    if work.empty:
        return pd.DataFrame(), 0.0

    if bin_step is not None:
        step = float(bin_step)
    elif tick_step is not None:
        step = float(tick_step)
    elif zone_bin_step_usdt is not None:
        step = float(zone_bin_step_usdt)
    else:
        step = default_bin_step_from_last_close(work)

    work_b = apply_tick_step_to_close(work, step)

    primary = find_accumulation_zones(
        work_b,
        bin_step=step,
        poc_merge_threshold_pct=poc_merge_threshold_pct,
        min_zone_hours=min_zone_hours,
        preprocess_tick=False,
    )

    if rescan:
        final = rescan_to_master_levels(primary, work_b, step, cluster_threshold_pct=cluster_threshold_pct)
    else:
        final = list(primary)

    if top_n_per_band > 0:
        band_w = float(price_band_usdt) if price_band_usdt is not None else float(price_band_tick_multiplier) * step
        final = take_top_n_per_price_band(final, band_width_usdt=band_w, top_n=int(top_n_per_band))

    if weighted_merge_threshold_pct is not None and float(weighted_merge_threshold_pct) > 0.0:
        final = merge_close_zones_weighted(
            final, merge_threshold_pct=float(weighted_merge_threshold_pct)
        )

    assign_tiers_by_original_spec(final)
    return golden_levels_to_dataframe(final), step
