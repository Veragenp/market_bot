"""
Зоны накопления через DBSCAN по минутным close/volume (без почасовой привязки).
Требуется scikit-learn.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

__all__ = ["find_zones_dbscan"]


def find_zones_dbscan(
    df: pd.DataFrame,
    eps_pct: float = 0.001,
    min_vol_mult: float = 5.0,
    min_samples: int | None = None,
) -> pd.DataFrame:
    """
    df: колонки close, volume; индекс DatetimeIndex или колонка timestamp (unix сек, UTC).

    eps_pct: радиус в log(price); 0.001 ≈ 0.1% цены — выше детализация кластеров.
    min_vol_mult: порог суммарного объёма зоны к среднему объёму за час (BTC-экспорт: 5.0).
    min_samples: для DBSCAN (целое ≥1). По умолчанию — от длины ряда.
    """
    try:
        from sklearn.cluster import DBSCAN
    except ImportError as exc:
        raise RuntimeError(
            "Для find_zones_dbscan нужен scikit-learn: pip install scikit-learn"
        ) from exc

    work = df.copy()
    if not isinstance(work.index, pd.DatetimeIndex):
        if "timestamp" not in work.columns:
            raise ValueError("Нужен DatetimeIndex или колонка timestamp (unix)")
        work = work.set_index(pd.to_datetime(work["timestamp"], unit="s", utc=True))
    work = work.sort_index()
    work = work.dropna(subset=["close", "volume"])
    work = work[work["volume"] > 0]
    if work.empty:
        return pd.DataFrame(
            columns=["Price", "Volume", "Duration_Hrs", "Tier", "Start", "End"]
        )

    prices = work["close"].to_numpy(dtype=np.float64).reshape(-1, 1)
    log_prices = np.log(prices)
    eps_val = float(np.log(1.0 + eps_pct))

    n = len(work)
    ms = min_samples if min_samples is not None else max(2, min(500, int(max(1, n * 0.0001))))

    db = DBSCAN(eps=eps_val, min_samples=ms, metric="euclidean")
    vol_w = work["volume"].to_numpy(dtype=np.float64)
    try:
        clusters = db.fit_predict(log_prices, sample_weight=vol_w)
    except TypeError:
        clusters = db.fit_predict(log_prices)

    work = work.copy()
    work["cluster"] = clusters

    avg_hourly_vol = float(work["volume"].sum()) / max(float(n) / 60.0, 1e-9)
    zones: list[dict] = []

    for cluster_id in set(clusters):
        if cluster_id == -1:
            continue
        cluster_data = work[work["cluster"] == cluster_id]
        total_vol = float(cluster_data["volume"].sum())
        vwap_price = float(
            (cluster_data["close"] * cluster_data["volume"]).sum() / max(total_vol, 1e-18)
        )
        start_time = cluster_data.index.min()
        end_time = cluster_data.index.max()
        duration_hrs = (end_time - start_time).total_seconds() / 3600.0

        if total_vol > avg_hourly_vol * min_vol_mult:
            tier = "Tier 3"
            if duration_hrs > 24 and total_vol > avg_hourly_vol * 10:
                tier = "Tier 1"
            elif duration_hrs > 8:
                tier = "Tier 2"
            zones.append(
                {
                    "Price": round(vwap_price, 2),
                    "Volume": round(total_vol, 2),
                    "Duration_Hrs": round(duration_hrs, 1),
                    "Tier": tier,
                    "Start": start_time,
                    "End": end_time,
                }
            )

    if not zones:
        return pd.DataFrame(
            columns=["Price", "Volume", "Duration_Hrs", "Tier", "Start", "End"]
        )
    return pd.DataFrame(zones).sort_values("Volume", ascending=False).reset_index(drop=True)
