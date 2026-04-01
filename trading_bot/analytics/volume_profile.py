"""Volume Level Extractor: POC/HVN + confluence."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class ExtractorConfig:
    period_days: int
    step_size: float
    top_n: int
    horizon: str  # "global" or "local"
    smoothing_window: int = 5
    poc_bonus: float = 1.5


def _normalize_input_df(df: pd.DataFrame) -> pd.DataFrame:
    required = {"timestamp", "open", "high", "low", "close", "volume"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns: {sorted(missing)}")

    out = df.copy()
    out["timestamp"] = pd.to_datetime(out["timestamp"], unit="s", utc=True, errors="coerce")
    out = out.dropna(subset=["timestamp", "high", "low", "close", "volume"])
    out = out.sort_values("timestamp")
    return out


def extract_volume_levels(df: pd.DataFrame, *, period_days: int, step_size: float, top_n: int, horizon: str) -> pd.DataFrame:
    """
    Build volume profile levels from minute candles.

    Returns columns:
    price, volume_total, type, horizon, score
    """
    if period_days <= 0:
        raise ValueError("period_days must be > 0")
    if step_size <= 0:
        raise ValueError("step_size must be > 0")
    if top_n <= 0:
        raise ValueError("top_n must be > 0")

    cfg = ExtractorConfig(period_days=period_days, step_size=step_size, top_n=top_n, horizon=horizon)
    src = _normalize_input_df(df)
    if src.empty:
        return pd.DataFrame(columns=["price", "volume_total", "type", "horizon", "score"])

    cutoff = datetime.now(timezone.utc) - pd.Timedelta(days=cfg.period_days)
    src = src[src["timestamp"] >= cutoff]
    if src.empty:
        return pd.DataFrame(columns=["price", "volume_total", "type", "horizon", "score"])

    src["typical_price"] = (src["high"] + src["low"] + src["close"]) / 3.0
    src["price_bin"] = (src["typical_price"] / cfg.step_size).round() * cfg.step_size

    profile = (
        src.groupby("price_bin", as_index=False)["volume"]
        .sum()
        .rename(columns={"price_bin": "price", "volume": "volume_total"})
        .sort_values("price")
        .reset_index(drop=True)
    )
    if profile.empty:
        return pd.DataFrame(columns=["price", "volume_total", "type", "horizon", "score"])

    profile["vol_smooth"] = profile["volume_total"].rolling(cfg.smoothing_window, min_periods=1, center=True).mean()
    mean_vol = float(profile["volume_total"].mean()) if float(profile["volume_total"].mean()) > 0 else 1.0

    poc_idx = int(profile["volume_total"].idxmax())
    levels: List[Dict[str, float | str]] = []
    for i in range(1, len(profile) - 1):
        c = float(profile.loc[i, "vol_smooth"])
        l = float(profile.loc[i - 1, "vol_smooth"])
        r = float(profile.loc[i + 1, "vol_smooth"])
        if c <= l or c <= r:
            continue

        lvl_type = "HVN"
        score = float(profile.loc[i, "volume_total"]) / mean_vol
        if i == poc_idx:
            lvl_type = "POC"
            score *= cfg.poc_bonus

        levels.append(
            {
                "price": float(profile.loc[i, "price"]),
                "volume_total": float(profile.loc[i, "volume_total"]),
                "type": "Global_POC" if (lvl_type == "POC" and cfg.horizon == "global") else ("Local_POC" if lvl_type == "POC" else "HVN"),
                "horizon": cfg.horizon,
                "score": float(score),
            }
        )

    # Ensure POC exists even if not local peak due to smoothing.
    if not any(str(x["type"]).endswith("POC") for x in levels):
        poc_score = (float(profile.loc[poc_idx, "volume_total"]) / mean_vol) * cfg.poc_bonus
        levels.append(
            {
                "price": float(profile.loc[poc_idx, "price"]),
                "volume_total": float(profile.loc[poc_idx, "volume_total"]),
                "type": "Global_POC" if cfg.horizon == "global" else "Local_POC",
                "horizon": cfg.horizon,
                "score": float(poc_score),
            }
        )

    out = pd.DataFrame(levels).sort_values("score", ascending=False).head(cfg.top_n).reset_index(drop=True)
    return out


def mark_ultra_strong(global_levels: pd.DataFrame, local_levels: pd.DataFrame, tolerance_pct: float = 0.001) -> pd.DataFrame:
    """
    Merge global/local levels and mark local levels as Ultra_Strong
    when they match global prices within tolerance (default 0.1%).
    """
    if global_levels.empty and local_levels.empty:
        return pd.DataFrame(columns=["price", "volume_total", "type", "horizon", "score", "confluence"])

    g = global_levels.copy()
    l = local_levels.copy()
    if g.empty:
        l["confluence"] = "Local_Only"
        return l
    if l.empty:
        g["confluence"] = "Global_Only"
        return g

    g_prices = g["price"].astype(float).tolist()
    confluence = []
    for _, row in l.iterrows():
        price = float(row["price"])
        matched = any(abs(price - gp) / max(abs(gp), 1e-12) <= tolerance_pct for gp in g_prices)
        confluence.append("Ultra_Strong" if matched else "Local_Only")
    l["confluence"] = confluence
    g["confluence"] = "Global_Only"
    return pd.concat([l, g], ignore_index=True)
