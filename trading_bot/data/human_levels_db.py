"""
Сохранение автоматических человеческих уровней в price_levels.

Архивируются только строки level_type=human и origin=auto (ручные human не трогаем).
Якорная цена в БД — середина зоны (zone_low + zone_high) / 2; границы — в tier.
"""

from __future__ import annotations

import time
import uuid
from typing import Optional

import pandas as pd

from trading_bot.analytics.human_levels import (
    DEFAULT_CLUSTER_ATR_MULT,
    HumanLevelsResult,
    HumanZone,
    filter_human_zones,
    run_human_levels_pipeline,
)
from trading_bot.config.settings import (
    HUMAN_LEVELS_MIN_FRACTAL_COUNT,
    HUMAN_LEVELS_MIN_STRENGTH,
    HUMAN_LEVELS_ZONE_MIN_GAP_ATR,
)
from trading_bot.data.db import get_connection
from trading_bot.data.schema import init_db, run_migrations
from trading_bot.data.volume_profile_peaks_db import (
    LEVEL_STATUS_ACTIVE,
    LEVEL_STATUS_ARCHIVED,
    LEVEL_TYPE_HUMAN,
    ORIGIN_AUTO,
)


def _timestamp_range_from_dfs(*dfs: pd.DataFrame) -> tuple[Optional[int], Optional[int]]:
    ts_min: Optional[int] = None
    ts_max: Optional[int] = None
    for df in dfs:
        if df is None or df.empty or "timestamp" not in df.columns:
            continue
        a = int(df["timestamp"].min())
        b = int(df["timestamp"].max())
        ts_min = a if ts_min is None else min(ts_min, a)
        ts_max = b if ts_max is None else max(ts_max, b)
    return ts_min, ts_max


def _lookback_days(t_start: Optional[int], t_end: Optional[int]) -> Optional[int]:
    if t_start is None or t_end is None or t_end < t_start:
        return None
    return int(round((t_end - t_start) / 86400))


def _tier_for_zone(z: HumanZone) -> str:
    """Компактное описание зоны для tier (VP-стиль строки)."""
    return f"{z.timeframe}|zl={z.zone_low:.12g}|zh={z.zone_high:.12g}|n{z.fractal_count}"


def archive_active_human_auto_levels(symbol: str, *, now_ts: Optional[int] = None) -> int:
    """Архивирует active human+auto. Возвращает число обновлённых строк."""
    init_db()
    run_migrations()
    now = int(now_ts) if now_ts is not None else int(time.time())
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE price_levels
        SET is_active = 0,
            status = ?,
            updated_at = ?
        WHERE symbol = ?
          AND level_type = ?
          AND origin = ?
          AND is_active = 1
          AND status = ?
        """,
        (LEVEL_STATUS_ARCHIVED, now, symbol, LEVEL_TYPE_HUMAN, ORIGIN_AUTO, LEVEL_STATUS_ACTIVE),
    )
    n = cur.rowcount if cur.rowcount is not None else 0
    conn.commit()
    conn.close()
    return int(n)


def save_human_levels_auto_to_db(
    symbol: str,
    result: HumanLevelsResult,
    *,
    layer: str,
    now_ts: Optional[int] = None,
    t_start_unix: Optional[int] = None,
    t_end_unix: Optional[int] = None,
) -> int:
    """
    Архивирует предыдущие human/auto по символу, вставляет зоны из result.
    Возвращает число вставленных строк.
    """
    init_db()
    run_migrations()
    created_at = int(now_ts) if now_ts is not None else int(time.time())
    lb = _lookback_days(t_start_unix, t_end_unix)

    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE price_levels
        SET is_active = 0,
            status = ?,
            updated_at = ?
        WHERE symbol = ?
          AND level_type = ?
          AND origin = ?
          AND is_active = 1
          AND status = ?
        """,
        (LEVEL_STATUS_ARCHIVED, created_at, symbol, LEVEL_TYPE_HUMAN, ORIGIN_AUTO, LEVEL_STATUS_ACTIVE),
    )

    inserted = 0
    for z in list(result.zones_d1) + list(result.zones_w1):
        price = float(z.zone_low + z.zone_high) / 2.0
        sid = str(uuid.uuid4())
        tf = z.timeframe
        cur.execute(
            """
            INSERT INTO price_levels (
                symbol, price, level_type, layer,
                origin, status, stable_level_id,
                strength, volume_peak,
                tier,
                duration_hours,
                t_start_unix, t_end_unix,
                lookback_days, timeframe,
                created_at, updated_at, last_matched_calc_at,
                expires_at,
                is_active
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, NULL, ?, ?, ?, ?, ?, ?, ?, NULL, 1)
            """,
            (
                symbol,
                price,
                LEVEL_TYPE_HUMAN,
                layer,
                ORIGIN_AUTO,
                LEVEL_STATUS_ACTIVE,
                sid,
                float(z.strength),
                _tier_for_zone(z),
                t_start_unix,
                t_end_unix,
                lb,
                tf,
                created_at,
                created_at,
                created_at,
            ),
        )
        inserted += 1

    conn.commit()
    conn.close()
    return inserted


def run_human_levels_and_save(
    symbol: str,
    df_d1: pd.DataFrame,
    df_w1: pd.DataFrame,
    *,
    layer: Optional[str] = None,
    now_ts: Optional[int] = None,
    atr_d1: Optional[float] = None,
    cluster_atr_mult: float = DEFAULT_CLUSTER_ATR_MULT,
    min_fractal_count: Optional[int] = None,
    min_strength: Optional[float] = None,
    zone_min_gap_atr_d1: Optional[float] = None,
) -> HumanLevelsResult:
    """
    Пайплайн human_levels + сохранение в БД.
    `atr_d1` — Gerchik из `instruments.atr`, если передан; иначе тот же Gerchik по хвосту df_d1.
    Окно t_start/t_end — по min/max timestamp среди D1 и W1 (если есть колонка).
    Перед сохранением зоны фильтруются по HUMAN_LEVELS_MIN_* из settings,
    если не переданы явные min_fractal_count / min_strength.
    Разрежение D1 по центрам: HUMAN_LEVELS_ZONE_MIN_GAP_ATR, если не задано zone_min_gap_atr_d1.
    """
    zgap = HUMAN_LEVELS_ZONE_MIN_GAP_ATR if zone_min_gap_atr_d1 is None else float(zone_min_gap_atr_d1)
    result = run_human_levels_pipeline(
        df_d1,
        df_w1,
        atr_d1=atr_d1,
        cluster_atr_mult=cluster_atr_mult,
        zone_min_gap_atr_d1=zgap,
    )
    mfc = HUMAN_LEVELS_MIN_FRACTAL_COUNT if min_fractal_count is None else min_fractal_count
    ms = HUMAN_LEVELS_MIN_STRENGTH if min_strength is None else min_strength
    fd1 = filter_human_zones(result.zones_d1, min_fractal_count=mfc, min_strength=ms)
    fw1 = filter_human_zones(result.zones_w1, min_fractal_count=mfc, min_strength=ms)
    result_save = HumanLevelsResult(
        zones_d1=fd1,
        zones_w1=fw1,
        atr_d1_last=result.atr_d1_last,
        atr_w1_equiv=result.atr_w1_equiv,
        fractals_d1=result.fractals_d1,
        fractals_w1=result.fractals_w1,
    )
    ts = int(now_ts) if now_ts is not None else int(time.time())
    lay = layer if layer is not None else f"human_auto_{ts}"
    t0, t1 = _timestamp_range_from_dfs(df_d1, df_w1)
    save_human_levels_auto_to_db(symbol, result_save, layer=lay, now_ts=ts, t_start_unix=t0, t_end_unix=t1)
    return result_save


__all__ = [
    "archive_active_human_auto_levels",
    "run_human_levels_and_save",
    "save_human_levels_auto_to_db",
]
