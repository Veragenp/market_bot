from __future__ import annotations

import time
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

import pandas as pd

from trading_bot.data.db import get_connection
from trading_bot.data.schema import init_db, run_migrations

# --- level_type (VP + экспериментальный HTF) ---
LEVEL_TYPE_VP_LOCAL = "vp_local"
LEVEL_TYPE_VP_GLOBAL = "vp_global"
LEVEL_TYPE_VP_GLOBAL_4H_90D = "vp_global_4h_90d"
LEVEL_TYPE_HUMAN = "human"

# Обратная совместимость имён (значения = новые строки)
LEVEL_TYPE_VOLUME_PROFILE_PEAKS = LEVEL_TYPE_VP_LOCAL
LEVEL_TYPE_VOLUME_PROFILE_HTF = LEVEL_TYPE_VP_GLOBAL
LEVEL_TYPE_VOLUME_PROFILE_HTF_4H_90D = LEVEL_TYPE_VP_GLOBAL_4H_90D

LEVEL_STATUS_ACTIVE = "active"
LEVEL_STATUS_WORKED = "worked"
LEVEL_STATUS_INVALIDATED = "invalidated"
LEVEL_STATUS_ARCHIVED = "archived"

ORIGIN_AUTO = "auto"
ORIGIN_MANUAL = "manual"

# Порог слияния новой точки VP с существующей активной строкой: |Δprice| <= MERGE_DISTANCE_ATR_MULT * ATR_D1
MERGE_DISTANCE_ATR_MULT = 0.1

# Если в instruments нет ATR — fallback: доля от медианы цен новых уровней
MERGE_FALLBACK_PRICE_FRAC = 0.0005


def _iso_utc_to_unix(ts: object) -> Optional[int]:
    if ts is None:
        return None
    s = str(ts).strip()
    if not s:
        return None
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def _lookback_days_from_window(t_start: Optional[int], t_end: Optional[int]) -> Optional[int]:
    if t_start is None or t_end is None or t_end < t_start:
        return None
    return int(round((t_end - t_start) / 86400))


def _get_atr_daily(symbol: str, cur: Any) -> Optional[float]:
    for sym in (symbol.replace("/", ""), symbol):
        row = cur.execute(
            "SELECT atr FROM instruments WHERE symbol = ? AND exchange = 'bybit_futures'",
            (sym,),
        ).fetchone()
        if row is not None and row["atr"] is not None:
            return float(row["atr"])
    return None


def _merge_epsilon_atr(
    atr: Optional[float],
    new_prices: list[float],
) -> float:
    if atr is not None and atr > 0:
        return float(MERGE_DISTANCE_ATR_MULT) * atr
    if not new_prices:
        return 1e-8
    med = float(sorted(new_prices)[len(new_prices) // 2])
    return max(1e-8, abs(med) * MERGE_FALLBACK_PRICE_FRAC)


def deactivate_active_price_levels(symbol: str, *, level_type: str = LEVEL_TYPE_VP_LOCAL) -> None:
    """
    Принудительно снимает активные уровни (архив + is_active=0) для symbol/level_type.
    Обычный VP-пайплайн использует merge в save_volume_profile_peaks_levels_to_db.
    """
    init_db()
    run_migrations()
    now = int(time.time())
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE price_levels
        SET is_active = 0,
            status = ?,
            updated_at = ?
        WHERE symbol = ? AND level_type = ? AND is_active = 1
        """,
        (LEVEL_STATUS_ARCHIVED, now, symbol, level_type),
    )
    conn.commit()
    conn.close()


def save_volume_profile_peaks_levels_to_db(
    symbol: str,
    final_levels_df: pd.DataFrame,
    *,
    layer: str,
    level_type: str = LEVEL_TYPE_VP_LOCAL,
    now_ts: Optional[int] = None,
    timeframe: Optional[str] = None,
    origin: str = ORIGIN_AUTO,
) -> None:
    """
    Сохраняет уровни VP с слиянием по цене: совпадение в пределах MERGE_DISTANCE_ATR_MULT * ATR(d).
    Цена существующей строки не меняется; обновляются сила/тир/окно/layer и last_matched_calc_at.
    Активные строки, не сматчившиеся с текущим расчётом → status=archived, is_active=0.
    """
    init_db()
    run_migrations()

    created_at = int(now_ts) if now_ts is not None else int(time.time())
    conn = get_connection()
    cur = conn.cursor()

    if final_levels_df is None or final_levels_df.empty:
        cur.execute(
            """
            UPDATE price_levels
            SET is_active = 0,
                status = ?,
                updated_at = ?
            WHERE symbol = ? AND level_type = ?
              AND is_active = 1
              AND status = ?
            """,
            (LEVEL_STATUS_ARCHIVED, created_at, symbol, level_type, LEVEL_STATUS_ACTIVE),
        )
        conn.commit()
        conn.close()
        return

    required = {"Price", "Volume", "Duration_Hrs", "Tier", "start_utc", "end_utc"}
    missing = required.difference(set(final_levels_df.columns))
    if missing:
        conn.close()
        raise ValueError(f"final_levels_df missing columns: {sorted(missing)}")

    atr = _get_atr_daily(symbol, cur)
    new_prices = [float(r["Price"]) for _, r in final_levels_df.iterrows()]
    eps = _merge_epsilon_atr(atr, new_prices)

    cur.execute(
        """
        SELECT id, price, stable_level_id
        FROM price_levels
        WHERE symbol = ?
          AND level_type = ?
          AND is_active = 1
          AND status = ?
        """,
        (symbol, level_type, LEVEL_STATUS_ACTIVE),
    )
    old_rows = [{k: r[k] for k in r.keys()} for r in cur.fetchall()]
    used_old: set[int] = set()
    updates: list[tuple[int, Any]] = []
    inserts: list[Any] = []

    for _, r in final_levels_df.iterrows():
        new_price = float(r["Price"])
        best_id: Optional[int] = None
        best_d: Optional[float] = None
        for o in old_rows:
            oid = int(o["id"])
            if oid in used_old:
                continue
            d = abs(float(o["price"]) - new_price)
            if d <= eps and (best_d is None or d < best_d):
                best_id = oid
                best_d = d
        if best_id is not None:
            used_old.add(best_id)
            updates.append((best_id, r))
        else:
            inserts.append(r)

    for oid, r in updates:
        new_price = float(r["Price"])
        volume_peak = float(r["Volume"])
        duration_hours = float(r["Duration_Hrs"])
        tier = str(r["Tier"])
        strength = volume_peak
        t_start_unix = _iso_utc_to_unix(r.get("start_utc"))
        t_end_unix = _iso_utc_to_unix(r.get("end_utc"))
        lookback_days = _lookback_days_from_window(t_start_unix, t_end_unix)
        cur.execute(
            """
            UPDATE price_levels
            SET price = ?,
                strength = ?,
                volume_peak = ?,
                tier = ?,
                duration_hours = ?,
                t_start_unix = ?,
                t_end_unix = ?,
                layer = ?,
                lookback_days = ?,
                last_matched_calc_at = ?,
                updated_at = ?,
                timeframe = COALESCE(?, timeframe)
            WHERE id = ?
            """,
            (
                new_price,
                strength,
                volume_peak,
                tier,
                duration_hours,
                t_start_unix,
                t_end_unix,
                layer,
                lookback_days,
                created_at,
                created_at,
                timeframe,
                oid,
            ),
        )

    archive_ids = [int(o["id"]) for o in old_rows if int(o["id"]) not in used_old]
    for aid in archive_ids:
        cur.execute(
            """
            UPDATE price_levels
            SET is_active = 0,
                status = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (LEVEL_STATUS_ARCHIVED, created_at, aid),
        )

    for r in inserts:
        price = float(r["Price"])
        volume_peak = float(r["Volume"])
        duration_hours = float(r["Duration_Hrs"])
        tier = str(r["Tier"])
        strength = volume_peak
        t_start_unix = _iso_utc_to_unix(r.get("start_utc"))
        t_end_unix = _iso_utc_to_unix(r.get("end_utc"))
        lookback_days = _lookback_days_from_window(t_start_unix, t_end_unix)
        sid = str(uuid.uuid4())
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
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 1)
            """,
            (
                symbol,
                price,
                level_type,
                layer,
                origin,
                LEVEL_STATUS_ACTIVE,
                sid,
                strength,
                volume_peak,
                tier,
                duration_hours,
                t_start_unix,
                t_end_unix,
                lookback_days,
                timeframe,
                created_at,
                created_at,
                created_at,
            ),
        )

    conn.commit()
    conn.close()


__all__ = [
    "LEVEL_TYPE_VP_LOCAL",
    "LEVEL_TYPE_VP_GLOBAL",
    "LEVEL_TYPE_VP_GLOBAL_4H_90D",
    "LEVEL_TYPE_HUMAN",
    "LEVEL_TYPE_VOLUME_PROFILE_PEAKS",
    "LEVEL_TYPE_VOLUME_PROFILE_HTF",
    "LEVEL_TYPE_VOLUME_PROFILE_HTF_4H_90D",
    "LEVEL_STATUS_ACTIVE",
    "LEVEL_STATUS_WORKED",
    "LEVEL_STATUS_INVALIDATED",
    "LEVEL_STATUS_ARCHIVED",
    "ORIGIN_AUTO",
    "ORIGIN_MANUAL",
    "MERGE_DISTANCE_ATR_MULT",
    "deactivate_active_price_levels",
    "save_volume_profile_peaks_levels_to_db",
]
