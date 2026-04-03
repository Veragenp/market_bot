from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Optional

import pandas as pd

from trading_bot.data.db import get_connection
from trading_bot.data.schema import init_db, run_migrations

LEVEL_TYPE_VOLUME_PROFILE_PEAKS = "volume_profile_peaks"


def _iso_utc_to_unix(ts: object) -> Optional[int]:
    if ts is None:
        return None
    s = str(ts).strip()
    if not s:
        return None
    # start_utc/end_utc из volume_profile_peaks — isoformat с tz=UTC.
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def deactivate_active_price_levels(symbol: str, *, level_type: str = LEVEL_TYPE_VOLUME_PROFILE_PEAKS) -> None:
    """
    Деактивируем старые активные уровни только для:
      - данного `symbol`
      - данного `level_type`

    История остаётся в таблице (старые записи переводятся в `is_active=0`).
    """
    init_db()
    run_migrations()

    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE price_levels
        SET is_active = 0
        WHERE symbol = ? AND level_type = ? AND is_active = 1
        """,
        (symbol, level_type),
    )
    conn.commit()
    conn.close()


def save_volume_profile_peaks_levels_to_db(
    symbol: str,
    final_levels_df: pd.DataFrame,
    *,
    layer: str,
    level_type: str = LEVEL_TYPE_VOLUME_PROFILE_PEAKS,
    now_ts: Optional[int] = None,
) -> None:
    """
    Сохраняет в `price_levels` только итоговые уровни (final_levels), полученные из `find_pro_levels()`.
    """
    if final_levels_df is None or final_levels_df.empty:
        return

    required = {"Price", "Volume", "Duration_Hrs", "Tier", "start_utc", "end_utc"}
    missing = required.difference(set(final_levels_df.columns))
    if missing:
        raise ValueError(f"final_levels_df missing columns: {sorted(missing)}")

    init_db()
    run_migrations()

    created_at = int(now_ts) if now_ts is not None else int(time.time())

    conn = get_connection()
    cur = conn.cursor()

    # В одной транзакции: сначала деактивация старых активных, затем вставка.
    cur.execute(
        """
        UPDATE price_levels
        SET is_active = 0
        WHERE symbol = ? AND level_type = ? AND is_active = 1
        """,
        (symbol, level_type),
    )

    for _, r in final_levels_df.iterrows():
        price = float(r["Price"])
        volume_peak = float(r["Volume"])
        duration_hours = float(r["Duration_Hrs"])
        tier = str(r["Tier"])

        # strength — отдельное числовое поле в БД; пока используем volume_peak как proxy.
        strength = volume_peak

        t_start_unix = _iso_utc_to_unix(r.get("start_utc"))
        t_end_unix = _iso_utc_to_unix(r.get("end_utc"))

        cur.execute(
            """
            INSERT INTO price_levels (
                symbol, price, level_type, layer,
                strength, volume_peak,
                tier,
                duration_hours,
                t_start_unix, t_end_unix,
                created_at, expires_at,
                is_active
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 1)
            """,
            (
                symbol,
                price,
                level_type,
                layer,
                strength,
                volume_peak,
                tier,
                duration_hours,
                t_start_unix,
                t_end_unix,
                created_at,
            ),
        )

    conn.commit()
    conn.close()


__all__ = [
    "LEVEL_TYPE_VOLUME_PROFILE_PEAKS",
    "deactivate_active_price_levels",
    "save_volume_profile_peaks_levels_to_db",
]

