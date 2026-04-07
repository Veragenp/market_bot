"""Слияние VP при сохранении: порог 0.1*ATR; при merge цена обновляется на новый POC."""

import pandas as pd

from trading_bot.data.db import get_connection
from trading_bot.data.volume_profile_peaks_db import (
    LEVEL_TYPE_VP_LOCAL,
    MERGE_DISTANCE_ATR_MULT,
    save_volume_profile_peaks_levels_to_db,
)


def _row(cur, sql, params=()):
    cur.execute(sql, params)
    return cur.fetchone()


def test_vp_merge_updates_price_and_archives_unmatched(clean_db):
    conn = get_connection()
    cur = conn.cursor()
    sym = "BTC/USDT"
    cur.execute(
        """
        INSERT INTO instruments (symbol, exchange, tick_size, atr, updated_at)
        VALUES (?, 'bybit_futures', 0.1, 100.0, 0)
        """,
        (sym.replace("/", ""),),
    )
    conn.commit()
    conn.close()

    eps = MERGE_DISTANCE_ATR_MULT * 100.0  # 10.0

    df1 = pd.DataFrame(
        [
            {
                "Price": 50000.0,
                "Volume": 1.0,
                "Duration_Hrs": 1.0,
                "Tier": "T1",
                "start_utc": "2024-01-01T00:00:00+00:00",
                "end_utc": "2024-01-30T00:00:00+00:00",
            },
            {
                "Price": 51000.0,
                "Volume": 2.0,
                "Duration_Hrs": 1.0,
                "Tier": "T2",
                "start_utc": "2024-01-01T00:00:00+00:00",
                "end_utc": "2024-01-30T00:00:00+00:00",
            },
        ]
    )
    save_volume_profile_peaks_levels_to_db(sym, df1, layer="L1", level_type=LEVEL_TYPE_VP_LOCAL, timeframe="1m")

    conn = get_connection()
    cur = conn.cursor()
    assert int(_row(cur, "SELECT COUNT(*) AS c FROM price_levels WHERE is_active=1", ())["c"]) == 2
    id_low = int(_row(cur, "SELECT id FROM price_levels WHERE price = 50000", ())["id"])
    sid_low = _row(cur, "SELECT stable_level_id FROM price_levels WHERE id = ?", (id_low,))["stable_level_id"]
    conn.close()

    df2 = pd.DataFrame(
        [
            {
                "Price": 50000.0 + (eps * 0.5),
                "Volume": 10.0,
                "Duration_Hrs": 2.0,
                "Tier": "T1x",
                "start_utc": "2024-02-01T00:00:00+00:00",
                "end_utc": "2024-02-28T00:00:00+00:00",
            },
            {
                "Price": 52000.0,
                "Volume": 3.0,
                "Duration_Hrs": 1.0,
                "Tier": "T3",
                "start_utc": "2024-02-01T00:00:00+00:00",
                "end_utc": "2024-02-28T00:00:00+00:00",
            },
        ]
    )
    save_volume_profile_peaks_levels_to_db(sym, df2, layer="L2", level_type=LEVEL_TYPE_VP_LOCAL, timeframe="1m")

    conn = get_connection()
    cur = conn.cursor()
    all_rows = cur.execute(
        """
        SELECT id, price, volume_peak, status, is_active, stable_level_id
        FROM price_levels ORDER BY price
        """
    ).fetchall()
    conn.close()

    merged_price = 50000.0 + (eps * 0.5)
    active_only = [r for r in all_rows if int(r["is_active"]) == 1]
    by_price = {float(r["price"]): r for r in active_only}
    assert merged_price in by_price
    assert float(by_price[merged_price]["volume_peak"]) == 10.0
    assert str(by_price[merged_price]["stable_level_id"]) == str(sid_low)

    archived = [r for r in all_rows if int(r["is_active"]) == 0]
    assert any(float(r["price"]) == 51000.0 for r in archived)
    assert 52000.0 in by_price
    assert len(active_only) == 2
