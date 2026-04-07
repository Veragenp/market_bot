import pandas as pd

from trading_bot.analytics.human_levels import HumanLevelsResult, HumanZone
from trading_bot.data.db import get_connection
from trading_bot.data.human_levels_db import run_human_levels_and_save, save_human_levels_auto_to_db
from trading_bot.data.volume_profile_peaks_db import (
    LEVEL_STATUS_ACTIVE,
    LEVEL_TYPE_HUMAN,
    ORIGIN_AUTO,
    ORIGIN_MANUAL,
)


def _sample_result() -> HumanLevelsResult:
    return HumanLevelsResult(
        zones_d1=[HumanZone(99.0, 101.0, "1d", 3.0, 2)],
        zones_w1=[HumanZone(95.0, 98.0, "1w", 5.0, 1)],
        atr_d1_last=2.0,
        atr_w1_equiv=4.0,
    )


def test_run_human_levels_and_save_inserts_human_auto(clean_db):
    # OHLC с явным фракталом high в центре (иначе зон может не быть)
    base = 1_700_000_000
    highs = [100.0, 100.0, 100.0, 100.0, 100.0, 110.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0]
    lows = [99.0] * 15
    closes = [99.5] * 15
    rows_d1 = []
    for i, (h, l, c) in enumerate(zip(highs, lows, closes)):
        rows_d1.append(
            {
                "timestamp": base + i * 86400,
                "open": c,
                "high": h,
                "low": l,
                "close": c,
                "volume": 1.0,
            }
        )
    df_d1 = pd.DataFrame(rows_d1)
    df_w1 = pd.DataFrame(
        [
            {
                "timestamp": base,
                "open": 100.0,
                "high": 115.0,
                "low": 90.0,
                "close": 100.0,
                "volume": 1.0,
            },
            {
                "timestamp": base + 7 * 86400,
                "open": 100.0,
                "high": 105.0,
                "low": 95.0,
                "close": 98.0,
                "volume": 1.0,
            },
        ]
    )
    run_human_levels_and_save(
        "BTC/USDT",
        df_d1,
        df_w1,
        layer="test_human_layer",
        cluster_atr_mult=2.0,
        min_fractal_count=1,
        zone_min_gap_atr_d1=0.0,
    )

    conn = get_connection()
    cur = conn.cursor()
    rows = cur.execute(
        """
        SELECT level_type, origin, is_active, status, layer, timeframe
        FROM price_levels
        WHERE symbol = ?
        """,
        ("BTC/USDT",),
    ).fetchall()
    conn.close()

    assert len(rows) >= 1
    for r in rows:
        assert r["level_type"] == LEVEL_TYPE_HUMAN
        assert r["origin"] == ORIGIN_AUTO
        assert int(r["is_active"]) == 1
        assert r["status"] == LEVEL_STATUS_ACTIVE
        assert r["layer"] == "test_human_layer"


def test_save_human_auto_does_not_archive_manual_human(clean_db):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO price_levels (
            symbol, price, level_type, layer, origin, status, stable_level_id,
            strength, tier, created_at, updated_at, last_matched_calc_at, is_active
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
        """,
        (
            "BTC/USDT",
            50_000.0,
            LEVEL_TYPE_HUMAN,
            "manual_global",
            ORIGIN_MANUAL,
            LEVEL_STATUS_ACTIVE,
            "manual-stable-id-1",
            99.0,
            "hand",
            1,
            1,
            1,
        ),
    )
    conn.commit()
    conn.close()

    result = _sample_result()
    save_human_levels_auto_to_db("BTC/USDT", result, layer="auto_run", t_start_unix=1, t_end_unix=86400)

    conn = get_connection()
    cur = conn.cursor()
    manual = cur.execute(
        """
        SELECT is_active, status FROM price_levels
        WHERE symbol = ? AND origin = ?
        """,
        ("BTC/USDT", ORIGIN_MANUAL),
    ).fetchone()
    auto_count = cur.execute(
        """
        SELECT COUNT(*) AS c FROM price_levels
        WHERE symbol = ? AND origin = ? AND is_active = 1
        """,
        ("BTC/USDT", ORIGIN_AUTO),
    ).fetchone()["c"]
    conn.close()

    assert int(manual["is_active"]) == 1
    assert manual["status"] == LEVEL_STATUS_ACTIVE
    assert int(auto_count) >= 1
