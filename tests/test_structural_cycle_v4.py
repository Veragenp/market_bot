"""structural_cycle_v4: сильнейший уровень в полосе 0.8–2 ATR ниже/выше ref."""

from __future__ import annotations

import time

from trading_bot.analytics.structural_cycle_v4 import build_structural_v4_report_df
from trading_bot.data.db import get_connection


def _insert_instrument(cur, symbol: str, atr: float) -> None:
    bybit = symbol.replace("/", "")
    cur.execute(
        """
        INSERT INTO instruments (symbol, exchange, atr, updated_at)
        VALUES (?, 'bybit_futures', ?, ?)
        """,
        (bybit, atr, int(time.time())),
    )


def _insert_ohlcv_close(cur, symbol: str, close: float) -> None:
    ts = int(time.time())
    cur.execute(
        """
        INSERT INTO ohlcv (symbol, timeframe, timestamp, open, high, low, close, volume, updated_at)
        VALUES (?, '1m', ?, ?, ?, ?, ?, 0, ?)
        """,
        (symbol, ts, close, close, close, close, ts),
    )


def _insert_level(
    cur,
    symbol: str,
    price: float,
    level_type: str,
    volume_peak: float,
    *,
    strength: float = 1.0,
) -> None:
    ts = int(time.time())
    cur.execute(
        """
        INSERT INTO price_levels (
            symbol, price, level_type, volume_peak, strength, tier,
            created_at, status, origin
        )
        VALUES (?, ?, ?, ?, ?, 't1', ?, 'active', 'auto')
        """,
        (symbol, price, level_type, volume_peak, strength, ts),
    )


def test_v4_picks_strongest_and_manual_tiebreak(clean_db, monkeypatch):
    monkeypatch.setattr("trading_bot.config.settings.STRUCTURAL_V4_BAND_MIN_ATR", 0.8)
    monkeypatch.setattr("trading_bot.config.settings.STRUCTURAL_V4_BAND_MAX_ATR", 2.0)
    sym = "TST/USDT"
    ref = 100.0
    atr = 1.0
    # Нижняя полоса: [98, 99.2]; верхняя: [100.8, 102]

    conn = get_connection()
    cur = conn.cursor()
    _insert_instrument(cur, sym, atr)
    _insert_ohlcv_close(cur, sym, ref)
    # vp ниже: слабее
    _insert_level(cur, sym, 99.0, "vp_local", 10.0)
    # manual ниже: та же сила 10 — должен выиграть tie-break
    _insert_level(cur, sym, 99.1, "manual_global_hvn", 10.0)
    # верх: vp сильнее manual
    _insert_level(cur, sym, 101.0, "vp_local", 200.0)
    _insert_level(cur, sym, 101.5, "manual_global_hvn", 50.0)
    conn.commit()
    conn.close()

    conn = get_connection()
    cur = conn.cursor()
    df = build_structural_v4_report_df(cur, symbols=[sym])
    conn.close()

    row = df.iloc[0]
    assert row["lower_level_type"] == "manual_global_hvn"
    assert abs(float(row["lower_level_price"]) - 99.1) < 1e-9
    assert row["upper_level_type"] == "vp_local"
    assert abs(float(row["upper_level_price"]) - 101.0) < 1e-9
    assert row["v4_status"] == "ok"
