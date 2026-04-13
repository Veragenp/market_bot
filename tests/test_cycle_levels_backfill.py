"""Дозаполнение противоположной стороны cycle_levels (etalon W* как в structural)."""

from __future__ import annotations

import time

import pytest

from trading_bot.data.cycle_levels_db import backfill_missing_cycle_side
from trading_bot.data.db import get_connection
from trading_bot.data.schema import init_db, run_migrations


def _setup_btc_long_only(cur, *, cid: str, ts: int) -> None:
    cur.execute(
        """
        UPDATE trading_state SET cycle_id = ?, structural_cycle_id = ?, levels_frozen = 1
        WHERE id = 1
        """,
        (cid, cid),
    )
    cur.execute("DELETE FROM cycle_levels WHERE cycle_id = ?", (cid,))
    cur.execute(
        """
        INSERT INTO cycle_levels (
            cycle_id, symbol, direction, level_step, level_price,
            is_primary, is_active, frozen_at, updated_at
        )
        VALUES (?, 'BTC/USDT', 'long', 1, 100.0, 1, 1, ?, ?)
        """,
        (cid, ts, ts),
    )
    cur.execute("DELETE FROM instruments WHERE symbol = 'BTCUSDT' AND exchange = 'bybit_futures'")
    cur.execute(
        """
        INSERT INTO instruments (symbol, exchange, atr, updated_at)
        VALUES ('BTCUSDT', 'bybit_futures', 100.0, ?)
        """,
        (ts,),
    )


def test_backfill_missing_short_inserts_level(clean_db, monkeypatch):
    monkeypatch.setattr("trading_bot.data.cycle_levels_db.STRUCTURAL_MIN_POOL_SYMBOLS", 1)
    monkeypatch.setattr("trading_bot.data.cycle_levels_db.STRUCTURAL_N_ETALON", 1)
    init_db()
    run_migrations()
    ts = int(time.time())
    cid = "pytest-backfill-1"
    conn = get_connection()
    cur = conn.cursor()
    _setup_btc_long_only(cur, cid=cid, ts=ts)
    cur.execute(
        """
        INSERT INTO price_levels (
            symbol, price, level_type, volume_peak, strength, tier, created_at, status, origin, is_active
        )
        VALUES ('BTC/USDT', 205.0, 'vp_local', 100.0, 1.0, 't1', ?, 'active', 'auto', 1)
        """,
        (ts,),
    )
    conn.commit()

    rb = backfill_missing_cycle_side(
        cur,
        cycle_id=cid,
        symbols=["BTC/USDT"],
        missing_direction="short",
        ref_prices={"BTC/USDT": 99.0},
        ref_source="test",
    )
    conn.commit()
    n = cur.execute(
        """
        SELECT COUNT(*) AS c FROM cycle_levels
        WHERE cycle_id = ? AND symbol = 'BTC/USDT' AND direction = 'short' AND is_active = 1
        """,
        (cid,),
    ).fetchone()["c"]
    conn.close()
    assert rb.get("inserted") == 1
    assert int(n) == 1


def test_backfill_fails_insufficient_etalon_rebuild(clean_db, monkeypatch):
    """Два символа в запросе, но только у одного есть кандидат в [W_MIN,W_MAX] — голосов < N_ETALON."""
    monkeypatch.setattr("trading_bot.data.cycle_levels_db.STRUCTURAL_MIN_POOL_SYMBOLS", 1)
    monkeypatch.setattr("trading_bot.data.cycle_levels_db.STRUCTURAL_N_ETALON", 2)
    monkeypatch.setattr("trading_bot.data.cycle_levels_db.STRUCTURAL_W_MAX", 2.5)
    monkeypatch.setattr("trading_bot.data.cycle_levels_db.STRUCTURAL_W_MIN", 0.7)
    init_db()
    run_migrations()
    ts = int(time.time())
    cid = "pytest-backfill-2"
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE trading_state SET cycle_id = ?, structural_cycle_id = ?, levels_frozen = 1
        WHERE id = 1
        """,
        (cid, cid),
    )
    cur.execute("DELETE FROM cycle_levels WHERE cycle_id = ?", (cid,))
    for sym, long_p in (("BTC/USDT", 100.0), ("ETH/USDT", 50.0)):
        cur.execute(
            """
            INSERT INTO cycle_levels (
                cycle_id, symbol, direction, level_step, level_price,
                is_primary, is_active, frozen_at, updated_at
            )
            VALUES (?, ?, 'long', 1, ?, 1, 1, ?, ?)
            """,
            (cid, sym, long_p, ts, ts),
        )
    for sym, ex in (("BTCUSDT", "bybit_futures"), ("ETHUSDT", "bybit_futures")):
        cur.execute("DELETE FROM instruments WHERE symbol = ? AND exchange = ?", (sym, ex))
        cur.execute(
            """
            INSERT INTO instruments (symbol, exchange, atr, updated_at)
            VALUES (?, ?, 10.0, ?)
            """,
            (sym, ex, ts),
        )
    # BTC: short кандидат даёт W=(205-100)/100=1.05 — в диапазоне
    cur.execute(
        """
        INSERT INTO price_levels (
            symbol, price, level_type, volume_peak, strength, tier, created_at, status, origin, is_active
        )
        VALUES ('BTC/USDT', 205.0, 'vp_local', 100.0, 1.0, 't1', ?, 'active', 'auto', 1)
        """,
        (ts,),
    )
    # ETH: только далёкий short — W > W_MAX, не даёт голос
    cur.execute(
        """
        INSERT INTO price_levels (
            symbol, price, level_type, volume_peak, strength, tier, created_at, status, origin, is_active
        )
        VALUES ('ETH/USDT', 500.0, 'vp_local', 100.0, 1.0, 't1', ?, 'active', 'auto', 1)
        """,
        (ts,),
    )
    conn.commit()

    rb = backfill_missing_cycle_side(
        cur,
        cycle_id=cid,
        symbols=["BTC/USDT", "ETH/USDT"],
        missing_direction="short",
        ref_prices={"BTC/USDT": 99.0, "ETH/USDT": 48.0},
        ref_source="test",
    )
    conn.commit()
    conn.close()
    assert rb.get("inserted") == 0
    assert rb.get("reason") == "insufficient_etalon_rebuild"
