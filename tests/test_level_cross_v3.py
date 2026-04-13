"""Level cross monitor + entry gate (tutorial V3 parity on cycle_levels)."""

from __future__ import annotations

import time

from trading_bot.analytics.entry_gate import process_v3_signal
from trading_bot.analytics.entry_gate import run_opposite_rebuild_maintenance_tick
from trading_bot.analytics.level_cross_monitor import (
    LevelCrossMonitor,
    load_cycle_level_pairs,
    run_level_cross_tick,
)
from trading_bot.data.db import get_connection
from trading_bot.data.schema import init_db, run_migrations
def _setup_frozen_cycle(cur, *, ts: int, long_level: float = 50000.0, short_level: float = 51000.0) -> str:
    cid = "pytest-v3-cycle"
    cur.execute(
        """
        UPDATE trading_state SET
            cycle_id = ?, structural_cycle_id = ?, levels_frozen = 1,
            position_state = 'none', cycle_phase = 'arming',
            allow_long_entry = 1, allow_short_entry = 1,
            last_transition_at = ?, updated_at = ?
        WHERE id = 1
        """,
        (cid, cid, ts, ts),
    )
    cur.execute("DELETE FROM cycle_levels WHERE cycle_id = ?", (cid,))
    cur.execute(
        """
        INSERT INTO cycle_levels (
            cycle_id, symbol, direction, level_step, level_price,
            is_primary, is_active, frozen_at, updated_at
        )
        VALUES
            (?, 'BTC/USDT', 'long', 1, ?, 1, 1, ?, ?),
            (?, 'BTC/USDT', 'short', 1, ?, 1, 1, ?, ?)
        """,
        (cid, long_level, ts, ts, cid, short_level, ts, ts),
    )
    cur.execute(
        "DELETE FROM instruments WHERE symbol = 'BTCUSDT' AND exchange = 'bybit_futures'"
    )
    cur.execute(
        """
        INSERT INTO instruments (symbol, exchange, atr, updated_at)
        VALUES ('BTCUSDT', 'bybit_futures', 100.0, ?)
        """,
        (ts,),
    )
    return cid


def _add_structural_members(cur, *, cycle_id: str, symbols: list[str], ts: int) -> None:
    cur.execute("DELETE FROM structural_cycle_symbols WHERE cycle_id = ?", (cycle_id,))
    for s in symbols:
        cur.execute(
            """
            INSERT INTO structural_cycle_symbols (
                cycle_id, symbol, status, level_below_id, level_above_id,
                L_price, U_price, atr, W_atr, mid_price, mid_band_low, mid_band_high,
                ref_price_ws, evaluated_at
            )
            VALUES (?, ?, 'ok', 1, 2, 100.0, 200.0, 10.0, 10.0, 150.0, 149.0, 151.0, 150.0, ?)
            """,
            (cycle_id, s, ts),
        )


def test_v3_long_cross_and_entry_signal(clean_db, monkeypatch):
    monkeypatch.setattr("trading_bot.config.settings.LEVEL_CROSS_MIN_ALERTS_COUNT", 1)
    monkeypatch.setattr("trading_bot.config.settings.LEVEL_CROSS_ALERT_TIMEOUT_MINUTES", 0.0)
    monkeypatch.setattr("trading_bot.config.settings.LEVEL_CROSS_TELEGRAM", False)

    init_db()
    run_migrations()
    ts = int(time.time())
    conn = get_connection()
    cur = conn.cursor()
    _setup_frozen_cycle(cur, ts=ts, long_level=100.0, short_level=200.0)
    conn.commit()
    conn.close()

    mon = LevelCrossMonitor()

    def run_tick(px: float):
        conn = get_connection()
        cur = conn.cursor()
        signals, _ = run_level_cross_tick(cur, prices={"BTC/USDT": px}, monitor=mon)
        conn.commit()
        conn.close()
        return signals

    run_tick(101.0)
    signals = run_tick(99.0)
    assert "LONG" in signals


def test_entry_gate_long_confirms(clean_db, monkeypatch):
    monkeypatch.setattr("trading_bot.config.settings.ENTRY_GATE_LONG_ATR_THRESHOLD_PCT", 5.0)
    monkeypatch.setattr("trading_bot.config.settings.LEVEL_CROSS_TELEGRAM", False)

    init_db()
    run_migrations()
    ts = int(time.time())
    conn = get_connection()
    cur = conn.cursor()
    _setup_frozen_cycle(cur, ts=ts, long_level=100.0, short_level=200.0)
    conn.commit()
    conn.close()

    mon = LevelCrossMonitor()
    prices = {"BTC/USDT": 99.0}
    conn = get_connection()
    cur = conn.cursor()
    r = process_v3_signal(cur, signal_type="LONG", monitor=mon, prices=prices)
    conn.commit()
    conn.close()
    assert "BTC/USDT" in r.get("entered", [])
    conn = get_connection()
    n = conn.execute("SELECT COUNT(*) AS c FROM entry_gate_confirmations").fetchone()["c"]
    ts = conn.execute(
        """
        SELECT position_state, cycle_phase, channel_mode, known_side, need_rebuild_opposite
        FROM trading_state
        WHERE id = 1
        """
    ).fetchone()
    conn.close()
    assert n >= 1
    assert ts["position_state"] == "long"
    assert ts["cycle_phase"] == "in_position"
    assert ts["channel_mode"] == "two_sided"
    assert ts["known_side"] == "both"
    assert int(ts["need_rebuild_opposite"]) == 0


def test_entry_gate_long_can_rebuild_missing_short_side(clean_db, monkeypatch):
    monkeypatch.setattr("trading_bot.config.settings.ENTRY_GATE_LONG_ATR_THRESHOLD_PCT", 5.0)
    monkeypatch.setattr("trading_bot.config.settings.LEVEL_CROSS_TELEGRAM", False)
    monkeypatch.setattr("trading_bot.config.settings.STRUCTURAL_OPPOSITE_REBUILD_ENABLED", True)
    monkeypatch.setattr("trading_bot.data.cycle_levels_db.STRUCTURAL_MIN_POOL_SYMBOLS", 1)
    init_db()
    run_migrations()
    ts = int(time.time())
    conn = get_connection()
    cur = conn.cursor()
    cid = _setup_frozen_cycle(cur, ts=ts, long_level=100.0, short_level=200.0)
    cur.execute(
        "DELETE FROM cycle_levels WHERE cycle_id = ? AND symbol = 'BTC/USDT' AND direction = 'short'",
        (cid,),
    )
    cur.execute(
        """
        INSERT INTO price_levels (
            symbol, price, level_type, volume_peak, strength, tier, created_at, status, origin, is_active
        )
        VALUES ('BTC/USDT', 205.0, 'vp_local', 100.0, 1.0, 't1', ?, 'active', 'auto', 1)
        """,
        (ts,),
    )
    cur.execute(
        """
        INSERT INTO ohlcv (
            symbol, timeframe, timestamp, open, high, low, close, volume, source, extra, updated_at
        )
        VALUES ('ETH/USDT', '1m', ?, 100.0, 101.0, 99.0, 100.0, 1.0, 'binance', NULL, ?)
        """,
        (ts, ts),
    )
    conn.commit()
    conn.close()

    mon = LevelCrossMonitor()
    prices = {"BTC/USDT": 99.0}
    conn = get_connection()
    cur = conn.cursor()
    r = process_v3_signal(cur, signal_type="LONG", monitor=mon, prices=prices)
    conn.commit()
    conn.close()
    assert "BTC/USDT" in r.get("entered", [])

    conn = get_connection()
    row = conn.execute(
        """
        SELECT COUNT(*) AS c
        FROM cycle_levels
        WHERE cycle_id = ? AND symbol = 'BTC/USDT' AND direction = 'short' AND level_step = 1 AND is_active = 1
        """,
        (cid,),
    ).fetchone()
    ts_row = conn.execute(
        """
        SELECT channel_mode, known_side, need_rebuild_opposite
        FROM trading_state
        WHERE id = 1
        """
    ).fetchone()
    conn.close()
    assert int(row["c"]) == 1
    assert ts_row["channel_mode"] == "two_sided"
    assert ts_row["known_side"] == "both"
    assert int(ts_row["need_rebuild_opposite"]) == 0


def test_entry_gate_blocks_opposite_signal_in_single_sided_mode(clean_db, monkeypatch):
    monkeypatch.setattr("trading_bot.config.settings.ENTRY_GATE_LONG_ATR_THRESHOLD_PCT", 5.0)
    monkeypatch.setattr("trading_bot.config.settings.LEVEL_CROSS_TELEGRAM", False)
    init_db()
    run_migrations()
    ts = int(time.time())
    conn = get_connection()
    cur = conn.cursor()
    _setup_frozen_cycle(cur, ts=ts, long_level=100.0, short_level=200.0)
    cur.execute(
        """
        UPDATE trading_state
        SET channel_mode = 'single_sided',
            known_side = 'short',
            need_rebuild_opposite = 1,
            opposite_rebuild_deadline_ts = ?,
            position_state = 'short',
            cycle_phase = 'in_position'
        WHERE id = 1
        """,
        (ts + 3600,),
    )
    conn.commit()
    conn.close()

    mon = LevelCrossMonitor()
    conn = get_connection()
    cur = conn.cursor()
    r = process_v3_signal(cur, signal_type="LONG", monitor=mon, prices={"BTC/USDT": 99.0})
    conn.commit()
    conn.close()
    assert r.get("ok") is False
    assert r.get("error") == "opposite_side_not_ready"


def test_entry_gate_closes_cycle_on_opposite_rebuild_timeout(clean_db, monkeypatch):
    monkeypatch.setattr("trading_bot.config.settings.ENTRY_GATE_LONG_ATR_THRESHOLD_PCT", 5.0)
    monkeypatch.setattr("trading_bot.config.settings.LEVEL_CROSS_TELEGRAM", False)
    init_db()
    run_migrations()
    ts = int(time.time())
    conn = get_connection()
    cur = conn.cursor()
    _setup_frozen_cycle(cur, ts=ts, long_level=100.0, short_level=200.0)
    cur.execute(
        """
        UPDATE trading_state
        SET channel_mode = 'single_sided',
            known_side = 'short',
            need_rebuild_opposite = 1,
            opposite_rebuild_deadline_ts = ?,
            position_state = 'short',
            cycle_phase = 'in_position'
        WHERE id = 1
        """,
        (ts - 1,),
    )
    conn.commit()
    conn.close()

    mon = LevelCrossMonitor()
    conn = get_connection()
    cur = conn.cursor()
    r = process_v3_signal(cur, signal_type="LONG", monitor=mon, prices={"BTC/USDT": 99.0})
    conn.commit()
    row = cur.execute(
        "SELECT cycle_phase, levels_frozen, close_reason FROM trading_state WHERE id = 1"
    ).fetchone()
    conn.close()
    assert r.get("ok") is False
    assert r.get("error") == "opposite_rebuild_timeout"
    assert row["cycle_phase"] == "closed"
    assert int(row["levels_frozen"]) == 0
    assert row["close_reason"] == "opposite_rebuild_timeout"


def test_maintenance_tick_rebuilds_missing_opposite_side(clean_db, monkeypatch):
    monkeypatch.setattr("trading_bot.config.settings.STRUCTURAL_OPPOSITE_REBUILD_ENABLED", True)
    monkeypatch.setattr("trading_bot.data.cycle_levels_db.STRUCTURAL_MIN_POOL_SYMBOLS", 1)
    init_db()
    run_migrations()
    ts = int(time.time())
    conn = get_connection()
    cur = conn.cursor()
    cid = _setup_frozen_cycle(cur, ts=ts, long_level=100.0, short_level=200.0)
    cur.execute(
        "DELETE FROM cycle_levels WHERE cycle_id = ? AND symbol = 'BTC/USDT' AND direction = 'short'",
        (cid,),
    )
    cur.execute(
        """
        UPDATE trading_state
        SET channel_mode = 'single_sided',
            known_side = 'long',
            need_rebuild_opposite = 1,
            opposite_rebuild_deadline_ts = ?,
            opposite_rebuild_attempts = 0
        WHERE id = 1
        """,
        (ts + 3600,),
    )
    cur.execute(
        """
        INSERT INTO price_levels (
            symbol, price, level_type, volume_peak, strength, tier, created_at, status, origin, is_active
        )
        VALUES ('BTC/USDT', 205.0, 'vp_local', 110.0, 1.0, 't1', ?, 'active', 'auto', 1)
        """,
        (ts,),
    )
    conn.commit()

    out = run_opposite_rebuild_maintenance_tick(cur, prices={"BTC/USDT": 99.0})
    conn.commit()
    cnt = cur.execute(
        """
        SELECT COUNT(*) AS c
        FROM cycle_levels
        WHERE cycle_id = ? AND symbol = 'BTC/USDT' AND direction = 'short' AND is_active = 1
        """,
        (cid,),
    ).fetchone()
    state = cur.execute(
        """
        SELECT channel_mode, known_side, need_rebuild_opposite, opposite_rebuild_attempts
        FROM trading_state WHERE id = 1
        """
    ).fetchone()
    conn.close()
    assert out.get("ok") is True
    assert int(cnt["c"]) == 1
    assert state["channel_mode"] == "two_sided"
    assert state["known_side"] == "both"
    assert int(state["need_rebuild_opposite"]) == 0
    assert int(state["opposite_rebuild_attempts"]) == 1


def test_maintenance_uses_structural_membership_not_only_entered_symbols(clean_db, monkeypatch):
    monkeypatch.setattr("trading_bot.config.settings.STRUCTURAL_OPPOSITE_REBUILD_ENABLED", True)
    monkeypatch.setattr("trading_bot.data.cycle_levels_db.STRUCTURAL_MIN_POOL_SYMBOLS", 1)
    init_db()
    run_migrations()
    ts = int(time.time())
    conn = get_connection()
    cur = conn.cursor()
    cid = _setup_frozen_cycle(cur, ts=ts, long_level=100.0, short_level=200.0)
    _add_structural_members(cur, cycle_id=cid, symbols=["BTC/USDT", "ETH/USDT"], ts=ts)
    # ETH has only LONG in cycle_levels (missing SHORT), but should still be considered
    # because it is in structural membership.
    cur.execute(
        """
        INSERT INTO cycle_levels (
            cycle_id, symbol, direction, level_step, level_price,
            is_primary, is_active, frozen_at, updated_at
        )
        VALUES (?, 'ETH/USDT', 'long', 1, 100.0, 1, 1, ?, ?)
        """,
        (cid, ts, ts),
    )
    cur.execute(
        """
        UPDATE trading_state
        SET channel_mode = 'single_sided',
            known_side = 'long',
            need_rebuild_opposite = 1,
            opposite_rebuild_deadline_ts = ?,
            opposite_rebuild_attempts = 0
        WHERE id = 1
        """,
        (ts + 3600,),
    )
    cur.execute(
        """
        INSERT INTO instruments (symbol, exchange, atr, updated_at)
        VALUES ('ETHUSDT', 'bybit_futures', 100.0, ?)
        ON CONFLICT(symbol, exchange) DO UPDATE SET atr = excluded.atr, updated_at = excluded.updated_at
        """,
        (ts,),
    )
    cur.execute(
        """
        INSERT INTO price_levels (
            symbol, price, level_type, volume_peak, strength, tier, created_at, status, origin, is_active
        )
        VALUES ('ETH/USDT', 205.0, 'vp_local', 120.0, 1.0, 't1', ?, 'active', 'auto', 1)
        """,
        (ts,),
    )
    cur.execute(
        """
        INSERT INTO ohlcv (
            symbol, timeframe, timestamp, open, high, low, close, volume, source, extra, updated_at
        )
        VALUES ('ETH/USDT', '1m', ?, 100.0, 101.0, 99.0, 100.0, 1.0, 'binance', NULL, ?)
        """,
        (ts, ts),
    )
    conn.commit()

    out = run_opposite_rebuild_maintenance_tick(cur, prices={"BTC/USDT": 99.0})
    conn.commit()
    row = cur.execute(
        """
        SELECT COUNT(*) AS c
        FROM cycle_levels
        WHERE cycle_id = ? AND symbol = 'ETH/USDT' AND direction = 'short' AND is_active = 1
        """,
        (cid,),
    ).fetchone()
    conn.close()
    assert out.get("ok") is True
    assert int(row["c"]) == 1


def test_load_cycle_level_pairs_requires_both_sides(clean_db):
    init_db()
    run_migrations()
    conn = get_connection()
    cur = conn.cursor()
    cid = "x"
    cur.execute(
        """
        INSERT INTO cycle_levels (
            cycle_id, symbol, direction, level_step, level_price,
            is_primary, is_active, frozen_at, updated_at
        )
        VALUES (?, 'ETH/USDT', 'long', 1, 1.0, 1, 1, 0, 0)
        """,
        (cid,),
    )
    conn.commit()
    pairs = load_cycle_level_pairs(cur, cid)
    conn.close()
    assert pairs == {}
