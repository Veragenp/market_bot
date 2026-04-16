"""Structural cycle: пул (L,U) + MAD → freeze cycle_levels / trading_state."""

from __future__ import annotations

import time

import pytest

from trading_bot.data.db import get_connection
from trading_bot.data.structural_cycle_db import run_structural_pipeline
from trading_bot.tools.price_feed import PricePoint


def _insert_instrument(cur, symbol: str, atr: float) -> None:
    bybit = symbol.replace("/", "")
    cur.execute(
        """
        INSERT INTO instruments (symbol, exchange, atr, updated_at)
        VALUES (?, 'bybit_futures', ?, ?)
        """,
        (bybit, atr, int(time.time())),
    )


def _insert_vp_local_side(cur, symbol: str, prices_vols: list[tuple[float, float]]) -> None:
    ts = int(time.time())
    for price, vol in prices_vols:
        cur.execute(
            """
            INSERT INTO price_levels (
                symbol, price, level_type, volume_peak, strength, tier,
                created_at, status, origin
            )
            VALUES (?, ?, 'vp_local', ?, 1.0, 't1', ?, 'active', 'auto')
            """,
            (symbol, price, vol, ts),
        )
        ts += 1


@pytest.fixture
def three_pool_symbols(clean_db):
    syms = ["AAA/USDT", "BBB/USDT", "CCC/USDT"]
    conn = get_connection()
    cur = conn.cursor()
    for s in syms:
        _insert_instrument(cur, s, atr=2.0)
        below = [(99.0 - i, 100.0 - i) for i in range(5)]
        above = [(101.0 + i, 100.0 - i) for i in range(5)]
        _insert_vp_local_side(cur, s, below)
        _insert_vp_local_side(cur, s, above)
    conn.commit()
    conn.close()
    return syms


def test_structural_pipeline_freezes_cycle_levels(three_pool_symbols, monkeypatch):
    monkeypatch.setattr("trading_bot.config.settings.STRUCTURAL_N_ETALON", 3)
    monkeypatch.setattr("trading_bot.config.settings.STRUCTURAL_MIN_POOL_SYMBOLS", 3)
    syms = three_pool_symbols
    ref = {s: PricePoint(price=100.0, ts=int(time.time()), source="test") for s in syms}
    r = run_structural_pipeline(symbols=syms, ref_prices_override=ref, auto_freeze=True)
    assert r.get("error") is None
    assert r["phase"] == "armed"
    assert r["symbols_ok"] == 3
    assert r["symbols_with_levels"] == 3
    assert r["long_count"] == 3
    assert r["short_count"] == 3
    assert r["cycle_levels_rows"] == 6

    conn = get_connection()
    cur = conn.cursor()
    n = cur.execute("SELECT COUNT(*) AS c FROM cycle_levels").fetchone()["c"]
    assert n == 6
    row = cur.execute(
        "SELECT cycle_id, structural_cycle_id, levels_frozen FROM trading_state WHERE id = 1"
    ).fetchone()
    assert row["levels_frozen"] == 1
    assert row["cycle_id"] == r["structural_cycle_id"]
    assert row["structural_cycle_id"] == r["structural_cycle_id"]
    conn.close()


def test_structural_pipeline_refuses_second_run_when_frozen(three_pool_symbols, monkeypatch):
    """Пока freeze и arming/in_position — второй полный pipeline не стартует (как в supervisor)."""
    monkeypatch.setattr("trading_bot.config.settings.STRUCTURAL_N_ETALON", 3)
    monkeypatch.setattr("trading_bot.config.settings.STRUCTURAL_MIN_POOL_SYMBOLS", 3)
    monkeypatch.setattr(
        "trading_bot.config.settings.SUPERVISOR_STRUCTURAL_SKIP_WHEN_CYCLE_ACTIVE", True
    )
    syms = three_pool_symbols
    ref = {s: PricePoint(price=100.0, ts=int(time.time()), source="test") for s in syms}
    r1 = run_structural_pipeline(symbols=syms, ref_prices_override=ref, auto_freeze=True)
    assert r1.get("error") is None
    r2 = run_structural_pipeline(symbols=syms, ref_prices_override=ref, auto_freeze=True)
    assert r2.get("error") == "refuse_structural_while_frozen_active"
    assert r2.get("cycle_phase") == "arming"


def test_structural_pipeline_with_one_level_each_side(clean_db, monkeypatch):
    """Достаточно одного активного уровня снизу и одного сверху (без top-5 запаса)."""
    monkeypatch.setattr("trading_bot.config.settings.STRUCTURAL_N_ETALON", 1)
    monkeypatch.setattr("trading_bot.config.settings.STRUCTURAL_MIN_POOL_SYMBOLS", 1)
    syms = ["ZZZ/USDT"]
    conn = get_connection()
    cur = conn.cursor()
    _insert_instrument(cur, syms[0], atr=2.0)
    ts = int(time.time())
    cur.execute(
        """
        INSERT INTO price_levels (
            symbol, price, level_type, volume_peak, strength, tier,
            created_at, status, origin
        )
        VALUES (?, 97.0, 'vp_local', 100.0, 1.0, 't1', ?, 'active', 'auto')
        """,
        (syms[0], ts),
    )
    cur.execute(
        """
        INSERT INTO price_levels (
            symbol, price, level_type, volume_peak, strength, tier,
            created_at, status, origin
        )
        VALUES (?, 103.0, 'vp_local', 100.0, 1.0, 't1', ?, 'active', 'auto')
        """,
        (syms[0], ts + 1),
    )
    conn.commit()
    conn.close()

    ref = {syms[0]: PricePoint(price=100.0, ts=int(time.time()), source="test")}
    r = run_structural_pipeline(symbols=syms, ref_prices_override=ref, auto_freeze=True)
    assert r.get("error") is None
    assert r["phase"] == "armed"
    assert r["symbols_ok"] == 1
    assert r["symbols_with_levels"] == 1
    assert r["long_count"] == 1
    assert r["short_count"] == 1
    assert r["cycle_levels_rows"] == 2


def test_structural_fit_band_covers_etalon_votes_skewed_median(clean_db, monkeypatch):
    """Строгий v4-отбор: если по каждой монете есть band-уровни с обеих сторон, freeze строит полный двусторонний пул."""
    monkeypatch.setattr("trading_bot.config.settings.STRUCTURAL_N_ETALON", 3)
    monkeypatch.setattr("trading_bot.config.settings.STRUCTURAL_MIN_POOL_SYMBOLS", 3)
    monkeypatch.setattr("trading_bot.config.settings.STRUCTURAL_W_SLACK_PCT", 50)
    monkeypatch.setattr("trading_bot.config.settings.STRUCTURAL_TOP_K", 3)

    syms = ["LOWW/USDT", "MID1/USDT", "MID2/USDT"]
    atr = 2.0
    refp = 100.0
    ts = int(time.time())
    conn = get_connection()
    cur = conn.cursor()
    for s in syms:
        _insert_instrument(cur, s, atr=atr)

    # LOWW: в band попадает пара 97/103.
    for price, peak in (
        (97.0, 1000.0),
        (99.0, 50.0),
        (103.0, 50.0),
        (105.0, 1000.0),
    ):
        cur.execute(
            """
            INSERT INTO price_levels (
                symbol, price, level_type, volume_peak, strength, tier,
                created_at, status, origin
            )
            VALUES (?, ?, 'vp_local', ?, 1.0, 't1', ?, 'active', 'auto')
            """,
            ("LOWW/USDT", price, peak, ts),
        )
        ts += 1

    # MID*: одна band-пара на символ.
    for sym in ("MID1/USDT", "MID2/USDT"):
        cur.execute(
            """
            INSERT INTO price_levels (
                symbol, price, level_type, volume_peak, strength, tier,
                created_at, status, origin
            )
            VALUES (?, 97.0, 'vp_local', 200.0, 1.0, 't1', ?, 'active', 'auto')
            """,
            (sym, ts),
        )
        ts += 1
        cur.execute(
            """
            INSERT INTO price_levels (
                symbol, price, level_type, volume_peak, strength, tier,
                created_at, status, origin
            )
            VALUES (?, 103.0, 'vp_local', 200.0, 1.0, 't1', ?, 'active', 'auto')
            """,
            (sym, ts),
        )
        ts += 1

    conn.commit()
    conn.close()

    ref = {s: PricePoint(price=refp, ts=int(time.time()), source="test") for s in syms}
    r = run_structural_pipeline(symbols=syms, ref_prices_override=ref, auto_freeze=True)
    assert r.get("error") is None
    assert r["phase"] == "armed"
    assert r["symbols_ok"] == 3
    assert r["cycle_levels_rows"] == 6


def test_structural_pipeline_cancel_insufficient_pool(three_pool_symbols, monkeypatch):
    monkeypatch.setattr("trading_bot.config.settings.STRUCTURAL_MIN_POOL_SYMBOLS", 10)
    syms = three_pool_symbols
    ref = {s: PricePoint(price=100.0, ts=int(time.time()), source="test") for s in syms}
    r = run_structural_pipeline(symbols=syms, ref_prices_override=ref, auto_freeze=True)
    assert r["phase"] == "cancelled"
    assert r["frozen"] is False
    assert r["cycle_levels_rows"] == 0

    conn = get_connection()
    cur = conn.cursor()
    ph = cur.execute("SELECT phase FROM structural_cycles WHERE id = ?", (r["structural_cycle_id"],)).fetchone()
    assert ph["phase"] == "cancelled"
    conn.close()


def test_trading_state_has_structural_cycle_id_column(clean_db):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(trading_state)")
    cols = {row[1] for row in cur.fetchall()}
    assert "structural_cycle_id" in cols
    assert "last_package_exit_reason" in cols
    conn.close()


