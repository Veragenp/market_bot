"""Закрытие противоположного пакета перед сигналом flip (LONG после short и т.д.)."""

from __future__ import annotations

import time
import uuid

from trading_bot.analytics.entry_gate import _flip_close_opposite_if_needed
from trading_bot.data.db import get_connection
from trading_bot.data.schema import init_db, run_migrations


def test_flip_skips_when_no_opposite_legs(monkeypatch, clean_db):
    monkeypatch.setattr("trading_bot.config.settings.BYBIT_EXECUTION_ENABLED", True)
    monkeypatch.setattr("trading_bot.config.settings.ENTRY_CLOSE_OPPOSITE_ON_FLIP_SIGNAL", True)
    init_db()
    run_migrations()
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "UPDATE trading_state SET cycle_id='c', structural_cycle_id='c', position_state='long' WHERE id=1"
    )
    conn.commit()
    out = _flip_close_opposite_if_needed(
        cur, cycle_id="c", incoming_direction="long", structural_cycle_id="c"
    )
    conn.close()
    assert out.get("skipped") == "no_opposite_legs"


def test_flip_cancel_stop_and_market_close_short(monkeypatch, clean_db):
    monkeypatch.setattr("trading_bot.config.settings.BYBIT_EXECUTION_ENABLED", True)
    monkeypatch.setattr("trading_bot.config.settings.ENTRY_CLOSE_OPPOSITE_ON_FLIP_SIGNAL", True)
    monkeypatch.setattr("trading_bot.config.settings.LEVEL_CROSS_TELEGRAM", False)

    calls: list = []

    def fake_cancel(**kwargs):
        calls.append(("cancel", kwargs))
        return {"retCode": 0}

    def fake_mkt(**kwargs):
        calls.append(("market", kwargs))
        return {"retCode": 0}

    monkeypatch.setattr("trading_bot.analytics.entry_gate.cancel_linear_order", fake_cancel)
    monkeypatch.setattr(
        "trading_bot.analytics.entry_gate.place_linear_market_order", fake_mkt
    )

    init_db()
    run_migrations()
    now = int(time.time())
    conn = get_connection()
    cur = conn.cursor()
    cid = "flip-test-cycle"
    cur.execute(
        """
        UPDATE trading_state SET cycle_id=?, structural_cycle_id=?, levels_frozen=1,
            position_state='short', cycle_phase='in_position', channel_mode='single_sided',
            known_side='short', need_rebuild_opposite=1, allow_long_entry=1, allow_short_entry=0,
            updated_at=?
        WHERE id=1
        """,
        (cid, cid, now),
    )
    cur.execute(
        """
        INSERT INTO exec_orders (
            created_at, updated_at, order_role, client_order_id, bybit_order_id,
            symbol, side, order_type, qty, price, status, reduce_only
        ) VALUES (?, ?, 'stop', 's1', 'st1', 'BTCUSDT', 'Buy', 'StopMarket', 1.0, 95000, 'open', 1)
        """,
        (now, now),
    )
    sid = int(cur.lastrowid)
    cur.execute(
        """
        INSERT INTO position_records (
            uuid, created_at, updated_at, cycle_id, symbol, side, status, qty,
            filled_qty, stop_exec_order_id, entry_exec_order_id
        ) VALUES (?, ?, ?, ?, 'BTC/USDT', 'short', 'open', 1.0, 1.0, ?, NULL)
        """,
        (str(uuid.uuid4()), now, now, cid, sid),
    )
    conn.commit()

    out = _flip_close_opposite_if_needed(
        cur, cycle_id=cid, incoming_direction="long", structural_cycle_id=cid
    )
    conn.commit()
    ts = cur.execute(
        "SELECT position_state, channel_mode, known_side FROM trading_state WHERE id=1"
    ).fetchone()
    conn.close()

    assert out.get("skipped") is None
    assert any(a.get("action") == "cancel_stop" for a in out["actions"])
    assert any(a.get("action") == "reduce_market" for a in out["actions"])
    assert ts["position_state"] == "none"
    assert ts["channel_mode"] == "two_sided"
    assert any(c[0] == "market" for c in calls)
    mkt_kw = [c[1] for c in calls if c[0] == "market"][0]
    assert mkt_kw["reduce_only"] is True
    assert mkt_kw["side_buy"] is True
