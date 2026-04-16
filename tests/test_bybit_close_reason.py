"""Классификация take/stop по Bybit execution и закрытие position_records."""

from __future__ import annotations

import json
import time

import pytest

from trading_bot.data.bybit_close_reason import (
    classify_bybit_execution_close_reason,
    resolve_close_reason,
)
from trading_bot.data.db import get_connection
from trading_bot.data.schema import init_db, run_migrations


def test_classify_take_from_stop_order_type():
    assert classify_bybit_execution_close_reason({"stopOrderType": "TakeProfit"}) == "take"
    assert classify_bybit_execution_close_reason({"stopOrderType": "PartialTakeProfit"}) == "take"


def test_classify_stop_from_stop_order_type():
    assert classify_bybit_execution_close_reason({"stopOrderType": "StopLoss"}) == "stop"
    assert classify_bybit_execution_close_reason({"stopOrderType": "StopLossFull"}) == "stop"


def test_classify_adl():
    assert classify_bybit_execution_close_reason({"execType": "AdlTrade"}) == "adl"


def test_resolve_prefers_bybit_over_local():
    ex = {"stopOrderType": "TakeProfit"}
    local = {"order_role": "stop", "order_type": "StopMarket", "reduce_only": 1}
    assert resolve_close_reason(bybit_ex=ex, local_order_row=local) == "take"


def test_resolve_local_stop_role():
    assert (
        resolve_close_reason(
            bybit_ex=None,
            local_order_row={"order_role": "stop", "order_type": "StopMarket", "reduce_only": 1},
        )
        == "stop"
    )


def test_resolve_local_tp_role():
    assert (
        resolve_close_reason(
            bybit_ex=None,
            local_order_row={"order_role": "tp1", "order_type": "Limit", "reduce_only": 1},
        )
        == "take"
    )


def test_position_closed_stop_sets_close_reason(clean_db):
    import trading_bot.data.trade_reconciliation as tr

    init_db()
    run_migrations()
    now = int(time.time())
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO exec_orders (
            created_at, updated_at, order_role, client_order_id, bybit_order_id,
            symbol, side, order_type, qty, price, status, reduce_only
        )
        VALUES (?, ?, 'entry', 'c1', 'oid1', 'BTCUSDT', 'Buy', 'Market', 1.0, NULL, 'filled', 0)
        """,
        (now, now),
    )
    eid = int(cur.lastrowid)
    cur.execute(
        """
        INSERT INTO exec_orders (
            created_at, updated_at, order_role, client_order_id, bybit_order_id,
            symbol, side, order_type, qty, price, status, reduce_only, parent_exec_order_id
        )
        VALUES (?, ?, 'stop', 'c2', 'oid2', 'BTCUSDT', 'Sell', 'StopMarket', 1.0, 90000.0, 'submitted', 1, ?)
        """,
        (now, now, eid),
    )
    sid = int(cur.lastrowid)
    cur.execute(
        """
        INSERT INTO position_records (
            uuid, created_at, updated_at, cycle_id, symbol, side, status, qty,
            entry_exec_order_id, stop_exec_order_id, filled_qty, entry_price_fact, opened_at
        )
        VALUES (?, ?, ?, 'cyc', 'BTC/USDT', 'long', 'open', 1.0, ?, ?, 1.0, 100000.0, ?)
        """,
        ("u1", now, now, eid, sid, now),
    )
    pid = int(cur.lastrowid)
    cur.execute(
        "UPDATE exec_orders SET position_record_id = ? WHERE id IN (?, ?)",
        (pid, eid, sid),
    )
    raw = json.dumps(
        {"stopOrderType": "StopLoss", "execType": "Trade", "orderType": "Market"},
        ensure_ascii=False,
    )
    cur.execute(
        """
        INSERT INTO exec_fills (
            exec_order_id, position_record_id, cycle_id, symbol, side, trade_id,
            fill_price, fill_qty, fee, fee_currency, ts, raw_json
        )
        VALUES (?, ?, 'cyc', 'BTC/USDT', 'Sell', 't1', 90000.0, 1.0, 0, 'USDT', ?, ?)
        """,
        (sid, pid, now, raw),
    )
    conn.commit()

    tr._refresh_exec_order_from_fills(cur, exec_order_id=sid)
    tr._refresh_position_from_orders(cur, position_record_id=pid)
    conn.commit()
    row = cur.execute(
        "SELECT status, close_reason, exit_price_fact FROM position_records WHERE id = ?",
        (pid,),
    ).fetchone()
    conn.close()
    assert row["status"] == "closed"
    assert row["close_reason"] == "stop"
    assert float(row["exit_price_fact"] or 0) == pytest.approx(90000.0)
