"""
Черновик позиции из `entry_gate_confirmations` + расчёт `position_math`.

Опционально — рыночный вход через `bybit_trading.place_linear_market_order` при
`execute_market=True` и `BYBIT_EXECUTION_ENABLED`.
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from typing import Any, Dict, List, Literal, Optional, Sequence

from trading_bot.analytics.position_math import compute_position_plan, plan_to_dict
from trading_bot.config import settings as st
from trading_bot.tools import bybit_trading as bt

logger = logging.getLogger(__name__)

Side = Literal["long", "short"]


def _load_instrument(cur, symbol_bybit: str) -> Optional[Dict[str, Any]]:
    row = cur.execute(
        """
        SELECT tick_size, min_qty, atr
        FROM instruments
        WHERE symbol = ? AND exchange = 'bybit_futures'
        """,
        (symbol_bybit,),
    ).fetchone()
    if not row:
        return None
    return dict(row)


def create_draft_position_from_confirmation(
    cur,
    *,
    confirmation_id: int,
    execute_market: bool = False,
    execute_limit: bool = False,
    risk_usdt: Optional[float] = None,
    stop_atr_mult: Optional[float] = None,
    tp1_atr_mult: Optional[float] = None,
    tp2_atr_mult: Optional[float] = None,
    tp3_atr_mult: Optional[float] = None,
    tp1_share_pct: Optional[float] = None,
    tp2_share_pct: Optional[float] = None,
    qty_step: Optional[float] = None,
) -> Dict[str, Any]:
    """
    Вставляет строку в `position_records` (pending; open после рыночного входа).

    Ровно один из флагов: `execute_market` (рынок + стоп) или `execute_limit` (лимит GTC, без стопа до исполнения).
    Нужен `BYBIT_EXECUTION_ENABLED=1`.
    """
    row = cur.execute(
        """
        SELECT id, ts, cycle_id, structural_cycle_id, symbol, direction,
               level_price, entry_price, atr
        FROM entry_gate_confirmations
        WHERE id = ?
        """,
        (int(confirmation_id),),
    ).fetchone()
    if not row:
        return {"ok": False, "error": "confirmation_not_found"}

    conf = dict(row)
    sym_trade = str(conf["symbol"])
    sym_bybit = bt.to_bybit_symbol(sym_trade)
    side: Side = "long" if str(conf["direction"]).lower() == "long" else "short"

    atr = conf.get("atr")
    if atr is None or float(atr) <= 0:
        return {"ok": False, "error": "confirmation_atr_missing"}

    inst = _load_instrument(cur, sym_bybit)
    if not inst:
        return {"ok": False, "error": "instrument_not_found", "symbol": sym_bybit}

    tick = inst.get("tick_size")
    if tick is None or float(tick) <= 0:
        return {"ok": False, "error": "instrument_tick_size_missing", "symbol": sym_bybit}

    min_q = inst.get("min_qty")
    # Лот-шаг только из instruments.min_qty (никаких ручных override).
    if min_q is None or float(min_q) <= 0:
        return {"ok": False, "error": "instrument_min_qty_missing", "symbol": sym_bybit}
    step = float(min_q)

    r_usdt = float(risk_usdt if risk_usdt is not None else st.POSITION_RISK_USDT)
    plan = compute_position_plan(
        side=side,
        base_price=float(conf["level_price"]),
        entry_price_raw=float(conf["entry_price"]),
        atr=float(atr),
        risk_usdt=r_usdt,
        stop_atr_mult=float(stop_atr_mult if stop_atr_mult is not None else st.POSITION_STOP_ATR_MULT),
        tp1_atr_mult=float(tp1_atr_mult if tp1_atr_mult is not None else st.POSITION_TP1_ATR_MULT),
        tp2_atr_mult=float(tp2_atr_mult if tp2_atr_mult is not None else st.POSITION_TP2_ATR_MULT),
        tp3_atr_mult=float(tp3_atr_mult if tp3_atr_mult is not None else st.POSITION_TP3_ATR_MULT),
        tp1_share_pct=float(tp1_share_pct if tp1_share_pct is not None else st.POSITION_TP1_SHARE_PCT),
        tp2_share_pct=float(tp2_share_pct if tp2_share_pct is not None else st.POSITION_TP2_SHARE_PCT),
        price_tick=float(tick),
        qty_step=step,
        entry_offset=0.0,
        entry_offset_pct=float(st.POSITION_ENTRY_OFFSET_PCT),
        use_entry_offset=True,
        tp_in_stop_ranges=True,
        min_order_qty=float(min_q) if min_q is not None else None,
    )

    now = int(time.time())
    pos_uuid = str(uuid.uuid4())
    meta: Dict[str, Any] = {
        "plan": plan_to_dict(plan),
        "entry_gate_confirmation_id": int(conf["id"]),
        "level_price": float(conf["level_price"]),
        "instrument": {"tick_size": tick, "min_qty": min_q, "qty_step_used": step},
        "params": {
            "risk_usdt": r_usdt,
            "stop_atr_mult": float(stop_atr_mult if stop_atr_mult is not None else st.POSITION_STOP_ATR_MULT),
            "tp1_atr_mult": float(tp1_atr_mult if tp1_atr_mult is not None else st.POSITION_TP1_ATR_MULT),
            "tp2_atr_mult": float(tp2_atr_mult if tp2_atr_mult is not None else st.POSITION_TP2_ATR_MULT),
            "tp3_atr_mult": float(tp3_atr_mult if tp3_atr_mult is not None else st.POSITION_TP3_ATR_MULT),
            "tp1_share_pct": float(tp1_share_pct if tp1_share_pct is not None else st.POSITION_TP1_SHARE_PCT),
            "tp2_share_pct": float(tp2_share_pct if tp2_share_pct is not None else st.POSITION_TP2_SHARE_PCT),
            "entry_offset_pct": float(st.POSITION_ENTRY_OFFSET_PCT),
            "use_entry_offset": True,
            "tp_in_stop_ranges": True,
        },
    }

    status = "pending"
    entry_exec_id: Optional[int] = None
    stop_exec_id: Optional[int] = None
    order_raw: Optional[Dict[str, Any]] = None
    stop_order_raw: Optional[Dict[str, Any]] = None

    if execute_market and execute_limit:
        return {"ok": False, "error": "ambiguous_execute_flags", "plan": plan_to_dict(plan)}

    if execute_market:
        if not st.BYBIT_EXECUTION_ENABLED:
            return {
                "ok": False,
                "error": "execution_disabled",
                "hint": "set BYBIT_EXECUTION_ENABLED=1",
                "plan": plan_to_dict(plan),
            }
        client_oid = bt.build_client_order_id("pos")
        side_buy = side == "long"
        try:
            order_raw = bt.place_linear_market_order(
                symbol_trade=sym_trade,
                side_buy=side_buy,
                qty=float(plan.qty_total),
                reduce_only=False,
            )
        except Exception:
            logger.exception("place_linear_market_order failed %s", sym_bybit)
            return {"ok": False, "error": "order_failed", "plan": plan_to_dict(plan)}

        rc = order_raw.get("retCode")
        if rc not in (0, "0", None):
            return {
                "ok": False,
                "error": "order_rejected",
                "ret_code": rc,
                "ret_msg": order_raw.get("retMsg"),
                "plan": plan_to_dict(plan),
            }

        bybit_oid = None
        try:
            bybit_oid = (order_raw.get("result") or {}).get("orderId")
        except Exception:
            pass

        entry_exec_id = _insert_exec_order(
            cur,
            {
                "created_at": now,
                "updated_at": now,
                "cycle_id": conf.get("cycle_id"),
                "structural_cycle_id": conf.get("structural_cycle_id"),
                "position_record_id": None,
                "order_role": "entry",
                "client_order_id": client_oid,
                "bybit_order_id": str(bybit_oid) if bybit_oid else None,
                "symbol": sym_bybit,
                "side": "Buy" if side_buy else "Sell",
                "order_type": "Market",
                "qty": float(plan.qty_total),
                "price": None,
                "status": "submitted",
                "exchange_status": "accepted",
                "reduce_only": 0,
                "error_message": None,
                "raw_json": json.dumps(order_raw, ensure_ascii=False)[:8000],
            },
        )
        # SL выставляется совместно с входом (одним execution-актом).
        stop_side_buy = side == "short"
        stop_client_oid = bt.build_client_order_id("sl")
        try:
            stop_order_raw = bt.place_linear_stop_market_order(
                symbol_trade=sym_trade,
                side_buy=stop_side_buy,
                qty=float(plan.qty_total),
                trigger_price=float(plan.stop_price),
                reduce_only=True,
                close_on_trigger=True,
            )
        except Exception:
            logger.exception("place_linear_stop_market_order failed %s", sym_bybit)
            return {
                "ok": False,
                "error": "stop_order_failed",
                "plan": plan_to_dict(plan),
                "entry_order_response": order_raw,
            }
        stop_rc = stop_order_raw.get("retCode")
        if stop_rc not in (0, "0", None):
            return {
                "ok": False,
                "error": "stop_order_rejected",
                "ret_code": stop_rc,
                "ret_msg": stop_order_raw.get("retMsg"),
                "plan": plan_to_dict(plan),
                "entry_order_response": order_raw,
                "stop_order_response": stop_order_raw,
            }
        stop_bybit_oid = None
        try:
            stop_bybit_oid = (stop_order_raw.get("result") or {}).get("orderId")
        except Exception:
            pass
        stop_exec_id = _insert_exec_order(
            cur,
            {
                "created_at": now,
                "updated_at": now,
                "cycle_id": conf.get("cycle_id"),
                "structural_cycle_id": conf.get("structural_cycle_id"),
                "position_record_id": None,
                "order_role": "stop",
                "parent_exec_order_id": entry_exec_id,
                "client_order_id": stop_client_oid,
                "bybit_order_id": str(stop_bybit_oid) if stop_bybit_oid else None,
                "symbol": sym_bybit,
                "side": "Buy" if stop_side_buy else "Sell",
                "order_type": "StopMarket",
                "qty": float(plan.qty_total),
                "price": float(plan.stop_price),
                "status": "submitted",
                "exchange_status": "accepted",
                "reduce_only": 1,
                "error_message": None,
                "raw_json": json.dumps(stop_order_raw, ensure_ascii=False)[:8000],
            },
        )
        status = "open"

    elif execute_limit:
        if not st.BYBIT_EXECUTION_ENABLED:
            return {
                "ok": False,
                "error": "execution_disabled",
                "hint": "set BYBIT_EXECUTION_ENABLED=1",
                "plan": plan_to_dict(plan),
            }
        client_oid = bt.build_client_order_id("pos")
        side_buy = side == "long"
        limit_px = float(plan.entry_price)
        try:
            order_raw = bt.place_linear_limit_order(
                symbol_trade=sym_trade,
                side_buy=side_buy,
                qty=float(plan.qty_total),
                price=limit_px,
            )
        except Exception:
            logger.exception("place_linear_limit_order failed %s", sym_bybit)
            return {"ok": False, "error": "limit_order_failed", "plan": plan_to_dict(plan)}

        rc = order_raw.get("retCode")
        if rc not in (0, "0", None):
            return {
                "ok": False,
                "error": "limit_order_rejected",
                "ret_code": rc,
                "ret_msg": order_raw.get("retMsg"),
                "plan": plan_to_dict(plan),
            }

        bybit_oid = None
        try:
            bybit_oid = (order_raw.get("result") or {}).get("orderId")
        except Exception:
            pass

        entry_exec_id = _insert_exec_order(
            cur,
            {
                "created_at": now,
                "updated_at": now,
                "cycle_id": conf.get("cycle_id"),
                "structural_cycle_id": conf.get("structural_cycle_id"),
                "position_record_id": None,
                "order_role": "entry",
                "client_order_id": client_oid,
                "bybit_order_id": str(bybit_oid) if bybit_oid else None,
                "symbol": sym_bybit,
                "side": "Buy" if side_buy else "Sell",
                "order_type": "Limit",
                "qty": float(plan.qty_total),
                "price": limit_px,
                "status": "submitted",
                "exchange_status": "accepted",
                "reduce_only": 0,
                "error_message": None,
                "raw_json": json.dumps(order_raw, ensure_ascii=False)[:8000],
            },
        )

    cols = _table_columns(cur, "position_records")
    if "entry_gate_confirmation_id" in cols:
        pos_id = _insert_position_record(
            cur,
            {
                "uuid": pos_uuid,
                "created_at": now,
                "updated_at": now,
                "cycle_id": conf.get("cycle_id"),
                "structural_cycle_id": conf.get("structural_cycle_id"),
                "symbol": sym_trade,
                "side": side,
                "status": status,
                "qty": float(plan.qty_total),
                "entry_price": float(plan.entry_price),
                "entry_price_fact": float(plan.entry_price) if status == "open" else None,
                "filled_qty": float(plan.qty_total) if status == "open" else None,
                "entry_exec_order_id": entry_exec_id,
                "stop_exec_order_id": stop_exec_id,
                "opened_at": now if status == "open" else None,
                "meta_json": json.dumps(meta, ensure_ascii=False),
                "entry_gate_confirmation_id": int(conf["id"]),
            },
        )
    else:
        pos_id = _insert_position_record(
            cur,
            {
                "uuid": pos_uuid,
                "created_at": now,
                "updated_at": now,
                "cycle_id": conf.get("cycle_id"),
                "structural_cycle_id": conf.get("structural_cycle_id"),
                "symbol": sym_trade,
                "side": side,
                "status": status,
                "qty": float(plan.qty_total),
                "entry_price": float(plan.entry_price),
                "entry_price_fact": float(plan.entry_price) if status == "open" else None,
                "filled_qty": float(plan.qty_total) if status == "open" else None,
                "entry_exec_order_id": entry_exec_id,
                "stop_exec_order_id": stop_exec_id,
                "opened_at": now if status == "open" else None,
                "meta_json": json.dumps(meta, ensure_ascii=False),
            },
        )

    if entry_exec_id is not None:
        cur.execute(
            "UPDATE exec_orders SET position_record_id = ?, updated_at = ? WHERE id = ?",
            (pos_id, now, entry_exec_id),
        )
    if stop_exec_id is not None:
        cur.execute(
            "UPDATE exec_orders SET position_record_id = ?, updated_at = ? WHERE id = ?",
            (pos_id, now, stop_exec_id),
        )

    return {
        "ok": True,
        "position_record_id": pos_id,
        "uuid": pos_uuid,
        "status": status,
        "plan": plan_to_dict(plan),
        "entry_exec_order_id": entry_exec_id,
        "stop_exec_order_id": stop_exec_id,
        "order_response": order_raw,
        "stop_order_response": stop_order_raw,
    }


def auto_open_after_gate_confirmations(
    cur,
    confirmation_ids: Sequence[int],
    *,
    use_limit: bool = True,
) -> List[Dict[str, Any]]:
    """Для каждого id из гейта — черновик позиции и ордер (лимит или рынок+стоп)."""
    out: List[Dict[str, Any]] = []
    for cid in confirmation_ids:
        r = create_draft_position_from_confirmation(
            cur,
            confirmation_id=int(cid),
            execute_market=not use_limit,
            execute_limit=use_limit,
        )
        row: Dict[str, Any] = {"confirmation_id": int(cid), **r}
        out.append(row)
        if r.get("ok"):
            logger.info(
                "auto_open_after_gate: confirmation_id=%s ok position_record_id=%s",
                cid,
                r.get("position_record_id"),
            )
        else:
            logger.warning(
                "auto_open_after_gate: confirmation_id=%s failed %s",
                cid,
                r.get("error"),
            )
    return out


def _table_columns(cursor, table: str) -> set:
    cursor.execute(f"PRAGMA table_info({table})")
    return {row[1] for row in cursor.fetchall()}


def _insert_exec_order(cur, values: Dict[str, Any]) -> int:
    cols = _table_columns(cur, "exec_orders")
    payload = {k: v for k, v in values.items() if k in cols}
    keys = list(payload.keys())
    placeholders = ",".join("?" for _ in keys)
    cur.execute(
        f"INSERT INTO exec_orders ({','.join(keys)}) VALUES ({placeholders})",
        tuple(payload[k] for k in keys),
    )
    return int(cur.lastrowid)


def _insert_position_record(cur, values: Dict[str, Any]) -> int:
    cols = _table_columns(cur, "position_records")
    payload = {k: v for k, v in values.items() if k in cols}
    keys = list(payload.keys())
    placeholders = ",".join("?" for _ in keys)
    cur.execute(
        f"INSERT INTO position_records ({','.join(keys)}) VALUES ({placeholders})",
        tuple(payload[k] for k in keys),
    )
    return int(cur.lastrowid)


__all__ = ["auto_open_after_gate_confirmations", "create_draft_position_from_confirmation"]
