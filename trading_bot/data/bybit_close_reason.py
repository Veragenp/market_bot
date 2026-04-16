"""
Классификация причины закрытия позиции по данным Bybit (execution) и локальному exec_orders.

Значения close_reason для position_records: take, stop, adl, liquidation, reduce_close, unknown.
"""

from __future__ import annotations

import json
from typing import Any, Dict, Optional


def _norm(s: Any) -> str:
    return str(s or "").strip()


def classify_bybit_execution_close_reason(ex: Optional[Dict[str, Any]]) -> Optional[str]:
    """
    По одной строке execution из get_executions (V5 linear).
    Возвращает take | stop | adl | liquidation | reduce_close или None (не распознано).
    """
    if not ex or not isinstance(ex, dict):
        return None

    exec_type = _norm(ex.get("execType")).lower()
    if exec_type == "adltrade":
        return "adl"
    if exec_type in ("busttrade", "settle", "delivery"):
        return "liquidation"

    sot = _norm(ex.get("stopOrderType"))
    if sot:
        sot_u = sot.replace(" ", "")
        take_keys = (
            "TakeProfit",
            "PartialTakeProfit",
            "TakeProfitMarket",
            "PartialTakeProfitMarket",
            "MovingTakeProfit",
            "TakeProfitActive",
            "TrailingTakeProfit",
        )
        stop_keys = (
            "StopLoss",
            "StopLossFull",
            "PartialStopLoss",
            "StopLossMarket",
            "PartialStopLossMarket",
            "Stop",
            "TrailingStop",
        )
        for k in take_keys:
            if sot_u == k or sot.lower() == k.lower():
                return "take"
        for k in stop_keys:
            if sot_u == k or sot.lower() == k.lower():
                return "stop"
        if "takeprofit" in sot.lower() or "take" == sot.lower():
            return "take"
        if "stop" in sot.lower() or "loss" in sot.lower():
            return "stop"

    ot = _norm(ex.get("orderType")).lower()
    ro = ex.get("reduceOnly")
    is_ro = ro in (True, 1, "1", "true", "True")
    if is_ro and ot in ("market", "limit"):
        return "reduce_close"

    return None


def close_reason_from_local_exec_order(row: Optional[Dict[str, Any]]) -> Optional[str]:
    """По строке exec_orders (order_role, order_type, reduce_only)."""
    if not row:
        return None
    role = _norm(row.get("order_role")).lower()
    if role in ("stop", "stop_loss", "sl"):
        return "stop"
    if role in ("tp", "take_profit", "takeprofit", "tp1", "tp2", "tp3", "exit_tp"):
        return "take"
    if role in ("exit", "close", "reduce"):
        return "reduce_close"

    ot = _norm(row.get("order_type"))
    ro = int(row.get("reduce_only") or 0)
    if ro and ot.lower() in ("stopmarket", "stop_market", "market", "limit"):
        if "stop" in ot.lower():
            return "stop"
        return "reduce_close"
    return None


def resolve_close_reason(
    *,
    bybit_ex: Optional[Dict[str, Any]] = None,
    local_order_row: Optional[Dict[str, Any]] = None,
) -> str:
    """Приоритет: поля Bybit execution, затем локальный ордер, иначе unknown."""
    if bybit_ex:
        r = classify_bybit_execution_close_reason(bybit_ex)
        if r:
            return r
    if local_order_row:
        r = close_reason_from_local_exec_order(local_order_row)
        if r:
            return r
    return "unknown"


def parse_fill_raw_json(raw: Any) -> Optional[Dict[str, Any]]:
    if raw is None:
        return None
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            o = json.loads(raw)
            return o if isinstance(o, dict) else None
        except Exception:
            return None
    return None


__all__ = [
    "classify_bybit_execution_close_reason",
    "close_reason_from_local_exec_order",
    "resolve_close_reason",
    "parse_fill_raw_json",
]
