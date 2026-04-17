"""
Второй слой после level_cross_monitor: логика v4 с групповыми сигналами и флипом.

При групповом сигнале LONG/SHORT:
- закрываем противоположную сторону (flip), если нужно
- перестраиваем противоположную сторону для будущего флипа (rebuild)
- для каждого символа в наборе проверяем ATR-порог (как в старой логике)
- если условие выполнено – выставляем лимитный ордер (без entry_gate_confirmations)
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from trading_bot.config import settings as st
from trading_bot.analytics.structural_cycle import rebuild_side_on_cursor
from trading_bot.data.structural_cycle_db import refresh_cycle_levels_from_structural
from trading_bot.analytics.level_cross_monitor import load_cycle_level_pairs
from trading_bot.tools.bybit_trading import (
    cancel_linear_order,
    get_linear_open_orders,
    get_linear_positions,
    linear_position_sizes_by_symbol,
    place_linear_limit_order,
    place_linear_market_order,
    pool_symbols_flat_on_linear_exchange,
    to_bybit_symbol,
)
from trading_bot.tools.telegram_notify import escape_html_telegram, get_telegram_notifier

if TYPE_CHECKING:
    from trading_bot.analytics.level_cross_monitor import LevelCrossMonitor

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Вспомогательные функции (оставшиеся от старой версии)
# ---------------------------------------------------------------------------
def _to_bool(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    s = str(v).strip().lower()
    return s in ("1", "true", "yes", "y")


def _load_open_entry_orders_by_symbol_and_side() -> Dict[tuple[str, str], List[str]]:
    """Загружает открытые ордера с биржи (только не reduceOnly) – используется только во flip."""
    resp = get_linear_open_orders()
    out: Dict[tuple[str, str], List[str]] = {}
    rows = ((resp or {}).get("result") or {}).get("list") or []
    for r in rows:
        sym_bybit = str(r.get("symbol") or "").upper()
        order_id = str(r.get("orderId") or "")
        side = str(r.get("side") or "").strip().lower()
        if not sym_bybit or not order_id or side not in ("buy", "sell"):
            continue
        if _to_bool(r.get("reduceOnly")):
            continue
        sym_trade = sym_bybit.replace("USDT", "/USDT")
        key = (sym_trade, side)
        out.setdefault(key, []).append(order_id)
    return out


def _get_atr(cur, symbol_trade: str) -> Optional[float]:
    """Получить ATR для символа из таблицы instruments."""
    # Преобразовать BTC/USDT → BTCUSDT (формат instruments)
    symbol_bybit = symbol_trade.replace("/", "").upper()
    row = cur.execute(
        "SELECT atr FROM instruments WHERE symbol = ? AND exchange = 'bybit_futures'",
        (symbol_bybit,),
    ).fetchone()
    if not row or row["atr"] is None:
        return None
    v = float(row["atr"])
    return v if v > 0 else None


def _log_v4_event(
    cur,
    *,
    cycle_id: str,
    structural_cycle_id: Optional[str],
    event_type: str,
    symbol: Optional[str],
    price: Optional[float],
    meta: Optional[Dict[str, Any]] = None,
) -> None:
    """Логирование событий в entry_detector_events."""
    ts = int(time.time())
    cur.execute(
        """
        INSERT INTO entry_detector_events (
            ts, cycle_id, structural_cycle_id, symbol, event_type,
            price, long_level_price, short_level_price, atr_used,
            distance_to_long_atr, distance_to_short_atr, meta_json
        )
        VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, NULL, ?)
        """,
        (
            ts,
            cycle_id,
            structural_cycle_id,
            symbol or "",
            event_type,
            price,
            json.dumps(meta, ensure_ascii=False) if meta else None,
        ),
    )


def _load_cycle_side_counts(cur, *, cycle_id: str) -> Dict[str, int]:
    """Количество символов в long и short наборах (для статистики)."""
    rows = cur.execute(
        """
        SELECT direction, COUNT(DISTINCT symbol) AS c
        FROM cycle_levels
        WHERE cycle_id = ? AND level_step = 1 AND is_active = 1
        GROUP BY direction
        """,
        (cycle_id,),
    ).fetchall()
    out = {"long_count": 0, "short_count": 0}
    for r in rows:
        d = str(r["direction"] or "").lower()
        if d == "long":
            out["long_count"] = int(r["c"] or 0)
        elif d == "short":
            out["short_count"] = int(r["c"] or 0)
    return out


def _load_cycle_member_symbols(cur, *, cycle_id: str) -> List[str]:
    """Символы из structural_cycle_symbols (для других нужд)."""
    rows = cur.execute(
        """
        SELECT DISTINCT symbol
        FROM structural_cycle_symbols
        WHERE cycle_id = ? AND status = 'ok'
        ORDER BY symbol
        """,
        (cycle_id,),
    ).fetchall()
    syms = [str(r["symbol"]) for r in rows]
    if syms:
        return syms
    rows = cur.execute(
        """
        SELECT DISTINCT symbol
        FROM cycle_levels
        WHERE cycle_id = ? AND is_active = 1
        ORDER BY symbol
        """,
        (cycle_id,),
    ).fetchall()
    return [str(r["symbol"]) for r in rows]


# ---------------------------------------------------------------------------
# Завершение эпохи и переход после закрытия всех позиций (без изменений)
# ---------------------------------------------------------------------------
def close_trading_epoch_v4_cancel(
    cur,
    *,
    monitor: "LevelCrossMonitor",
    signal_type: str,
) -> Dict[str, Any]:
    """Закрывает торговую эпоху (при CANCEL_LONG / CANCEL_SHORT)."""
    now = int(time.time())
    row = cur.execute(
        "SELECT cycle_id, structural_cycle_id FROM trading_state WHERE id = 1"
    ).fetchone()
    if not row:
        return {"ok": False, "error": "no_trading_state"}
    cycle_id = str(row["cycle_id"]) if row["cycle_id"] else ""
    scid = str(row["structural_cycle_id"]) if row["structural_cycle_id"] else None
    reason = "v4_cancel_long" if signal_type == "CANCEL_LONG" else "v4_cancel_short"
    if cycle_id:
        _log_v4_event(
            cur,
            cycle_id=cycle_id,
            structural_cycle_id=scid,
            event_type=f"v4_{signal_type.lower()}",
            symbol=None,
            price=None,
            meta={"action": "close_trading_epoch", "close_reason": reason},
        )
    cur.execute(
        """
        UPDATE trading_state SET
            cycle_phase = 'closed',
            levels_frozen = 0,
            cycle_id = NULL,
            structural_cycle_id = NULL,
            position_state = 'none',
            close_reason = ?,
            allow_long_entry = 1,
            allow_short_entry = 1,
            last_transition_at = ?,
            updated_at = ?
        WHERE id = 1
        """,
        (reason, now, now),
    )
    monitor.reset()
    return {"ok": True, "cancel": signal_type, "close_reason": reason}


def _infer_last_package_exit_reason_from_db(reason_list: List[str]) -> str:
    if not reason_list:
        return "package_flat"
    joined = [str(x).strip().lower() for x in reason_list if x]
    if any(x in ("take", "tp") for x in joined):
        return "take"
    if any(x in ("stop", "sl", "adl", "liquidation", "reduce_close") for x in joined):
        return "stop"
    blob = " ".join(joined)
    if any(x in blob for x in ("stop", "sl", "liquidat", "stop_loss", "adl")):
        return "stop"
    if any(x in blob for x in ("take", "tp", "profit", "target")):
        return "take"
    return "package_flat"


def _apply_closed_after_package_flat(
    cur,
    *,
    cycle_id: str,
    structural_cycle_id: Optional[str],
    now: int,
    last_package_exit_reason: str,
    meta: Dict[str, Any],
) -> None:
    cur.execute(
        """
        UPDATE trading_state SET
            cycle_phase = 'closed',
            levels_frozen = 0,
            cycle_id = NULL,
            structural_cycle_id = NULL,
            position_state = 'none',
            allow_long_entry = 1,
            allow_short_entry = 1,
            last_rebuild_reason = 'package_all_flat_close_epoch',
            last_package_exit_reason = ?,
            last_transition_at = ?,
            updated_at = ?
        WHERE id = 1
        """,
        (last_package_exit_reason, now, now),
    )
    _log_v4_event(
        cur,
        cycle_id=cycle_id,
        structural_cycle_id=structural_cycle_id,
        event_type="v4_package_all_flat_close_epoch",
        symbol=None,
        price=None,
        meta={**meta, "last_package_exit_reason": last_package_exit_reason},
    )


def maybe_transition_arming_after_package_all_flat(cur) -> Dict[str, Any]:
    """Проверяет, все ли позиции пакета закрыты, и если да – закрывает эпоху."""
    if not st.ENTRY_PACKAGE_FLAT_TRANSITION:
        return {"ok": True, "skipped": "disabled"}

    now = int(time.time())
    row = cur.execute(
        """
        SELECT cycle_id, structural_cycle_id, cycle_phase, levels_frozen
        FROM trading_state WHERE id = 1
        """
    ).fetchone()
    if not row:
        return {"ok": False, "error": "no_trading_state"}
    phase = str(row["cycle_phase"] or "")
    if phase not in ("in_position", "arming"):
        return {"ok": True, "skipped": "not_in_position_or_arming"}
    if not int(row["levels_frozen"] or 0):
        return {"ok": True, "skipped": "not_frozen"}
    cid = row["cycle_id"]
    if not cid:
        return {"ok": True, "skipped": "no_cycle_id"}
    cycle_id = str(cid)
    scid = row["structural_cycle_id"]
    scid = str(scid) if scid else None
    pool_cycle = str(scid) if scid else cycle_id
    pool = _load_cycle_member_symbols(cur, cycle_id=pool_cycle)
    if not pool:
        return {"ok": True, "skipped": "no_pool_symbols"}

    open_row = cur.execute(
        "SELECT COUNT(*) AS c FROM position_records WHERE cycle_id = ? AND status IN ('pending', 'open')",
        (cycle_id,),
    ).fetchone()
    n_open = int(open_row["c"] if open_row else 0)
    if n_open > 0:
        return {"ok": True, "skipped": "positions_open_or_pending", "n_open": n_open}

    any_row = cur.execute(
        "SELECT COUNT(*) AS c FROM position_records WHERE cycle_id = ?",
        (cycle_id,),
    ).fetchone()
    n_any = int(any_row["c"] if any_row else 0)

    use_bybit = st.ENTRY_PACKAGE_FLAT_USE_BYBIT_POSITIONS or st.BYBIT_EXECUTION_ENABLED

    if n_any > 0:
        reasons_rows = cur.execute(
            """
            SELECT close_reason FROM position_records
            WHERE cycle_id = ? AND status = 'closed' AND closed_at IS NOT NULL
            ORDER BY closed_at DESC LIMIT 8
            """,
            (cycle_id,),
        ).fetchall()
        reason_list = [str(r["close_reason"]) for r in reasons_rows if r["close_reason"]]
        pkg_reason = _infer_last_package_exit_reason_from_db(reason_list)
        _apply_closed_after_package_flat(
            cur,
            cycle_id=cycle_id,
            structural_cycle_id=scid,
            now=now,
            last_package_exit_reason=pkg_reason,
            meta={"source": "position_records", "position_records": n_any, "recent_close_reasons": reason_list},
        )
        return {"ok": True, "transitioned": True, "cycle_phase": "closed", "position_records_seen": n_any,
                "source": "position_records", "last_package_exit_reason": pkg_reason}

    if not use_bybit:
        return {"ok": True, "skipped": "no_position_records_and_bybit_flat_disabled",
                "hint": "set BYBIT_EXECUTION_ENABLED=1 or ENTRY_PACKAGE_FLAT_USE_BYBIT_POSITIONS=1"}

    resp = get_linear_positions()
    if resp is None:
        return {"ok": True, "skipped": "bybit_positions_unavailable"}
    sizes = linear_position_sizes_by_symbol(resp)
    if not pool_symbols_flat_on_linear_exchange(pool, sizes):
        nonzero = {to_bybit_symbol(s): sizes.get(to_bybit_symbol(s), 0.0) for s in pool}
        return {"ok": True, "skipped": "exchange_has_open_size", "source": "bybit_positions", "pool_sizes": nonzero}

    if phase == "arming":
        return {"ok": True, "skipped": "arming_bybit_flat_ignored", "source": "bybit_positions",
                "cycle_phase": phase, "position_records_seen": 0}

    _apply_closed_after_package_flat(
        cur,
        cycle_id=cycle_id,
        structural_cycle_id=scid,
        now=now,
        last_package_exit_reason="bybit_flat",
        meta={"source": "bybit_positions", "position_records": 0},
    )
    return {"ok": True, "transitioned": True, "cycle_phase": "closed", "position_records_seen": 0,
            "source": "bybit_positions", "last_package_exit_reason": "bybit_flat"}


# ---------------------------------------------------------------------------
# Flip – закрытие противоположной стороны
# ---------------------------------------------------------------------------
def _flip_close_opposite_if_needed(
    cur,
    *,
    cycle_id: str,
    incoming_direction: str,
    structural_cycle_id: Optional[str],
) -> Dict[str, Any]:
    out: Dict[str, Any] = {"ok": True, "skipped": None, "incoming": incoming_direction,
                           "actions": [], "errors": []}
    if not st.ENTRY_CLOSE_OPPOSITE_ON_FLIP_SIGNAL:
        out["skipped"] = "disabled"
        return out
    if not st.BYBIT_EXECUTION_ENABLED:
        out["skipped"] = "execution_disabled"
        return out

    opposite = "short" if incoming_direction == "long" else "long"
    rows = cur.execute(
        """
        SELECT id, symbol, side, status, qty, filled_qty,
               entry_exec_order_id, stop_exec_order_id
        FROM position_records
        WHERE cycle_id = ? AND LOWER(side) = ? AND status IN ('open', 'pending')
        """,
        (cycle_id, opposite),
    ).fetchall()
    if not rows:
        out["skipped"] = "no_opposite_legs"
        return out

    open_entry_orders: Dict[tuple[str, str], List[str]] = {}
    try:
        open_entry_orders = _load_open_entry_orders_by_symbol_and_side()
    except Exception:
        logger.exception("flip: load open orders fallback failed")

    now = int(time.time())
    for r in rows:
        rid = int(r["id"])
        sym_trade = str(r["symbol"])
        st_rec = str(r["status"])
        qty_total = float(r["qty"] or 0.0)
        fq = float(r["filled_qty"] or 0.0)

        # Отмена стоп-ордера, если есть
        if r["stop_exec_order_id"]:
            try:
                sor = cur.execute(
                    "SELECT bybit_order_id, symbol FROM exec_orders WHERE id = ?",
                    (int(r["stop_exec_order_id"]),),
                ).fetchone()
                if sor and sor["bybit_order_id"]:
                    sym_o = str(sor["symbol"] or sym_trade)
                    cancel_linear_order(symbol_trade=sym_o, order_id=str(sor["bybit_order_id"]))
                    out["actions"].append({"position_record_id": rid, "action": "cancel_stop",
                                           "order_id": str(sor["bybit_order_id"])})
            except Exception:
                logger.exception("flip: cancel stop failed position_record_id=%s", rid)
                out["errors"].append({"position_record_id": rid, "step": "cancel_stop"})
                out["ok"] = False

        # Если ордер входа ещё не исполнен (pending) – отменяем
        if st_rec == "pending" and r["entry_exec_order_id"]:
            try:
                eor = cur.execute(
                    "SELECT bybit_order_id, symbol FROM exec_orders WHERE id = ?",
                    (int(r["entry_exec_order_id"]),),
                ).fetchone()
                if eor and eor["bybit_order_id"]:
                    sym_o = str(eor["symbol"] or sym_trade)
                    cancel_linear_order(symbol_trade=sym_o, order_id=str(eor["bybit_order_id"]))
                    cur.execute(
                        "UPDATE position_records SET status = 'cancelled', updated_at = ? WHERE id = ?",
                        (now, rid),
                    )
                    out["actions"].append({"position_record_id": rid, "action": "cancel_pending_entry"})
            except Exception:
                logger.exception("flip: cancel pending entry position_record_id=%s", rid)
                out["errors"].append({"position_record_id": rid, "step": "cancel_entry"})
                out["ok"] = False
            continue

        # fallback: если нет entry_exec_order_id, ищем открытые ордера на бирже
        if st_rec == "pending":
            side_str = "buy" if opposite == "long" else "sell"
            key = (sym_trade, side_str)
            fallback_order_ids = list(open_entry_orders.get(key, []))
            if not fallback_order_ids:
                out["errors"].append({"position_record_id": rid, "step": "cancel_entry_fallback",
                                      "error": "missing_entry_exec_order_id_and_no_open_orders_found",
                                      "symbol": sym_trade})
                out["ok"] = False
                continue
            fallback_ok = True
            for oid in fallback_order_ids:
                try:
                    cancel_linear_order(symbol_trade=sym_trade, order_id=str(oid))
                    out["actions"].append({"position_record_id": rid, "action": "cancel_pending_entry_fallback",
                                           "order_id": str(oid)})
                except Exception:
                    logger.exception("flip: fallback cancel pending entry failed position_record_id=%s order_id=%s",
                                     rid, oid)
                    out["errors"].append({"position_record_id": rid, "step": "cancel_entry_fallback",
                                          "order_id": str(oid)})
                    out["ok"] = False
                    fallback_ok = False
            if fallback_ok:
                cur.execute(
                    "UPDATE position_records SET status = 'cancelled', updated_at = ? WHERE id = ?",
                    (now, rid),
                )
            continue

        # Если позиция уже открыта – закрываем рыночным ордером
        if st_rec == "open":
            qty_close = fq if fq > 0 else qty_total
            if qty_close <= 0:
                continue
            side_buy_close = opposite == "short"
            try:
                resp = place_linear_market_order(
                    symbol_trade=sym_trade,
                    side_buy=side_buy_close,
                    qty=qty_close,
                    reduce_only=True,
                )
                rc = resp.get("retCode")
                ok_rc = rc in (0, "0", None)
                if not ok_rc:
                    out["ok"] = False
                    out["errors"].append({"position_record_id": rid, "step": "reduce_market",
                                          "retCode": rc, "retMsg": resp.get("retMsg")})
                out["actions"].append({"position_record_id": rid, "action": "reduce_market",
                                       "qty": qty_close, "retCode": rc})
            except Exception as e:
                logger.exception("flip: market close position_record_id=%s", rid)
                out["errors"].append({"position_record_id": rid, "step": "reduce_market", "error": str(e)})
                out["ok"] = False

    # Проверяем, остались ли ещё открытые/ожидающие позиции противоположной стороны
    rest = cur.execute(
        """
        SELECT COUNT(*) AS c
        FROM position_records
        WHERE cycle_id = ? AND LOWER(side) = ? AND status IN ('open', 'pending')
        """,
        (cycle_id, opposite),
    ).fetchone()
    remaining_opposite = int(rest["c"] if rest else 0)
    out["remaining_opposite_open_or_pending"] = remaining_opposite

    if out["ok"] and remaining_opposite == 0:
        cur.execute(
            """
            UPDATE trading_state SET
                position_state = 'none',
                allow_long_entry = 1,
                allow_short_entry = 1,
                updated_at = ?
            WHERE id = 1
            """,
            (now,),
        )
        out["state_reset"] = True
    else:
        out["state_reset"] = False

    _log_v4_event(
        cur,
        cycle_id=cycle_id,
        structural_cycle_id=structural_cycle_id,
        event_type="v4_flip_close_opposite",
        symbol=None,
        price=None,
        meta={"incoming": incoming_direction, "opposite": opposite, "actions": out["actions"],
              "errors": out["errors"], "remaining_opposite_open_or_pending": remaining_opposite,
              "state_reset": out.get("state_reset", False)},
    )
    if st.LEVEL_CROSS_TELEGRAM and out["actions"]:
        try:
            msg = f"Flip: закрыт пакет {opposite.upper()} перед {incoming_direction.upper()} ({len(out['actions'])} действий)"
            get_telegram_notifier().send_message(f"<pre>{escape_html_telegram(msg)}</pre>", parse_mode="HTML")
        except Exception:
            logger.exception("flip: telegram failed")

    return out


# ---------------------------------------------------------------------------
# Новая логика: выставление лимитного ордера на один символ (без confirmation)
# ---------------------------------------------------------------------------
def _load_instrument(cur, symbol_bybit: str) -> Optional[Dict[str, Any]]:
    """Загружает информацию об инструменте (tick_size, min_qty, atr)."""
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


def _insert_exec_order(cur, values: Dict[str, Any]) -> int:
    """Вставляет запись в exec_orders и возвращает id."""
    cur.execute("PRAGMA table_info(exec_orders)")
    cols = {row[1] for row in cur.fetchall()}
    payload = {k: v for k, v in values.items() if k in cols}
    keys = list(payload.keys())
    placeholders = ",".join("?" for _ in keys)
    cur.execute(
        f"INSERT INTO exec_orders ({','.join(keys)}) VALUES ({placeholders})",
        tuple(payload[k] for k in keys),
    )
    return int(cur.lastrowid)


def _insert_position_record(cur, values: Dict[str, Any]) -> int:
    """Вставляет запись в position_records и возвращает id."""
    cur.execute("PRAGMA table_info(position_records)")
    cols = {row[1] for row in cur.fetchall()}
    payload = {k: v for k, v in values.items() if k in cols}
    keys = list(payload.keys())
    placeholders = ",".join("?" for _ in keys)
    cur.execute(
        f"INSERT INTO position_records ({','.join(keys)}) VALUES ({placeholders})",
        tuple(payload[k] for k in keys),
    )
    return int(cur.lastrowid)


def place_entry_order_for_symbol(
    cur,
    *,
    cycle_id: str,
    structural_cycle_id: Optional[str],
    symbol: str,
    direction: str,          # "long" или "short"
    level_price: float,
) -> Dict[str, Any]:
    """
    Выставляет лимитный ордер на вход по указанному символу и уровню.
    Возвращает словарь с результатом.
    """
    from trading_bot.analytics.position_math import compute_position_plan, plan_to_dict

    sym_trade = symbol
    sym_bybit = to_bybit_symbol(sym_trade)
    side: str = direction   # "long" или "short"

    # 1. Получить ATR и параметры инструмента
    inst = _load_instrument(cur, sym_bybit)
    if not inst:
        return {"ok": False, "error": "instrument_not_found", "symbol": sym_bybit}
    atr = inst.get("atr")
    if atr is None or float(atr) <= 0:
        return {"ok": False, "error": "instrument_atr_missing", "symbol": sym_bybit}
    tick = inst.get("tick_size")
    if tick is None or float(tick) <= 0:
        return {"ok": False, "error": "instrument_tick_size_missing", "symbol": sym_bybit}
    min_qty = inst.get("min_qty")
    if min_qty is None or float(min_qty) <= 0:
        return {"ok": False, "error": "instrument_min_qty_missing", "symbol": sym_bybit}
    step = float(min_qty)

    # 2. Рассчитать план позиции
    risk_usdt = float(st.POSITION_RISK_USDT)
    plan = compute_position_plan(
        side=side,
        base_price=level_price,                    # уровень – цена входа
        entry_price_raw=level_price,               # лимитный ордер по уровню
        atr=float(atr),
        risk_usdt=risk_usdt,
        stop_atr_mult=float(st.POSITION_STOP_ATR_MULT),
        tp1_atr_mult=float(st.POSITION_TP1_ATR_MULT),
        tp2_atr_mult=float(st.POSITION_TP2_ATR_MULT),
        tp3_atr_mult=float(st.POSITION_TP3_ATR_MULT),
        tp1_share_pct=float(st.POSITION_TP1_SHARE_PCT),
        tp2_share_pct=float(st.POSITION_TP2_SHARE_PCT),
        price_tick=float(tick),
        qty_step=step,
        entry_offset=0.0,
        entry_offset_pct=float(st.POSITION_ENTRY_OFFSET_PCT),
        use_entry_offset=True,
        tp_in_stop_ranges=True,
        min_order_qty=float(min_qty),
    )
    if plan.qty_total <= 0:
        return {"ok": False, "error": "zero_qty", "plan": plan_to_dict(plan)}

    # 3. Выставить лимитный ордер (со встроенным stopLoss)
    if not st.BYBIT_EXECUTION_ENABLED:
        return {"ok": False, "error": "execution_disabled", "plan": plan_to_dict(plan)}

    side_buy = (side == "long")
    client_oid = f"mb_{uuid.uuid4().hex[:24]}"
    try:
        order_resp = place_linear_limit_order(
            symbol_trade=sym_trade,
            side_buy=side_buy,
            qty=float(plan.qty_total),
            price=level_price,
            stop_loss=float(plan.stop_price),   # встроенный стоп-лосс
        )
    except Exception as e:
        logger.exception("place_linear_limit_order failed for %s", sym_bybit)
        return {"ok": False, "error": "limit_order_exception", "exception": str(e), "plan": plan_to_dict(plan)}

    rc = order_resp.get("retCode")
    if rc not in (0, "0", None):
        logger.error(
            "❌ ORDER REJECTED: %s %s | retCode: %s | retMsg: %s",
            sym_bybit, side.upper(), rc, order_resp.get("retMsg")
        )
        return {
            "ok": False,
            "error": "limit_order_rejected",
            "ret_code": rc,
            "ret_msg": order_resp.get("retMsg"),
            "plan": plan_to_dict(plan),
        }

    bybit_oid = None
    try:
        bybit_oid = (order_resp.get("result") or {}).get("orderId")
    except Exception:
        pass

    now = int(time.time())
    # 4. Записать в exec_orders
    exec_id = _insert_exec_order(
        cur,
        {
            "created_at": now,
            "updated_at": now,
            "cycle_id": cycle_id,
            "structural_cycle_id": structural_cycle_id,
            "position_record_id": None,
            "order_role": "entry",
            "client_order_id": client_oid,
            "bybit_order_id": str(bybit_oid) if bybit_oid else None,
            "symbol": sym_bybit,
            "side": "Buy" if side_buy else "Sell",
            "order_type": "Limit",
            "qty": float(plan.qty_total),
            "price": level_price,
            "status": "submitted",
            "exchange_status": "accepted",
            "reduce_only": 0,
            "error_message": None,
            "raw_json": json.dumps(order_resp, ensure_ascii=False)[:8000],
        },
    )

    # 5. Записать в position_records (статус pending)
    meta = {
        "plan": plan_to_dict(plan),
        "level_price": level_price,
        "instrument": {"tick_size": tick, "min_qty": min_qty, "qty_step_used": step},
        "params": {
            "risk_usdt": risk_usdt,
            "stop_atr_mult": float(st.POSITION_STOP_ATR_MULT),
            "tp1_atr_mult": float(st.POSITION_TP1_ATR_MULT),
            "tp2_atr_mult": float(st.POSITION_TP2_ATR_MULT),
            "tp3_atr_mult": float(st.POSITION_TP3_ATR_MULT),
            "tp1_share_pct": float(st.POSITION_TP1_SHARE_PCT),
            "tp2_share_pct": float(st.POSITION_TP2_SHARE_PCT),
            "entry_offset_pct": float(st.POSITION_ENTRY_OFFSET_PCT),
        },
    }
    pos_uuid = str(uuid.uuid4())
    pos_id = _insert_position_record(
        cur,
        {
            "uuid": pos_uuid,
            "created_at": now,
            "updated_at": now,
            "cycle_id": cycle_id,
            "structural_cycle_id": structural_cycle_id,
            "symbol": sym_trade,
            "side": side,
            "status": "pending",
            "qty": float(plan.qty_total),
            "entry_price": level_price,
            "entry_price_fact": None,
            "filled_qty": None,
            "entry_exec_order_id": exec_id,
            "stop_exec_order_id": None,
            "opened_at": None,
            "meta_json": json.dumps(meta, ensure_ascii=False),
        },
    )

    # Обновить exec_orders ссылкой на position_record_id
    cur.execute(
        "UPDATE exec_orders SET position_record_id = ?, updated_at = ? WHERE id = ?",
        (pos_id, now, exec_id),
    )

    # Логирование успешного размещения ордера
    logger.info(
        "✅ ORDER PLACED: %s %s | OrderID: %s | Qty: %.4f | Price: %.4f | Stop: %.4f",
        sym_bybit, side.upper(), bybit_oid, float(plan.qty_total), level_price, float(plan.stop_price)
    )

    return {
        "ok": True,
        "symbol": sym_trade,
        "position_record_id": pos_id,
        "order_id": bybit_oid,
        "qty": float(plan.qty_total),
        "price": level_price,
        "stop_price": float(plan.stop_price),
        "plan": plan_to_dict(plan),
    }


# ---------------------------------------------------------------------------
# Основная функция обработки группового сигнала (v4) с проверкой ATR-порога
# ---------------------------------------------------------------------------
def process_v4_signal(
    cur,
    *,
    signal_type: str,
    monitor: "LevelCrossMonitor",
    prices: Dict[str, float],
) -> Dict[str, Any]:
    """
    Обрабатывает групповой сигнал LONG/SHORT.
    - Flip при смене стороны
    - Для каждого символа проверяем ATR-порог (как в старой логике)
    - Если условие выполнено – выставляем лимитный ордер
    """
    # 1. Чтение состояния
    row = cur.execute(
        "SELECT cycle_id, structural_cycle_id, COALESCE(position_state, 'none') AS position_state, "
        "levels_frozen FROM trading_state WHERE id = 1"
    ).fetchone()
    if not row or not row["cycle_id"]:
        return {"ok": False, "error": "no_cycle"}
    if not int(row["levels_frozen"] or 0):
        return {"ok": False, "error": "levels_not_frozen"}

    cycle_id = str(row["cycle_id"])
    scid = row["structural_cycle_id"]
    scid = str(scid) if scid else None
    current_pos = str(row["position_state"] or "none")
    incoming_dir = "long" if signal_type == "LONG" else "short"

    # 2. Flip, если нужно
    flip_out = None
    rebuild_done = False
    if current_pos in ("long", "short") and current_pos != incoming_dir:
        flip_out = _flip_close_opposite_if_needed(
            cur, cycle_id=cycle_id, incoming_direction=incoming_dir, structural_cycle_id=scid,
        )
        if not flip_out.get("ok"):
            return {"ok": False, "error": "flip_close_failed", "flip_close": flip_out}
        # rebuild противоположной стороны (для будущего флипа)
        rebuild_ok = rebuild_side_on_cursor(cur, scid, target_direction=incoming_dir, prices=prices)
        if not rebuild_ok:
            return {"ok": False, "error": "rebuild_side_failed", "flip_close": flip_out}
        refresh_cycle_levels_from_structural(cur, scid)
        rebuild_done = True

    # 3. Загрузить список символов и уровней для данного направления
    levels_rows = cur.execute(
        """
        SELECT symbol, level_price
        FROM cycle_levels
        WHERE cycle_id = ? AND direction = ? AND level_step = 1 AND is_active = 1
        """,
        (cycle_id, incoming_dir),
    ).fetchall()
    logger.info("Loaded %d %s levels: %s", len(levels_rows), incoming_dir, [r[0] for r in levels_rows][:20])
    if not levels_rows:
        return {"ok": False, "error": f"no_{incoming_dir}_levels", "flip_close": flip_out}

    symbols_levels = [(str(r["symbol"]), float(r["level_price"])) for r in levels_rows]

    # 4. Параметры ATR-порога (как в старой логике)
    long_pct = float(st.ENTRY_GATE_LONG_ATR_THRESHOLD_PCT)
    short_pct = float(st.ENTRY_GATE_SHORT_ATR_THRESHOLD_PCT)

    entered = []
    rejected = []

    for symbol, level_price in symbols_levels:
        # Получить текущую цену
        current_price = prices.get(symbol)
        if current_price is None:
            rejected.append(symbol)
            continue

        # Получить ATR
        atr = _get_atr(cur, symbol)
        if atr is None or atr <= 0:
            rejected.append(symbol)
            continue
        logger.debug("Checking %s: price=%s level=%s atr=%s", symbol, current_price, level_price, atr)
        # Проверка условия (как в старой entry_gate)
        if signal_type == "LONG":
            threshold = long_pct / 100.0 * atr
            condition_met = float(current_price) >= level_price - threshold
            logger.debug("EntryGate LONG %s price=%s level=%s atr=%s thr=%s ok=%s",
                         symbol, current_price, level_price, atr, threshold, condition_met)
        else:  # SHORT
            threshold = short_pct / 100.0 * atr
            condition_met = float(current_price) < level_price - threshold
            logger.debug("EntryGate SHORT %s price=%s level=%s atr=%s thr=%s ok=%s",
                         symbol, current_price, level_price, atr, threshold, condition_met)

        if condition_met:
            # Выставляем лимитный ордер
            result = place_entry_order_for_symbol(
                cur,
                cycle_id=cycle_id,
                structural_cycle_id=scid,
                symbol=symbol,
                direction=incoming_dir,
                level_price=level_price,
            )
            if result.get("ok"):
                entered.append(symbol)
                logger.info("Limit order placed for %s %s at %s", symbol, incoming_dir, level_price)
            else:
                rejected.append(symbol)
                logger.warning("Failed to place order for %s %s: %s", symbol, incoming_dir, result.get("error"))
        else:
            rejected.append(symbol)

    # 5. Обновить trading_state, если есть входы
    now = int(time.time())
    if entered:
        cur.execute(
            """
            UPDATE trading_state
            SET position_state = ?, cycle_phase = 'in_position',
                allow_long_entry = ?, allow_short_entry = ?,
                updated_at = ?
            WHERE id = 1
            """,
            (incoming_dir, 1 if incoming_dir == "short" else 0, 1 if incoming_dir == "long" else 0, now),
        )
        _log_v4_event(
            cur,
            cycle_id=cycle_id,
            structural_cycle_id=scid,
            event_type=f"v4_entry_{incoming_dir}",
            symbol=None,
            price=None,
            meta={"entered": entered, "rejected": rejected, "flip_done": rebuild_done},
        )
    else:
        # Если ни один ордер не прошёл условие – не меняем состояние
        logger.warning("No symbols passed entry condition for %s signal", signal_type)
        # Но всё равно логируем событие
        _log_v4_event(
            cur,
            cycle_id=cycle_id,
            structural_cycle_id=scid,
            event_type=f"v4_entry_{incoming_dir}_no_candidates",
            symbol=None,
            price=None,
            meta={"entered": entered, "rejected": rejected, "flip_done": rebuild_done},
        )

    # 6. Telegram уведомление о входе в позицию
    if st.LEVEL_CROSS_TELEGRAM and (entered or rejected):
        msg = f"🟢 СИГНАЛ {signal_type} ОБРАБОТАН:\n"
        
        if entered:
            entered_list = ', '.join(entered[:5])
            if len(entered) > 5:
                entered_list += f" и ещё {len(entered)-5}"
            msg += f"✅ Прошли проверку ({len(entered)}): {entered_list}\n"
        
        if rejected:
            rejected_list = ', '.join(rejected[:5])
            if len(rejected) > 5:
                rejected_list += f" и ещё {len(rejected)-5}"
            msg += f"❌ Не прошли ATR-фильтр ({len(rejected)}): {rejected_list}"
        
        get_telegram_notifier().send_message(f"<pre>{escape_html_telegram(msg)}</pre>", parse_mode="HTML")

    # 7. Telegram уведомление об исполнении ордеров (открытие позиций)
    if st.LEVEL_CROSS_TELEGRAM and entered:
        try:
            # Через 1-2 минуты проверить исполнены ли ордера и отправить уведомление
            # Это делается в reconcile, но можно добавить отдельный тик
            pass  # Уведомление будет при закрытии позиции с причиной
        except Exception:
            logger.exception("position open telegram failed")

    # 8. Выгрузка в Google Sheets (сразу после входа)
    try:
        from trading_bot.data.trading_cycle_sheets import export_open_orders_to_sheets, sync_trading_positions_and_stats_to_sheets
        export_open_orders_to_sheets(cur)
        sync_trading_positions_and_stats_to_sheets(cur)
    except Exception as e:
        logger.warning("Sheets export after entry failed: %s", e)

    # 8. Результат
    side_counts = _load_cycle_side_counts(cur, cycle_id=cycle_id)
    return {
        "ok": True,
        "signal": signal_type,
        "entered": entered,
        "rejected": rejected,
        "flip_close": flip_out,
        "cycle_level_side_counts": side_counts,
    }


# ---------------------------------------------------------------------------
# Сохранение старого имени для обратной совместимости
# ---------------------------------------------------------------------------
process_v3_signal = process_v4_signal

__all__ = [
    "process_v3_signal",
    "process_v4_signal",
    "close_trading_epoch_v4_cancel",
    "maybe_transition_arming_after_package_all_flat",
]