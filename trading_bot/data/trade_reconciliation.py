from __future__ import annotations

import json
import logging
import time
from typing import Any, Dict, List, Optional

from trading_bot.config import settings as st
from trading_bot.data.bybit_close_reason import (
    classify_bybit_execution_close_reason,
    parse_fill_raw_json,
    resolve_close_reason,
)
from trading_bot.tools import bybit_trading as bt

logger = logging.getLogger(__name__)


def _safe_num(v: Any) -> float:
    try:
        return float(v)
    except Exception:
        return 0.0


def _fetch_exec_orders_to_reconcile(cur, *, lookback_hours: int) -> List[Dict[str, Any]]:
    """
    Ордера с незавершённой сверкой по времени + все ордера, привязанные к open/pending позициям
    (чтобы поймать fill стопа/выхода после перехода exec_orders в filled).
    """
    cutoff = int(time.time()) - int(max(1, lookback_hours)) * 3600
    rows = cur.execute(
        """
        SELECT DISTINCT e.id, e.bybit_order_id, e.symbol, e.side, e.status,
               e.cycle_id, e.structural_cycle_id, e.position_record_id, e.order_role
        FROM exec_orders e
        WHERE e.bybit_order_id IS NOT NULL
          AND e.bybit_order_id != ''
          AND (
            (
              e.created_at >= ?
              AND e.status IN ('submitted', 'partially_filled', 'open')
            )
            OR e.id IN (
              SELECT entry_exec_order_id FROM position_records
              WHERE status IN ('pending', 'open') AND entry_exec_order_id IS NOT NULL
              UNION
              SELECT stop_exec_order_id FROM position_records
              WHERE status IN ('pending', 'open') AND stop_exec_order_id IS NOT NULL
              UNION
              SELECT exit_exec_order_id FROM position_records
              WHERE status IN ('pending', 'open') AND exit_exec_order_id IS NOT NULL
            )
          )
        ORDER BY e.created_at DESC
        """,
        (cutoff,),
    ).fetchall()
    return [dict(r) for r in rows]


def _upsert_fill(cur, *, order: Dict[str, Any], ex: Dict[str, Any]) -> None:
    trade_id = str(ex.get("execId") or ex.get("tradeId") or "")
    if not trade_id:
        return
    ts_ms = ex.get("execTime") or ex.get("createdTime") or ex.get("updatedTime")
    if ts_ms is None:
        ts = int(time.time())
    else:
        ts = int(_safe_num(ts_ms) / 1000.0)
    cur.execute(
        """
        INSERT INTO exec_fills (
            exec_order_id, position_record_id, cycle_id, structural_cycle_id,
            symbol, side, trade_id, fill_price, fill_qty, fee, fee_currency, ts, raw_json
        )
        SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        WHERE NOT EXISTS (
            SELECT 1 FROM exec_fills WHERE exec_order_id = ? AND trade_id = ?
        )
        """,
        (
            int(order["id"]),
            order.get("position_record_id"),
            order.get("cycle_id"),
            order.get("structural_cycle_id"),
            str(order.get("symbol") or ""),
            str(order.get("side") or ""),
            trade_id,
            _safe_num(ex.get("execPrice") or ex.get("price")),
            _safe_num(ex.get("execQty") or ex.get("qty")),
            _safe_num(ex.get("execFee") or ex.get("fee")),
            str(ex.get("feeCurrency") or ex.get("feeCoin") or ""),
            ts,
            json.dumps(ex, ensure_ascii=False)[:8000],
            int(order["id"]),
            trade_id,
        ),
    )


def _refresh_exec_order_from_fills(cur, *, exec_order_id: int) -> Dict[str, Any]:
    row = cur.execute(
        """
        SELECT
            COALESCE(SUM(fill_qty), 0) AS filled_qty,
            CASE WHEN COALESCE(SUM(fill_qty), 0) > 0
                 THEN SUM(fill_price * fill_qty) / SUM(fill_qty)
                 ELSE NULL END AS avg_fill_price
        FROM exec_fills
        WHERE exec_order_id = ?
        """,
        (exec_order_id,),
    ).fetchone()
    filled_qty = float(row["filled_qty"] or 0.0) if row else 0.0
    avg_fill_price = float(row["avg_fill_price"]) if row and row["avg_fill_price"] is not None else None
    order = cur.execute(
        "SELECT qty FROM exec_orders WHERE id = ?",
        (exec_order_id,),
    ).fetchone()
    qty = float(order["qty"] or 0.0) if order else 0.0
    if qty > 0 and filled_qty >= qty * 0.999:
        status = "filled"
    elif filled_qty > 0:
        status = "partially_filled"
    else:
        status = "submitted"
    cur.execute(
        """
        UPDATE exec_orders
        SET filled_qty = ?, avg_fill_price = ?, status = ?, exchange_status = ?, updated_at = ?
        WHERE id = ?
        """,
        (filled_qty, avg_fill_price, status, status, int(time.time()), exec_order_id),
    )
    return {"status": status, "filled_qty": filled_qty, "avg_fill_price": avg_fill_price}


def _latest_fill_raw_as_execution(cur, *, exec_order_id: int) -> Optional[Dict[str, Any]]:
    row = cur.execute(
        """
        SELECT raw_json FROM exec_fills
        WHERE exec_order_id = ?
        ORDER BY ts DESC, id DESC
        LIMIT 1
        """,
        (exec_order_id,),
    ).fetchone()
    if not row:
        return None
    return parse_fill_raw_json(row["raw_json"])


def _close_reason_for_exec_order(cur, *, exec_order_id: int) -> str:
    bybit_ex = _latest_fill_raw_as_execution(cur, exec_order_id=exec_order_id)
    lor = cur.execute(
        """
        SELECT order_role, order_type, reduce_only
        FROM exec_orders WHERE id = ?
        """,
        (exec_order_id,),
    ).fetchone()
    local = dict(lor) if lor else None
    return resolve_close_reason(bybit_ex=bybit_ex, local_order_row=local)


def _order_fill_snapshot(
    cur, *, exec_order_id: int
) -> tuple[str, float, Optional[float]]:
    r = cur.execute(
        """
        SELECT status, filled_qty, avg_fill_price
        FROM exec_orders WHERE id = ?
        """,
        (exec_order_id,),
    ).fetchone()
    if not r:
        return "", 0.0, None
    st_o = str(r["status"] or "")
    fq = float(r["filled_qty"] or 0.0)
    ap = float(r["avg_fill_price"]) if r["avg_fill_price"] is not None else None
    return st_o, fq, ap


def _refresh_position_from_orders(cur, *, position_record_id: int) -> None:
    pos = cur.execute(
        """
        SELECT id, status, qty, entry_exec_order_id, exit_exec_order_id, stop_exec_order_id
        FROM position_records
        WHERE id = ?
        """,
        (position_record_id,),
    ).fetchone()
    if not pos:
        return
    now = int(time.time())
    pr_id = int(position_record_id)

    if pos["entry_exec_order_id"]:
        e = cur.execute(
            "SELECT status, filled_qty, avg_fill_price FROM exec_orders WHERE id = ?",
            (int(pos["entry_exec_order_id"]),),
        ).fetchone()
        if e:
            e_status = str(e["status"] or "")
            e_filled = float(e["filled_qty"] or 0.0)
            e_price = float(e["avg_fill_price"]) if e["avg_fill_price"] is not None else None
            if e_status in ("filled", "partially_filled") and e_filled > 0:
                    # Проверяем была ли позиция ранее 'pending' (новый вход)
                    was_pending_row = cur.execute("""
                        SELECT status FROM position_records WHERE id = ?
                    """, (pr_id,)).fetchone()
                    was_pending = was_pending_row and str(was_pending_row["status"]) == "pending"
                    
                    cur.execute(
                        """
                        UPDATE position_records
                        SET status = CASE WHEN status = 'pending' THEN 'open' ELSE status END,
                            filled_qty = ?,
                            entry_price_fact = COALESCE(entry_price_fact, ?),
                            opened_at = COALESCE(opened_at, ?),
                            updated_at = ?
                        WHERE id = ?
                        """,
                        (e_filled, e_price, now, now, pr_id),
                    )

                    # Telegram уведомление об открытии позиции (только если была 'pending')
                    if was_pending:
                        logger.info("Position %s changed from pending to open - sending Telegram notification", pr_id)
                        if st.LEVEL_CROSS_TELEGRAM:
                            try:
                                from trading_bot.tools.telegram_notify import escape_html_telegram, get_telegram_notifier
                                
                                pos_info = cur.execute("""
                                    SELECT symbol, side, qty, entry_price_fact
                                    FROM position_records WHERE id = ?
                                """, (pr_id,)).fetchone()
                                
                                if pos_info:
                                    symbol = str(pos_info['symbol'])
                                    side = str(pos_info['side']).upper()
                                    qty = float(pos_info['qty'] or 0)
                                    entry_price = pos_info['entry_price_fact']
                                    
                                    msg = (
                                        f"🟢 ОТКРЫТИЕ позиции: {symbol} {side}\n"
                                        f"Цена: {entry_price:.4f}, Кол-во: {qty}"
                                    )
                                    logger.info("Sending Telegram message: %s", msg)
                                    get_telegram_notifier().send_message(f"<pre>{escape_html_telegram(msg)}</pre>", parse_mode="HTML")
                                    logger.info("Telegram: Position open notification sent for %s %s", symbol, side)
                                else:
                                    logger.error("Could not fetch position info for %s", pr_id)
                            except Exception:
                                logger.exception("position open telegram failed")
                        else:
                            logger.info("LEVEL_CROSS_TELEGRAM is disabled, skipping notification")

    pos2 = cur.execute(
        """
        SELECT status, qty, exit_exec_order_id, stop_exec_order_id
        FROM position_records WHERE id = ?
        """,
        (pr_id,),
    ).fetchone()
    if not pos2 or str(pos2["status"] or "") == "closed":
        return

    qty = float(pos2["qty"] or 0.0)

    def _meaningful_exit(st_o: str, fq: float) -> bool:
        if st_o not in ("filled", "partially_filled") or fq <= 0:
            return False
        if qty <= 0:
            return True
        return fq >= qty * 0.999

    closing_oid: Optional[int] = None
    exit_px: Optional[float] = None

    ex_id = pos2["exit_exec_order_id"]
    if ex_id:
        st_o, fq, ap = _order_fill_snapshot(cur, exec_order_id=int(ex_id))
        if _meaningful_exit(st_o, fq):
            closing_oid = int(ex_id)
            exit_px = ap

    if closing_oid is None and pos2["stop_exec_order_id"]:
        st_o, fq, ap = _order_fill_snapshot(cur, exec_order_id=int(pos2["stop_exec_order_id"]))
        if _meaningful_exit(st_o, fq):
            closing_oid = int(pos2["stop_exec_order_id"])
            exit_px = ap

    if closing_oid is None:
        return

    reason = _close_reason_for_exec_order(cur, exec_order_id=closing_oid)
    cur.execute(
        """
        UPDATE position_records
        SET status = 'closed',
            exit_price_fact = COALESCE(exit_price_fact, ?),
            closed_at = COALESCE(closed_at, ?),
            close_reason = ?,
            updated_at = ?
        WHERE id = ? AND status != 'closed'
        """,
        (exit_px, now, reason, now, pr_id),
    )

    # Telegram уведомление о закрытии позиции
    logger.info("Position %s closed - checking Telegram notification", pr_id)
    if st.LEVEL_CROSS_TELEGRAM:
        try:
            from trading_bot.tools.telegram_notify import escape_html_telegram, get_telegram_notifier
            
            pos_info = cur.execute("""
                SELECT symbol, side, qty, entry_price_fact, exit_price_fact, close_reason
                FROM position_records WHERE id = ?
            """, (pr_id,)).fetchone()
            
            if pos_info:
                symbol = str(pos_info['symbol'])
                side = str(pos_info['side']).upper()
                qty = float(pos_info['qty'] or 0)
                entry_price = pos_info['entry_price_fact']
                exit_price = pos_info['exit_price_fact']
                raw_reason = str(pos_info['close_reason'] or 'unknown')
                
                logger.info("Closing position: %s %s, reason: %s", symbol, side, raw_reason)
                
                # Маппинг причин закрытия на читаемые сообщения
                reason_map = {
                    'take_profit': '✅ TAKE PROFIT',
                    'tp1': '✅ TAKE PROFIT 1',
                    'tp2': '✅ TAKE PROFIT 2',
                    'tp3': '✅ TAKE PROFIT 3',
                    'stop_loss': '❌ STOP LOSS',
                    'flip_long': '🔄 FLIP (LONG → SHORT)',
                    'flip_short': '🔄 FLIP (SHORT → LONG)',
                    'flip': '🔄 FLIP (переворот рынка)',
                    'timeout': '⏰ TIMEOUT (истекло время)',
                    'manual_close': '👤 РУЧНОЕ ЗАКРЫТИЕ',
                    'exchange_close': '🏢 БИРЖА (техническое)',
                    'reduce_only': '📉 REDUCE_ONLY',
                }
                
                # Поиск подходящей причины (частичное совпадение)
                display_reason = raw_reason.upper()
                for key, display in reason_map.items():
                    if key.lower() in raw_reason.lower():
                        display_reason = display
                        break
                
                # Расчёт PnL
                pnl_pct = None
                pnl_usdt = None
                if entry_price and exit_price:
                    if side == 'SELL':  # SHORT
                        pnl_pct = (entry_price - exit_price) / entry_price * 100
                    else:  # LONG
                        pnl_pct = (exit_price - entry_price) / entry_price * 100
                
                    # PnL в USDT (примерно)
                    pnl_usdt = pnl_pct * qty * entry_price / 100
                
                pnl_str = f"{pnl_pct:+.2f}%" if pnl_pct is not None else "?"
                pnl_usdt_str = f" (~{pnl_usdt:+.2f} USDT)" if pnl_usdt is not None else ""
                
                # Иконка результата
                result_icon = "✅" if pnl_pct and pnl_pct > 0 else "❌" if pnl_pct and pnl_pct < 0 else "⚪"
                
                msg = (
                    f"{result_icon} 🔴 ЗАКРЫТИЕ позиции: {symbol} {side}\n"
                    f"Цена: {entry_price:.4f} → {exit_price:.4f}\n"
                    f"PnL: {pnl_str}{pnl_usdt_str}\n"
                    f"Причина: {display_reason}"
                )
                logger.info("Sending Telegram close message: %s", msg)
                get_telegram_notifier().send_message(f"<pre>{escape_html_telegram(msg)}</pre>", parse_mode="HTML")
                logger.info("Telegram: Position close notification sent for %s %s", symbol, side)
            else:
                logger.error("Could not fetch position info for %s", pr_id)
        except Exception:
            logger.exception("position close telegram failed")
    else:
        logger.info("LEVEL_CROSS_TELEGRAM is disabled, skipping close notification")


def reconcile_recent_exec_orders(cur, *, lookback_hours: int = 24) -> Dict[str, Any]:
    """
    Сверка локальных exec_orders с фактическими сделками Bybit (fills).
    Безопасный no-op, если execution/ключи выключены.
    """
    if not st.BYBIT_EXECUTION_ENABLED:
        return {"ok": True, "skipped": "execution_disabled"}
    try:
        _ = bt._session()
    except Exception:
        return {"ok": False, "error": "bybit_session_unavailable"}

    orders = _fetch_exec_orders_to_reconcile(cur, lookback_hours=lookback_hours)
    if not orders:
        return {"ok": True, "orders_checked": 0, "fills_upserted": 0}

    fills_upserted = 0
    checked = 0
    for o in orders:
        checked += 1
        symbol_trade = str(o.get("symbol") or "")
        symbol_trade = symbol_trade if "/" in symbol_trade else symbol_trade.replace("USDT", "/USDT")
        bybit_oid = str(o.get("bybit_order_id") or "")
        try:
            ex_resp = bt._session().get_executions(
                category="linear",
                symbol=bt.to_bybit_symbol(symbol_trade),
                orderId=bybit_oid,
                limit=50,
            )
            ex_list = ((ex_resp or {}).get("result") or {}).get("list") or []
        except Exception:
            logger.exception("reconcile executions failed for order_id=%s", bybit_oid)
            continue
        before = cur.execute(
            "SELECT COUNT(*) AS c FROM exec_fills WHERE exec_order_id = ?",
            (int(o["id"]),),
        ).fetchone()
        before_n = int(before["c"] or 0) if before else 0
        for ex in ex_list:
            _upsert_fill(cur, order=o, ex=ex)
        after = cur.execute(
            "SELECT COUNT(*) AS c FROM exec_fills WHERE exec_order_id = ?",
            (int(o["id"]),),
        ).fetchone()
        after_n = int(after["c"] or 0) if after else 0
        fills_upserted += max(0, after_n - before_n)
        _refresh_exec_order_from_fills(cur, exec_order_id=int(o["id"]))
        pid = o.get("position_record_id")
        if pid:
            _refresh_position_from_orders(cur, position_record_id=int(pid))

    return {
        "ok": True,
        "orders_checked": checked,
        "fills_upserted": fills_upserted,
    }


__all__ = [
    "reconcile_recent_exec_orders",
    "classify_bybit_execution_close_reason",
]
