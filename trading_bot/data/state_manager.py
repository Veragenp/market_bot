"""
State Manager - управление торговым состоянием и детерминированный старт.

Отвечает за:
- Чтение/запись trading_state
- Определение режима старта (determine_start_mode)
- Обработку режимов старта
- Session tracking
"""

from __future__ import annotations

import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

from trading_bot.data.db import get_connection
from trading_bot.tools.bybit_trading import (
    get_linear_positions,
    get_linear_open_orders,
    linear_position_sizes_by_symbol,
    pool_symbols_flat_on_linear_exchange,
    to_bybit_symbol,
)

logger = None  # Импортируется при необходимости


def get_trading_state() -> Dict[str, Any]:
    """Получить текущее состояние trading_state."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        row = cur.execute("SELECT * FROM trading_state WHERE id = 1").fetchone()
        if not row:
            return {}
        return dict(row)
    finally:
        conn.close()


def update_trading_state(**kwargs) -> None:
    """Обновить поля trading_state."""
    if not kwargs:
        return
    
    conn = get_connection()
    try:
        cur = conn.cursor()
        set_clause = ", ".join(f"{k} = ?" for k in kwargs.keys())
        values = list(kwargs.values())
        values.append(1)  # WHERE id = 1
        
        cur.execute(f"UPDATE trading_state SET {set_clause}, updated_at = ? WHERE id = ?", 
                    (*values, int(time.time())))
        conn.commit()
    finally:
        conn.close()


def set_session_id() -> str:
    """Сгенерировать и сохранить новый session_id."""
    session_id = str(uuid.uuid4())
    now = int(time.time())
    update_trading_state(
        last_session_id=session_id,
        last_start_ts=now,
    )
    return session_id


def get_structural_cycle_symbols(structural_cycle_id: str) -> List[str]:
    """Получить список символов из structural_cycle."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        rows = cur.execute(
            "SELECT DISTINCT symbol FROM structural_cycle_symbols WHERE cycle_id = ?",
            (structural_cycle_id,)
        ).fetchall()
        return [str(r["symbol"]) for r in rows]
    finally:
        conn.close()


def get_exchange_positions(symbols: List[str]) -> Dict[str, float]:
    """
    Получить позиции с биржи по символам.
    Возвращает dict: {symbol_bybit: size}
    """
    if not symbols:
        return {}
    
    pos_resp = get_linear_positions()
    if pos_resp is None:
        return {}
    
    sizes = linear_position_sizes_by_symbol(pos_resp)
    
    # Фильтруем только нужные символы
    result = {}
    for sym in symbols:
        bybit_sym = to_bybit_symbol(sym)
        if bybit_sym in sizes:
            result[bybit_sym] = sizes[bybit_sym]
    
    return result


def get_db_positions(cycle_id: str, status_filter: List[str] = None) -> List[Dict[str, Any]]:
    """
    Получить позиции из БД для цикла.
    По умолчанию: только open/pending
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        
        if status_filter:
            placeholders = ", ".join("?" for _ in status_filter)
            rows = cur.execute(
                f"""
                SELECT * FROM position_records
                WHERE cycle_id = ? AND status IN ({placeholders})
                """,
                (cycle_id, *status_filter)
            ).fetchall()
        else:
            rows = cur.execute(
                """
                SELECT * FROM position_records
                WHERE cycle_id = ? AND status IN ('open', 'pending')
                """,
                (cycle_id,)
            ).fetchall()
        
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_exchange_open_orders(symbols: List[str]) -> List[Dict[str, Any]]:
    """Получить открытые ордера с биржи по символам."""
    if not symbols:
        return []
    
    oo_resp = get_linear_open_orders()
    if oo_resp is None:
        return []
    
    rows = ((oo_resp.get("result") or {}).get("list") or []) if isinstance(oo_resp, dict) else []
    bybit_pool = {to_bybit_symbol(s) for s in symbols}
    
    result = []
    for row in rows:
        sym = str(row.get("symbol") or "").upper()
        if sym in bybit_pool:
            result.append(dict(row))
    
    return result


def positions_match(exchange_positions: Dict[str, float], 
                    db_positions: List[Dict[str, Any]]) -> bool:
    """
    Проверить совпадение позиций на бирже и в БД.
    Сравниваем по symbol + side + size (приблизительно)
    """
    if not exchange_positions and not db_positions:
        return True
    
    if len(exchange_positions) != len(db_positions):
        return False
    
    # Собираем БД позиции по symbol
    db_by_symbol = {}
    for pos in db_positions:
        sym = to_bybit_symbol(pos.get("symbol", ""))
        side = pos.get("side", "")
        qty = float(pos.get("qty", 0))
        key = (sym, side)
        db_by_symbol[key] = qty
    
    # Сравниваем
    for sym_bybit, size in exchange_positions.items():
        if abs(size) < 1e-12:
            continue
        
        side = "Buy" if size > 0 else "Sell"
        key = (sym_bybit, side)
        
        if key not in db_by_symbol:
            return False
        
        db_qty = db_by_symbol[key]
        if abs(db_qty - abs(size)) > 1e-6:  # Допуск на округление
            return False
    
    return True


def determine_start_mode() -> Tuple[str, str, Optional[Dict[str, Any]]]:
    """
    Детерминированное определение режима старта.
    
    Возвращает: (mode, session_id, details)
    
    Моды:
    - FRESH_START: нет цикла или нет позиций нигде
    - RECOVERY_ADD_MISSING: позиции на бирже, нет в БД
    - CLEAN_STALE_POSITIONS: позиции в БД, нет на бирже
    - RECOVERY_CONTINUE: позиции совпадают
    - RECOVERY_SYNC_MISMATCH: рассинхрон (разные size/side)
    """
    state = get_trading_state()
    session_id = str(uuid.uuid4())
    
    # 1. Если cycle_id отсутствует -> всегда FRESH_START
    cycle_id = state.get("cycle_id")
    if not cycle_id:
        return "FRESH_START", session_id, None
    
    # 2. Получаем список символов цикла
    structural_id = state.get("structural_cycle_id") or cycle_id
    symbols = get_structural_cycle_symbols(structural_id)
    
    if not symbols:
        return "FRESH_START", session_id, {"reason": "no_cycle_symbols"}
    
    # 3. Запрашиваем позиции с биржи
    exchange_positions = get_exchange_positions(symbols)
    exchange_positions = {k: v for k, v in exchange_positions.items() if abs(v) > 1e-12}
    
    # 4. Запрашиваем позиции в БД
    db_positions = get_db_positions(cycle_id, ["open", "pending"])
    
    # 5. Принимаем решение
    has_exchange_pos = len(exchange_positions) > 0
    has_db_pos = len(db_positions) > 0
    
    if not has_exchange_pos and not has_db_pos:
        # Нет позиций нигде -> FRESH_START
        return "FRESH_START", session_id, {
            "reason": "no_positions_anywhere",
            "cycle_id": cycle_id[:8],
        }
    
    elif has_exchange_pos and not has_db_pos:
        # На бирже есть, в БД нет -> RECOVERY_ADD_MISSING
        return "RECOVERY_ADD_MISSING", session_id, {
            "reason": "exchange_has_positions_db_empty",
            "exchange_positions": exchange_positions,
            "cycle_id": cycle_id[:8],
        }
    
    elif not has_exchange_pos and has_db_pos:
        # В БД есть, на бирже нет -> CLEAN_STALE_POSITIONS
        return "CLEAN_STALE_POSITIONS", session_id, {
            "reason": "db_has_positions_exchange_flat",
            "db_positions": db_positions,
            "cycle_id": cycle_id[:8],
        }
    
    else:
        # И там, и там есть -> проверяем совпадение
        if positions_match(exchange_positions, db_positions):
            return "RECOVERY_CONTINUE", session_id, {
                "reason": "positions_match",
                "positions_count": len(db_positions),
                "cycle_id": cycle_id[:8],
            }
        else:
            return "RECOVERY_SYNC_MISMATCH", session_id, {
                "reason": "positions_dont_match",
                "exchange_positions": exchange_positions,
                "db_positions": db_positions,
                "cycle_id": cycle_id[:8],
            }


def handle_fresh_start() -> Dict[str, Any]:
    """
    Обработчик FRESH_START:
    - Установить session_id
    - Закрыть старый цикл (если есть)
    - Установить фазу 'arming'
    """
    session_id = set_session_id()
    now = int(time.time())
    
    conn = get_connection()
    try:
        cur = conn.cursor()
        
        # Закрыть старый цикл
        cur.execute(
            """
            UPDATE trading_state
            SET 
                cycle_phase = 'arming',
                levels_frozen = 0,
                cycle_id = NULL,
                structural_cycle_id = NULL,
                position_state = 'none',
                close_reason = 'fresh_start',
                channel_mode = 'two_sided',
                known_side = 'both',
                need_rebuild_opposite = 0,
                opposite_rebuild_deadline_ts = NULL,
                opposite_rebuild_attempts = 0,
                opposite_rebuild_in_progress = 0,
                allow_long_entry = 1,
                allow_short_entry = 1,
                last_rebuild_reason = 'fresh_start',
                last_start_mode = 'fresh',
                last_transition_at = ?,
                updated_at = ?
            WHERE id = 1
            """,
            (now, now)
        )
        
        # Пометить старые позиции как cancelled
        cur.execute(
            """
            UPDATE position_records
            SET status = 'cancelled',
                close_reason = 'fresh_start',
                updated_at = ?
            WHERE status IN ('pending', 'open')
            """,
            (now,)
        )
        
        # Пометить старые ордера как cancelled
        cur.execute(
            """
            UPDATE exec_orders
            SET status = 'cancelled',
                updated_at = ?
            WHERE lower(COALESCE(status, '')) NOT IN 
                ('filled', 'cancelled', 'canceled', 'rejected', 'closed', 'failed', 'expired')
            """,
            (now,)
        )
        
        conn.commit()
        
        return {
            "ok": True,
            "mode": "FRESH_START",
            "session_id": session_id,
        }
    finally:
        conn.close()


def handle_recovery_add_missing(details: Dict[str, Any]) -> Dict[str, Any]:
    """
    Обработчик RECOVERY_ADD_MISSING:
    - Добавить недостающие позиции в БД
    - Установить фазу 'in_position'
    """
    session_id = set_session_id()
    exchange_positions = details.get("exchange_positions", {})
    
    now = int(time.time())
    conn = get_connection()
    try:
        cur = conn.cursor()
        
        # Получаем cycle_id и structural_id
        row = cur.execute("SELECT cycle_id, structural_cycle_id FROM trading_state WHERE id=1").fetchone()
        cycle_id = row["cycle_id"]
        structural_id = row["structural_cycle_id"] or cycle_id
        
        # Добавляем позиции в БД
        for sym_bybit, size in exchange_positions.items():
            if abs(size) < 1e-12:
                continue
            
            side = "long" if size > 0 else "short"
            
            # Находим symbol в trading format
            symbol_trade = sym_bybit  # Пока оставляем как есть
            
            cur.execute(
                """
                INSERT INTO position_records (
                    uuid, created_at, updated_at, cycle_id, structural_cycle_id,
                    symbol, side, status, qty, entry_price,
                    exchange_position_id, last_sync_ts, sync_status,
                    entry_price_fact, filled_qty, opened_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(uuid.uuid4()),
                    now, now,
                    cycle_id, structural_id,
                    symbol_trade, side, "open",
                    abs(size), None,  # entry_price неизвестен
                    sym_bybit,  # exchange_position_id
                    now, "synced",
                    None, abs(size), now
                )
            )
        
        # Обновляем фазу
        cur.execute(
            """
            UPDATE trading_state
            SET
                cycle_phase = 'in_position',
                levels_frozen = 1,
                position_state = 'in_position',
                last_session_id = ?,
                last_start_ts = ?,
                last_start_mode = 'recovery_add_missing',
                updated_at = ?
            WHERE id = 1
            """,
            (session_id, now, now)
        )
        
        conn.commit()
        
        return {
            "ok": True,
            "mode": "RECOVERY_ADD_MISSING",
            "session_id": session_id,
            "positions_added": len(exchange_positions),
        }
    finally:
        conn.close()


def handle_clean_stale_positions(details: Dict[str, Any]) -> Dict[str, Any]:
    """
    Обработчик CLEAN_STALE_POSITIONS:
    - Закрыть "зависшие" позиции в БД
    - Сбросить цикл
    - Начать новый FRESH_START
    """
    session_id = set_session_id()
    db_positions = details.get("db_positions", [])
    
    now = int(time.time())
    conn = get_connection()
    try:
        cur = conn.cursor()
        
        # Помечаем позиции как closed
        for pos in db_positions:
            pos_id = pos.get("id")
            cur.execute(
                """
                UPDATE position_records
                SET status = 'closed',
                    close_reason = 'stale_position',
                    closed_at = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (now, now, pos_id)
            )
        
        # Сбрасываем цикл (аналог fresh_start)
        cur.execute(
            """
            UPDATE trading_state
            SET
                cycle_phase = 'arming',
                levels_frozen = 0,
                cycle_id = NULL,
                structural_cycle_id = NULL,
                position_state = 'none',
                close_reason = 'stale_positions_cleaned',
                channel_mode = 'two_sided',
                known_side = 'both',
                need_rebuild_opposite = 0,
                opposite_rebuild_deadline_ts = NULL,
                opposite_rebuild_attempts = 0,
                opposite_rebuild_in_progress = 0,
                allow_long_entry = 1,
                allow_short_entry = 1,
                last_rebuild_reason = 'stale_positions_cleaned',
                last_session_id = ?,
                last_start_ts = ?,
                last_start_mode = 'recovery_clean_stale',
                last_transition_at = ?,
                updated_at = ?
            WHERE id = 1
            """,
            (session_id, now, now, now)
        )
        
        conn.commit()
        
        return {
            "ok": True,
            "mode": "CLEAN_STALE_POSITIONS",
            "session_id": session_id,
            "positions_closed": len(db_positions),
        }
    finally:
        conn.close()


def handle_recovery_continue(details: Dict[str, Any]) -> Dict[str, Any]:
    """
    Обработчик RECOVERY_CONTINUE:
    - Синхронизировать last_sync_ts
    - Продолжить работу без изменений
    """
    session_id = set_session_id()
    now = int(time.time())
    
    conn = get_connection()
    try:
        cur = conn.cursor()
        
        # Получаем cycle_id
        row = cur.execute("SELECT cycle_id FROM trading_state WHERE id=1").fetchone()
        cycle_id = row["cycle_id"]
        
        # Синхронизируем позиции
        cur.execute(
            """
            UPDATE position_records
            SET last_sync_ts = ?, sync_status = 'synced'
            WHERE cycle_id = ? AND status IN ('open', 'pending')
            """,
            (now, cycle_id)
        )
        
        # Обновляем session
        cur.execute(
            """
            UPDATE trading_state
            SET
                last_session_id = ?,
                last_start_ts = ?,
                last_start_mode = 'recovery_continue',
                updated_at = ?
            WHERE id = 1
            """,
            (session_id, now, now)
        )
        
        conn.commit()
        
        return {
            "ok": True,
            "mode": "RECOVERY_CONTINUE",
            "session_id": session_id,
        }
    finally:
        conn.close()


def handle_recovery_sync_mismatch(details: Dict[str, Any]) -> Dict[str, Any]:
    """
    Обработчик RECOVERY_SYNC_MISMATCH:
    - Логировать ошибку
    - Предложить полный сброс (пока только возвращаем информацию)
    """
    session_id = set_session_id()
    
    return {
        "ok": False,
        "mode": "RECOVERY_SYNC_MISMATCH",
        "session_id": session_id,
        "error": "positions_sync_mismatch_requires_manual_reset",
        "details": details,
        "hint": "Run full_reset.py --force to resolve",
    }


__all__ = [
    "get_trading_state",
    "update_trading_state",
    "determine_start_mode",
    "handle_fresh_start",
    "handle_recovery_add_missing",
    "handle_clean_stale_positions",
    "handle_recovery_continue",
    "handle_recovery_sync_mismatch",
]
