"""
Тестовый генератор уровней для быстрой проверки торгового контура.

Вместо сложного structural cycle pipeline генерирует искусственные уровни:
- LONG: current_price - offset * ATR
- SHORT: current_price + offset * ATR

Это позволяет быстро тестировать entry detector, открытие/закрытие позиций
без ожидания реальных движений цен.
"""

from __future__ import annotations

import logging
import time
import uuid
from typing import Any, Dict, List, Optional

from trading_bot.config import settings as st
from trading_bot.config.symbols import TRADING_SYMBOLS
from trading_bot.data.db import get_connection

logger = logging.getLogger(__name__)


def get_test_cycle_symbols() -> List[str]:
    """Получить список символов для тестового цикла."""
    # Берём первые N символов из TRADING_SYMBOLS
    count = st.TEST_CYCLE_SYMBOLS_COUNT
    symbols = list(TRADING_SYMBOLS)[:count]
    
    if not symbols:
        logger.warning("TEST_MODE: No trading symbols available")
        return []
    
    logger.info("TEST_MODE: Selected %d symbols for test cycle", len(symbols))
    return symbols


def get_symbol_atr(symbol: str) -> Optional[float]:
    """Получить ATR для символа из instruments."""
    from trading_bot.tools.price_feed import to_bybit_symbol
    
    conn = get_connection()
    try:
        cur = conn.cursor()
        bybit_sym = to_bybit_symbol(symbol)
        row = cur.execute(
            "SELECT atr FROM instruments WHERE symbol = ? AND exchange = 'bybit_futures'",
            (bybit_sym,)
        ).fetchone()
        
        if row and row["atr"] and row["atr"] > 0:
            return float(row["atr"])
        
        # Fallback: попробовать без преобразования
        row = cur.execute(
            "SELECT atr FROM instruments WHERE symbol = ? AND exchange = 'bybit_futures'",
            (symbol,)
        ).fetchone()
        
        if row and row["atr"] and row["atr"] > 0:
            return float(row["atr"])
        
        logger.warning("TEST_MODE: ATR not found for %s", symbol)
        return None
    finally:
        conn.close()


def get_symbol_current_price(symbol: str) -> Optional[float]:
    """Получить текущую цену символа."""
    from trading_bot.tools.price_feed import PricePoint, get_price_feed
    
    try:
        feed = get_price_feed()
        price_point = feed.get_price(symbol)
        
        if price_point and price_point.price and price_point.price > 0:
            return float(price_point.price)
        
        logger.warning("TEST_MODE: Current price not found for %s", symbol)
        return None
    except Exception as e:
        logger.warning("TEST_MODE: Failed to get price for %s: %s", symbol, e)
        return None


def generate_test_levels() -> Dict[str, Any]:
    """
    Генерация тестовых уровней для всех символов.
    
    Создаёт:
    - Один LONG уровень: current_price - TEST_LEVEL_OFFSET_ATR * atr
    - Один SHORT уровень: current_price + TEST_LEVEL_OFFSET_ATR * atr
    
    Возвращает:
    {
        "ok": True,
        "structural_cycle_id": "...",
        "symbols_count": N,
        "levels_created": M,
    }
    """
    if not st.TEST_MODE:
        logger.warning("TEST_MODE: Not enabled, skipping test level generation")
        return {"ok": False, "error": "test_mode_disabled"}
    
    logger.info("TEST_MODE: Starting test level generation")
    
    # 1. Проверить есть ли активный тестовый цикл
    conn = get_connection()
    try:
        cur = conn.cursor()
        active_cycle = cur.execute(
            """
            SELECT sc.id, sc.phase, sc.created_at
            FROM structural_cycles sc
            WHERE sc.ref_price_source = 'test' 
              AND sc.phase IN ('armed', 'touch_window', 'entry_timer')
            ORDER BY sc.created_at DESC
            LIMIT 1
            """
        ).fetchone()
        
        if active_cycle:
            cycle_id = active_cycle["id"]
            phase = active_cycle["phase"]
            logger.info(
                "TEST_MODE: Active test cycle exists (id=%s phase=%s), skipping regeneration",
                cycle_id[:8], phase
            )
            conn.close()
            return {
                "ok": True,
                "structural_cycle_id": cycle_id,
                "symbols_count": 0,
                "levels_created": 0,
                "skipped": True,
            }
    finally:
        conn.close()

    # 2. Если активного цикла нет - продолжаем создание нового
    # 3. Сбросить старый завершенный цикл
    _clear_test_cycle()

    # 4. Получить символы
    symbols = get_test_cycle_symbols()
    if not symbols:
        return {"ok": False, "error": "no_symbols"}
    
    # 3. Создать structural_cycle
    cycle_id = str(uuid.uuid4())
    structural_id = str(uuid.uuid4())
    now = int(time.time())
    
    conn = get_connection()
    try:
        cur = conn.cursor()
        
        # Создать structural_cycle запись
        cur.execute(
            """
            INSERT INTO structural_cycles (
                id, phase, created_at, updated_at,
                symbols_valid_count, scan_duration_sec,
                pool_median_w, pool_mad, pool_k,
                ref_price_source, target_w_band_k,
                target_center_weight, target_width_weight,
                allowed_level_types, anchor_symbols,
                center_mad_k, center_filter_enabled,
                target_align_enabled, min_pool_symbols
            ) VALUES (?, 'armed', ?, ?, 0, 0, 0, 0, 1, 'test', 0, 0, 0, 'test', '[]', 0, 0, 0, 1)
            """,
            (structural_id, now, now)
        )
        
        # Создать structural_cycle_symbols и cycle_levels для каждого символа
        levels_created = 0
        
        for symbol in symbols:
            current_price = get_symbol_current_price(symbol)
            atr = get_symbol_atr(symbol)
            
            if current_price is None or atr is None:
                logger.warning("TEST_MODE: Skipping %s (price=%s, atr=%s)", 
                              symbol, current_price, atr)
                continue
            
            # Расчёт уровней
            offset = st.TEST_LEVEL_OFFSET_ATR
            long_price = current_price - offset * atr
            short_price = current_price + offset * atr
            
            #ROUND to tick size
            from trading_bot.tools.price_feed import to_bybit_symbol
            bybit_sym = to_bybit_symbol(symbol)
            tick = _get_tick_size(bybit_sym)
            if tick:
                long_price = _round_to_tick(long_price, tick)
                short_price = _round_to_tick(short_price, tick)
            
            # Создать structural_cycle_symbols запись
            cur.execute(
                """
                INSERT INTO structural_cycle_symbols (
                    cycle_id, symbol, status,
                    L_price, U_price, atr, W_atr,
                    ref_price_ws, mid_price, mid_band_low, mid_band_high,
                    volume_peak_below, volume_peak_above,
                    tier_below, tier_above
                ) VALUES (?, ?, 'ok', ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 'test', 'test')
                """,
                (structural_id, symbol, long_price, short_price, atr, atr, 
                 current_price, current_price, long_price, short_price)
            )
            
            # Создать cycle_levels записи (LONG и SHORT)
            cur.execute(
                """
                INSERT INTO cycle_levels (
                    cycle_id, symbol, direction, level_type, 
                    level_price, strength, volume_peak,
                    tier, layer, is_active, status,
                    created_at, updated_at, origin, timeframe
                ) VALUES (?, ?, 'long', 'test', ?, 1, 0, 'test', 1, 1, 'active', ?, ?, 'test', '1h')
                """,
                (cycle_id, symbol, long_price, now, now)
            )
            
            cur.execute(
                """
                INSERT INTO cycle_levels (
                    cycle_id, symbol, direction, level_type,
                    level_price, strength, volume_peak,
                    tier, layer, is_active, status,
                    created_at, updated_at, origin, timeframe
                ) VALUES (?, ?, 'short', 'test', ?, 1, 0, 'test', 1, 1, 'active', ?, ?, 'test', '1h')
                """,
                (cycle_id, symbol, short_price, now, now)
            )
            
            levels_created += 2
            
            logger.info(
                "TEST_MODE: %s - current=%.2f LONG=%.2f SHORT=%.2f ATR=%.2f",
                symbol, current_price, long_price, short_price, atr
            )
        
        # Обновить trading_state
        cur.execute(
            """
            UPDATE trading_state
            SET
                cycle_phase = 'arming',
                levels_frozen = 1,
                cycle_id = ?,
                structural_cycle_id = ?,
                position_state = 'none',
                last_start_mode = 'test_mode',
                last_transition_at = ?,
                updated_at = ?
            WHERE id = 1
            """,
            (cycle_id, structural_id, now, now)
        )
        
        conn.commit()
        
        logger.info(
            "TEST_MODE: Generated %d levels for %d symbols (cycle=%s)",
            levels_created, len(symbols), cycle_id[:8]
        )
        
        return {
            "ok": True,
            "structural_cycle_id": structural_id,
            "cycle_id": cycle_id,
            "symbols_count": len(symbols),
            "levels_created": levels_created,
        }
        
    except Exception as e:
        conn.rollback()
        logger.exception("TEST_MODE: Failed to generate test levels")
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()


def rebuild_opposite_test_levels(cycle_id: str, known_side: str) -> Dict[str, Any]:
    """
    Пересбор противоположной стороны для тестового цикла.
    
    После входа в LONG: создаём SHORT уровни на +0.4 ATR от текущей цены
    После входа в SHORT: создаём LONG уровни на -0.4 ATR от текущей цены
    
    Args:
        cycle_id: ID цикла
        known_side: 'long' или 'short' (которую сторону мы закрыли)
    
    Returns:
        {"ok": True, "levels_created": N}
    """
    if not st.TEST_MODE:
        return {"ok": False, "error": "test_mode_disabled"}
    
    logger.info("TEST_MODE: Rebuilding opposite levels for cycle=%s side=%s", 
                cycle_id[:8] if cycle_id else None, known_side)
    
    target_side = 'short' if known_side == 'long' else 'long'
    offset = st.TEST_OPPOSITE_OFFSET_ATR
    
    conn = get_connection()
    try:
        cur = conn.cursor()
        
        # Получить символы текущего цикла
        rows = cur.execute(
            "SELECT symbol FROM structural_cycle_symbols WHERE cycle_id = ?",
            (cycle_id,)
        ).fetchall()
        
        if not rows:
            return {"ok": False, "error": "cycle_not_found"}
        
        symbols = [str(r["symbol"]) for r in rows]
        levels_created = 0
        now = int(time.time())
        
        for symbol in symbols:
            current_price = get_symbol_current_price(symbol)
            atr = get_symbol_atr(symbol)
            
            if current_price is None or atr is None:
                continue
            
            # Расчёт уровня
            if target_side == 'long':
                level_price = current_price - offset * atr
            else:
                level_price = current_price + offset * atr
            
            # ROUND to tick size
            from trading_bot.tools.price_feed import to_bybit_symbol
            bybit_sym = to_bybit_symbol(symbol)
            tick = _get_tick_size(bybit_sym)
            if tick:
                level_price = _round_to_tick(level_price, tick)
            
            # Обновить или создать уровень
            cur.execute(
                """
                UPDATE cycle_levels
                SET level_price = ?, updated_at = ?, is_active = 1, status = 'active'
                WHERE cycle_id = ? AND symbol = ? AND direction = ?
                """,
                (level_price, now, cycle_id, symbol, target_side)
            )
            
            if cur.rowcount == 0:
                # Уровень не существует, создаём
                cur.execute(
                    """
                    INSERT INTO cycle_levels (
                        cycle_id, symbol, direction, level_type,
                        level_price, strength, volume_peak,
                        tier, layer, is_active, status,
                        created_at, updated_at, origin, timeframe
                    ) VALUES (?, ?, ?, 'test', ?, 1, 0, 'test', 1, 1, 'active', ?, ?, 'test', '1h')
                    """,
                    (cycle_id, symbol, target_side, level_price, now, now)
                )
            
            levels_created += 1
            logger.info(
                "TEST_MODE: Opposite %s level for %s: %.2f (offset=%.2f*A TR=%.2f)",
                target_side.upper(), symbol, level_price, offset, atr
            )
        
        conn.commit()
        
        logger.info("TEST_MODE: Rebuilt %d opposite levels", levels_created)
        return {"ok": True, "levels_created": levels_created}
        
    except Exception as e:
        conn.rollback()
        logger.exception("TEST_MODE: Failed to rebuild opposite levels")
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()


def _clear_test_cycle() -> None:
    """
    Очистить старый тестовый цикл ТОЛЬКО если:
    - фаза 'closed' (цикл завершён)
    - или прошло >24 часа с создания
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        
        # Найти старый тестовый цикл
        row = cur.execute(
            """
            SELECT id, phase, created_at 
            FROM structural_cycles 
            WHERE ref_price_source = 'test' 
            ORDER BY created_at DESC 
            LIMIT 1
            """
        ).fetchone()
        
        if row:
            old_id = row["id"]
            phase = row["phase"]
            created_at = row["created_at"]
            now = int(time.time())
            
            # Удаляем только если цикл закрыт или старее 24 часов
            should_delete = (phase == 'closed') or (now - created_at > 86400)
            
            if should_delete:
                cur.execute("DELETE FROM cycle_levels WHERE cycle_id = ?", (old_id,))
                cur.execute("DELETE FROM structural_cycle_symbols WHERE cycle_id = ?", (old_id,))
                cur.execute("DELETE FROM structural_cycles WHERE id = ?", (old_id,))
                logger.info(
                    "TEST_MODE: Cleared old test cycle %s (phase=%s, age=%dh)",
                    old_id[:8], phase, (now - created_at) // 3600
                )
            else:
                logger.info(
                    "TEST_MODE: Keeping active test cycle %s (phase=%s)",
                    old_id[:8], phase
                )
        
        conn.commit()
    finally:
        conn.close()


def _get_tick_size(symbol: str) -> Optional[float]:
    """Получить tick_size для символа."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        row = cur.execute(
            "SELECT tick_size FROM instruments WHERE symbol = ? AND exchange = 'bybit_futures'",
            (symbol,)
        ).fetchone()
        
        if row and row["tick_size"]:
            return float(row["tick_size"])
        return None
    finally:
        conn.close()


def _round_to_tick(price: float, tick_size: float) -> float:
    """Округлить цену до tick_size."""
    return round(price / tick_size) * tick_size


__all__ = ["generate_test_levels", "rebuild_opposite_test_levels"]
