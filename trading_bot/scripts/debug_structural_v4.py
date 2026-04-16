#!/usr/bin/env python
"""
Диагностика structural v4: показывает, какие long и short уровни были бы выбраны
при независимом отборе (без требования пары на одном символе).
Запуск: python -m trading_bot.scripts.debug_structural_v4
"""

from __future__ import annotations

import logging
import sys
import os
from typing import Dict, List, Tuple

# Добавляем путь к проекту
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from trading_bot.config import settings as st
from trading_bot.config.symbols import TRADING_SYMBOLS
from trading_bot.data.db import get_connection
from trading_bot.data.repositories import get_instruments_atr_bybit_futures_cur
from trading_bot.tools.price_feed import get_price_feed
from trading_bot.analytics.structural_cycle import (
    StructuralParams,
    _fetch_top_levels,
    StrongLevel,
    _band_bounds,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _get_ref_prices(symbols: List[str]) -> Dict[str, float]:
    """Получает референсные цены (сначала price_feed, затем db_1m_close)."""
    feed = get_price_feed()
    prices = feed.get_prices(symbols)
    ref = {}
    conn = get_connection()
    cur = conn.cursor()
    for s in symbols:
        pp = prices.get(s)
        if pp is not None and pp.price > 0:
            ref[s] = float(pp.price)
        else:
            row = cur.execute(
                "SELECT close FROM ohlcv WHERE symbol = ? AND timeframe = '1m' ORDER BY timestamp DESC LIMIT 1",
                (s,),
            ).fetchone()
            if row and row["close"] is not None:
                ref[s] = float(row["close"])
            else:
                ref[s] = 0.0
    conn.close()
    return ref


def debug_structural_selection():
    """Основная диагностика: показывает top-K long и short уровней по силе."""
    # Параметры structural (как в .env / settings)
    params = StructuralParams(
        min_candidates_per_side=st.STRUCTURAL_MIN_CANDIDATES_PER_SIDE,
        top_k=st.STRUCTURAL_TOP_K,
        min_pool_symbols=st.STRUCTURAL_MIN_POOL_SYMBOLS,
        w_min=st.STRUCTURAL_W_MIN,
        w_max=st.STRUCTURAL_W_MAX,
        allowed_level_types=tuple(st.STRUCTURAL_ALLOWED_LEVEL_TYPES),
        strength_first_enabled=st.STRUCTURAL_STRENGTH_FIRST_ENABLED,
        mid_band_pct=st.STRUCTURAL_MID_BAND_PCT,
    )
    types = params.allowed_level_types
    k = params.top_k

    symbols = list(TRADING_SYMBOLS)
    logger.info("Анализируем %d символов: %s ...", len(symbols), symbols[:5])

    ref_prices = _get_ref_prices(symbols)
    conn = get_connection()
    cur = conn.cursor()

    # Каждый элемент: (symbol, price, volume_peak, distance_atr, level_id, tier, level_type)
    long_candidates: List[Tuple] = []
    short_candidates: List[Tuple] = []

    for symbol in symbols:
        ref = ref_prices.get(symbol, 0.0)
        if ref <= 0:
            logger.warning("%s: нет референсной цены", symbol)
            continue
        atr = get_instruments_atr_bybit_futures_cur(cur, symbol)
        if atr is None or atr <= 0:
            logger.warning("%s: нет ATR", symbol)
            continue

        # Ищем long-уровни (ниже цены)
        long_levels = _fetch_top_levels(cur, symbol, ref, atr, "long", types, k)
        if long_levels:
            best_long = long_levels[0]
            # Вычисляем расстояние в ATR (текущая цена - уровень) / atr
            dist = (ref - best_long.price) / atr
            long_candidates.append((
                symbol, best_long.price, best_long.volume_peak, dist,
                best_long.id, best_long.tier, best_long.level_type
            ))

        # Ищем short-уровни (выше цены)
        short_levels = _fetch_top_levels(cur, symbol, ref, atr, "short", types, k)
        if short_levels:
            best_short = short_levels[0]
            dist = (best_short.price - ref) / atr
            short_candidates.append((
                symbol, best_short.price, best_short.volume_peak, dist,
                best_short.id, best_short.tier, best_short.level_type
            ))

    conn.close()

    # Сортируем по volume_peak (сила) по убыванию
    long_candidates.sort(key=lambda x: x[2], reverse=True)
    short_candidates.sort(key=lambda x: x[2], reverse=True)

    # Берём топ-N для long и топ-M для short (можно менять)
    TOP_LONG = 15
    TOP_SHORT = 16
    top_long = long_candidates[:TOP_LONG]
    top_short = short_candidates[:TOP_SHORT]

    print("\n" + "="*80)
    print("ТОП LONG-уровней (сильнейшие по volume_peak)")
    print("="*80)
    print(f"{'Символ':<12} {'Цена':<10} {'VolumePeak':<12} {'DistATR':<8} {'Tier':<8} {'Тип'}")
    for sym, price, vol, dist, lid, tier, ltype in top_long:
        print(f"{sym:<12} {price:<10.4f} {vol:<12.2f} {dist:<8.2f} {tier:<8} {ltype}")

    print("\n" + "="*80)
    print("ТОП SHORT-уровней (сильнейшие по volume_peak)")
    print("="*80)
    print(f"{'Символ':<12} {'Цена':<10} {'VolumePeak':<12} {'DistATR':<8} {'Tier':<8} {'Тип'}")
    for sym, price, vol, dist, lid, tier, ltype in top_short:
        print(f"{sym:<12} {price:<10.4f} {vol:<12.2f} {dist:<8.2f} {tier:<8} {ltype}")

    # Пересечение множеств
    long_syms = {s[0] for s in top_long}
    short_syms = {s[0] for s in top_short}
    common = long_syms & short_syms
    print("\n" + "="*80)
    print(f"Символы, попавшие в оба топа: {len(common)} – {sorted(common)}")
    print(f"Всего unique символов в long+short: {len(long_syms | short_syms)}")
    if (len(long_syms | short_syms)) > 0:
        print(f"Доля общих: {len(common)/len(long_syms | short_syms)*100:.1f}%")

    # Рекомендация по параметрам N_TRIGGER
    min_group = st.LEVEL_CROSS_MIN_ALERTS_COUNT
    print(f"\nДля группового сигнала нужно минимум {min_group} монет.")
    if len(long_syms) >= min_group:
        print(f"LONG-набор достаточен ({len(long_syms)} >= {min_group})")
    else:
        print(f"LONG-набор мал ({len(long_syms)} < {min_group}) – увеличьте TOP_LONG или ослабьте фильтры")
    if len(short_syms) >= min_group:
        print(f"SHORT-набор достаточен ({len(short_syms)} >= {min_group})")
    else:
        print(f"SHORT-набор мал ({len(short_syms)} < {min_group}) – увеличьте TOP_SHORT или ослабьте фильтры")

    # Если нужно увидеть все кандидаты (без ограничения), раскомментируйте:
    # print("\nВсе long-кандидаты (сортировка по volume_peak):")
    # for i, (sym, price, vol, dist, lid, tier, ltype) in enumerate(long_candidates):
    #     print(f"{i+1:3}. {sym:<12} {price:<10.4f} {vol:<12.2f} {dist:<8.2f} {tier:<8} {ltype}")
    # print("\nВсе short-кандидаты:")
    # for i, (sym, price, vol, dist, lid, tier, ltype) in enumerate(short_candidates):
    #     print(f"{i+1:3}. {sym:<12} {price:<10.4f} {vol:<12.2f} {dist:<8.2f} {tier:<8} {ltype}")


if __name__ == "__main__":
    debug_structural_selection()