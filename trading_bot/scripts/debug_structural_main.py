#!/usr/bin/env python
"""
Диагностика структурного цикла через compute_structural_symbol_results.
Сравнивает результат с debug_structural_v4.
"""

from __future__ import annotations

import logging
import sys
import os
from typing import Dict, List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from trading_bot.config import settings as st
from trading_bot.config.symbols import TRADING_SYMBOLS
from trading_bot.data.db import get_connection
from trading_bot.tools.price_feed import get_price_feed
from trading_bot.analytics.structural_cycle import (
    StructuralParams,
    compute_structural_symbol_results,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

def get_ref_prices(symbols: List[str]) -> Dict[str, float]:
    """Та же логика, что в отладочном модуле."""
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

def main():
    symbols = list(TRADING_SYMBOLS)
    logger.info("Символов: %d", len(symbols))
    ref_prices = get_ref_prices(symbols)

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

    conn = get_connection()
    cur = conn.cursor()
    results, _ = compute_structural_symbol_results(cur, symbols, ref_prices, params)
    conn.close()

    # Собрать long и short кандидатов как в отладочном модуле
    long_candidates = []
    short_candidates = []
    for r in results:
        if r.L_price is not None and r.volume_peak_below is not None:
            dist = (r.ref_price - r.L_price) / r.atr if r.atr and r.atr > 0 else 0.0
            long_candidates.append((
                r.symbol, r.L_price, r.volume_peak_below, dist,
                r.level_below_id, r.tier_below, "long"
            ))
        if r.U_price is not None and r.volume_peak_above is not None:
            dist = (r.U_price - r.ref_price) / r.atr if r.atr and r.atr > 0 else 0.0
            short_candidates.append((
                r.symbol, r.U_price, r.volume_peak_above, dist,
                r.level_above_id, r.tier_above, "short"
            ))

    long_candidates.sort(key=lambda x: x[2], reverse=True)
    short_candidates.sort(key=lambda x: x[2], reverse=True)

    TOP_LONG = 15
    TOP_SHORT = 16
    print("\n" + "="*80)
    print("ТОП LONG-уровней (через compute_structural_symbol_results)")
    print("="*80)
    print(f"{'Символ':<12} {'Цена':<10} {'VolumePeak':<12} {'DistATR':<8} {'Tier':<8}")
    for sym, price, vol, dist, lid, tier, _ in long_candidates[:TOP_LONG]:
        print(f"{sym:<12} {price:<10.4f} {vol:<12.2f} {dist:<8.2f} {tier:<8}")

    print("\n" + "="*80)
    print("ТОП SHORT-уровней (через compute_structural_symbol_results)")
    print("="*80)
    print(f"{'Символ':<12} {'Цена':<10} {'VolumePeak':<12} {'DistATR':<8} {'Tier':<8}")
    for sym, price, vol, dist, lid, tier, _ in short_candidates[:TOP_SHORT]:
        print(f"{sym:<12} {price:<10.4f} {vol:<12.2f} {dist:<8.2f} {tier:<8}")

    print("\n" + "="*80)
    long_syms = {c[0] for c in long_candidates[:TOP_LONG]}
    short_syms = {c[0] for c in short_candidates[:TOP_SHORT]}
    common = long_syms & short_syms
    print(f"Символы в обоих томах: {len(common)} – {sorted(common)}")
    print(f"Всего уникальных: {len(long_syms | short_syms)}")

if __name__ == "__main__":
    main()