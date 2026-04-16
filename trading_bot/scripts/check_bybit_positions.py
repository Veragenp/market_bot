"""
Проверка позиций на Bybit.
Показать все открытые позиции по USDT linear.
"""

from __future__ import annotations

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'entrypoints'))


def main() -> int:
    print("=" * 80)
    print("ПРОВЕРКА ПОЗИЦИЙ НА BYBIT")
    print("=" * 80)
    
    from trading_bot.tools.bybit_trading import get_linear_positions, linear_position_sizes_by_symbol, to_bybit_symbol
    
    # Проверяем API ключи
    api_key = os.getenv("BYBIT_API_KEY")
    api_secret = os.getenv("BYBIT_API_SECRET")
    
    print(f"\nBYBIT_API_KEY: {'***' if api_key else 'NOT SET'}")
    print(f"BYBIT_API_SECRET: {'***' if api_secret else 'NOT SET'}")
    
    if not api_key or not api_secret:
        print("\n❌ API ключи не настроены!")
        return 1
    
    print("\nЗапрос позиций...")
    resp = get_linear_positions()
    
    if resp is None:
        print("❌ Не удалось получить позиции (ответ None)")
        return 1
    
    if resp.get("retCode") != 0:
        print(f"❌ Bybit API error: {resp}")
        return 1
    
    sizes = linear_position_sizes_by_symbol(resp)
    
    print(f"\nОткрытые позиции на Bybit:")
    print("-" * 80)
    
    has_positions = False
    for symbol, size in sorted(sizes.items()):
        if abs(size) > 1e-12:
            has_positions = True
            side = "LONG" if size > 0 else "SHORT"
            print(f"  {symbol:20s} size={size:12.8f} ({side})")
    
    if not has_positions:
        print("  Нет открытых позиций (FLAT)")
    
    print("-" * 80)
    print(f"\nВсего символов с позициями: {sum(1 for s in sizes.values() if abs(s) > 1e-12)}")
    
    # Проверяем pool structural
    from trading_bot.data.db import get_connection
    
    conn = get_connection()
    try:
        cur = conn.cursor()
        row = cur.execute(
            "SELECT phase, symbols_valid_count FROM structural_cycles ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        
        if row:
            print(f"\nПоследний structural cycle:")
            print(f"  phase: {row['phase']}")
            print(f"  symbols_valid_count: {row['symbols_valid_count']}")
        
        # Проверка trading_state
        row2 = cur.execute(
            "SELECT cycle_id, structural_cycle_id, cycle_phase, levels_frozen FROM trading_state WHERE id=1"
        ).fetchone()
        
        if row2:
            print(f"\nTrading state:")
            print(f"  cycle_id: {row2['cycle_id'][:8] if row2['cycle_id'] else 'None'}")
            print(f"  structural_cycle_id: {row2['structural_cycle_id'][:8] if row2['structural_cycle_id'] else 'None'}")
            print(f"  cycle_phase: {row2['cycle_phase']}")
            print(f"  levels_frozen: {row2['levels_frozen']}")
    finally:
        conn.close()
    
    print("\n" + "=" * 80)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
