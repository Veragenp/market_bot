from trading_bot.data.db import get_connection

conn = get_connection()
cur = conn.cursor()

# Проверить cycle_levels
cur.execute('SELECT COUNT(*) as cnt FROM cycle_levels')
print(f"cycle_levels rows: {cur.fetchone()['cnt']}")

# Показать первые 10 уровней
cur.execute('SELECT cycle_id, symbol, direction, level_price, tier, volume_peak FROM cycle_levels LIMIT 10')
rows = cur.fetchall()
print("\nSample levels:")
for r in rows:
    print(f"  {r['cycle_id'][:8]} {r['symbol']} {r['direction']} {r['level_price']} tier={r['tier']} vol={r['volume_peak']}")

# Показать trading_state
cur.execute('SELECT cycle_id, structural_cycle_id, cycle_phase, levels_frozen FROM trading_state WHERE id=1')
state = cur.fetchone()
print(f"\nTrading state:")
print(f"  cycle_id={state['cycle_id']}")
print(f"  structural_cycle_id={state['structural_cycle_id']}")
print(f"  phase={state['cycle_phase']}")
print(f"  frozen={state['levels_frozen']}")

conn.close()
