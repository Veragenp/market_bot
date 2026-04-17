from trading_bot.data.db import get_connection

conn = get_connection()
cur = conn.cursor()

# Удалить ВСЕ циклы в фазах armed/touch_window/entry_timer
# Сначала получаем IDs циклов
cur.execute('SELECT id FROM structural_cycles WHERE phase IN ("armed", "touch_window", "entry_timer")')
rows = cur.fetchall()
cycle_ids = [r["id"] for r in rows]

if cycle_ids:
    # Удалить уровни
    for cid in cycle_ids:
        cur.execute('DELETE FROM cycle_levels WHERE cycle_id = ?', (cid,))
        cur.execute('DELETE FROM structural_cycle_symbols WHERE cycle_id = ?', (cid,))
    # Удалить циклы
    for cid in cycle_ids:
        cur.execute('DELETE FROM structural_cycles WHERE id = ?', (cid,))
    
    print(f"Deleted {len(cycle_ids)} cycles: {cycle_ids[:3]}...")
else:
    print("No active cycles to delete")

# Сбросить trading_state
cur.execute(
    'UPDATE trading_state SET cycle_id=NULL, structural_cycle_id=NULL, cycle_phase="closed", levels_frozen=0 WHERE id=1'
)
conn.commit()
print("Trading state reset")
conn.close()
