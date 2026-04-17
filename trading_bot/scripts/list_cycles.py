from trading_bot.data.db import get_connection

conn = get_connection()
cur = conn.cursor()

print("Structural cycles:")
cur.execute('SELECT id, phase FROM structural_cycles')
rows = cur.fetchall()
for r in rows:
    print(f"  {r['id']} {r['phase']}")

conn.close()
