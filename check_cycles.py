from trading_bot.data.db import get_connection

conn = get_connection()
cur = conn.cursor()

print("=== CYCLES (LAST 5) ===")
cycles = cur.execute(
    "SELECT id, phase, created_at, symbols_valid_count, pool_median_w FROM structural_cycles ORDER BY created_at DESC LIMIT 5"
).fetchall()

for c in cycles:
    print(f"{c['id'][:8]} | phase={c['phase']} | symbols={c['symbols_valid_count']} | w={c['pool_median_w']}")

print("\n=== TRADING STATE ===")
ts = cur.execute("SELECT * FROM trading_state LIMIT 1").fetchone()
if ts:
    # sqlite3.Row supports dictionary-style access
    for key in ts.keys():
        print(f"{key}: {ts[key]}")

print("\n=== POSITIONS ===")
pos = cur.execute("SELECT symbol, side, status, qty FROM position_records WHERE status IN ('open','pending')").fetchall()
for p in pos:
    print(f"{p['symbol']} | {p['side']} | {p['status']} | {p['qty']}")

conn.close()
