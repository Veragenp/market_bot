#!/usr/bin/env python
"""
Проверка сохранённых уровней в cycle_levels (market_data.db).
Запуск: python check_cycle_levels.py
"""

import sqlite3
import os

DB_PATH = os.path.join("trading_bot", "data", "market_data.db")

def main():
    if not os.path.exists(DB_PATH):
        print(f"❌ БД не найдена: {DB_PATH}")
        print("Убедитесь, что запускаете скрипт из корня проекта (рядом с папкой trading_bot)")
        return

    print(f"✅ Используем БД: {DB_PATH}\n")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # 1. Проверим, есть ли таблица trading_state
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='trading_state'")
    if not cur.fetchone():
        print("⚠️ Таблица trading_state не найдена. Возможно, структура БД другая.")
        conn.close()
        return

    # 2. Получить последний активный cycle_id
    row = cur.execute("SELECT cycle_id, structural_cycle_id FROM trading_state WHERE id = 1").fetchone()
    if not row or not row["cycle_id"]:
        print("⚠️ Нет активного цикла (cycle_id = NULL)")
        conn.close()
        return

    cycle_id = row["cycle_id"]
    scid = row["structural_cycle_id"]
    print(f"Текущий cycle_id: {cycle_id}")
    print(f"structural_cycle_id: {scid}\n")

    # 3. Проверим, есть ли таблица cycle_levels
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='cycle_levels'")
    if not cur.fetchone():
        print("❌ Таблица cycle_levels не найдена!")
        conn.close()
        return

    # 4. Посчитать уровни в cycle_levels для этого цикла
    cur.execute("""
        SELECT direction, COUNT(*) as cnt, GROUP_CONCAT(DISTINCT symbol) as symbols
        FROM cycle_levels
        WHERE cycle_id = ? AND level_step = 1 AND is_active = 1
        GROUP BY direction
    """, (cycle_id,))
    rows = cur.fetchall()
    long_cnt = short_cnt = 0
    long_syms = []
    short_syms = []
    for r in rows:
        if r["direction"] == "long":
            long_cnt = r["cnt"]
            long_syms = r["symbols"].split(",") if r["symbols"] else []
        elif r["direction"] == "short":
            short_cnt = r["cnt"]
            short_syms = r["symbols"].split(",") if r["symbols"] else []

    print(f"📈 LONG уровней: {long_cnt}")
    print(f"📉 SHORT уровней: {short_cnt}")

    set_long = set(long_syms)
    set_short = set(short_syms)
    only_long = set_long - set_short
    only_short = set_short - set_long
    both = set_long & set_short

    print(f"\n🔄 Символы с обоими уровнями: {len(both)}")
    print(f"🔹 Только LONG: {len(only_long)} -> {sorted(only_long)[:20]}")
    print(f"🔸 Только SHORT: {len(only_short)} -> {sorted(only_short)[:20]}")

    if only_long:
        print("\n📈 Примеры только LONG:")
        for sym in list(only_long)[:10]:
            cur.execute("""
                SELECT level_price, distance_atr, tier, volume_peak
                FROM cycle_levels
                WHERE cycle_id = ? AND symbol = ? AND direction = 'long'
            """, (cycle_id, sym))
            lvl = cur.fetchone()
            if lvl:
                print(f"  {sym}: price={lvl['level_price']:.4f}, distATR={lvl['distance_atr']:.2f}, tier={lvl['tier']}, vol={lvl['volume_peak']:.2f}")

    if only_short:
        print("\n📉 Примеры только SHORT:")
        for sym in list(only_short)[:10]:
            cur.execute("""
                SELECT level_price, distance_atr, tier, volume_peak
                FROM cycle_levels
                WHERE cycle_id = ? AND symbol = ? AND direction = 'short'
            """, (cycle_id, sym))
            lvl = cur.fetchone()
            if lvl:
                print(f"  {sym}: price={lvl['level_price']:.4f}, distATR={lvl['distance_atr']:.2f}, tier={lvl['tier']}, vol={lvl['volume_peak']:.2f}")

    conn.close()

if __name__ == "__main__":
    main()