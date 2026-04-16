"""
Проверка открытых позиций в БД.

Запуск:
  PYTHONPATH=. python -m trading_bot.scripts.check_open_positions

Показывает:
- Все позиции со статусом pending/open
- cycle_id для каждой позиции
- Рекомендации по очистке
"""

from __future__ import annotations

from trading_bot.data.db import get_connection
from trading_bot.data.schema import init_db, run_migrations


def main() -> None:
    init_db()
    run_migrations()
    conn = get_connection()
    try:
        cur = conn.cursor()

        # Все открытые/pending позиции
        print("=" * 80)
        print("ОТКРЫТЫЕ ПОЗИЦИИ В БД")
        print("=" * 80)
        rows = cur.execute(
            """
            SELECT id, cycle_id, symbol, direction, status, size, entry_price, created_at, updated_at
            FROM position_records
            WHERE status IN ('pending', 'open')
            ORDER BY cycle_id, created_at
            """
        ).fetchall()

        if not rows:
            print("Нет открытых позиций")
        else:
            print(f"Всего открытых позиций: {len(rows)}")
            print()
            for r in rows:
                print(f"  id={r['id']} cycle_id={r['cycle_id'][:8] if r['cycle_id'] else 'NULL'} "
                      f"symbol={r['symbol']:12} direction={r['direction']:6} "
                      f"status={r['status']:10} size={r['size']} "
                      f"entry={r['entry_price']}")

        # Текущее trading_state
        print("\n" + "=" * 80)
        print("ТЕКУЩЕЕ СОСТОЯНИЕ (trading_state)")
        print("=" * 80)
        ts = cur.execute("SELECT * FROM trading_state WHERE id = 1").fetchone()
        print(f"cycle_id:           {ts['cycle_id']}")
        print(f"structural_cycle_id: {ts['structural_cycle_id']}")
        print(f"cycle_phase:        {ts['cycle_phase']}")
        print(f"levels_frozen:      {ts['levels_frozen']}")
        print(f"position_state:     {ts['position_state']}")
        print(f"close_reason:       {ts['close_reason']}")

        # Посчитать позиции для текущего cycle_id
        if ts['cycle_id']:
            pos_count = cur.execute(
                "SELECT COUNT(*) as c FROM position_records WHERE cycle_id = ?",
                (ts['cycle_id'],)
            ).fetchone()["c"]
            print(f"\nПозиций для cycle_id={ts['cycle_id'][:8]}: {pos_count}")

            # Разбить по статусам
            for status in ['pending', 'open', 'filled', 'cancelled']:
                cnt = cur.execute(
                    "SELECT COUNT(*) as c FROM position_records WHERE cycle_id = ? AND status = ?",
                    (ts['cycle_id'], status)
                ).fetchone()["c"]
                print(f"  {status}: {cnt}")

        print("\n" + "=" * 80)
        print("РЕКОМЕНДАЦИИ")
        print("=" * 80)

        if rows:
            print("⚠️  Есть открытые позиции в БД!")
            print("   Supervisor НЕ сбросит цикл, пока есть эти позиции.")
            print()
            print("Варианты:")
            print("   1. Если позиции РЕАЛЬНЫЕ - дождаться их закрытия")
            print("   2. Если позиции ЗАВИСШИЕ - очистить вручную:")
            print()
            print("   PYTHONPATH=. python -m trading_bot.scripts.check_open_positions --close-all")
        else:
            print("✅ Нет открытых позиций - цикл может быть сброшен автоматически")

    finally:
        conn.close()


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "--close-all":
        # Очистка всех позиций (ОПАСНО!)
        conn = get_connection()
        try:
            cur = conn.cursor()
            count = cur.execute(
                "DELETE FROM position_records WHERE status IN ('pending', 'open')"
            ).rowcount
            conn.commit()
            print(f"✅ Очистил {count} позиций")
        finally:
            conn.close()
    else:
        main()
