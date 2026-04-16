"""
Принудительный сброс торгового цикла (trading_state).

Используется когда:
- Терминал закрыт/упал, но цикл остался активным
- Нужно вручную сбросить залипший цикл
- Подготовка к новому запуску

Запуск:
  PYTHONPATH=. python -m trading_bot.scripts.reset_trading_state [--force]

Без флага --force: только показывает текущее состояние.
С флагом --force: сбрасывает цикл в closed.
"""

from __future__ import annotations

import argparse
import time

from trading_bot.data.db import get_connection
from trading_bot.data.schema import init_db, run_migrations


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true", help="Выполнить сброс (без флага — только показать состояние)")
    args = parser.parse_args()

    init_db()
    run_migrations()
    conn = get_connection()
    try:
        cur = conn.cursor()
        row = cur.execute(
            """
            SELECT 
                cycle_id, 
                structural_cycle_id,
                position_state, 
                cycle_phase, 
                levels_frozen, 
                cycle_version,
                close_reason,
                last_transition_at,
                updated_at
            FROM trading_state WHERE id = 1
            """
        ).fetchone()

        if not row:
            print("ERROR: trading_state не найден")
            return

        cycle_id = row["cycle_id"]
        structural_id = row["structural_cycle_id"]
        phase = row["cycle_phase"]
        frozen = int(row["levels_frozen"] or 0)
        pos_state = row["position_state"]
        version = int(row["cycle_version"] or 0)
        last_trans = int(row["last_transition_at"] or 0)
        now = int(time.time())

        print("=" * 80)
        print("ТЕКУЩЕЕ СОСТОЯНИЕ ЦИКЛА (trading_state)")
        print("=" * 80)
        print(f"cycle_id:           {cycle_id}")
        print(f"structural_cycle_id: {structural_id}")
        print(f"cycle_phase:        {phase}")
        print(f"levels_frozen:      {frozen}")
        print(f"position_state:     {pos_state}")
        print(f"cycle_version:      {version}")
        print(f"close_reason:       {row['close_reason']}")
        print(f"last_transition_at: {last_trans} ({time.ctime(last_trans) if last_trans else 'N/A'})")
        print(f"updated_at:         {row['updated_at']}")

        # Проверка позиций
        if cycle_id:
            pos_count = cur.execute(
                "SELECT COUNT(*) AS c FROM position_records WHERE cycle_id = ? AND status IN ('pending', 'open')",
                (cycle_id,)
            ).fetchone()
            n_open = int(pos_count["c"] if pos_count else 0)
            print(f"\nОткрытые позиции в цикле: {n_open}")

            # Символы в цикле
            struct_id = structural_id or cycle_id
            syms = cur.execute(
                "SELECT DISTINCT symbol FROM structural_cycle_symbols WHERE cycle_id = ?",
                (struct_id,)
            ).fetchall()
            print(f"Символов в structural_cycle: {len(syms)}")
            if len(syms) <= 20:
                print(f"  {', '.join([s['symbol'] for s in syms])}")

            # Уровни в cycle_levels
            levels = cur.execute(
                "SELECT direction, COUNT(*) AS c FROM cycle_levels WHERE cycle_id = ? GROUP BY direction",
                (cycle_id,)
            ).fetchall()
            print(f"\nУровни в cycle_levels:")
            for lvl in levels:
                print(f"  {lvl['direction'].upper()}: {lvl['c']}")
        print("=" * 80)

        if not args.force:
            print("\n⚠️  Для сброса цикла используйте флаг --force")
            print("   PYTHONPATH=. python -m trading_bot.scripts.reset_trading_state --force")
            return

        # СБРОС ЦИКЛА
        confirm = input("\n⚠️  Сбросить цикл в closed? (yes/no): ").strip().lower()
        if confirm != "yes":
            print("Отменено")
            return

        cur.execute(
            """
            UPDATE trading_state
            SET 
                cycle_phase = 'closed',
                levels_frozen = 0,
                cycle_id = NULL,
                structural_cycle_id = NULL,
                position_state = 'none',
                close_reason = 'manual_reset',
                last_transition_at = ?,
                updated_at = ?
            WHERE id = 1
            """,
            (now, now)
        )

        conn.commit()
        print("\n✅ Цикл успешно сброшен в closed")
        print(f"   close_reason: manual_reset")
        
        # Перепроверить
        row2 = cur.execute("SELECT * FROM trading_state WHERE id = 1").fetchone()
        print(f"\nНовое состояние:")
        print(f"  cycle_phase: {row2['cycle_phase']}")
        print(f"  levels_frozen: {row2['levels_frozen']}")
        print(f"  cycle_id: {row2['cycle_id']}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
