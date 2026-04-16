"""
Полный сброс торгового бота перед новым запуском.

Используется когда:
- Вы хотите начать новый цикл с чистого состояния
- Закрываете текущую сессию и планируете новую
- Ручной старт после останова

Запуск:
  PYTHONPATH=. python -m trading_bot.scripts.full_reset [--dry-run]

Что делает:
1. Закрывает все открытые позиции (маркирует как 'cancelled')
2. Отменяет все pending ордера
3. Сбрасывает trading_state в 'arming'
4. Очищает cycle_id, structural_cycle_id
5. Устанавливает start_reason='manual'

Варианты:
  --dry-run          Показать что будет сделано, но не выполнять
  --no-close-pos     Не закрывать позиции (только сброс состояния)
  --force            Выполнить без подтверждения
  --close-exchange   Закрыть позиции на бирже (через API)
"""

from __future__ import annotations

import argparse
import time

from trading_bot.data.db import get_connection
from trading_bot.data.schema import init_db, run_migrations
from trading_bot.tools.bybit_trading import get_linear_positions, linear_position_sizes_by_symbol


def main() -> None:
    parser = argparse.ArgumentParser(description="Полный сброс торгового бота перед новым запуском")
    parser.add_argument("--dry-run", action="store_true", help="Показать что будет сделано, но не выполнять")
    parser.add_argument("--no-close-pos", action="store_true", help="Не закрывать позиции (только сброс состояния)")
    parser.add_argument("--force", action="store_true", help="Выполнить без подтверждения")
    parser.add_argument("--close-exchange", action="store_true", help="Закрыть позиции на бирже (через API)")
    args = parser.parse_args()

    init_db()
    run_migrations()
    conn = get_connection()
    try:
        cur = conn.cursor()

        # Текущее состояние
        ts = cur.execute("SELECT * FROM trading_state WHERE id = 1").fetchone()
        cycle_id = ts['cycle_id']
        structural_id = ts['structural_cycle_id']
        phase = ts['cycle_phase']
        frozen = int(ts['levels_frozen'] or 0)
        pos_state = ts['position_state']

        print("=" * 80)
        print("ТЕКУЩЕЕ СОСТОЯНИЕ")
        print("=" * 80)
        print(f"cycle_id:             {cycle_id}")
        print(f"structural_cycle_id:  {structural_id}")
        print(f"cycle_phase:          {phase}")
        print(f"levels_frozen:        {frozen}")
        print(f"position_state:       {pos_state}")

        # Подсчитать позиции
        pos_rows = cur.execute(
            "SELECT status, COUNT(*) as c FROM position_records GROUP BY status"
        ).fetchall()
        print("\nПозиции по статусам:")
        for r in pos_rows:
            print(f"  {r['status']:12}: {r['c']}")

        open_pos = cur.execute(
            "SELECT COUNT(*) as c FROM position_records WHERE status IN ('pending', 'open')"
        ).fetchone()["c"]
        print(f"\nОткрытые позиции: {open_pos}")

        # Подсчитать pending ордера
        pending_orders = cur.execute(
            """
            SELECT COUNT(*) as c FROM exec_orders
            WHERE lower(COALESCE(status, '')) NOT IN ('filled', 'cancelled', 'canceled', 'rejected', 'closed', 'failed', 'expired')
            """
        ).fetchone()["c"]
        print(f"Pending ордера: {pending_orders}")

        if not args.force and not args.dry_run:
            print("\n" + "=" * 80)
            confirm = input("⚠️  Выполнить полный сброс? (yes/no): ").strip().lower()
            if confirm != "yes":
                print("Отменено")
                return

        now = int(time.time())

        if not args.no_close_pos and open_pos > 0:
            print(f"\n{'=' * 80}")
            print(f"ЗАКРЫТИЕ {open_pos} открытых позиций (статус → 'cancelled')")
            print(f"{'=' * 80}")

            if args.dry_run:
                print("[DRY RUN] Позиции были бы помечены как 'cancelled'")
            else:
                cur.execute(
                    """
                    UPDATE position_records
                    SET status = 'cancelled',
                        close_reason = 'manual_reset_before_new_session',
                        updated_at = ?
                    WHERE status IN ('pending', 'open')
                    """,
                    (now,)
                )
                print(f"✅ Помечено {cur.rowcount} позиций как 'cancelled'")

        # Закрыть позиции на бирже (если --close-exchange)
        if args.close_exchange and not args.dry_run:
            print(f"\n{'=' * 80}")
            print("ЗАКРЫТИЕ ПОЗИЦИЙ НА БИРЖЕ (Bybit)")
            print(f"{'=' * 80}")

            try:
                pos_resp = get_linear_positions()
                if pos_resp and pos_resp.get("retCode") == 0:
                    sizes = linear_position_sizes_by_symbol(pos_resp)
                    closed_count = 0
                    
                    for sym, size in sizes.items():
                        if abs(size) > 1e-12:
                            print(f"  Закрываю позицию {sym}: size={size}")
                            # TODO: Реализовать закрытие через API
                            # place_linear_market_order(...)
                            closed_count += 1
                    
                    print(f"✅ Попыток закрытия: {closed_count}")
                    print("⚠️  Примечание: фактическое закрытие пока не реализовано (TODO)")
                else:
                    print("⚠️  Не удалось получить позиции с биржи")
            except Exception as e:
                print(f"❌ Ошибка при закрытии позиций: {e}")

        if pending_orders > 0:
            print(f"\n{'=' * 80}")
            print(f"ОТМЕНА {pending_orders} pending ордеров")
            print(f"{'=' * 80}")

            if args.dry_run:
                print("[DRY RUN] Ордера были бы помечены как 'cancelled'")
            else:
                cur.execute(
                    """
                    UPDATE exec_orders
                    SET status = 'cancelled',
                        updated_at = ?
                    WHERE lower(COALESCE(status, '')) NOT IN ('filled', 'cancelled', 'canceled', 'rejected', 'closed', 'failed', 'expired')
                    """,
                    (now,)
                )
                print(f"✅ Помечено {cur.rowcount} ордеров как 'cancelled'")

        # Сброс trading_state
        print(f"\n{'=' * 80}")
        print("СБРОС trading_state")
        print(f"{'=' * 80}")

        if args.dry_run:
            print("[DRY RUN] trading_state был бы сброшен:")
            print("  cycle_phase → 'arming'")
            print("  levels_frozen → 0")
            print("  cycle_id → NULL")
            print("  structural_cycle_id → NULL")
            print("  position_state → 'none'")
            print("  last_start_mode → 'manual_reset'")
            print("  opposite_rebuild_in_progress → 0")
        else:
            cur.execute(
                """
                UPDATE trading_state
                SET 
                    cycle_phase = 'arming',
                    levels_frozen = 0,
                    cycle_id = NULL,
                    structural_cycle_id = NULL,
                    position_state = 'none',
                    close_reason = 'manual_reset_before_new_session',
                    channel_mode = 'two_sided',
                    known_side = 'both',
                    need_rebuild_opposite = 0,
                    opposite_rebuild_deadline_ts = NULL,
                    opposite_rebuild_attempts = 0,
                    opposite_rebuild_in_progress = 0,
                    allow_long_entry = 1,
                    allow_short_entry = 1,
                    last_rebuild_reason = 'manual_reset_before_new_session',
                    last_start_mode = 'manual_reset',
                    last_transition_at = ?,
                    updated_at = ?
                WHERE id = 1
                """,
                (now, now)
            )
            print("✅ trading_state сброшен")

        # Закрыть structural_cycles
        if structural_id:
            print(f"\n{'=' * 80}")
            print(f"ЗАКРЫТИЕ structural_cycle {structural_id[:8]}")
            print(f"{'=' * 80}")

            if args.dry_run:
                print(f"[DRY RUN] structural_cycle был бы закрыт")
            else:
                cur.execute(
                    """
                    UPDATE structural_cycles
                    SET phase = 'closed',
                        cancel_reason = 'manual_reset_before_new_session',
                        updated_at = ?
                    WHERE id = ? AND phase != 'closed'
                    """,
                    (now, structural_id)
                )
                print("✅ structural_cycle закрыт")

        if not args.dry_run:
            conn.commit()
            print(f"\n{'=' * 80}")
            print("✅ ПОЛНЫЙ СБРОЗ ЗАВЕРШЁН")
            print(f"{'=' * 80}")
            print("Теперь можно запускать supervisor с чистого состояния")
        else:
            print(f"\n{'=' * 80}")
            print("[DRY RUN] Ничего не выполнено. Используйте --force для применения")
            print(f"{'=' * 80}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
