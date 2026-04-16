"""
Быстрая проверка: что записано в БД после structural scan.

Запуск:
  PYTHONPATH=. python -m trading_bot.scripts.check_structural_db

Показывает:
- Активный cycle_id
- Сколько уровней в structural_cycle_symbols
- Сколько уровней в cycle_levels
- Пример уровней
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

        # Текущее состояние trading_state
        ts = cur.execute("SELECT * FROM trading_state WHERE id = 1").fetchone()
        print("=" * 80)
        print("ТЕКУЩЕЕ СОСТОЯНИЕ (trading_state)")
        print("=" * 80)
        print(f"cycle_id:           {ts['cycle_id']}")
        print(f"structural_cycle_id: {ts['structural_cycle_id']}")
        print(f"cycle_phase:        {ts['cycle_phase']}")
        print(f"levels_frozen:      {ts['levels_frozen']}")
        print(f"position_state:     {ts['position_state']}")

        # Последний structural_cycle
        cycle = cur.execute(
            "SELECT * FROM structural_cycles ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        if cycle:
            print("\n" + "=" * 80)
            print("ПОСЛЕДНИЙ structural_cycle")
            print("=" * 80)
            print(f"id:                 {cycle['id']}")
            print(f"phase:              {cycle['phase']}")
            print(f"symbols_valid_count: {cycle['symbols_valid_count']}")
            print(f"created_at:         {cycle['created_at']}")

            # Уровни в structural_cycle_symbols
            symbols_count = cur.execute(
                "SELECT COUNT(*) as c FROM structural_cycle_symbols WHERE cycle_id = ?",
                (cycle['id'],)
            ).fetchone()["c"]
            print(f"structural_cycle_symbols rows: {symbols_count}")

            # LONG и SHORT
            long_count = cur.execute(
                "SELECT COUNT(*) as c FROM structural_cycle_symbols WHERE cycle_id = ? AND level_below_id IS NOT NULL",
                (cycle['id'],)
            ).fetchone()["c"]
            short_count = cur.execute(
                "SELECT COUNT(*) as c FROM structural_cycle_symbols WHERE cycle_id = ? AND level_above_id IS NOT NULL",
                (cycle['id'],)
            ).fetchone()["c"]
            print(f"  LONG levels:  {long_count}")
            print(f"  SHORT levels: {short_count}")

            # Пример уровней
            print("\nПример LONG уровней:")
            rows = cur.execute(
                """
                SELECT symbol, L_price, tier_below, volume_peak_below, atr, ref_price_ws
                FROM structural_cycle_symbols
                WHERE cycle_id = ? AND level_below_id IS NOT NULL
                ORDER BY volume_peak_below DESC
                LIMIT 5
                """,
                (cycle['id'],)
            ).fetchall()
            for r in rows:
                dist = (r['ref_price_ws'] - r['L_price']) / r['atr'] if r['atr'] else 0
                print(f"  {r['symbol']:12} L={r['L_price']:10.4f} tier={r['tier_below']:15} vol={r['volume_peak_below']:12.2f} dist={dist:.2f} ATR")

            print("\nПример SHORT уровней:")
            rows = cur.execute(
                """
                SELECT symbol, U_price, tier_above, volume_peak_above, atr, ref_price_ws
                FROM structural_cycle_symbols
                WHERE cycle_id = ? AND level_above_id IS NOT NULL
                ORDER BY volume_peak_above DESC
                LIMIT 5
                """,
                (cycle['id'],)
            ).fetchrows()
            for r in rows:
                dist = (r['U_price'] - r['ref_price_ws']) / r['atr'] if r['atr'] else 0
                print(f"  {r['symbol']:12} U={r['U_price']:10.4f} tier={r['tier_above']:15} vol={r['volume_peak_above']:12.2f} dist={dist:.2f} ATR")

        # Уровни в cycle_levels (если есть замороженный цикл)
        if ts['cycle_id']:
            levels_count = cur.execute(
                "SELECT COUNT(*) as c FROM cycle_levels WHERE cycle_id = ?",
                (ts['cycle_id'],)
            ).fetchone()["c"]
            print("\n" + "=" * 80)
            print(f"ЦИКЛ УРОВНЕЙ (cycle_levels) для cycle_id={ts['cycle_id']}")
            print("=" * 80)
            print(f"Всего уровней: {levels_count}")

            long_levels = cur.execute(
                "SELECT COUNT(*) as c FROM cycle_levels WHERE cycle_id = ? AND direction = 'long'",
                (ts['cycle_id'],)
            ).fetchone()["c"]
            short_levels = cur.execute(
                "SELECT COUNT(*) as c FROM cycle_levels WHERE cycle_id = ? AND direction = 'short'",
                (ts['cycle_id'],)
            ).fetchone()["c"]
            print(f"  LONG: {long_levels}")
            print(f"  SHORT: {short_levels}")

            if levels_count > 0:
                print("\nПример уровней:")
                rows = cur.execute(
                    """
                    SELECT symbol, direction, level_price, tier, volume_peak, distance_atr
                    FROM cycle_levels
                    WHERE cycle_id = ?
                    ORDER BY direction, volume_peak DESC
                    LIMIT 10
                    """,
                    (ts['cycle_id'],)
                ).fetchall()
                for r in rows:
                    print(f"  {r['symbol']:12} {r['direction']:6} price={r['level_price']:12.4f} tier={r['tier']:15} vol={r['volume_peak']:12.2f} dist={r['distance_atr']:.2f} ATR")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
