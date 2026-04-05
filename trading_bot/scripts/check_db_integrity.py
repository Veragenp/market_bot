"""
Проверки целостности и минимального наполнения БД.

  PYTHONPATH=. python -m trading_bot.scripts.check_db_integrity
  PYTHONPATH=. python -m trading_bot.scripts.check_db_integrity --strict  # exit 1 при провале

Рекомендуется после смены пути к файлу, миграций или trading_bot/entrypoints/load_all_data.py.
"""

from __future__ import annotations

import argparse
import sys

from trading_bot.data.db_integrity import run_db_integrity_checks


def main() -> int:
    parser = argparse.ArgumentParser(description="Проверки SQLite (схема, ссылки, минимум данных)")
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Требовать свежие 1m и покрытие spot 1d; иначе только критичные проверки",
    )
    args = parser.parse_args()

    results, ok = run_db_integrity_checks(strict=args.strict)
    for r in results:
        status = "OK " if r.ok else "FAIL"
        req = "[required]" if r.required else "[warn]  "
        print(f"{status} {req} {r.name}: {r.detail}")

    if not ok:
        print("\nOne or more required checks failed.")
        return 1
    print("\nAll required checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
