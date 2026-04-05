"""
Сводка: откуда работает проект, какой файл БД, есть ли .env, миграции и проверки целостности.

Запуск (из любого cwd, пакет на PYTHONPATH):

  python -m trading_bot.scripts.data_foundation_status
  python -m trading_bot.scripts.data_foundation_status --strict

Код выхода: 0 если все обязательные проверки integrity прошли, иначе 1.
См. также: `python -m trading_bot.scripts.verify_db`, `trading_bot/entrypoints/healthcheck_data.py`.
"""

from __future__ import annotations

import argparse
import os
import sys


def _fmt_mb(path: str) -> str:
    try:
        n = os.path.getsize(path)
        return f"{n / (1024 * 1024):.2f} MiB ({n} bytes)"
    except OSError as e:
        return f"(нет доступа: {e})"


def main() -> int:
    parser = argparse.ArgumentParser(description="Пути, БД, .env и проверки данных")
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Строгий режим integrity (свежесть 1m и т.д.), как check_db_integrity --strict",
    )
    args = parser.parse_args()

    from trading_bot.config.settings import (
        BASE_DIR,
        DATA_DIR,
        DB_PATH,
        ENTRYPOINTS_DIR,
        REPO_ROOT,
        TRADING_BOT_DIR,
    )
    from trading_bot.config.symbols import TRADING_SYMBOLS
    from trading_bot.data.db import get_connection
    from trading_bot.data.db_integrity import run_db_integrity_checks
    from trading_bot.data.schema import init_db, run_migrations

    env_tb = os.path.join(TRADING_BOT_DIR, ".env")
    env_repo = os.path.join(REPO_ROOT, ".env")

    print("=== Где «живёт» проект ===")
    print("  Пакет приложения (код, БД по умолчанию): TRADING_BOT_DIR =", TRADING_BOT_DIR)
    print("  То же как BASE_DIR в settings:           BASE_DIR        =", BASE_DIR)
    print("  Корень git-репо (config.py, tests):      REPO_ROOT       =", REPO_ROOT)
    print("  Точки входа (load_all_data, …):          ENTRYPOINTS_DIR =", ENTRYPOINTS_DIR)
    print("  Каталог данных SQLite:                   DATA_DIR        =", DATA_DIR)
    print("  Файл базы:                               DB_PATH         =", DB_PATH)
    print()
    print("  Импорт: `from trading_bot…` — на PYTHONPATH должен быть REPO_ROOT (родитель пакета).")
    print("  Для subprocess (Sheets и т.д.) cwd часто REPO_ROOT (credentials.json в корне репо).")
    print("  MARKET_BOT_DB_PATH / MARKET_BOT_DATA_DIR переопределяют только файл БД.")
    print()

    print("=== Файл БД ===")
    if os.path.isfile(DB_PATH):
        print("  существует: да, размер:", _fmt_mb(DB_PATH))
    else:
        print("  существует: нет (после init_db появится при первом подключении)")
    print()

    print("=== .env (наличие файлов, без содержимого) ===")
    print("  trading_bot/.env :", "да" if os.path.isfile(env_tb) else "нет", "|", env_tb)
    print("  REPO_ROOT/.env   :", "да" if os.path.isfile(env_repo) else "нет", "|", env_repo)
    extra = (os.environ.get("MARKET_BOT_ENV_PATH") or "").strip()
    if extra:
        print("  MARKET_BOT_ENV_PATH:", "да" if os.path.isfile(extra) else "нет (путь задан)", "|", extra)
    print()

    print("=== Конфиг символов ===")
    print("  TRADING_SYMBOLS:", len(TRADING_SYMBOLS), "шт.")
    print()

    init_db()
    run_migrations()
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT MAX(version) FROM db_version")
    print("=== Схема ===")
    print("  db_version (миграции):", cur.fetchone()[0])
    print()

    print("=== Объём ключевых таблиц ===")
    for table in (
        "ohlcv",
        "metadata",
        "open_interest",
        "liquidations",
        "instruments",
        "price_levels",
        "level_events",
    ):
        try:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            print(f"  {table:16}", cur.fetchone()[0])
        except Exception as e:
            print(f"  {table:16}", f"(ошибка: {e})")

    cur.execute(
        """
        SELECT COUNT(DISTINCT symbol) FROM instruments
        WHERE exchange = 'bybit_futures' AND atr IS NOT NULL AND atr > 0
        """
    )
    n_atr_sym = int(cur.fetchone()[0])
    print()
    print("=== Сверка с TRADING_SYMBOLS (инструменты + ATR) ===")
    print(f"  символов с atr>0 (bybit_futures): {n_atr_sym} / {len(TRADING_SYMBOLS)} ожидается после load_all_data + ATR job")
    conn.close()
    print()

    print("=== Проверки целостности (db_integrity) ===")
    results, ok = run_db_integrity_checks(strict=args.strict)
    for r in results:
        tag = "OK  " if r.ok else "FAIL"
        req = "[обяз.]" if r.required else "[предупр.]"
        print(f"  {tag} {req} {r.name}: {r.detail}")
    print()
    if ok:
        print("Итог: обязательные проверки пройдены.")
        return 0
    print("Итог: есть провал обязательных проверок — см. выше и `python -m trading_bot.scripts.check_db_integrity`.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
