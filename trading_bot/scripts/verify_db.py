"""
Проверка пути к БД, версии миграций и объёма ключевых таблиц.

  python -m trading_bot.scripts.verify_db

Полная сводка (пути, .env, integrity): `python -m trading_bot.scripts.data_foundation_status`
"""

from __future__ import annotations

import os

from trading_bot.config.settings import BASE_DIR, DATA_DIR, DB_PATH, REPO_ROOT, TRADING_BOT_DIR
from trading_bot.data.db import get_connection
from trading_bot.data.schema import init_db, run_migrations


def main() -> None:
    print("TRADING_BOT_DIR:", TRADING_BOT_DIR)
    print("BASE_DIR (=TB) :", BASE_DIR)
    print("REPO_ROOT      :", REPO_ROOT)
    print("DATA_DIR       :", DATA_DIR)
    print("DB_PATH        :", DB_PATH)
    if os.path.isfile(DB_PATH):
        print("DB file size   :", os.path.getsize(DB_PATH), "bytes")
    init_db()
    run_migrations()
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT MAX(version) FROM db_version")
    print("db_version     :", cur.fetchone()[0])
    for table in (
        "ohlcv",
        "open_interest",
        "liquidations",
        "price_levels",
        "instruments",
        "metadata",
        "level_events",
    ):
        try:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            print(f"rows {table:14}:", cur.fetchone()[0])
        except Exception as e:
            print(f"rows {table:14}: (error: {e})")
    conn.close()


if __name__ == "__main__":
    main()
