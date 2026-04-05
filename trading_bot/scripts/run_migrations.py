"""Применить миграции SQLite (`trading_bot/data/schema.py`)."""


def main() -> None:
    from trading_bot.config.settings import DB_PATH
    from trading_bot.data.db import get_connection
    from trading_bot.data.schema import init_db, run_migrations

    init_db()
    run_migrations()
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT MAX(version) FROM db_version")
    ver = cur.fetchone()[0]
    conn.close()
    print("Migrations applied. DB:", DB_PATH, "db_version:", ver)


if __name__ == "__main__":
    main()
