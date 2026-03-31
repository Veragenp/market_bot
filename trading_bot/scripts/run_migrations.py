"""Применить миграции SQLite (схема в src/database/schema.py)."""


def main() -> None:
    from trading_bot.data.schema import init_db, run_migrations

    init_db()
    run_migrations()
    print("Migrations applied.")


if __name__ == "__main__":
    main()
