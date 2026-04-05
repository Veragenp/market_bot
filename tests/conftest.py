import os

import pytest

from trading_bot.data.schema import init_db, run_migrations


@pytest.fixture
def clean_db(monkeypatch, tmp_path):
    """
    Изолированная SQLite в tmp. Обязательно патчить trading_bot.data.db.DB_PATH:
    никогда не удалять os.remove(settings.DB_PATH) в тестах — это рабочая market_data.db.
    """
    db_path = str(tmp_path / "pytest_market.db")
    monkeypatch.setattr("trading_bot.data.db.DB_PATH", db_path)
    if os.path.exists(db_path):
        os.remove(db_path)
    init_db()
    run_migrations()
    yield
    if os.path.exists(db_path):
        try:
            os.remove(db_path)
        except OSError:
            pass
