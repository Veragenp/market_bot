import os
import sqlite3
import pytest
from trading_bot.data.db import get_connection, DB_PATH
from trading_bot.data.schema import init_db, run_migrations

@pytest.fixture
def clean_db():
    """Фикстура для тестов: удаляет БД перед тестом, инициализирует, после теста удаляет."""
    # Удаляем БД, если она существует
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    init_db()
    run_migrations()
    yield
    # После теста удаляем БД
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

def test_tables_exist(clean_db):
    """Проверяет, что все таблицы созданы."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = {row[0] for row in cursor.fetchall()}
    expected = {
        "ohlcv",
        "liquidations",
        "open_interest",
        "metadata",
        "db_version",
        "instruments",
        "trade_levels",
        "level_hits",
        "trade_decisions",
    }
    assert expected.issubset(tables)
    conn.close()

def test_indices_exist(clean_db):
    """Проверяет, что все индексы созданы."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='index';")
    indices = {row[0] for row in cursor.fetchall()}
    expected = {
        "idx_ohlcv_unique",
        "idx_ohlcv_symbol_timeframe_timestamp",
        "idx_ohlcv_timeframe_timestamp",
        "idx_ohlcv_source",
        "idx_liquidations_symbol_timeframe_timestamp",
        "idx_liquidations_source",
        "idx_oi_symbol_timeframe_timestamp",
        "idx_oi_source",
        "idx_metadata_source",
        "idx_instruments_exchange",
    }
    assert expected.issubset(indices)
    conn.close()

def test_wal_enabled(clean_db):
    """Проверяет, что включён WAL-режим."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode;")
    mode = cursor.fetchone()[0]
    assert mode == 'wal'
    conn.close()