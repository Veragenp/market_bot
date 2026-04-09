import sqlite3
import pytest
from trading_bot.data.db import get_connection
from trading_bot.data.schema import init_db, run_migrations


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
        "price_levels",
        "market_context",
        "level_events",
        "structural_cycles",
        "structural_cycle_symbols",
        "structural_events",
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
        "idx_pl_symbol_active",
        "idx_pl_symbol_layer",
        "idx_pl_symbol_type_status",
        "idx_pl_stable_level_id",
        "idx_pl_strength",
        "idx_pl_tier",
        "idx_mc_timestamp",
        "idx_tl_symbol_status",
        "idx_tl_status_gen",
        "idx_tl_price_level",
        "idx_le_touch",
        "idx_le_symbol_touch",
        "idx_le_stable",
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


def test_instruments_has_atr_column(clean_db):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(instruments)")
    cols = {row[1] for row in cursor.fetchall()}
    assert "atr" in cols
    conn.close()