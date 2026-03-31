"""SQLite connection for the data layer (single source of truth)."""

from __future__ import annotations

import os
import sqlite3

from trading_bot.config.settings import DATA_DIR, DB_PATH

__all__ = ["get_connection", "DATA_DIR", "DB_PATH"]


def get_connection() -> sqlite3.Connection:
    """Возвращает соединение с SQLite, создавая папку data при необходимости и включая WAL."""
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.row_factory = sqlite3.Row
    return conn
