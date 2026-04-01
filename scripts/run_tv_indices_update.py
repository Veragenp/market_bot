"""Проверка .env + разовое обновление индексов TradingView."""

from __future__ import annotations

import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import sqlite3

from trading_bot.config.settings import (
    BASE_DIR,
    DB_PATH,
    TRADINGVIEW_PASSWORD,
    TRADINGVIEW_USERNAME,
)
from trading_bot.data.collectors import update_indices


def main() -> None:
    env_root = os.path.join(BASE_DIR, ".env")
    env_tb = os.path.join(BASE_DIR, "trading_bot", ".env")
    print(f"BASE_DIR: {BASE_DIR}")
    print(f"trading_bot/.env exists: {os.path.isfile(env_tb)}")
    print(f"root .env exists: {os.path.isfile(env_root)}")
    u_ok = bool(TRADINGVIEW_USERNAME and TRADINGVIEW_USERNAME.strip())
    p_ok = bool(TRADINGVIEW_PASSWORD and TRADINGVIEW_PASSWORD.strip())
    print(f"TRADINGVIEW_USERNAME set: {u_ok}")
    print(f"TRADINGVIEW_PASSWORD set: {p_ok}")

    print("Running update_indices(days_back=30)...")
    update_indices(days_back=30)

    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        "SELECT source, COUNT(*) FROM ohlcv GROUP BY source ORDER BY source"
    ).fetchall()
    conn.close()
    print("ohlcv by source:", rows)
    tv = next((c for s, c in rows if s == "tradingview"), 0)
    print(f"tradingview rows: {tv}")


if __name__ == "__main__":
    main()
