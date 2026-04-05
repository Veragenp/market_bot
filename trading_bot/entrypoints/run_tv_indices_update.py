"""Проверка .env + разовое обновление индексов TradingView."""

from __future__ import annotations

import os
import sys

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import sqlite3

from trading_bot.config.settings import (
    DB_PATH,
    REPO_ROOT,
    TRADING_BOT_DIR,
    TRADINGVIEW_PASSWORD,
    TRADINGVIEW_USERNAME,
)
from trading_bot.data.collectors import update_indices


def main() -> None:
    env_repo = os.path.join(REPO_ROOT, ".env")
    env_tb = os.path.join(TRADING_BOT_DIR, ".env")
    print(f"REPO_ROOT: {REPO_ROOT}")
    print(f"TRADING_BOT_DIR: {TRADING_BOT_DIR}")
    print(f"trading_bot/.env exists: {os.path.isfile(env_tb)}")
    print(f"repo root .env exists: {os.path.isfile(env_repo)}")
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
