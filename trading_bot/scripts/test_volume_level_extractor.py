from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

import pandas as pd

from trading_bot.analytics.volume_profile import extract_volume_levels, mark_ultra_strong
from trading_bot.config.settings import DB_PATH

ASSET_CONFIG = {
    "BTC": {"symbol": "BTC/USDT", "tick_size": 1.0, "profile_step": 10.0},
    "SP500": {"symbol": "SP500", "tick_size": 0.25, "profile_step": 0.5},
}


def load_minute_df(conn: sqlite3.Connection, symbol: str, days: int = 120) -> pd.DataFrame:
    end_ts = int(datetime.now(timezone.utc).timestamp())
    start_ts = end_ts - days * 86400
    df = pd.read_sql_query(
        """
        SELECT timestamp, open, high, low, close, volume
        FROM ohlcv
        WHERE symbol = ? AND timeframe = '1m'
          AND timestamp >= ? AND timestamp <= ?
        ORDER BY timestamp
        """,
        conn,
        params=(symbol, start_ts, end_ts),
    )
    return df


def run_for_symbol(conn: sqlite3.Connection, name: str, symbol: str, profile_step: float) -> pd.DataFrame:
    df = load_minute_df(conn, symbol, days=120)
    if df.empty:
        print(f"[WARN] {name}: no 1m data in DB.")
        return pd.DataFrame()

    global_levels = extract_volume_levels(
        df,
        period_days=90,
        step_size=profile_step,
        top_n=12,
        horizon="global",
    )
    local_levels = extract_volume_levels(
        df,
        period_days=14,
        step_size=profile_step,
        top_n=12,
        horizon="local",
    )
    merged = mark_ultra_strong(global_levels, local_levels, tolerance_pct=0.001)
    merged.insert(0, "asset", name)
    merged.insert(1, "symbol", symbol)
    return merged


def main() -> None:
    conn = sqlite3.connect(DB_PATH)
    all_rows = []
    for asset, cfg in ASSET_CONFIG.items():
        out = run_for_symbol(conn, asset, cfg["symbol"], cfg["profile_step"])
        if not out.empty:
            all_rows.append(out)
    conn.close()

    if not all_rows:
        print("No extracted levels. Check 1m data availability.")
        return

    result = pd.concat(all_rows, ignore_index=True)
    result = result.sort_values(["asset", "score"], ascending=[True, False])
    print(result.to_string(index=False))


if __name__ == "__main__":
    main()
