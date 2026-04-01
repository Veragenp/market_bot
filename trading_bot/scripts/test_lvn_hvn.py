from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from trading_bot.config.settings import DB_PATH

# ========== CONFIG ==========
SYMBOLS: dict[str, dict[str, Any]] = {
    "BTC": {"symbol": "BTC/USDT", "has_1h": True},
    "SP500": {"symbol": "SP500", "has_1h": False},
}
TEST_DAYS = 90
END_DATE = datetime.now().strftime("%Y-%m-%d")
START_DATE = (datetime.now() - timedelta(days=TEST_DAYS)).strftime("%Y-%m-%d")
LOOKAHEAD_DAYS = 3

# Windows per methodology:
# - global profile: 30 days D1
# - dynamic profile (BTC only): 7 days 1H
GLOBAL_WINDOW_DAYS = 30
DYNAMIC_WINDOW_DAYS = 7

BIN_SIZE_ATR_FACTOR = 4.0
LVN_THRESHOLD = 0.6
LVN_LOOKAHEAD = 3
MIN_TPO_GLOBAL = 1
MIN_TPO_DYNAMIC = 1
WEIGHT_VOL = 0.4
WEIGHT_TPO = 0.3
WEIGHT_LVN = 0.2
WEIGHT_GLOBAL_BONUS = 0.1


def get_atr(cursor: sqlite3.Cursor, symbol: str, date_str: str, period: int = 14) -> Optional[float]:
    ts = int(datetime.strptime(date_str, "%Y-%m-%d").timestamp())
    cursor.execute(
        """
        SELECT high, low, close
        FROM ohlcv
        WHERE symbol = ? AND timeframe = '1d' AND timestamp < ?
        ORDER BY timestamp DESC
        LIMIT ?
        """,
        (symbol, ts, period + 1),
    )
    rows = cursor.fetchall()
    if len(rows) < period + 1:
        return None

    df = pd.DataFrame(rows, columns=["high", "low", "close"]).iloc[::-1].reset_index(drop=True)
    tr = np.maximum(
        df["high"] - df["low"],
        np.abs(df["high"] - df["close"].shift(1)),
        np.abs(df["low"] - df["close"].shift(1)),
    )
    atr = tr.iloc[1 : period + 1].mean()
    if pd.isna(atr) or float(atr) <= 0:
        return None
    return float(atr)


def load_ohlcv(
    cursor: sqlite3.Cursor,
    symbol: str,
    timeframe: str,
    start_ts: int,
    end_ts: int,
) -> pd.DataFrame:
    cursor.execute(
        """
        SELECT timestamp, high, low, close, volume
        FROM ohlcv
        WHERE symbol = ? AND timeframe = ? AND timestamp >= ? AND timestamp < ?
        ORDER BY timestamp ASC
        """,
        (symbol, timeframe, int(start_ts), int(end_ts)),
    )
    rows = cursor.fetchall()
    if not rows:
        return pd.DataFrame(columns=["timestamp", "high", "low", "close", "volume"])
    return pd.DataFrame(rows, columns=["timestamp", "high", "low", "close", "volume"])


def build_volume_profile(df: pd.DataFrame, bin_size: float, min_tpo: int) -> List[Dict[str, float]]:
    if df.empty or bin_size <= 0:
        return []

    min_price = float(df["low"].min())
    max_price = float(df["high"].max())
    if min_price >= max_price:
        return []

    n_bins = int(np.ceil((max_price - min_price) / bin_size))
    n_bins = min(max(n_bins, 1), 200)

    bins: List[Dict[str, float]] = []
    for i in range(n_bins):
        low_bin = min_price + i * bin_size
        high_bin = low_bin + bin_size
        inside = df[(df["close"] >= low_bin) & (df["close"] < high_bin)]
        tpo_count = int(len(inside))
        if tpo_count < min_tpo:
            continue
        vol_sum = float(inside["volume"].fillna(0.0).sum())
        bins.append(
            {
                "price": (low_bin + high_bin) / 2.0,
                "volume": vol_sum,
                "tpo": float(tpo_count),
            }
        )

    if not bins:
        return []

    max_vol = max(b["volume"] for b in bins)
    max_tpo = max(b["tpo"] for b in bins)
    for b in bins:
        b["norm_vol"] = b["volume"] / max_vol if max_vol > 0 else 0.0
        b["norm_tpo"] = b["tpo"] / max_tpo if max_tpo > 0 else 0.0
    return bins


def find_hvn_lvn(
    bins: List[Dict[str, float]],
    lvn_threshold: float,
    lvn_lookahead: int,
) -> List[Dict[str, float]]:
    if len(bins) < 3:
        return []

    volumes = [b["norm_vol"] for b in bins]
    mean_vol = float(np.mean(volumes))
    levels: List[Dict[str, float]] = []

    for i in range(1, len(bins) - 1):
        if not (volumes[i] > volumes[i - 1] and volumes[i] > volumes[i + 1]):
            continue

        lvn_up = 0
        for j in range(1, lvn_lookahead + 1):
            idx = i + j
            if idx >= len(bins):
                break
            if bins[idx]["norm_vol"] < lvn_threshold * mean_vol:
                lvn_up += 1
            else:
                break

        lvn_down = 0
        for j in range(1, lvn_lookahead + 1):
            idx = i - j
            if idx < 0:
                break
            if bins[idx]["norm_vol"] < lvn_threshold * mean_vol:
                lvn_down += 1
            else:
                break

        lvn_width = max(lvn_up, lvn_down)
        if lvn_width < int(lvn_lookahead * 0.6):
            continue

        norm_lvn = lvn_width / float(lvn_lookahead)
        strength = (
            bins[i]["norm_vol"] * WEIGHT_VOL
            + bins[i]["norm_tpo"] * WEIGHT_TPO
            + norm_lvn * WEIGHT_LVN
        )
        levels.append(
            {
                "price": bins[i]["price"],
                "volume_peak": bins[i]["volume"],
                "touch_count": bins[i]["tpo"],
                "lvn_width": float(lvn_width),
                "strength": strength,
            }
        )
    return levels


def evaluate_success(
    cursor: sqlite3.Cursor,
    symbol: str,
    level_date: int,
    level_price: float,
    atr_value: float,
    lookahead_days: int,
) -> Tuple[Optional[int], Optional[float], float]:
    start_ts = int(level_date + 86400)
    end_ts = int(start_ts + lookahead_days * 86400)
    cursor.execute(
        """
        SELECT timestamp, high, low
        FROM ohlcv
        WHERE symbol = ? AND timeframe = '1d' AND timestamp >= ? AND timestamp < ?
        ORDER BY timestamp ASC
        """,
        (symbol, start_ts, end_ts),
    )
    rows = cursor.fetchall()
    if not rows:
        return None, None, 0.0

    hit_ts = None
    hit_price = None
    touch_dir = None
    tolerance = 0.2 * atr_value

    for ts, high, low in rows:
        if abs(float(low) - level_price) <= tolerance:
            hit_ts = int(ts)
            hit_price = float(low)
            touch_dir = "above"
            break
        if abs(float(high) - level_price) <= tolerance:
            hit_ts = int(ts)
            hit_price = float(high)
            touch_dir = "below"
            break

    if hit_ts is None or touch_dir is None:
        return None, None, 0.0

    after_start = hit_ts + 86400
    after_end = after_start + 2 * 86400
    cursor.execute(
        """
        SELECT high, low
        FROM ohlcv
        WHERE symbol = ? AND timeframe = '1d' AND timestamp >= ? AND timestamp < ?
        ORDER BY timestamp ASC
        """,
        (symbol, after_start, after_end),
    )
    after_rows = cursor.fetchall()
    if not after_rows:
        return hit_ts, hit_price, 0.0

    if touch_dir == "above":
        max_high = max(float(r[0]) for r in after_rows)
        reversal = (max_high - level_price) / atr_value
    else:
        min_low = min(float(r[1]) for r in after_rows)
        reversal = (level_price - min_low) / atr_value
    return hit_ts, hit_price, float(max(reversal, 0.0))


def process_symbol(cursor: sqlite3.Cursor, symbol_cfg: dict[str, Any], start_date: str, end_date: str) -> None:
    symbol = str(symbol_cfg["symbol"])
    has_1h = bool(symbol_cfg["has_1h"])
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    date_range = [start_dt + timedelta(days=i) for i in range((end_dt - start_dt).days + 1)]

    for dt in date_range:
        t_ts = int(dt.timestamp())
        t_date = dt.strftime("%Y-%m-%d")
        atr_val = get_atr(cursor, symbol, t_date, period=14)
        if atr_val is None:
            continue
        bin_size = atr_val / BIN_SIZE_ATR_FACTOR

        global_start = t_ts - GLOBAL_WINDOW_DAYS * 86400
        df_global = load_ohlcv(cursor, symbol, "1d", global_start, t_ts)
        if not df_global.empty:
            bins_global = build_volume_profile(df_global, bin_size, MIN_TPO_GLOBAL)
            levels_global = find_hvn_lvn(bins_global, LVN_THRESHOLD, LVN_LOOKAHEAD)
            for lvl in levels_global:
                strength = min(1.0, lvl["strength"] + WEIGHT_GLOBAL_BONUS)
                cursor.execute(
                    """
                    INSERT INTO backtest_levels
                    (symbol, level_date, price, level_type, layer, strength, volume_peak, touch_count, lvn_width, lookahead_days, success)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
                    """,
                    (
                        symbol,
                        t_ts,
                        float(lvl["price"]),
                        "poc",
                        "global",
                        float(strength),
                        float(lvl["volume_peak"]),
                        int(lvl["touch_count"]),
                        int(lvl["lvn_width"]),
                        LOOKAHEAD_DAYS,
                    ),
                )

        if has_1h:
            dynamic_start = t_ts - DYNAMIC_WINDOW_DAYS * 86400
            df_dyn = load_ohlcv(cursor, symbol, "1h", dynamic_start, t_ts)
            if not df_dyn.empty:
                bins_dyn = build_volume_profile(df_dyn, max(bin_size / 1.5, 1e-12), MIN_TPO_DYNAMIC)
                levels_dyn = find_hvn_lvn(bins_dyn, LVN_THRESHOLD, LVN_LOOKAHEAD)
                for lvl in levels_dyn:
                    cursor.execute(
                        """
                        INSERT INTO backtest_levels
                        (symbol, level_date, price, level_type, layer, strength, volume_peak, touch_count, lvn_width, lookahead_days, success)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
                        """,
                        (
                            symbol,
                            t_ts,
                            float(lvl["price"]),
                            "poc",
                            "dynamic",
                            float(min(1.0, lvl["strength"])),
                            float(lvl["volume_peak"]),
                            int(lvl["touch_count"]),
                            int(lvl["lvn_width"]),
                            LOOKAHEAD_DAYS,
                        ),
                    )
    cursor.connection.commit()


def evaluate_all_levels(cursor: sqlite3.Cursor) -> None:
    cursor.execute(
        """
        SELECT id, symbol, level_date, price, lookahead_days
        FROM backtest_levels
        WHERE success IS NULL
        """
    )
    rows = cursor.fetchall()
    for row_id, symbol, level_ts, price, lookahead in rows:
        level_date = datetime.fromtimestamp(int(level_ts)).strftime("%Y-%m-%d")
        atr_val = get_atr(cursor, str(symbol), level_date, period=14)
        if atr_val is None:
            continue
        hit_ts, hit_price, reversal = evaluate_success(
            cursor,
            str(symbol),
            int(level_ts),
            float(price),
            atr_val,
            int(lookahead),
        )
        success = 1 if reversal >= 0.5 else 0
        cursor.execute(
            """
            UPDATE backtest_levels
            SET hit_date = ?, hit_price = ?, reversal_amount = ?, success = ?
            WHERE id = ?
            """,
            (hit_ts, hit_price, reversal, success, int(row_id)),
        )
    cursor.connection.commit()


def print_statistics(conn: sqlite3.Connection) -> None:
    print("\n===== LEVEL SUCCESS STATS =====")
    q1 = """
    SELECT symbol,
           CASE
               WHEN strength < 0.3 THEN 'weak'
               WHEN strength <= 0.6 THEN 'medium'
               ELSE 'strong'
           END AS strength_group,
           COUNT(*) AS total,
           SUM(success) AS hits,
           ROUND(100.0 * SUM(success) / COUNT(*), 1) AS hit_rate
    FROM backtest_levels
    WHERE success IS NOT NULL
    GROUP BY symbol, strength_group
    ORDER BY symbol, strength_group
    """
    df1 = pd.read_sql_query(q1, conn)
    if df1.empty:
        print("No evaluated rows.")
    else:
        print(df1.to_string(index=False))

    q2 = """
    SELECT symbol, layer, COUNT(*) AS total,
           ROUND(100.0 * SUM(success) / COUNT(*), 1) AS hit_rate
    FROM backtest_levels
    WHERE success IS NOT NULL AND symbol = 'BTC/USDT'
    GROUP BY symbol, layer
    """
    df2 = pd.read_sql_query(q2, conn)
    print("\n----- BTC by layer -----")
    print(df2.to_string(index=False) if not df2.empty else "No BTC rows.")

    q3 = """
    SELECT symbol, COUNT(*) AS strong_total,
           ROUND(100.0 * SUM(success) / COUNT(*), 1) AS strong_hit_rate
    FROM backtest_levels
    WHERE success IS NOT NULL AND strength > 0.6
    GROUP BY symbol
    """
    df3 = pd.read_sql_query(q3, conn)
    print("\n----- Strong levels (strength > 0.6) -----")
    print(df3.to_string(index=False) if not df3.empty else "No strong rows.")


def main() -> None:
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS backtest_levels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            level_date INTEGER NOT NULL,
            price REAL NOT NULL,
            level_type TEXT,
            layer TEXT,
            strength REAL,
            volume_peak REAL,
            touch_count INTEGER,
            lvn_width INTEGER,
            hit_date INTEGER,
            hit_price REAL,
            reversal_amount REAL,
            success INTEGER,
            lookahead_days INTEGER
        )
        """
    )
    conn.commit()

    print("Generating levels...")
    for name, cfg in SYMBOLS.items():
        print(f"  Processing {name}...")
        process_symbol(cursor, cfg, START_DATE, END_DATE)

    print("Evaluating levels...")
    evaluate_all_levels(cursor)

    print("Statistics...")
    print_statistics(conn)
    conn.close()
    print("Done. Results stored in backtest_levels.")


if __name__ == "__main__":
    main()
