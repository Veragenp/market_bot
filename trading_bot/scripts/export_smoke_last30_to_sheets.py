from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd

from trading_bot.config.settings import (
    LIQUIDATIONS_AGGREGATE_TIMEFRAMES,
    OI_TIMEFRAMES,
)
from trading_bot.data.db import get_connection
from trading_bot.data.schema import init_db, run_migrations
from trading_bot.tools.sheets_exporter import SheetsExporter

from trading_bot.data.data_loader import DataLoaderManager
from trading_bot.config.symbols import ANALYTIC_SYMBOLS

SHEET_TITLE = os.getenv("MARKET_AUDIT_SHEET_TITLE", "Market Data Audit")
CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH", "credentials.json")
SHEET_URL = os.getenv("MARKET_AUDIT_SHEET_URL")
SHEET_ID = os.getenv("MARKET_AUDIT_SHEET_ID")


def _ts_to_iso_utc(ts: Optional[int]) -> Optional[str]:
    if ts is None:
        return None
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat()


def _read_ohlcv_last(
    *,
    conn,
    symbol: str,
    timeframe: str,
    source: str,
    limit: int = 30,
) -> List[Dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT symbol, timeframe, timestamp, open, high, low, close, volume, source, extra, updated_at
        FROM ohlcv
        WHERE symbol = ? AND timeframe = ? AND ifnull(source, '') = ?
        ORDER BY timestamp DESC
        LIMIT ?
        """,
        (symbol, timeframe, source, int(limit)),
    )
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    # newest->oldest for display: keep ascending
    rows.reverse()
    return rows


def _read_open_interest_last(
    *,
    conn,
    symbol: str,
    timeframe: str,
    source: str,
    limit: int = 30,
) -> List[Dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT symbol, timeframe, timestamp, oi_value, oi_change_24h, exchange, source, updated_at
        FROM open_interest
        WHERE symbol = ? AND timeframe = ? AND ifnull(source, '') = ?
        ORDER BY timestamp DESC
        LIMIT ?
        """,
        (symbol, timeframe, source, int(limit)),
    )
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    rows.reverse()
    return rows


def _read_liquidations_last(
    *,
    conn,
    symbol: str,
    timeframe: str,
    source: str,
    limit: int = 30,
) -> List[Dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT symbol, timeframe, timestamp, long_volume, short_volume, total_volume, exchange, source, updated_at
        FROM liquidations
        WHERE symbol = ? AND timeframe = ? AND ifnull(source, '') = ?
        ORDER BY timestamp DESC
        LIMIT ?
        """,
        (symbol, timeframe, source, int(limit)),
    )
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    rows.reverse()
    return rows


def _export_df(exporter: SheetsExporter, df: pd.DataFrame, worksheet_name: str) -> None:
    exporter.export_dataframe_to_sheet(df, SHEET_TITLE, worksheet_name=worksheet_name)


def main() -> None:
    init_db()
    run_migrations()

    exporter = SheetsExporter(
        credentials_path=CREDENTIALS_PATH,
        spreadsheet_title=SHEET_TITLE,
        spreadsheet_url=SHEET_URL,
        spreadsheet_id=SHEET_ID,
    )

    manager = DataLoaderManager()

    # Smoke-test symbols as per your smoke plan.
    spot_symbol = "BTC/USDT"
    macro_symbol = "SP500"
    indices_symbol = "TOTAL"
    futures_symbol_internal = "BTC/USDT"

    spot_timeframes = ["1m", "4h", "1d", "1w", "1M"]
    macro_timeframes = ["4h", "1d", "1w", "1M"]
    indices_timeframes = ["1d", "1w", "1M"]  # TradingView loader supports these in this repo

    n = 30

    # --- Spot OHLCV (Binance) ---
    df_spot_rows: List[Dict[str, Any]] = []
    for tf in spot_timeframes:
        conn = get_connection()
        source = manager.spot_loader.get_exchange_name()
        rows = _read_ohlcv_last(conn=conn, symbol=spot_symbol, timeframe=tf, source=source, limit=n)
        for r in rows:
            df_spot_rows.append(
                {
                    "symbol": r["symbol"],
                    "timeframe": r["timeframe"],
                    "timestamp_utc": _ts_to_iso_utc(r["timestamp"]),
                    "timestamp": r["timestamp"],
                    "open": r.get("open"),
                    "high": r.get("high"),
                    "low": r.get("low"),
                    "close": r.get("close"),
                    "volume": r.get("volume"),
                    "source": r.get("source"),
                    "updated_at_utc": _ts_to_iso_utc(r.get("updated_at")),
                    "extra": r.get("extra"),
                }
            )
    df_spot = pd.DataFrame(df_spot_rows)
    _export_df(exporter, df_spot, "spot_last30")

    # --- Macro OHLCV (Yahoo) ---
    df_macro_rows: List[Dict[str, Any]] = []
    for tf in macro_timeframes:
        conn = get_connection()
        source = manager.macro_loader.get_exchange_name()
        rows = _read_ohlcv_last(conn=conn, symbol=macro_symbol, timeframe=tf, source=source, limit=n)
        for r in rows:
            df_macro_rows.append(
                {
                    "symbol": r["symbol"],
                    "timeframe": r["timeframe"],
                    "timestamp_utc": _ts_to_iso_utc(r["timestamp"]),
                    "timestamp": r["timestamp"],
                    "open": r.get("open"),
                    "high": r.get("high"),
                    "low": r.get("low"),
                    "close": r.get("close"),
                    "volume": r.get("volume"),
                    "source": r.get("source"),
                    "updated_at_utc": _ts_to_iso_utc(r.get("updated_at")),
                    "extra": r.get("extra"),
                }
            )
    df_macro = pd.DataFrame(df_macro_rows)
    _export_df(exporter, df_macro, "macro_last30")

    # --- Indices OHLCV (TradingView) ---
    df_indices_rows: List[Dict[str, Any]] = []
    for tf in indices_timeframes:
        conn = get_connection()
        source = manager.tv_loader.get_exchange_name()
        rows = _read_ohlcv_last(conn=conn, symbol=indices_symbol, timeframe=tf, source=source, limit=n)
        for r in rows:
            df_indices_rows.append(
                {
                    "symbol": r["symbol"],
                    "timeframe": r["timeframe"],
                    "timestamp_utc": _ts_to_iso_utc(r["timestamp"]),
                    "timestamp": r["timestamp"],
                    "open": r.get("open"),
                    "high": r.get("high"),
                    "low": r.get("low"),
                    "close": r.get("close"),
                    "volume": r.get("volume"),
                    "source": r.get("source"),
                    "updated_at_utc": _ts_to_iso_utc(r.get("updated_at")),
                    "extra": r.get("extra"),
                }
            )
    df_indices = pd.DataFrame(df_indices_rows)
    _export_df(exporter, df_indices, "indices_last30")

    # --- Futures OI (Bybit) ---
    df_oi_rows: List[Dict[str, Any]] = []
    for tf in OI_TIMEFRAMES:
        conn = get_connection()
        source = manager.bybit_loader.get_exchange_name()
        rows = _read_open_interest_last(
            conn=conn,
            symbol=futures_symbol_internal,
            timeframe=tf,
            source=source,
            limit=n,
        )
        for r in rows:
            df_oi_rows.append(
                {
                    "symbol": r["symbol"],
                    "timeframe": r["timeframe"],
                    "timestamp_utc": _ts_to_iso_utc(r["timestamp"]),
                    "timestamp": r["timestamp"],
                    "oi_value": r.get("oi_value"),
                    "oi_change_24h": r.get("oi_change_24h"),
                    "source": r.get("source"),
                    "updated_at_utc": _ts_to_iso_utc(r.get("updated_at")),
                    "exchange": r.get("exchange"),
                }
            )
    df_oi = pd.DataFrame(df_oi_rows)
    _export_df(exporter, df_oi, "bybit_oi_last30")

    # --- Futures Liquidations (Bybit) ---
    df_liq_rows: List[Dict[str, Any]] = []
    for tf in LIQUIDATIONS_AGGREGATE_TIMEFRAMES:
        conn = get_connection()
        source = manager.bybit_loader.get_exchange_name()
        rows = _read_liquidations_last(
            conn=conn,
            symbol=futures_symbol_internal,
            timeframe=tf,
            source=source,
            limit=n,
        )
        for r in rows:
            df_liq_rows.append(
                {
                    "symbol": r["symbol"],
                    "timeframe": r["timeframe"],
                    "timestamp_utc": _ts_to_iso_utc(r["timestamp"]),
                    "timestamp": r["timestamp"],
                    "long_volume": r.get("long_volume"),
                    "short_volume": r.get("short_volume"),
                    "total_volume": r.get("total_volume"),
                    "source": r.get("source"),
                    "updated_at_utc": _ts_to_iso_utc(r.get("updated_at")),
                    "exchange": r.get("exchange"),
                }
            )
    df_liq = pd.DataFrame(df_liq_rows)
    _export_df(exporter, df_liq, "bybit_liquidations_last30")

    # --- Metadata cursors for sync check ---
    meta_rows: List[Dict[str, Any]] = []
    conn = get_connection()
    cur = conn.cursor()
    # We export only relevant subset: these symbols and sources.
    # (metadata can be large; keep it limited.)
    candidates = [
        # spot
        (spot_symbol, tf, manager.spot_loader.get_exchange_name()) for tf in spot_timeframes
    ] + [
        # macro
        (macro_symbol, tf, manager.macro_loader.get_exchange_name()) for tf in macro_timeframes
    ] + [
        # indices
        (indices_symbol, tf, manager.tv_loader.get_exchange_name()) for tf in indices_timeframes
    ] + [
        # oi
        (futures_symbol_internal, tf, manager.bybit_loader.get_exchange_name()) for tf in OI_TIMEFRAMES
    ] + [
        # liquidations
        (futures_symbol_internal, tf, manager.bybit_loader.get_exchange_name()) for tf in LIQUIDATIONS_AGGREGATE_TIMEFRAMES
    ]

    # Our metadata uses special keys with prefixes:
    # - for ohlcv: symbol is actual symbol
    # - for oi: symbol key is `open_interest:{symbol_internal}`
    # - for liquidations: symbol key is `liquidations:{symbol_internal}`
    # So for Bybit parts, we must read prefixed keys.
    # We'll fetch meta for both prefixed keys with `IN`.
    conn.close()

    # Simpler: read all metadata rows for these timeframe/source pairs and matching keys.
    conn = get_connection()
    cur = conn.cursor()

    meta_keys: List[str] = []
    meta_keys.extend([spot_symbol])
    meta_keys.extend([macro_symbol])
    meta_keys.extend([indices_symbol])
    meta_keys.extend([f"open_interest:{futures_symbol_internal}", f"liquidations:{futures_symbol_internal}"])

    placeholders = ",".join(["?"] * len(meta_keys))
    cur.execute(
        f"""
        SELECT symbol, timeframe, source, last_updated, last_full_update, last_cleaned_1m
        FROM metadata
        WHERE symbol IN ({placeholders})
        ORDER BY symbol, timeframe, source
        """,
        tuple(meta_keys),
    )
    for r in cur.fetchall():
        # sqlite3.Row supports dict-style access but not `.get()`
        lu = r["last_updated"]
        lfu = r["last_full_update"]
        lcu = r["last_cleaned_1m"]
        meta_rows.append(
            {
                "symbol": r["symbol"],
                "timeframe": r["timeframe"],
                "source": r["source"],
                "last_updated_utc": _ts_to_iso_utc(lu),
                "last_updated": lu,
                "last_full_update_utc": _ts_to_iso_utc(lfu),
                "last_full_update": lfu,
                "last_cleaned_1m_utc": _ts_to_iso_utc(lcu),
                "last_cleaned_1m": lcu,
            }
        )
    conn.close()

    df_meta = pd.DataFrame(meta_rows)
    _export_df(exporter, df_meta, "metadata_cursors")


if __name__ == "__main__":
    main()

