from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List

import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from config import (
    ANALYTIC_SYMBOLS,
    FILL_MISSING_WEEKENDS,
    SOURCE_BINANCE,
    TRADING_SYMBOLS,
)
from trading_bot.data.db import get_connection
from trading_bot.data.schema import init_db
from trading_bot.data.repositories import get_ohlcv_filled
from trading_bot.tools.sheets_exporter import SheetsExporter

SHEET_TITLE = os.getenv("MARKET_AUDIT_SHEET_TITLE", "Market Data Audit")
CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH", "credentials.json")
SHEET_URL = os.getenv("MARKET_AUDIT_SHEET_URL")
SHEET_ID = os.getenv("MARKET_AUDIT_SHEET_ID")


def _ts_to_iso_utc(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat()


def _fetch_ohlcv_sample(
    symbols: Iterable[str],
    timeframes: Iterable[str],
    limit: int,
    fill_weekends: bool = False,
) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for symbol in symbols:
        for timeframe in timeframes:
            if fill_weekends and timeframe == "1d":
                candles = get_ohlcv_filled(
                    symbol=symbol,
                    timeframe=timeframe,
                    limit=limit,
                    fill_weekends=True,
                )
            else:
                conn = get_connection()
                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT symbol, timeframe, timestamp, open, high, low, close, volume, source, extra, updated_at
                    FROM ohlcv
                    WHERE symbol = ? AND timeframe = ?
                    ORDER BY timestamp DESC
                    LIMIT ?
                    """,
                    (symbol, timeframe, limit),
                )
                candles = [dict(r) for r in cur.fetchall()]
                conn.close()
                candles.reverse()

            for c in candles:
                is_synthetic_fill = fill_weekends and timeframe == "1d" and c.get("open") is None
                rows.append(
                    {
                        "symbol": symbol,
                        "timeframe": timeframe,
                        "timestamp_utc": _ts_to_iso_utc(int(c["timestamp"])),
                        "open": c.get("open"),
                        "high": c.get("high"),
                        "low": c.get("low"),
                        "close": None if is_synthetic_fill else c.get("close"),
                        "close_filled": c.get("close"),
                        "volume": c.get("volume"),
                        "extra": c.get("extra"),
                        "source": c.get("source") or SOURCE_BINANCE,
                        "updated_at": c.get("updated_at"),
                    }
                )
    return pd.DataFrame(rows)


def _fetch_coinglass_sample(limit: int = 50) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    conn = get_connection()
    cur = conn.cursor()

    for symbol in TRADING_SYMBOLS:
        cur.execute(
            """
            SELECT symbol, timeframe, timestamp, long_volume, short_volume, total_volume, updated_at
            FROM liquidations
            WHERE symbol = ? AND timeframe = '4h'
            ORDER BY timestamp DESC
            LIMIT ?
            """,
            (symbol, limit),
        )
        liq = [dict(r) for r in cur.fetchall()]
        liq.reverse()
        for r in liq:
            rows.append(
                {
                    "symbol": symbol,
                    "timeframe": "4h",
                    "timestamp_utc": _ts_to_iso_utc(int(r["timestamp"])),
                    "open": None,
                    "high": None,
                    "low": None,
                    "close": None,
                    "volume": r.get("total_volume"),
                    "extra": json.dumps(
                        {
                            "long_volume": r.get("long_volume"),
                            "short_volume": r.get("short_volume"),
                        },
                        ensure_ascii=True,
                    ),
                    "source": r.get("exchange") or SOURCE_BINANCE,
                    "updated_at": r.get("updated_at"),
                }
            )

        cur.execute(
            """
            SELECT symbol, timeframe, timestamp, oi_value, oi_change_24h, updated_at
            FROM open_interest
            WHERE symbol = ? AND timeframe = '4h'
            ORDER BY timestamp DESC
            LIMIT ?
            """,
            (symbol, limit),
        )
        oi = [dict(r) for r in cur.fetchall()]
        oi.reverse()
        for r in oi:
            rows.append(
                {
                    "symbol": symbol,
                    "timeframe": "4h",
                    "timestamp_utc": _ts_to_iso_utc(int(r["timestamp"])),
                    "open": None,
                    "high": None,
                    "low": None,
                    "close": r.get("oi_value"),
                    "volume": None,
                    "extra": json.dumps(
                        {"oi_change_24h": r.get("oi_change_24h")},
                        ensure_ascii=True,
                    ),
                    "source": r.get("exchange") or SOURCE_BINANCE,
                    "updated_at": r.get("updated_at"),
                }
            )
    conn.close()
    return pd.DataFrame(rows)


def _build_audit_log(entries: List[Dict[str, Any]]) -> pd.DataFrame:
    return pd.DataFrame(entries, columns=["source", "worksheet", "rows", "last_exported_at_utc"])


def _fetch_indices_agg_sample(limit: int = 200) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    symbols = ["TOTAL", "TOTAL2", "TOTAL3", "BTCD"]
    conn = get_connection()
    cur = conn.cursor()
    for symbol in symbols:
        cur.execute(
            """
            SELECT symbol, timeframe, timestamp, open, high, low, close, volume, source, extra, updated_at
            FROM ohlcv
            WHERE symbol = ? AND source = 'coingecko_agg'
            ORDER BY timestamp DESC
            LIMIT ?
            """,
            (symbol, limit),
        )
        data = [dict(r) for r in cur.fetchall()]
        data.reverse()
        for c in data:
            rows.append(
                {
                    "symbol": c.get("symbol"),
                    "timeframe": c.get("timeframe"),
                    "timestamp_utc": _ts_to_iso_utc(int(c["timestamp"])),
                    "open": c.get("open"),
                    "high": c.get("high"),
                    "low": c.get("low"),
                    "close": c.get("close"),
                    "close_filled": c.get("close"),
                    "volume": c.get("volume"),
                    "extra": c.get("extra"),
                    "source": c.get("source"),
                    "updated_at": c.get("updated_at"),
                }
            )
    conn.close()
    return pd.DataFrame(rows)


def main() -> None:
    init_db()
    exporter = SheetsExporter(
        credentials_path=CREDENTIALS_PATH,
        spreadsheet_title=SHEET_TITLE,
        spreadsheet_url=SHEET_URL,
        spreadsheet_id=SHEET_ID,
    )
    exported_at = datetime.now(timezone.utc).isoformat()
    audit_entries: List[Dict[str, Any]] = []

    crypto_symbols = sorted(set(TRADING_SYMBOLS + ANALYTIC_SYMBOLS.get("crypto", [])))
    df_binance = _fetch_ohlcv_sample(
        symbols=crypto_symbols,
        timeframes=["1h", "4h", "1d", "1w", "1M"],
        limit=100,
    )
    exporter.export_dataframe_to_sheet(df_binance, SHEET_TITLE, "binance_ohlcv_sample")
    audit_entries.append(
        {
            "source": SOURCE_BINANCE,
            "worksheet": "binance_ohlcv_sample",
            "rows": len(df_binance),
            "last_exported_at_utc": exported_at,
        }
    )

    df_macro = _fetch_ohlcv_sample(
        symbols=ANALYTIC_SYMBOLS.get("macro", []),
        timeframes=["4h", "1d", "1w", "1M"],
        limit=100,
        fill_weekends=FILL_MISSING_WEEKENDS,
    )
    exporter.export_dataframe_to_sheet(df_macro, SHEET_TITLE, "macro_sample")
    audit_entries.append(
        {
            "source": "macro",
            "worksheet": "macro_sample",
            "rows": len(df_macro),
            "last_exported_at_utc": exported_at,
        }
    )

    df_indices = _fetch_ohlcv_sample(
        symbols=ANALYTIC_SYMBOLS.get("indices", []),
        timeframes=["1m", "4h", "1d", "1w", "1M"],
        limit=100,
    )
    exporter.export_dataframe_to_sheet(df_indices, SHEET_TITLE, "indices_sample")
    audit_entries.append(
        {
            "source": "indices",
            "worksheet": "indices_sample",
            "rows": len(df_indices),
            "last_exported_at_utc": exported_at,
        }
    )

    df_indices_agg = _fetch_indices_agg_sample(limit=200)
    exporter.export_dataframe_to_sheet(df_indices_agg, SHEET_TITLE, "indices_agg_sample")
    audit_entries.append(
        {
            "source": "coingecko_agg",
            "worksheet": "indices_agg_sample",
            "rows": len(df_indices_agg),
            "last_exported_at_utc": exported_at,
        }
    )

    df_coinglass = _fetch_coinglass_sample(limit=50)
    exporter.export_dataframe_to_sheet(df_coinglass, SHEET_TITLE, "coinglass_sample")
    audit_entries.append(
        {
            "source": "binance_futures",
            "worksheet": "coinglass_sample",
            "rows": len(df_coinglass),
            "last_exported_at_utc": exported_at,
        }
    )

    df_log = _build_audit_log(audit_entries)
    exporter.export_dataframe_to_sheet(df_log, SHEET_TITLE, "audit_log")


if __name__ == "__main__":
    main()
