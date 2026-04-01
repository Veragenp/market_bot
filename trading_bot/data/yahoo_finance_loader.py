"""Yahoo Finance loader (макро OHLCV).

Поддерживает:
- '1d', '1w', '1M' — напрямую через провайдер `YFinanceClient`.
- '4h' — загрузка '1h' и ресемплинг в '4H' (UTC сетка).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from trading_bot.config.settings import (
    SOURCE_YFINANCE,
    MAX_BARS_PER_REQUEST_YFINANCE_1H,
    YFINANCE_TIMEZONE,
    YFINANCE_TICKERS,
)
from trading_bot.data.base_loader import BaseDataLoader
from trading_bot.provider.clients.yfinance import YFinanceClient


class YahooFinanceDataLoader(BaseDataLoader):
    def __init__(
        self,
        ticker_map: Optional[Dict[str, str]] = None,
        timezone_str: str = YFINANCE_TIMEZONE,
    ) -> None:
        self._client = YFinanceClient(ticker_map=ticker_map or YFINANCE_TICKERS, timezone=timezone_str)
        self._source = SOURCE_YFINANCE
        try:
            import yfinance as yf  # type: ignore
        except ImportError as exc:
            raise RuntimeError("yfinance is required for YahooFinanceDataLoader. Install with `pip install yfinance`.") from exc
        self._yf = yf

    def get_exchange_name(self) -> str:
        return self._source

    def _ts_to_utc_dt(self, ts: Optional[int]) -> Optional[datetime]:
        if ts is None:
            return None
        return datetime.fromtimestamp(int(ts), tz=timezone.utc)

    @retry(
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=1, min=2, max=15),
        retry=retry_if_exception_type((ConnectionError, TimeoutError, OSError)),
        reraise=True,
    )
    def _download_1h(self, ticker: str, start_ts: int, end_ts: int) -> pd.DataFrame:
        # yfinance принимает datetime/строки.
        start_dt = self._ts_to_utc_dt(start_ts)
        end_dt = self._ts_to_utc_dt(end_ts)
        return self._yf.download(
            ticker,
            start=start_dt,
            end=end_dt,
            interval="1h",
            progress=False,
        )

    def _records_from_resampled_4h(self, df_4h: pd.DataFrame) -> List[Dict[str, Any]]:
        # df_4h ожидаем в колонках Open/High/Low/Close/Volume и индексе UTC.
        df_4h = df_4h.dropna(subset=["Close"])
        records: List[Dict[str, Any]] = []
        for ts, row in df_4h.iterrows():
            v = row["Volume"] if "Volume" in row.index else 0.0
            volume = 0.0 if pd.isna(v) else float(v)
            records.append(
                {
                    "timestamp": int(ts.timestamp()),
                    "open": None if pd.isna(row["Open"]) else float(row["Open"]),
                    "high": None if pd.isna(row["High"]) else float(row["High"]),
                    "low": None if pd.isna(row["Low"]) else float(row["Low"]),
                    "close": None if pd.isna(row["Close"]) else float(row["Close"]),
                    "volume": volume,
                    "source": self._source,
                }
            )
        return records

    def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        start_ts: Optional[int] = None,
        end_ts: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        if start_ts is None or end_ts is None:
            raise ValueError("YahooFinanceDataLoader requires start_ts and end_ts")

        if timeframe != "4h":
            # YFinanceClient поддерживает только 1d/1w/1M.
            records = self._client.fetch_ohlcv(symbol=symbol, timeframe=timeframe, start=start_ts, end=end_ts)
            # defensively filter by timestamp range
            return [r for r in records if int(r["timestamp"]) >= int(start_ts) and int(r["timestamp"]) <= int(end_ts)]

        # timeframe == '4h' (download 1h and resample 4H in chunks)
        if symbol not in self._client.ticker_map:
            raise ValueError(f"Unknown yfinance symbol mapping: {symbol}")

        ticker = self._client.ticker_map[symbol]
        start_ts = int(start_ts)
        end_ts = int(end_ts)

        if start_ts > end_ts:
            return []

        # yfinance 1h download can be heavy for long ranges — split by hours.
        chunk_hours = max(1, int(MAX_BARS_PER_REQUEST_YFINANCE_1H))
        chunk_sec = chunk_hours * 3600

        seen: Dict[int, Dict[str, Any]] = {}
        cursor = start_ts
        while cursor <= end_ts:
            chunk_end = min(end_ts, cursor + chunk_sec)
            df_1h = self._download_1h(ticker=ticker, start_ts=cursor, end_ts=chunk_end)
            if df_1h is not None and not df_1h.empty:
                if isinstance(df_1h.columns, pd.MultiIndex):
                    df_1h.columns = df_1h.columns.get_level_values(0)

                if df_1h.index.tz is None:
                    df_1h.index = df_1h.index.tz_localize("UTC")
                else:
                    df_1h.index = df_1h.index.tz_convert("UTC")

                df_4h = (
                    # Keep 4h labels on bucket start (12:00, 16:00, 20:00, ... UTC),
                    # which matches the expected audit verification grid.
                    df_1h.resample("4h", label="left", closed="left")
                    .agg(
                        {
                            "Open": "first",
                            "High": "max",
                            "Low": "min",
                            "Close": "last",
                            "Volume": "sum",
                        }
                    )
                )

                for rec in self._records_from_resampled_4h(df_4h):
                    ts = int(rec["timestamp"])
                    if ts < start_ts or ts > end_ts:
                        continue
                    seen[ts] = rec

            # advance
            cursor = chunk_end + 1
        return [seen[ts] for ts in sorted(seen.keys())]
