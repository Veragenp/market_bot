from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from trading_bot.provider.clients.base import BaseExchangeClient


class YFinanceClient(BaseExchangeClient):
    """Client for macro OHLCV via yfinance."""

    _SUPPORTED_TIMEFRAMES = {"1d": "1d", "1w": "1wk", "1M": "1mo"}

    def __init__(self, ticker_map: Dict[str, str], timezone: str = "US/Eastern") -> None:
        try:
            import yfinance as yf  # type: ignore
        except ImportError as exc:
            raise RuntimeError(
                "yfinance is required for YFinanceClient. Install with `pip install yfinance`."
            ) from exc

        self._yf = yf
        self.ticker_map = dict(ticker_map)
        self.timezone = timezone

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((ConnectionError, TimeoutError)),
        reraise=True,
    )
    def _download_data(
        self,
        ticker: str,
        start_str: Optional[str],
        end_str: Optional[str],
        interval: str,
    ) -> pd.DataFrame:
        return self._yf.download(
            ticker,
            start=start_str,
            end=end_str,
            interval=interval,
            progress=False,
        )

    @staticmethod
    def _to_date_str(ts: Optional[int]) -> Optional[str]:
        if ts is None:
            return None
        return datetime.utcfromtimestamp(int(ts)).strftime("%Y-%m-%d")

    @staticmethod
    def _align_index_to_candle_start(index: pd.DatetimeIndex, timeframe: str) -> pd.DatetimeIndex:
        base = index.normalize()
        if timeframe == "1d":
            return base
        if timeframe == "1w":
            return base - pd.to_timedelta(base.weekday, unit="D")
        if timeframe == "1M":
            return pd.DatetimeIndex(
                [ts.replace(day=1, hour=0, minute=0, second=0, microsecond=0) for ts in base],
                tz="UTC",
            )
        raise ValueError(f"Unsupported timeframe: {timeframe}")

    def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        since: Optional[int] = None,
        limit: Optional[int] = None,
        start: Optional[int] = None,
        end: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch OHLCV from yfinance.

        start/end are Unix seconds in UTC. If start is not provided, since is used.
        limit is intentionally ignored because yfinance works with date ranges.
        """
        _ = limit
        if symbol not in self.ticker_map:
            raise ValueError(f"Unknown yfinance symbol mapping: {symbol}")
        if timeframe not in self._SUPPORTED_TIMEFRAMES:
            raise ValueError(f"Unsupported timeframe for yfinance: {timeframe}")

        effective_start = start if start is not None else since
        start_str = self._to_date_str(effective_start)
        end_str = self._to_date_str(end)
        interval = self._SUPPORTED_TIMEFRAMES[timeframe]
        ticker = self.ticker_map[symbol]

        df = self._download_data(ticker=ticker, start_str=start_str, end_str=end_str, interval=interval)
        if df.empty:
            return []

        if isinstance(df.columns, pd.MultiIndex):
            # yfinance can return multi-index columns; keep OHLCV level.
            df.columns = df.columns.get_level_values(0)

        if df.index.tz is None:
            df.index = df.index.tz_localize(self.timezone).tz_convert("UTC")
        else:
            df.index = df.index.tz_convert("UTC")

        df.index = self._align_index_to_candle_start(df.index, timeframe)
        df = df.sort_index()
        df = df[~df.index.duplicated(keep="last")]

        if "Volume" not in df.columns:
            df["Volume"] = 0.0

        records: List[Dict[str, Any]] = []
        for ts, row in df.iterrows():
            records.append(
                {
                    "timestamp": int(ts.timestamp()),
                    "open": float(row["Open"]) if pd.notna(row["Open"]) else None,
                    "high": float(row["High"]) if pd.notna(row["High"]) else None,
                    "low": float(row["Low"]) if pd.notna(row["Low"]) else None,
                    "close": float(row["Close"]) if pd.notna(row["Close"]) else None,
                    "volume": float(row["Volume"]) if pd.notna(row["Volume"]) else 0.0,
                    "source": "yfinance",
                }
            )
        return records

    def fetch_open_interest(self, *args: Any, **kwargs: Any) -> List[Dict[str, Any]]:
        raise NotImplementedError("Open interest is not supported by YFinanceClient.")

    def fetch_liquidations(self, *args: Any, **kwargs: Any) -> List[Dict[str, Any]]:
        raise NotImplementedError("Liquidations are not supported by YFinanceClient.")

