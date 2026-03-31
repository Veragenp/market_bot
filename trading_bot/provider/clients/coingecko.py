from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from trading_bot.provider.clients.base import BaseExchangeClient


class RateLimitError(RuntimeError):
    """Raised when remote API responds with rate limit."""


class CoinGeckoClient(BaseExchangeClient):
    """
    CoinGecko global metrics client.

    Uses global historical endpoint and builds OHLCV-like index candles for:
    TOTAL, TOTAL2, BTCD.
    """

    def __init__(self, delay_seconds: int = 10, base_url: str = "https://api.coingecko.com/api/v3") -> None:
        self.delay_seconds = delay_seconds
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self._daily_cache: Dict[str, Dict[str, Any]] = {}

    @staticmethod
    def _date_key(dt: datetime) -> str:
        return dt.strftime("%d-%m-%Y")

    @staticmethod
    def _ts_day_start_utc(dt: datetime) -> int:
        day = dt.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
        return int(day.timestamp())

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=60),
        retry=retry_if_exception_type((requests.RequestException, RateLimitError)),
        reraise=True,
    )
    def fetch_global_metrics(self, date: datetime) -> Dict[str, Any]:
        date = date.astimezone(timezone.utc)
        key = self._date_key(date)
        if key in self._daily_cache:
            return self._daily_cache[key]

        url = f"{self.base_url}/global/history"
        resp = self.session.get(url, params={"date": key}, timeout=30)
        if resp.status_code == 429:
            raise RateLimitError("CoinGecko rate limit exceeded")
        resp.raise_for_status()
        payload = resp.json()
        data = payload.get("data") or {}
        if not data:
            raise RuntimeError(f"CoinGecko returned empty data for {key}")

        market_cap_by_cur = data.get("market_cap") or {}
        market_cap_pct = data.get("market_cap_percentage") or {}
        total_market_cap = float(market_cap_by_cur.get("usd", 0.0))
        btcd = float(market_cap_pct.get("btc", 0.0))
        btc_market_cap = total_market_cap * btcd / 100.0
        total2 = total_market_cap - btc_market_cap

        result = {
            "date": key,
            "timestamp": self._ts_day_start_utc(date),
            "total_market_cap": total_market_cap,
            "btc_market_cap": btc_market_cap,
            "total_market_cap_excluding_btc": total2,
            "btcd": btcd,
        }
        self._daily_cache[key] = result
        return result

    def fetch_global_range(self, start: int, end: int) -> List[Dict[str, Any]]:
        """Load daily global metrics in [start, end] (Unix seconds UTC)."""
        if start > end:
            return []
        dt = datetime.fromtimestamp(start, tz=timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        end_dt = datetime.fromtimestamp(end, tz=timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        out: List[Dict[str, Any]] = []
        while dt <= end_dt:
            try:
                out.append(self.fetch_global_metrics(dt))
            except Exception:
                # Keep loading other days; caller can inspect missing dates.
                pass
            dt += timedelta(days=1)
            time.sleep(self.delay_seconds)
        return out

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=60),
        retry=retry_if_exception_type((requests.RequestException, RateLimitError)),
        reraise=True,
    )
    def fetch_top_market_cap(self, limit: int = 500) -> pd.DataFrame:
        """
        Fetch top coins by market cap.

        Returns DataFrame with columns:
        symbol, market_cap, current_price, total_volume
        """
        per_page = min(max(int(limit), 1), 250)
        pages = (int(limit) + per_page - 1) // per_page
        rows: List[Dict[str, Any]] = []

        for page in range(1, pages + 1):
            url = f"{self.base_url}/coins/markets"
            resp = self.session.get(
                url,
                params={
                    "vs_currency": "usd",
                    "order": "market_cap_desc",
                    "per_page": per_page,
                    "page": page,
                    "sparkline": "false",
                },
                timeout=30,
            )
            if resp.status_code == 429:
                raise RateLimitError("CoinGecko rate limit exceeded")
            resp.raise_for_status()
            payload = resp.json()
            if not isinstance(payload, list):
                continue

            for item in payload:
                rows.append(
                    {
                        "symbol": str(item.get("symbol", "")).upper(),
                        "market_cap": float(item.get("market_cap") or 0.0),
                        "current_price": float(item.get("current_price") or 0.0),
                        "total_volume": float(item.get("total_volume") or 0.0),
                    }
                )
            time.sleep(self.delay_seconds)

        df = pd.DataFrame(rows, columns=["symbol", "market_cap", "current_price", "total_volume"])
        if df.empty:
            return df
        return df.head(limit).reset_index(drop=True)

    @staticmethod
    def _index_value(symbol: str, metric: Dict[str, Any]) -> float:
        if symbol == "TOTAL":
            return float(metric["total_market_cap"])
        if symbol == "TOTAL2":
            return float(metric["total_market_cap_excluding_btc"])
        if symbol == "BTCD":
            return float(metric["btcd"])
        raise ValueError(f"Unsupported CoinGecko index symbol: {symbol}")

    @staticmethod
    def _agg_from_daily(daily_rows: List[Dict[str, Any]], timeframe: str) -> List[Dict[str, Any]]:
        if timeframe == "1d":
            return daily_rows
        if not daily_rows:
            return []

        df = pd.DataFrame(daily_rows)
        df["dt"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
        df = df.sort_values("dt")
        if timeframe == "1w":
            df["bucket"] = df["dt"].dt.normalize() - pd.to_timedelta(df["dt"].dt.weekday, unit="D")
        elif timeframe == "1M":
            df["bucket"] = pd.to_datetime(df["dt"].dt.to_period("M").astype(str) + "-01", utc=True)
        else:
            raise ValueError(f"Unsupported timeframe for CoinGecko index: {timeframe}")

        out: List[Dict[str, Any]] = []
        for bucket, grp in df.groupby("bucket", sort=True):
            grp = grp.sort_values("dt")
            out.append(
                {
                    "timestamp": int(pd.Timestamp(bucket).timestamp()),
                    "open": float(grp.iloc[0]["open"]),
                    "high": float(grp["high"].max()),
                    "low": float(grp["low"].min()),
                    "close": float(grp.iloc[-1]["close"]),
                    "volume": 0.0,
                    "source": "coingecko",
                    "extra": grp.iloc[-1]["extra"],
                }
            )
        return out

    def fetch_index(
        self,
        symbol: str,
        timeframe: str,
        start: Optional[int] = None,
        end: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        end_ts = int(time.time()) if end is None else int(end)
        start_ts = 1483228800 if start is None else int(start)
        metrics = self.fetch_global_range(start_ts, end_ts)
        daily: List[Dict[str, Any]] = []
        for m in metrics:
            value = self._index_value(symbol, m)
            daily.append(
                {
                    "timestamp": int(m["timestamp"]),
                    "open": value,
                    "high": value,
                    "low": value,
                    "close": value,
                    "volume": 0.0,
                    "source": "coingecko",
                    "extra": json.dumps(
                        {
                            "total_market_cap": m["total_market_cap"],
                            "btc_market_cap": m["btc_market_cap"],
                            "total_market_cap_excluding_btc": m["total_market_cap_excluding_btc"],
                        },
                        ensure_ascii=True,
                    ),
                }
            )
        return self._agg_from_daily(daily, timeframe=timeframe)

    def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        since: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        _ = limit
        return self.fetch_index(symbol=symbol, timeframe=timeframe, start=since)

