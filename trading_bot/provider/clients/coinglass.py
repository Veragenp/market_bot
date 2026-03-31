from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential


class CoinGlassRateLimitError(RuntimeError):
    """Raised when CoinGlass API hits rate limits."""


class CoinGlassClient:
    """CoinGlass client for liquidations and open interest history."""

    def __init__(
        self,
        api_key: str,
        delay_seconds: int = 5,
        base_url: str = "https://open-api.coinglass.com/api/pro/v1",
    ) -> None:
        if not api_key:
            raise ValueError("COINGLASS_API_KEY is required.")
        self.api_key = api_key
        self.delay_seconds = delay_seconds
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()

    @staticmethod
    def _to_coinglass_symbol(symbol: str) -> str:
        return symbol.split("/")[0].upper()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=60),
        retry=retry_if_exception_type((requests.RequestException, CoinGlassRateLimitError)),
        reraise=True,
    )
    def _get(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        headers = {"coinglassSecret": self.api_key}
        resp = self.session.get(f"{self.base_url}{path}", params=params, headers=headers, timeout=30)
        if resp.status_code == 429:
            raise CoinGlassRateLimitError("CoinGlass rate limit exceeded")
        resp.raise_for_status()
        payload = resp.json()
        if isinstance(payload, dict) and payload.get("success") is False:
            raise RuntimeError(payload.get("msg", "CoinGlass API error"))
        return payload

    @staticmethod
    def _extract_points(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        data = payload.get("data")
        if isinstance(data, list):
            return [p for p in data if isinstance(p, dict)]
        if isinstance(data, dict):
            # Some responses may nest points in "list".
            if isinstance(data.get("list"), list):
                return [p for p in data["list"] if isinstance(p, dict)]
        return []

    @staticmethod
    def _resample_liquidations(points: List[Dict[str, Any]], timeframe: str, exchange: str) -> List[Dict[str, Any]]:
        if not points:
            return []
        df = pd.DataFrame(points)
        if "timestamp" not in df.columns:
            # Common fallback names.
            for col in ("time", "ts"):
                if col in df.columns:
                    df["timestamp"] = df[col]
                    break
        if "timestamp" not in df.columns:
            return []

        df["timestamp"] = (pd.to_numeric(df["timestamp"], errors="coerce").fillna(0) // 1000).astype("int64")
        df["dt"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
        df["long_volume"] = pd.to_numeric(df.get("longVol", df.get("long_volume", 0)), errors="coerce").fillna(0.0)
        df["short_volume"] = pd.to_numeric(df.get("shortVol", df.get("short_volume", 0)), errors="coerce").fillna(0.0)
        rule = {"1h": "1H", "4h": "4H", "1d": "1D"}[timeframe]
        agg = (
            df.set_index("dt")
            .resample(rule, label="left", closed="left")
            .agg({"long_volume": "sum", "short_volume": "sum"})
            .reset_index()
        )
        agg["total_volume"] = agg["long_volume"] + agg["short_volume"]
        agg = agg[agg["total_volume"] > 0]
        return [
            {
                "timestamp": int(row["dt"].timestamp()),
                "exchange": exchange,
                "long_volume": float(row["long_volume"]),
                "short_volume": float(row["short_volume"]),
                "total_volume": float(row["total_volume"]),
            }
            for _, row in agg.iterrows()
        ]

    @staticmethod
    def _resample_open_interest(points: List[Dict[str, Any]], timeframe: str, exchange: str) -> List[Dict[str, Any]]:
        if not points:
            return []
        df = pd.DataFrame(points)
        if "timestamp" not in df.columns:
            for col in ("time", "ts"):
                if col in df.columns:
                    df["timestamp"] = df[col]
                    break
        if "timestamp" not in df.columns:
            return []

        df["timestamp"] = (pd.to_numeric(df["timestamp"], errors="coerce").fillna(0) // 1000).astype("int64")
        df["dt"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
        df["oi_value"] = pd.to_numeric(df.get("openInterest", df.get("oi", df.get("value", 0))), errors="coerce").fillna(0.0)
        rule = {"1h": "1H", "4h": "4H", "1d": "1D"}[timeframe]
        pct_periods = {"1h": 24, "4h": 6, "1d": 1}[timeframe]
        agg = (
            df.set_index("dt")
            .resample(rule, label="left", closed="left")
            .agg({"oi_value": "last"})
            .reset_index()
        )
        agg["oi_change_24h"] = agg["oi_value"].pct_change(periods=pct_periods).fillna(0.0) * 100.0
        agg = agg[agg["oi_value"] > 0]
        return [
            {
                "timestamp": int(row["dt"].timestamp()),
                "exchange": exchange,
                "oi_value": float(row["oi_value"]),
                "oi_change_24h": float(row["oi_change_24h"]),
            }
            for _, row in agg.iterrows()
        ]

    def fetch_liquidations(self, symbol: str, start: int, end: int, interval: str) -> List[Dict[str, Any]]:
        pair = self._to_coinglass_symbol(symbol)
        payload = self._get(
            "/futures/liquidation/history",
            {"symbol": pair, "startTime": int(start * 1000), "endTime": int(end * 1000), "interval": "1h"},
        )
        points = self._extract_points(payload)
        out = self._resample_liquidations(points, timeframe=interval, exchange="Binance")
        time.sleep(self.delay_seconds)
        return out

    def fetch_open_interest(self, symbol: str, start: int, end: int, interval: str) -> List[Dict[str, Any]]:
        pair = self._to_coinglass_symbol(symbol)
        payload = self._get(
            "/futures/open-interest/history",
            {"symbol": pair, "startTime": int(start * 1000), "endTime": int(end * 1000), "interval": "1h"},
        )
        points = self._extract_points(payload)
        out = self._resample_open_interest(points, timeframe=interval, exchange="Binance")
        time.sleep(self.delay_seconds)
        return out

