from __future__ import annotations

import logging
import os
import time
import hmac
import hashlib
from urllib.parse import urlencode
from typing import Any, Dict, List, Optional

import requests

from trading_bot.provider.clients.base import BaseExchangeClient

logger = logging.getLogger(__name__)


class BinanceClient(BaseExchangeClient):
    """Binance client via CCXT."""

    def __init__(
        self,
        retry_attempts: int = 3,
        retry_delay: int = 2,
        enable_rate_limit: bool = True,
    ) -> None:
        self.retry_attempts = retry_attempts
        self.retry_delay = retry_delay

        try:
            import ccxt  # type: ignore
        except ImportError as exc:
            raise RuntimeError(
                "ccxt is required for BinanceClient. Install with `pip install ccxt`."
            ) from exc

        self._ccxt = ccxt
        self.exchange = ccxt.binance({"enableRateLimit": enable_rate_limit})
        self.futures_base_url = "https://fapi.binance.com"
        self.session = requests.Session()
        self.api_key = os.getenv("BINANCE_API_KEY", "")
        self.api_secret = os.getenv("BINANCE_API_SECRET", "")

    @staticmethod
    def _normalize_timeframe(timeframe: str) -> str:
        if timeframe == "1W":
            return "1w"
        return timeframe

    def _retryable_exceptions(self) -> tuple[type[Exception], ...]:
        return (
            self._ccxt.NetworkError,
            self._ccxt.RateLimitExceeded,
            self._ccxt.RequestTimeout,
            self._ccxt.ExchangeNotAvailable,
            self._ccxt.DDoSProtection,
        )

    def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        since: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch OHLCV candles from Binance.

        since is expected in Unix seconds and converted to milliseconds for CCXT.
        Returns timestamps in Unix seconds UTC.
        """
        tf = self._normalize_timeframe(timeframe)
        since_ms = int(since * 1000) if since is not None else None

        for attempt in range(1, self.retry_attempts + 1):
            try:
                rows = self.exchange.fetch_ohlcv(
                    symbol=symbol,
                    timeframe=tf,
                    since=since_ms,
                    limit=limit,
                )
                return [
                    {
                        "timestamp": int(r[0] // 1000),
                        "open": float(r[1]) if r[1] is not None else None,
                        "high": float(r[2]) if r[2] is not None else None,
                        "low": float(r[3]) if r[3] is not None else None,
                        "close": float(r[4]) if r[4] is not None else None,
                        "volume": float(r[5]) if r[5] is not None else None,
                        "source": "binance",
                    }
                    for r in rows
                ]
            except self._retryable_exceptions() as exc:
                if attempt >= self.retry_attempts:
                    logger.error(
                        "Binance fetch failed after retries for %s %s: %s",
                        symbol,
                        timeframe,
                        exc,
                    )
                    raise
                delay = self.retry_delay * (2 ** (attempt - 1))
                logger.warning(
                    "Retry %s/%s for %s %s in %ss due to: %s",
                    attempt,
                    self.retry_attempts,
                    symbol,
                    timeframe,
                    delay,
                    exc,
                )
                time.sleep(delay)
            except self._ccxt.BadSymbol as exc:
                logger.error("Binance symbol not found %s: %s", symbol, exc)
                return []
            except self._ccxt.BaseError:
                raise

        return []

    def get_symbol_listing_ts(self, symbol: str) -> Optional[int]:
        """
        Try to get listing timestamp for symbol (Unix seconds).
        Returns None when exchange metadata does not expose it.
        """
        for attempt in range(1, self.retry_attempts + 1):
            try:
                markets = self.exchange.fetch_markets()
                for market in markets:
                    if market.get("symbol") == symbol:
                        since_ms = market.get("info", {}).get("onboardDate") or market.get("since")
                        if since_ms is None:
                            return None
                        return int(int(since_ms) // 1000)
                return None
            except self._retryable_exceptions() as exc:
                if attempt >= self.retry_attempts:
                    logger.warning("Unable to fetch markets for listing date: %s", exc)
                    return None
                delay = self.retry_delay * (2 ** (attempt - 1))
                time.sleep(delay)
            except self._ccxt.BaseError:
                return None
        return None

    @staticmethod
    def to_futures_symbol(symbol: str) -> str:
        return symbol.replace("/", "").upper()

    def _get_with_retry(self, path: str, params: Dict[str, Any]) -> Any:
        url = f"{self.futures_base_url}{path}"
        for attempt in range(1, self.retry_attempts + 1):
            try:
                resp = self.session.get(url, params=params, timeout=30)
                if resp.status_code in (418, 429):
                    raise requests.HTTPError(f"Rate limited: {resp.status_code}")
                resp.raise_for_status()
                return resp.json()
            except requests.RequestException as exc:
                if attempt >= self.retry_attempts:
                    raise RuntimeError(f"Binance futures request failed for {path}: {exc}") from exc
                delay = self.retry_delay * (2 ** (attempt - 1))
                time.sleep(delay)
        return None

    def fetch_open_interest_history(self, symbol: str, period: str, limit: int = 500) -> List[Dict[str, Any]]:
        """
        Fetch open interest history from Binance futures.
        Returns rows with timestamp (sec), oi_value, oi_change_24h.
        """
        payload = self._get_with_retry(
            "/futures/data/openInterestHist",
            {"symbol": symbol, "period": period, "limit": limit},
        )
        if not isinstance(payload, list):
            return []

        rows: List[Dict[str, Any]] = []
        for item in payload:
            ts = int(int(item.get("timestamp", 0)) // 1000)
            oi = float(item.get("sumOpenInterestValue") or item.get("sumOpenInterest") or 0.0)
            rows.append({"timestamp": ts, "oi_value": oi, "oi_change_24h": 0.0})

        rows = sorted(rows, key=lambda x: x["timestamp"])
        by_ts = {r["timestamp"]: r for r in rows}
        deduped = [by_ts[k] for k in sorted(by_ts.keys())]
        for i, row in enumerate(deduped):
            prev_idx = i - 24 if period == "1h" else i - 6 if period == "4h" else i - 1
            if prev_idx >= 0 and deduped[prev_idx]["oi_value"] > 0:
                prev = deduped[prev_idx]["oi_value"]
                row["oi_change_24h"] = (row["oi_value"] - prev) / prev * 100.0
        return deduped

    def fetch_liquidation_orders(
        self,
        symbol: str,
        start_time: int,
        end_time: int,
        limit: int = 1000,
    ) -> List[Dict[str, Any]]:
        """
        Fetch liquidation orders from Binance futures within [start_time, end_time].
        """
        params = {
            "symbol": symbol,
            "startTime": int(start_time * 1000),
            "endTime": int(end_time * 1000),
            "limit": limit,
            "timestamp": int(time.time() * 1000),
        }
        if not (self.api_key and self.api_secret):
            logger.warning("BINANCE_API_KEY/BINANCE_API_SECRET not set; liquidation orders unavailable.")
            return []

        query = urlencode(params)
        signature = hmac.new(self.api_secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()
        params["signature"] = signature
        headers = {"X-MBX-APIKEY": self.api_key}
        try:
            resp = self.session.get(
                f"{self.futures_base_url}/fapi/v1/forceOrders",
                params=params,
                headers=headers,
                timeout=30,
            )
            resp.raise_for_status()
            payload = resp.json()
        except requests.RequestException as exc:
            logger.warning("Unable to fetch liquidation orders for %s: %s", symbol, exc)
            return []
        if not isinstance(payload, list):
            return []

        out: List[Dict[str, Any]] = []
        for item in payload:
            ts_ms = item.get("time") or item.get("updateTime") or 0
            ts = int(int(ts_ms) // 1000)
            price = float(item.get("ap") or item.get("price") or 0.0)
            qty = float(item.get("q") or item.get("origQty") or item.get("executedQty") or 0.0)
            side = str(item.get("S") or item.get("side") or "").upper()
            out.append(
                {
                    "timestamp": ts,
                    "price": price,
                    "original_quantity": qty,
                    "side": side,
                    "symbol": symbol,
                }
            )
        return sorted(out, key=lambda x: x["timestamp"])

