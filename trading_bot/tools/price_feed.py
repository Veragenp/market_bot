from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Dict, Iterable, Optional

import requests

from trading_bot.config.settings import BYBIT_BASE_URL, PRICE_FEED_MAX_STALE_SEC, PRICE_FEED_WS_WARMUP_SEC


def _to_bybit_symbol(symbol: str) -> str:
    return symbol.replace("/", "").upper()


def _to_trade_symbol(bybit_symbol: str) -> str:
    s = bybit_symbol.upper()
    if s.endswith("USDT"):
        return f"{s[:-4]}/USDT"
    return s


@dataclass
class PricePoint:
    price: float
    ts: int
    source: str


class PriceFeed:
    """
    Unified current-price source for cycle selection and detector:
      1) Bybit WebSocket ticker (if pybit is available)
      2) REST fallback /v5/market/tickers
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._prices: Dict[str, PricePoint] = {}
        self._ws = None
        self._ws_started = False
        self._session = requests.Session()

    def _on_ws_tick(self, message: dict) -> None:
        try:
            topic = str(message.get("topic") or "")
            data = message.get("data") or {}
            bybit_symbol = topic.split(".")[1] if "." in topic else str(data.get("symbol") or "")
            last_price = data.get("lastPrice")
            if not bybit_symbol or last_price is None:
                return
            trade_symbol = _to_trade_symbol(bybit_symbol)
            p = float(last_price)
            now_ts = int(time.time())
            with self._lock:
                self._prices[trade_symbol] = PricePoint(price=p, ts=now_ts, source="ws")
        except Exception:
            return

    def start_ws(self, symbols: Iterable[str]) -> bool:
        if self._ws_started:
            return True
        try:
            from pybit.unified_trading import WebSocket  # type: ignore
        except Exception:
            return False
        try:
            ws = WebSocket(testnet=False, channel_type="linear")
            for sym in symbols:
                ws.ticker_stream(symbol=_to_bybit_symbol(sym), callback=self._on_ws_tick)
            self._ws = ws
            self._ws_started = True
            return True
        except Exception:
            self._ws_started = False
            self._ws = None
            return False

    def _rest_snapshot(self, symbols: Iterable[str]) -> Dict[str, PricePoint]:
        url = f"{BYBIT_BASE_URL.rstrip('/')}/v5/market/tickers"
        resp = self._session.get(url, params={"category": "linear"}, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        items = ((data.get("result") or {}).get("list") or [])
        wanted = {_to_bybit_symbol(s) for s in symbols}
        now_ts = int(time.time())
        out: Dict[str, PricePoint] = {}
        for it in items:
            bybit_symbol = str(it.get("symbol") or "").upper()
            if bybit_symbol not in wanted:
                continue
            lp = it.get("lastPrice")
            if lp is None:
                continue
            ts_raw = it.get("time") or it.get("updatedTime")
            ts = int(int(ts_raw) / 1000) if ts_raw is not None else now_ts
            out[_to_trade_symbol(bybit_symbol)] = PricePoint(
                price=float(lp),
                ts=ts,
                source="rest",
            )
        return out

    def get_prices(self, symbols: Iterable[str]) -> Dict[str, PricePoint]:
        syms = list(symbols)
        # try ws first
        ws_ok = self.start_ws(syms)
        if ws_ok:
            deadline = time.time() + max(1, int(PRICE_FEED_WS_WARMUP_SEC))
            while time.time() < deadline:
                with self._lock:
                    ready = sum(1 for s in syms if s in self._prices)
                if ready >= max(1, int(0.6 * len(syms))):
                    break
                time.sleep(0.2)

        now_ts = int(time.time())
        out: Dict[str, PricePoint] = {}
        with self._lock:
            for s in syms:
                p = self._prices.get(s)
                if p is None:
                    continue
                if now_ts - int(p.ts) > int(PRICE_FEED_MAX_STALE_SEC):
                    continue
                out[s] = p
        missing = [s for s in syms if s not in out]
        if missing:
            try:
                snap = self._rest_snapshot(missing)
            except Exception:
                snap = {}
            out.update(snap)
        return out


_FEED: Optional[PriceFeed] = None


def get_price_feed() -> PriceFeed:
    global _FEED
    if _FEED is None:
        _FEED = PriceFeed()
    return _FEED

