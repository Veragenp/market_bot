"""Bybit USDT linear — текущая цена + метаданные инструментов.

Источники:
- /v5/market/instruments-info (public) — tick size / min order qty
- /v5/market/tickers (public) — lastPrice + turnover24h

Комиссии maker/taker:
- Если установлены BYBIT_API_KEY/BYBIT_API_SECRET и доступен pybit — пытаемся через get_fee_rates (как в tutorial_v3).
- Если нет ключей/pybit — комиссии не заполняем (None).
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Dict, Iterable, List, Optional

import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from trading_bot.config.settings import LIQUIDATIONS_MAX_RECORDS, LIQUIDATIONS_UPDATE_INTERVAL
from trading_bot.config.settings import (
    BYBIT_API_KEY,
    BYBIT_API_SECRET,
    BYBIT_BASE_URL,
    INSTRUMENTS_LOAD_FEES,
)
from trading_bot.data.base_loader import BaseDataLoader


class BybitFuturesDataLoader(BaseDataLoader):
    def __init__(
        self,
        api_key: str = BYBIT_API_KEY,
        api_secret: str = BYBIT_API_SECRET,
        base_url: str = BYBIT_BASE_URL,
        timeout_s: int = 30,
    ) -> None:
        self._api_key = api_key
        self._api_secret = api_secret
        self._base_url = base_url.rstrip("/")
        self._timeout_s = timeout_s
        self._source = "bybit_futures"
        self._fee_client = None
        self._session = requests.Session()

        # Try to initialize fee client like tutorial_v3 (pybit), but keep it optional.
        if self._api_key and self._api_secret:
            try:
                from pybit.unified_trading import HTTP  # type: ignore

                self._fee_client = HTTP(api_key=self._api_key, api_secret=self._api_secret, testnet=False)
            except Exception:
                self._fee_client = None

    def get_exchange_name(self) -> str:
        return self._source

    @staticmethod
    def _to_bybit_symbol(symbol: str) -> str:
        return symbol.replace("/", "").upper()

    def _require_linear(self) -> Dict[str, Any]:
        return {"category": "linear"}

    @retry(
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=1, min=2, max=15),
        retry=retry_if_exception_type((requests.RequestException, TimeoutError)),
        reraise=True,
    )
    def _get_json(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self._base_url}{path}"
        resp = self._session.get(url, params=params, timeout=self._timeout_s)
        resp.raise_for_status()
        return resp.json()

    def fetch_instrument_info(self, symbol: str) -> Dict[str, Any]:
        bybit_symbol = self._to_bybit_symbol(symbol)

        # instruments-info is public and returns tickSize / lotSizeFilter / etc.
        data = self._get_json(
            "/v5/market/instruments-info",
            {**self._require_linear(), "symbol": bybit_symbol},
        )
        result_list = (data.get("result") or {}).get("list") or []
        if not result_list:
            raise ValueError(f"Bybit instruments-info empty for {symbol} ({bybit_symbol})")

        info = result_list[0]
        price_filter = info.get("priceFilter") or {}
        lot_filter = info.get("lotSizeFilter") or {}

        tick_size = float(price_filter.get("tickSize") or 0.0) if price_filter.get("tickSize") is not None else None
        min_qty = float(lot_filter.get("minOrderQty") or 0.0) if lot_filter.get("minOrderQty") is not None else None

        # turnover24h from tickers (public)
        tickers = self._get_json(
            "/v5/market/tickers",
            {**self._require_linear(), "symbol": bybit_symbol},
        )
        tick_list = (tickers.get("result") or {}).get("list") or []
        turnover24h = None
        if tick_list:
            t0 = tick_list[0]
            # turnover24h is in quote currency (USDT)
            if t0.get("turnover24h") is not None:
                turnover24h = float(t0.get("turnover24h"))

        # Maker/taker fees (requires auth + may be optional)
        commission_open = None
        commission_close = None
        if self._fee_client is not None:
            try:
                fee_resp = self._fee_client.get_fee_rates(category="linear", symbol=bybit_symbol)
                fee_list = (fee_resp.get("result") or {}).get("list") or []
                if fee_list:
                    fee_data = fee_list[0]
                    maker_fee = float(fee_data.get("makerFeeRate", 0) or 0)
                    taker_fee = float(fee_data.get("takerFeeRate", 0) or 0)
                    # For your DB columns:
                    # commission_open (open) = taker fee; commission_close (close) = maker fee
                    commission_open = taker_fee
                    commission_close = maker_fee
            except Exception:
                # Non-fatal: keep commissions None.
                commission_open = None
                commission_close = None

        return {
            "symbol": bybit_symbol,
            "exchange": self._source,
            "tick_size": tick_size,
            "min_qty": min_qty,
            "avg_volume_24h": turnover24h,
            "commission_open": commission_open,
            "commission_close": commission_close,
        }

    def _fetch_turnover24h_map(self, *, symbols_bybit: Optional[set[str]] = None) -> Dict[str, float]:
        """Получает turnover24h для линейных инструментов и мапит по symbol."""
        # Пробуем одним запросом без symbol.
        tickers = self._get_json(
            "/v5/market/tickers",
            {**self._require_linear()},
        )
        tick_list = (tickers.get("result") or {}).get("list") or []
        out: Dict[str, float] = {}
        for t in tick_list:
            sym = t.get("symbol")
            if not sym:
                continue
            if symbols_bybit is not None and sym not in symbols_bybit:
                continue
            turnover = t.get("turnover24h")
            if turnover is None:
                continue
            out[sym] = float(turnover)
        return out

    def fetch_all_instruments_info(
        self,
        symbols_to_filter: Optional[Iterable[str]] = None,
        *,
        page_limit: int = 500,
    ) -> List[Dict[str, Any]]:
        """
        Массовое получение tickSize / minOrderQty + turnover24h (avg_volume_24h).

        `symbols_to_filter` ожидается в формате TRADING_SYMBOLS (например 'BTC/USDT'),
        либо уже bybit формате ('BTCUSDT').
        """
        symbols_bybit: Optional[set[str]] = None
        if symbols_to_filter is not None:
            symbols_bybit = {self._to_bybit_symbol(s) for s in symbols_to_filter}

        instruments: Dict[str, Dict[str, Any]] = {}
        cursor: Optional[str] = None
        while True:
            params = {**self._require_linear(), "limit": int(page_limit), "cursor": cursor}
            # Bybit accepts empty cursor; remove it when None.
            if cursor is None:
                params.pop("cursor", None)

            data = self._get_json("/v5/market/instruments-info", params=params)
            result = data.get("result") or {}
            items = result.get("list") or []

            for info in items:
                bybit_symbol = info.get("symbol")
                if not bybit_symbol:
                    continue
                if symbols_bybit is not None and bybit_symbol not in symbols_bybit:
                    continue

                price_filter = info.get("priceFilter") or {}
                lot_filter = info.get("lotSizeFilter") or {}
                tick_size = float(price_filter.get("tickSize") or 0.0) if price_filter.get("tickSize") is not None else None
                min_qty = float(lot_filter.get("minOrderQty") or 0.0) if lot_filter.get("minOrderQty") is not None else None

                instruments[bybit_symbol] = {
                    "symbol": bybit_symbol,
                    "exchange": self._source,
                    "tick_size": tick_size,
                    "min_qty": min_qty,
                    "avg_volume_24h": None,
                    "commission_open": None,
                    "commission_close": None,
                }

            cursor = result.get("nextPageCursor")
            if not cursor or len(items) < page_limit:
                break
            # be gentle to rate limits
            import time as _time
            _time.sleep(0.1)

        if not instruments:
            return []

        turnover_map = self._fetch_turnover24h_map(symbols_bybit=set(instruments.keys()) if symbols_bybit is None else symbols_bybit)
        for sym, rec in instruments.items():
            rec["avg_volume_24h"] = turnover_map.get(sym)

        # Fees are optional and can be expensive (per-symbol auth call).
        if INSTRUMENTS_LOAD_FEES and self._fee_client is not None:
            for sym, rec in instruments.items():
                try:
                    fee_resp = self._fee_client.get_fee_rates(category="linear", symbol=sym)
                    fee_list = (fee_resp.get("result") or {}).get("list") or []
                    if fee_list:
                        fee_data = fee_list[0]
                        maker_fee = float(fee_data.get("makerFeeRate", 0) or 0)
                        taker_fee = float(fee_data.get("takerFeeRate", 0) or 0)
                        rec["commission_open"] = taker_fee
                        rec["commission_close"] = maker_fee
                except Exception:
                    # keep None
                    pass

        return list(instruments.values())

    def get_current_price(self, symbol: str) -> float:
        bybit_symbol = self._to_bybit_symbol(symbol)
        data = self._get_json(
            "/v5/market/tickers",
            {**self._require_linear(), "symbol": bybit_symbol},
        )
        result_list = (data.get("result") or {}).get("list") or []
        if not result_list:
            raise ValueError(f"Bybit tickers empty for {symbol} ({bybit_symbol})")
        last_price = result_list[0].get("lastPrice")
        if last_price is None:
            raise ValueError(f"Bybit tickers missing lastPrice for {symbol} ({bybit_symbol})")
        return float(last_price)

    # OHLCV is intentionally not implemented in this stage.
    def fetch_ohlcv(self, symbol: str, timeframe: str, start_ts: Optional[int] = None, end_ts: Optional[int] = None) -> List[Dict[str, Any]]:
        raise NotImplementedError("BybitFuturesDataLoader OHLCV is not implemented in this stage.")

    def fetch_liquidations(self, symbol: str, start_ts: Optional[int] = None, end_ts: Optional[int] = None) -> List[Dict[str, Any]]:
        # Note: in this codebase we store liquidations aggregated into timeframe buckets
        # (e.g. '1h'), so this method returns raw events.
        # Bybit provides liquidation feed via WebSocket; there is no stable REST endpoint
        # for "last 200 liquidation records" in this project.
        return self._collect_liquidation_events_via_ws(
            symbol=symbol,
            max_records=LIQUIDATIONS_MAX_RECORDS,
            # WebSocket feed is "live" (no history). We must allow enough time
            # to observe liquidation events; otherwise DB stays empty.
            timeout_s=min(60, int(LIQUIDATIONS_UPDATE_INTERVAL)),
        )

    @staticmethod
    def _liquidation_item_to_event(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Convert websocket liquidation payload to a normalized event.

        Expected fields (from Bybit liquidation stream):
          - T: timestamp (ms)
          - S: side ('Buy'/'Sell')
          - v: executed size
          - p: bankruptcy price
        """
        ts_ms = item.get("T") or item.get("ts") or item.get("timestamp")
        side = item.get("S") or item.get("side")
        qty = item.get("v") or item.get("qty") or item.get("size")
        price = item.get("p") or item.get("price") or item.get("bankruptcyPrice")

        if ts_ms is None or side is None or qty is None:
            return None

        try:
            ts_sec = int(ts_ms) // 1000
            side_str = str(side)
            qty_f = float(qty)
            price_f = float(price) if price is not None else 0.0
        except (TypeError, ValueError):
            return None

        return {
            "timestamp": ts_sec,
            "side": side_str,
            "qty": qty_f,
            "price": price_f,
        }

    def _collect_liquidation_events_via_ws(
        self,
        *,
        symbol: str,
        max_records: int,
        timeout_s: int,
    ) -> List[Dict[str, Any]]:
        """Collect latest liquidation events via WebSocket for a short time window."""
        try:
            from pybit.unified_trading import WebSocket  # type: ignore
        except Exception:
            logging.getLogger(__name__).warning("pybit is required for liquidation websocket collection.")
            return []

        from trading_bot.tools.bybit_ws import public_linear_websocket_kwargs

        bybit_symbol = self._to_bybit_symbol(symbol)

        events: List[Dict[str, Any]] = []
        lock = threading.Lock()
        deadline = time.time() + float(timeout_s)

        # `ws` is initialized and connects during __init__ (pybit), in a background thread.
        ws = WebSocket(channel_type="linear", **public_linear_websocket_kwargs())

        def callback(message: Dict[str, Any]) -> None:
            nonlocal events
            data = message.get("data")
            if not data:
                return

            items: List[Dict[str, Any]] = []
            if isinstance(data, list):
                items = data  # type: ignore[assignment]
            elif isinstance(data, dict):
                inner = data.get("data")
                if isinstance(inner, list):
                    items = inner  # type: ignore[assignment]
                else:
                    # Some versions may send a single object; wrap it.
                    items = [data]  # type: ignore[list-item]

            new_events: List[Dict[str, Any]] = []
            for it in items:
                ev = self._liquidation_item_to_event(it)
                if ev is not None:
                    new_events.append(ev)

            if not new_events:
                return

            with lock:
                for ev in new_events:
                    # hard cap
                    if len(events) >= max_records:
                        break
                    ev["exchange"] = self._source
                    events.append(ev)

                # Stop as soon as we got enough events.
                if len(events) >= max_records:
                    try:
                        ws.exit()
                    except Exception:
                        pass

        # Bybit exposes liquidation feed as `allLiquidation.{symbol}` on WebSocket.
        # pybit's helper `liquidation_stream` subscribes to `liquidation.{symbol}` which
        # is not compatible with the endpoint provided by Bybit (subscription fails).
        ws.subscribe(
            topic="allLiquidation.{symbol}",
            callback=callback,
            symbol=bybit_symbol,
        )

        try:
            while time.time() < deadline:
                with lock:
                    if len(events) >= max_records:
                        break
                time.sleep(0.05)
        finally:
            try:
                ws.exit()
            except Exception:
                pass

        # Returned events may already be close to sorted, but ensure deterministic order.
        events.sort(key=lambda e: int(e["timestamp"]))
        # Trim to max_records in case of overshoot.
        return events[-max_records:]

    def fetch_open_interest(
        self,
        symbol: str,
        interval: str,
        start_ts: Optional[int] = None,
        end_ts: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch open interest history (linear USDT contracts).

        Bybit REST:
          GET /v5/market/open-interest?category=linear&symbol=...&intervalTime=...&startTime=ms&endTime=ms&limit=1..200&cursor=...
        Response list is ordered descending by timestamp, so we normalize to ascending order.
        """
        bybit_symbol = self._to_bybit_symbol(symbol)

        supported_intervals = {"5min", "15min", "30min", "1h", "4h", "1d"}
        if interval not in supported_intervals:
            raise ValueError(f"Unsupported open interest intervalTime: {interval}")

        end_ts_s = int(time.time()) if end_ts is None else int(end_ts)
        if start_ts is not None:
            start_ts_s = int(start_ts)
            # Fetch an extra 24h window to compute `oi_change_24h`.
            query_start_ts_s = max(0, start_ts_s - 86400)
        else:
            start_ts_s = None
            query_start_ts_s = None

        query_params: Dict[str, Any] = {
            **self._require_linear(),
            "symbol": bybit_symbol,
            "intervalTime": interval,
            "limit": 200,
            "startTime": int(query_start_ts_s * 1000) if query_start_ts_s is not None else None,
            "endTime": int(end_ts_s * 1000),
        }
        # Remove None values (Bybit does not like startTime=None)
        query_params = {k: v for k, v in query_params.items() if v is not None}

        cursor: Optional[str] = None
        oi_by_ts: Dict[int, float] = {}
        while True:
            params = dict(query_params)
            if cursor:
                params["cursor"] = cursor
            elif "cursor" in params:
                params.pop("cursor", None)

            data = self._get_json("/v5/market/open-interest", params)
            result = data.get("result") or {}
            items = result.get("list") or []

            for it in items:
                ts_ms = it.get("timestamp")
                oi_val = it.get("openInterest")
                if ts_ms is None or oi_val is None:
                    continue
                try:
                    ts_sec = int(ts_ms) // 1000
                    oi_by_ts[ts_sec] = float(oi_val)
                except (TypeError, ValueError):
                    continue

            next_cursor = result.get("nextPageCursor")
            cursor = next_cursor if next_cursor else None

            if not cursor:
                break

            # Be gentle to rate limits.
            time.sleep(0.1)

        requested_records: List[Dict[str, Any]] = []
        for ts_sec, oi_val in oi_by_ts.items():
            if start_ts_s is not None and ts_sec < start_ts_s:
                continue
            if ts_sec > end_ts_s:
                continue

            prev_ts = ts_sec - 86400
            prev_val = oi_by_ts.get(prev_ts)
            oi_change = (oi_val - prev_val) if prev_val is not None else None

            requested_records.append(
                {
                    "timestamp": int(ts_sec),
                    "exchange": self._source,
                    "oi_value": float(oi_val),
                    "oi_change_24h": float(oi_change) if oi_change is not None else None,
                }
            )

        requested_records.sort(key=lambda r: int(r["timestamp"]))
        return requested_records
