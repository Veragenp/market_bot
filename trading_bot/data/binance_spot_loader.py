"""Binance spot OHLCV loader (инкрементальная/историческая загрузка).

Схема БД хранит временные метки в секундах Unix.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional

from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from trading_bot.config.settings import (
    MAX_BARS_PER_REQUEST_BINANCE,
    SOURCE_BINANCE,
)
from trading_bot.data.base_loader import BaseDataLoader
from trading_bot.provider.exchange_factory import get_exchange_client

logger = logging.getLogger(__name__)


class BinanceSpotDataLoader(BaseDataLoader):
    def __init__(self, max_bars_per_request: int = MAX_BARS_PER_REQUEST_BINANCE) -> None:
        self._max_bars = int(max_bars_per_request)
        self._client = get_exchange_client(SOURCE_BINANCE)
        self._source = SOURCE_BINANCE

    def get_exchange_name(self) -> str:
        return self._source

    @staticmethod
    def _tf_to_seconds(timeframe: str) -> Optional[int]:
        return {
            "1m": 60,
            "1h": 3600,
            "4h": 14400,
            "1d": 86400,
            "1w": 86400 * 7,
        }.get(timeframe)

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=20),
        retry=retry_if_exception_type((ConnectionError, TimeoutError, OSError)),
        reraise=True,
    )
    def _fetch_batch(
        self,
        symbol: str,
        timeframe: str,
        since_ts: int,
        limit: int,
    ) -> List[Dict[str, Any]]:
        # provider clients возвращают timestamps в секундах.
        return self._client.fetch_ohlcv(symbol=symbol, timeframe=timeframe, since=since_ts, limit=limit)

    def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        start_ts: Optional[int] = None,
        end_ts: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        if start_ts is None:
            raise ValueError("BinanceSpotDataLoader requires start_ts")
        if end_ts is None:
            end_ts = int(time.time())

        cursor = int(start_ts)
        target_end = int(end_ts)
        if cursor > target_end:
            return []

        # CCXT обычно возвращает свечи начиная от since_ts. Мы считаем end_ts включительным.
        out: List[Dict[str, Any]] = []
        seen: Dict[int, Dict[str, Any]] = {}

        while cursor <= target_end:
            batch = self._fetch_batch(symbol=symbol, timeframe=timeframe, since_ts=cursor, limit=self._max_bars)
            if not batch:
                break

            # Дедупликация и фильтр по end_ts (включительно).
            for rec in batch:
                ts = int(rec["timestamp"])
                if ts > target_end:
                    continue
                seen[ts] = {
                    "timestamp": ts,
                    "open": rec.get("open"),
                    "high": rec.get("high"),
                    "low": rec.get("low"),
                    "close": rec.get("close"),
                    "volume": rec.get("volume"),
                    "source": self._source,
                }

            last_ts = max(seen.keys()) if seen else None
            if last_ts is None:
                break

            # Если мы не смогли сдвинуть курсор — выходим, чтобы не уйти в цикл.
            if last_ts < cursor:
                break

            # Если пришло меньше лимита, значит, дальше данных нет (или API ограничил диапазон).
            if len(batch) < self._max_bars:
                break

            # Дальше начинаем со следующей секунды.
            cursor = last_ts + 1
            time.sleep(0.1)

            if cursor > target_end:
                break

        return [seen[ts] for ts in sorted(seen.keys())]


