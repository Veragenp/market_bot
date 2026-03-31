"""Базовый контракт загрузчиков данных."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class BaseDataLoader(ABC):
    @abstractmethod
    def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        start_ts: Optional[int] = None,
        end_ts: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        ...

    @abstractmethod
    def get_exchange_name(self) -> str:
        """Идентификатор источника (совпадает с полем source в БД)."""

    def fetch_instrument_info(self, symbol: str) -> Dict[str, Any]:
        raise NotImplementedError

    def fetch_liquidations(
        self,
        symbol: str,
        start_ts: Optional[int] = None,
        end_ts: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError

    def fetch_open_interest(
        self,
        symbol: str,
        interval: str,
        start_ts: Optional[int] = None,
        end_ts: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError

    def get_current_price(self, symbol: str) -> float:
        raise NotImplementedError
