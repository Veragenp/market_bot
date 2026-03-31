from __future__ import annotations

from typing import Any, Dict

from config import (
    API_RETRY_ATTEMPTS,
    API_RETRY_DELAY,
    SOURCE_BINANCE,
    SOURCE_COINGECKO,
    SOURCE_COINGLASS,
    SOURCE_YFINANCE,
    COINGECKO_DELAY,
    COINGLASS_API_KEY,
    COINGLASS_DELAY,
    YFINANCE_TICKERS,
    YFINANCE_TIMEZONE,
)
from trading_bot.provider.clients.base import BaseExchangeClient
from trading_bot.provider.clients.binance import BinanceClient
from trading_bot.provider.clients.coingecko import CoinGeckoClient
from trading_bot.provider.clients.coinglass import CoinGlassClient
from trading_bot.provider.clients.yfinance import YFinanceClient

_CLIENTS: Dict[str, Any] = {}


def get_exchange_client(source: str) -> Any:
    """Return cached exchange client instance by source name."""
    if source not in _CLIENTS:
        if source == SOURCE_BINANCE:
            _CLIENTS[source] = BinanceClient(
                retry_attempts=API_RETRY_ATTEMPTS,
                retry_delay=API_RETRY_DELAY,
            )
        elif source == SOURCE_YFINANCE:
            _CLIENTS[source] = YFinanceClient(
                ticker_map=YFINANCE_TICKERS,
                timezone=YFINANCE_TIMEZONE,
            )
        elif source == SOURCE_COINGECKO:
            _CLIENTS[source] = CoinGeckoClient(delay_seconds=COINGECKO_DELAY)
        elif source == SOURCE_COINGLASS:
            _CLIENTS[source] = CoinGlassClient(
                api_key=COINGLASS_API_KEY,
                delay_seconds=COINGLASS_DELAY,
            )
        else:
            raise ValueError(f"Unsupported exchange source: {source}")
    return _CLIENTS[source]

