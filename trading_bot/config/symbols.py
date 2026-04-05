"""Торговые и аналитические символы (единый источник).

`TRADING_SYMBOLS` задаёт полный список спот-пар для:
загрузки OHLCV (Binance), Bybit open interest, ликвидаций (Bybit WS), инструментов и ATR.
Менять состав рынков — только здесь (и при необходимости в тестах).
"""

from __future__ import annotations

# Торговые символы (спот) — OHLCV, ликвидации, OI, instruments
TRADING_SYMBOLS: list[str] = [
    "BTC/USDT",
    "ETH/USDT",
    "NEAR/USDT",
    "AAVE/USDT",
    "SOL/USDT",
    "ENA/USDT",
    "XRP/USDT",
    "ADA/USDT",
    "LINK/USDT",
    "DOT/USDT",
    "SUI/USDT",
    "WLD/USDT",
    "TIA/USDT",
    "LTC/USDT",
    "WIF/USDT",
    "DOGE/USDT",
    "AVAX/USDT",
    "APT/USDT",
    "ARB/USDT",
    "OP/USDT",
    "ORDI/USDT",
]

# Аналитика: крипто-пары, макро (Yahoo), индексы CRYPTOCAP (TradingView)
ANALYTIC_SYMBOLS: dict[str, list[str]] = {
    "macro": ["SP500", "RTY", "GOLD", "DXY"],
    "indices": ["TOTAL", "TOTAL2", "TOTAL3", "BTCD", "OTHERSD", "OTHERS"],
}
