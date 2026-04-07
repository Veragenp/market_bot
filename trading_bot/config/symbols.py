"""Торговые и аналитические символы (единый источник).

Разделение задач (не только «откуда грузить данные»):

- **TRADING_SYMBOLS** — контур **исполнения**: пары, по которым идут сделки (фьючи Bybit), инструменты,
  OI, ликвидации, спот Binance (в т.ч. 1m за окно), VP/human для этих пар.
  Список для модулей, которые не должны смешиваться с «чисто контекстными» обходами.

- **ANALYTIC_SYMBOLS** — ветки по **типу задачи**. Ключ — семантика потребителя: макро, индексы,
  контекст спота и т.д. Пересечение с TRADING_SYMBOLS **допустимо намеренно** (например BTC/USDT
  и там и в `crypto_context`): одни и те же свечи в БД, но **разные списки** для ваших логик,
  которые обходят только `ANALYTIC_SYMBOLS["…"]` и не зависят от торгового перечня.

Загрузчики при необходимости **не дублируют** запросы Binance для символа, который уже в TRADING_SYMBOLS
(см. `crypto_context_binance_spot_not_in_trading`).
"""

from __future__ import annotations

# Торговый контур: спот Binance под эти пары + Bybit futures (OI, instruments, …)
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

# Аналитика и контекст (подкатегории — разные потребители / пайплайны)
ANALYTIC_SYMBOLS: dict[str, list[str]] = {
    # Binance spot: длинная история D/W/M и т.д.; для логик «рыночный контекст».
    # BTC/USDT может дублировать TRADING_SYMBOLS — это контракт списка, а не ошибка.
    "crypto_context": [
        "BTC/USDT",
        "ETH/BTC",
    ],
    "macro": ["SP500", "RTY", "GOLD", "DXY"],
    "indices": ["TOTAL", "TOTAL2", "TOTAL3", "BTCD", "OTHERSD", "OTHERS"],
}


def crypto_context_binance_spot_not_in_trading() -> list[str]:
    """
    Символы из ANALYTIC_SYMBOLS['crypto_context'], которых нет в TRADING_SYMBOLS.
    Для load_all_data: доп. спот без повторной заливки уже торгуемых пар.
    """
    ts = frozenset(TRADING_SYMBOLS)
    return [s for s in ANALYTIC_SYMBOLS.get("crypto_context", []) if s not in ts]


__all__ = [
    "ANALYTIC_SYMBOLS",
    "TRADING_SYMBOLS",
    "crypto_context_binance_spot_not_in_trading",
]
