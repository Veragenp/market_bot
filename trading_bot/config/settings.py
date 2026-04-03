"""Пути, API, источники данных и маппинги тикеров."""

from __future__ import annotations

import os

# Корень проекта market_bot (родитель каталога trading_bot)
_SETTINGS_FILE = os.path.abspath(__file__)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(_SETTINGS_FILE)))

try:
    from dotenv import load_dotenv

    _env_tb = os.path.join(BASE_DIR, "trading_bot", ".env")
    _env_root = os.path.join(BASE_DIR, ".env")
    if os.path.isfile(_env_tb):
        load_dotenv(_env_tb, override=False)
    if os.path.isfile(_env_root):
        load_dotenv(_env_root, override=True)
except ImportError:
    pass

DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "market_data.db")

TIMEFRAMES = ["1m", "1h", "4h", "1d", "1w", "1M"]

# Базовые настройки истории
HISTORY_START_DATE = "2017-01-01"
HISTORY_START_TS = 1483228800  # 2017-01-01 00:00:00 UTC

# 1m глубина (интрадей)
INTRADAY_1M_DAYS = 60
MINUTE_DATA_RETENTION_DAYS = INTRADAY_1M_DAYS

# Лимиты API (для chunking)
MAX_BARS_PER_REQUEST_BINANCE = 1000
MAX_BARS_PER_REQUEST_BYBIT_FUTURES = 200
MAX_BARS_PER_REQUEST_YFINANCE_1H = 1000

# Instruments / liquidity filtering (Bybit)
MIN_AVG_VOLUME_24H = 10_000_000  # USDT
INSTRUMENTS_UPDATE_INTERVAL = 86400  # seconds (daily)
INSTRUMENTS_SYMBOLS_TO_UPDATE = None  # None => all, else list of TRADING_SYMBOLS values
INSTRUMENTS_LOAD_FEES = False  # fees can require auth + extra calls

# Open Interest (Bybit USDT linear)
OI_HISTORY_DAYS = 60  # how many days of OI history to load
OI_UPDATE_INTERVAL = 3600  # seconds
OI_TIMEFRAMES = ["1h", "4h", "1d"]  # Bybit intervalTime

# Liquidations (Bybit USDT linear)
# REST API for liquidation records is not available in this project environment;
# fresh liquidations are collected from Bybit WebSocket stream.
LIQUIDATIONS_UPDATE_INTERVAL = 300  # seconds
LIQUIDATIONS_MAX_RECORDS = 200  # approx. number of latest liquidation events to collect
# 1h + 4h: экспорт в Sheets и отчёты ожидают 4h-бакеты для блока ликвидаций.
LIQUIDATIONS_AGGREGATE_TIMEFRAMES = ["1h", "4h"]  # stored timeframe buckets in `liquidations`

# Таймфреймы по категориям.
# Важно: макро/индексы оставлены как списки для обратной совместимости с текущими вызовами,
# а для spot добавлен вложенный формат под ваш план.
TIMEFRAMES_BY_CATEGORY: dict[str, object] = {
    "spot": {
        "historical": ["4h", "1d", "1w", "1M"],
        "intraday": ["1m"],
    },
    "macro": ["4h", "1d", "1w", "1M"],
    "indices": ["1m", "4h", "1d", "1w", "1M"],
    # Bybit OHLCV пока не реализуем, но слот оставляем под дальнейшее расширение:
    "futures": {
        "ohlcv": ["1m", "5m", "15m", "1h", "4h", "1d"],
    },
}

# Источники
SOURCE_BINANCE = "binance"
SOURCE_YFINANCE = "yfinance"
SOURCE_COINGECKO = "coingecko"
SOURCE_COINGLASS = "coinglass"
SOURCE_TRADINGVIEW = "tradingview"
SOURCE_COINGECKO_AGG = "coingecko_agg"

# Метаданные по умолчанию (ключ для metadata и ohlcv)
DEFAULT_SOURCE_BINANCE = "binance"
DEFAULT_SOURCE_YFINANCE = "yfinance"
DEFAULT_SOURCE_TRADINGVIEW = "tradingview"

DATA_SOURCES: dict[str, str] = {
    "crypto_spot": SOURCE_BINANCE,
    "macro": SOURCE_YFINANCE,
    "indices": SOURCE_TRADINGVIEW,
}

BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "")

# Bybit
BYBIT_API_KEY = os.getenv("BYBIT_API_KEY", "")
BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET", "")

YFINANCE_TICKERS: dict[str, str] = {
    "SP500": "^GSPC",
    "RTY": "^RUT",
    "GOLD": "GC=F",
    "DXY": "DX-Y.NYB",
}

# Внутреннее имя индекса -> символ на CRYPTOCAP в TradingView
TRADINGVIEW_SYMBOLS: dict[str, str] = {
    "TOTAL": "TOTAL",
    "TOTAL2": "TOTAL2",
    "TOTAL3": "TOTAL3",
    "BTCD": "BTC.D",
    "OTHERSD": "OTHERS.D",
    "OTHERS": "OTHERS",
}

TRADINGVIEW_EXCHANGE = "CRYPTOCAP"
TRADINGVIEW_MAX_BARS = 10_000
TRADINGVIEW_USERNAME = os.getenv("TRADINGVIEW_USERNAME", "")
TRADINGVIEW_PASSWORD = os.getenv("TRADINGVIEW_PASSWORD", "")
# tvdatafeed по умолчанию ждёт WebSocket всего 5 с — мало для медленных сетей
TRADINGVIEW_WS_TIMEOUT = int(os.getenv("TRADINGVIEW_WS_TIMEOUT", "60"))

API_RETRY_ATTEMPTS = 3
API_RETRY_DELAY = 2
BYBIT_BASE_URL = os.getenv("BYBIT_BASE_URL", "https://api.bybit.com")
COINGECKO_DELAY = 10
COINGLASS_DELAY = 5
COINGLASS_API_KEY = os.getenv("COINGLASS_API_KEY", "")

FILL_MISSING_WEEKENDS = True
MACRO_TIMEZONE = "US/Eastern"
YFINANCE_TIMEZONE = MACRO_TIMEZONE

# -----------------------------------------------------------------------------
# Volume Profile Peaks (HVN) tuning (для поиска `final=2/3/...`)
# -----------------------------------------------------------------------------
#
# Важно: ранее эти параметры читались из env `PRO_LEVELS_*`.
# Сейчас они задаются в конфиге, чтобы расчет был встроен в проект.
# Любой параметр = None означает: использовать adaptive-значение из
# `get_adaptive_params()` (см. `analytics/volume_profile_peaks.py`).
#
# Настройка окна расчёта:
#   - если PRO_LEVELS_LOOKBACK_DAYS/HOURS = None → берём предыдущий календарный месяц
#   - если задано → окно считается от последней 1m-свечи в БД (anchor)

PRO_LEVELS_LOOKBACK_DAYS = None
PRO_LEVELS_LOOKBACK_HOURS = None

# Core toggles / thresholds
PRO_LEVELS_HEIGHT_MULT = None
PRO_LEVELS_DISTANCE_PCT = None
PRO_LEVELS_VALLEY_THRESHOLD = None

PRO_LEVELS_MIN_DURATION_HOURS = None
PRO_LEVELS_MAX_LEVELS = None
PRO_LEVELS_INCLUDE_ALL_TIERS = None

PRO_LEVELS_FINAL_MERGE_PCT = None  # если None — берётся dynamic_merge_pct (adaptive)
PRO_LEVELS_VALLEY_MERGE_THRESHOLD = None
PRO_LEVELS_ENABLE_VALLEY_MERGE = True

PRO_LEVELS_DEDUP_ROUND_PCT = None
PRO_LEVELS_FINAL_MERGE_VALLEY_THRESHOLD = None

PRO_LEVELS_LEGACY_WEAK_MERGE = False
PRO_LEVELS_RUN_SOFT_PASS = True

PRO_LEVELS_STRICT_HEIGHT_WEAK = None
PRO_LEVELS_STRICT_HEIGHT_MULT = None

PRO_LEVELS_SOFT_HEIGHT_STRONG = None
PRO_LEVELS_SOFT_HEIGHT_WEAK = None
PRO_LEVELS_SOFT_HEIGHT_MULT = None
PRO_LEVELS_SOFT_FINAL_MERGE_PCT = None

PRO_LEVELS_EXCLUDE_RESERVED_PCT = None
PRO_LEVELS_WEAK_MIN_DURATION = None

# Level events analytics (daily batch over 1m candles)
LEVEL_EVENTS_WINDOW_HOURS = 4
LEVEL_EVENTS_LOOKBACK_HOURS = 24
