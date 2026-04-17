"""Пути, API, источники данных и маппинги тикеров."""

from __future__ import annotations

import os
from dataclasses import dataclass

# Пути:
#   TRADING_BOT_DIR — каталог пакета `trading_bot` (config/, data/, entrypoints/, …).
#   REPO_ROOT — родитель TRADING_BOT_DIR (корень git-репозитория: config.py, tests, credentials.json).
#   BASE_DIR — синоним TRADING_BOT_DIR (точки входа и данные «приложения» живут здесь).
#   ENTRYPOINTS_DIR — `trading_bot/entrypoints/` (load_all_data, run_scheduler, export_to_sheets, …).
#   .env: сначала `trading_bot/.env`, затем `REPO_ROOT/.env`; опционально MARKET_BOT_ENV_PATH.
_SETTINGS_FILE = os.path.abspath(__file__)
TRADING_BOT_DIR = os.path.dirname(os.path.dirname(_SETTINGS_FILE))
REPO_ROOT = os.path.dirname(TRADING_BOT_DIR)
BASE_DIR = TRADING_BOT_DIR
ENTRYPOINTS_DIR = os.path.join(TRADING_BOT_DIR, "entrypoints")

try:
    from dotenv import load_dotenv

    _env_tb = os.path.normpath(os.path.join(TRADING_BOT_DIR, ".env"))
    if os.path.isfile(_env_tb):
        load_dotenv(_env_tb, override=False)
    _env_repo = os.path.normpath(os.path.join(REPO_ROOT, ".env"))
    if _env_repo != _env_tb and os.path.isfile(_env_repo):
        load_dotenv(_env_repo, override=True)
    _env_extra = (os.getenv("MARKET_BOT_ENV_PATH") or "").strip()
    if _env_extra and os.path.isfile(_env_extra):
        load_dotenv(_env_extra, override=True)
except ImportError:
    pass


def _env_strip_quotes(val: str) -> str:
    s = (val or "").strip()
    if len(s) >= 2 and s[0] == s[-1] and s[0] in ("'", '"'):
        return s[1:-1].strip()
    return s


# SQLite: по умолчанию только внутри `trading_bot/data/` (не в корне репозитория).
# Переопределение: MARKET_BOT_DATA_DIR или MARKET_BOT_DB_PATH (абсолютный путь к файлу).
_default_data_dir = os.path.join(TRADING_BOT_DIR, "data")
DATA_DIR = (os.getenv("MARKET_BOT_DATA_DIR") or _default_data_dir).strip() or _default_data_dir
_db_from_env = (os.getenv("MARKET_BOT_DB_PATH") or "").strip()
DB_PATH = _db_from_env or os.path.join(DATA_DIR, "market_data.db")

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
BYBIT_API_KEY = _env_strip_quotes(os.getenv("BYBIT_API_KEY", ""))
BYBIT_API_SECRET = _env_strip_quotes(os.getenv("BYBIT_API_SECRET", ""))
# Demo trading (api-demo.bybit.com, ключи из основного аккаунта в режиме Demo)
BYBIT_API_KEY_TEST = _env_strip_quotes(os.getenv("BYBIT_API_KEY_TEST", ""))
BYBIT_API_SECRET_TEST = _env_strip_quotes(os.getenv("BYBIT_API_SECRET_TEST", ""))
BYBIT_USE_DEMO = os.getenv("BYBIT_USE_DEMO", "1").strip().lower() in ("1", "true", "yes", "on")
# Реальные ордера: только при явном включении (демо или прод — по BYBIT_USE_DEMO)
BYBIT_EXECUTION_ENABLED = os.getenv("BYBIT_EXECUTION_ENABLED", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
# После группового LONG/SHORT в entry_gate: авто-открытие по строкам entry_gate_confirmations (нужен BYBIT_EXECUTION_ENABLED=1).
ENTRY_AUTO_OPEN_AFTER_GATE = os.getenv("ENTRY_AUTO_OPEN_AFTER_GATE", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
# Групповой сигнал LONG при пакете short (и наоборот): сначала reduce-only market + отмена pending лимитов, сброс single_sided.
ENTRY_CLOSE_OPPOSITE_ON_FLIP_SIGNAL = os.getenv(
    "ENTRY_CLOSE_OPPOSITE_ON_FLIP_SIGNAL", "1"
).strip().lower() in ("1", "true", "yes", "on")
# После reconcile: если пакет flat (нет open/pending), закрыть freeze-эпоху:
# cycle_phase=closed + levels_frozen=0 + сброс cycle_id/structural_cycle_id.
ENTRY_PACKAGE_FLAT_TRANSITION = os.getenv("ENTRY_PACKAGE_FLAT_TRANSITION", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
# Если нет position_records по циклу — опрос Bybit get_positions по пулу (нужны API-ключи).
# По умолчанию выкл.: при «бумажном» in_position биржа пустая — иначе сразу ушли бы в arming.
# Вкл. явно или автоматически при BYBIT_EXECUTION_ENABLED=1 (см. entry_gate).
ENTRY_PACKAGE_FLAT_USE_BYBIT_POSITIONS = os.getenv(
    "ENTRY_PACKAGE_FLAT_USE_BYBIT_POSITIONS", "0"
).strip().lower() in ("1", "true", "yes", "on")

# Детектор подхода к уровням cycle_levels (после freeze) — legacy near-band (см. level_cross_monitor)
ENTRY_DETECTOR_POLL_SEC = float(os.getenv("ENTRY_DETECTOR_POLL_SEC", "3"))
ENTRY_DETECTOR_NEAR_ATR = float(os.getenv("ENTRY_DETECTOR_NEAR_ATR", "0.12"))
ENTRY_DETECTOR_DEBOUNCE_SEC = int(os.getenv("ENTRY_DETECTOR_DEBOUNCE_SEC", "30"))

# Tutorial V3 → level cross monitor (traiding_monitor.py): окно и групповой порог по числу монет
# Совместимость с именами env из tutorial_v3/config.py
# Частота опроса цен/уровней для детектора пересечений.
LEVEL_CROSS_POLL_SEC = float(os.getenv("LEVEL_CROSS_POLL_SEC", os.getenv("MONITOR_POLL_SEC", "10")))
# Окно свежести алертов (в минутах) для группового сигнала.
LEVEL_CROSS_ALERT_TIMEOUT_MINUTES = float(
    os.getenv("LEVEL_CROSS_ALERT_TIMEOUT_MINUTES", os.getenv("ALERT_TIMEOUT_MINUTES", "5"))
)
# Минимум разных монет с алертом для подтверждения группового сигнала.
LEVEL_CROSS_MIN_ALERTS_COUNT = int(
    os.getenv("LEVEL_CROSS_MIN_ALERTS_COUNT", os.getenv("MIN_ALERTS_COUNT", "2"))
)
# Сколько дополнительных алертов допускаем после первого срабатывания.
LEVEL_CROSS_MAX_ADDITIONAL_ALERTS = int(
    os.getenv("LEVEL_CROSS_MAX_ADDITIONAL_ALERTS", os.getenv("MAX_ADDITIONAL_ALERTS", "3"))
)
# Одна строка INFO на тик: счётчики алертов, окно до группового сигнала, причина если сигнала ещё нет.
LEVEL_CROSS_TICK_SUMMARY_LOG = os.getenv("LEVEL_CROSS_TICK_SUMMARY_LOG", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)

# Tutorial V3 → entry gate (trade_signal_processor.py): ATR %% от уровня
ENTRY_GATE_LONG_ATR_THRESHOLD_PCT = float(
    os.getenv("ENTRY_GATE_LONG_ATR_THRESHOLD_PCT", os.getenv("LONG_ATR_THRESHOLD_PERCENT", "2"))
)
ENTRY_GATE_SHORT_ATR_THRESHOLD_PCT = float(
    os.getenv("ENTRY_GATE_SHORT_ATR_THRESHOLD_PCT", os.getenv("SHORT_ATR_THRESHOLD_PERCENT", "2"))
)
# Общий флаг Telegram-уведомлений для level-cross модуля.
LEVEL_CROSS_TELEGRAM = os.getenv("LEVEL_CROSS_TELEGRAM", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
# Отдельный флаг: уведомления о каждом пересечении LONG/SHORT уровня (шумно). Минимум — выкл.
LEVEL_CROSS_TELEGRAM_CROSSINGS = os.getenv("LEVEL_CROSS_TELEGRAM_CROSSINGS", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
# Старт тика entry detector (monitor + gate): уведомление в Telegram (часто — раз в SUPERVISOR_ENTRY_TICK_SEC).
ENTRY_DETECTOR_TELEGRAM_START = os.getenv("ENTRY_DETECTOR_TELEGRAM_START", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)

# Размер позиции / TP по карте tutorial_v3 (position_math + черновик position_records)
# Фиксированный риск на сделку в USDT (не процент депозита).
POSITION_RISK_USDT = float(os.getenv("POSITION_RISK_USDT", "1"))
# Стоп-лосс в ATR от точки входа (1.0 = расстояние 1*ATR).
POSITION_STOP_ATR_MULT = float(os.getenv("POSITION_STOP_ATR_MULT", "0.3"))
# Цели TP в ATR от входа.
POSITION_TP1_ATR_MULT = float(os.getenv("POSITION_TP1_ATR_MULT", "3"))
POSITION_TP2_ATR_MULT = float(os.getenv("POSITION_TP2_ATR_MULT", "2"))
POSITION_TP3_ATR_MULT = float(os.getenv("POSITION_TP3_ATR_MULT", "3"))
# Доли фиксации объема на TP1/TP2 (остаток идет на TP3).
POSITION_TP1_SHARE_PCT = float(os.getenv("POSITION_TP1_SHARE_PCT", "100"))
POSITION_TP2_SHARE_PCT = float(os.getenv("POSITION_TP2_SHARE_PCT", "0"))
# Люфт входа X в % от уровня K (X = K * pct / 100). Лимит на бирже: Y = MROUND(K) при pct=0,
# иначе long Y=K+X / short Y=K−X (цена в gate — только для порога, не для цены ордера).
POSITION_ENTRY_OFFSET_PCT = float(os.getenv("POSITION_ENTRY_OFFSET_PCT", "0"))

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
# Публичный WebSocket V5 (pybit): при ошибках подключения / 404 от ELB попробуйте BYBIT_WS_DOMAIN=bytick
# (эффект только при BYBIT_USE_DEMO=0; в демо bytick для WS не подставляется — см. bybit_ws.public_linear_websocket_kwargs).
BYBIT_WS_DOMAIN = os.getenv("BYBIT_WS_DOMAIN", "").strip().lower()
BYBIT_WS_TRACE_LOGGING = os.getenv("BYBIT_WS_TRACE_LOGGING", "0").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
BYBIT_WS_PING_INTERVAL = int(os.getenv("BYBIT_WS_PING_INTERVAL", "20"))
BYBIT_WS_PING_TIMEOUT = int(os.getenv("BYBIT_WS_PING_TIMEOUT", "10"))
BYBIT_WS_RETRIES = int(os.getenv("BYBIT_WS_RETRIES", "10"))
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
# Эти параметры можно переопределять через env `PRO_LEVELS_*`.
# Любой параметр = None означает: использовать adaptive-значение из
# `get_adaptive_params()` (см. `analytics/volume_profile_peaks.py`).
#
# Настройка окна расчёта:
#   - если PRO_LEVELS_LOOKBACK_DAYS/HOURS = None → берём предыдущий календарный месяц
#   - если задано → окно считается от последней 1m-свечи в БД (anchor)

def _env_opt_float(name: str, default: float | None) -> float | None:
    raw = (os.getenv(name, "") or "").strip()
    if not raw:
        return default
    if raw.lower() in ("none", "null"):
        return None
    return float(raw)


def _env_opt_int(name: str, default: int | None) -> int | None:
    raw = (os.getenv(name, "") or "").strip()
    if not raw:
        return default
    if raw.lower() in ("none", "null"):
        return None
    return int(raw)


def _env_bool(name: str, default: bool) -> bool:
    raw = (os.getenv(name, "") or "").strip().lower()
    if not raw:
        return default
    return raw in ("1", "true", "yes", "on")


PRO_LEVELS_LOOKBACK_DAYS = _env_opt_int("PRO_LEVELS_LOOKBACK_DAYS", 30)  # 30 дней от последней 1m-свечи в БД
PRO_LEVELS_LOOKBACK_HOURS = _env_opt_int("PRO_LEVELS_LOOKBACK_HOURS", None)

# Плановый пересчёт vp_local → `price_levels` (см. `data/scheduler.py`).
# >0: интервал в часах (например 1 или 4). 0: только ежедневный слот 02:45 UTC.
_raw_vp_rebuild_h = os.getenv("VP_LOCAL_REBUILD_INTERVAL_HOURS", "4").strip()
VP_LOCAL_REBUILD_INTERVAL_HOURS = int(_raw_vp_rebuild_h) if _raw_vp_rebuild_h else 0

# После rebuild: если по символу find_pro_levels пустой — снять активные vp_local (не показывать старый снимок).
# 0/false/off — оставить последний успешный активный набор (ручная отладка).
VP_LOCAL_CLEAR_ON_EMPTY_REBUILD = os.getenv("VP_LOCAL_CLEAR_ON_EMPTY_REBUILD", "1").strip().lower() not in (
    "0",
    "false",
    "no",
    "off",
    "",
)

# Автовыбор входных свечей для VP (rebuild): только пороги, без списков символов.
# Если 1m «плохие» — в памяти строится 5m из тех же 1m (ресемпл).
VP_OHLC_FLAT_BAR_MAX_FRAC = float(os.getenv("VP_OHLC_FLAT_BAR_MAX_FRAC", "0.45"))
VP_OHLC_MEDIAN_RANGE_MIN = float(os.getenv("VP_OHLC_MEDIAN_RANGE_MIN", "1e-7"))
VP_OHLC_RESAMPLE_MIN_1M_BARS = int(os.getenv("VP_OHLC_RESAMPLE_MIN_1M_BARS", "120"))

# Core toggles / thresholds
PRO_LEVELS_HEIGHT_MULT = _env_opt_float("PRO_LEVELS_HEIGHT_MULT", None)
PRO_LEVELS_DISTANCE_PCT = _env_opt_float("PRO_LEVELS_DISTANCE_PCT", None)
PRO_LEVELS_VALLEY_THRESHOLD = _env_opt_float("PRO_LEVELS_VALLEY_THRESHOLD", None)

PRO_LEVELS_MIN_DURATION_HOURS = _env_opt_float("PRO_LEVELS_MIN_DURATION_HOURS", None)
PRO_LEVELS_MAX_LEVELS = _env_opt_int("PRO_LEVELS_MAX_LEVELS", 18)
PRO_LEVELS_INCLUDE_ALL_TIERS = _env_opt_int("PRO_LEVELS_INCLUDE_ALL_TIERS", None)

PRO_LEVELS_FINAL_MERGE_PCT = _env_opt_float("PRO_LEVELS_FINAL_MERGE_PCT", 0.002)
PRO_LEVELS_VALLEY_MERGE_THRESHOLD = _env_opt_float("PRO_LEVELS_VALLEY_MERGE_THRESHOLD", None)
PRO_LEVELS_ENABLE_VALLEY_MERGE = _env_bool("PRO_LEVELS_ENABLE_VALLEY_MERGE", False)

PRO_LEVELS_DEDUP_ROUND_PCT = _env_opt_float("PRO_LEVELS_DEDUP_ROUND_PCT", None)
PRO_LEVELS_FINAL_MERGE_VALLEY_THRESHOLD = _env_opt_float("PRO_LEVELS_FINAL_MERGE_VALLEY_THRESHOLD", None)

PRO_LEVELS_LEGACY_WEAK_MERGE = _env_bool("PRO_LEVELS_LEGACY_WEAK_MERGE", False)
PRO_LEVELS_RUN_SOFT_PASS = _env_bool("PRO_LEVELS_RUN_SOFT_PASS", True)

PRO_LEVELS_STRICT_HEIGHT_WEAK = _env_opt_float("PRO_LEVELS_STRICT_HEIGHT_WEAK", None)
PRO_LEVELS_STRICT_HEIGHT_MULT = _env_opt_float("PRO_LEVELS_STRICT_HEIGHT_MULT", None)

PRO_LEVELS_SOFT_HEIGHT_STRONG = _env_opt_float("PRO_LEVELS_SOFT_HEIGHT_STRONG", None)
PRO_LEVELS_SOFT_HEIGHT_WEAK = _env_opt_float("PRO_LEVELS_SOFT_HEIGHT_WEAK", None)
PRO_LEVELS_SOFT_HEIGHT_MULT = _env_opt_float("PRO_LEVELS_SOFT_HEIGHT_MULT", None)
PRO_LEVELS_SOFT_FINAL_MERGE_PCT = _env_opt_float("PRO_LEVELS_SOFT_FINAL_MERGE_PCT", None)

PRO_LEVELS_EXCLUDE_RESERVED_PCT = _env_opt_float("PRO_LEVELS_EXCLUDE_RESERVED_PCT", None)
PRO_LEVELS_WEAK_MIN_DURATION = _env_opt_float("PRO_LEVELS_WEAK_MIN_DURATION", None)

# Level events analytics (daily batch over 1m candles)
LEVEL_EVENTS_WINDOW_HOURS = 4
LEVEL_EVENTS_LOOKBACK_HOURS = 24
LEVEL_EVENTS_MODE = os.getenv("LEVEL_EVENTS_MODE", "runtime").strip().lower()
# all — все активные vp_local (как раньше). active_cycle — только уровни из cycle_levels текущего цикла, окно от frozen_at.
LEVEL_EVENTS_SCOPE = os.getenv("LEVEL_EVENTS_SCOPE", "all").strip().lower() or "all"
LEVEL_EVENTS_MIN_PENETRATION_ATR = float(os.getenv("LEVEL_EVENTS_MIN_PENETRATION_ATR", "0.05"))
LEVEL_EVENTS_MIN_REBOUND_PURE_ATR = float(os.getenv("LEVEL_EVENTS_MIN_REBOUND_PURE_ATR", "0.03"))
LEVEL_EVENTS_RETURN_EPS_ATR = float(os.getenv("LEVEL_EVENTS_RETURN_EPS_ATR", "0.05"))
LEVEL_EVENTS_REBOUND_HORIZON_BARS = int(os.getenv("LEVEL_EVENTS_REBOUND_HORIZON_BARS", "240"))
LEVEL_EVENTS_CONFIRM_ATR_PCT = float(os.getenv("LEVEL_EVENTS_CONFIRM_ATR_PCT", "0.30"))
LEVEL_EVENTS_STALE_OPEN_MINUTES = int(os.getenv("LEVEL_EVENTS_STALE_OPEN_MINUTES", "180"))
# Порог отбоя в долях ATR для строк `level_strength_report` / `level_stop_profile` (≥ 0.30 = 30% ATR).
# Не путать с LEVEL_EVENTS_RETURN_EPS_ATR (допуск возврата через уровень).
LEVEL_STRENGTH_REPORT_MIN_REBOUND_PURE_ATR = float(
    os.getenv("LEVEL_STRENGTH_REPORT_MIN_REBOUND_PURE_ATR", "0.30")
)

# -----------------------------------------------------------------------------
# HTF volume profile (отдельно от 1m `volume_profile_peaks`: крупный ТФ + окно в днях)
# Скрипт: `python -m trading_bot.scripts.run_htf_volume_levels`
# -----------------------------------------------------------------------------
HTF_LEVELS_LOOKBACK_DAYS = int(os.getenv("HTF_LEVELS_LOOKBACK_DAYS", "365"))
HTF_LEVELS_TIMEFRAME = os.getenv("HTF_LEVELS_TIMEFRAME", "1d")
HTF_LEVELS_MIN_BARS = int(os.getenv("HTF_LEVELS_MIN_BARS", "8"))
HTF_LEVELS_TOP_N = int(os.getenv("HTF_LEVELS_TOP_N", "12"))
HTF_LEVELS_MIN_DURATION_HOURS = float(os.getenv("HTF_LEVELS_MIN_DURATION_HOURS", "2"))
HTF_LEVELS_DURATION_TIER1_H = float(os.getenv("HTF_LEVELS_DURATION_TIER1_H", "168"))
HTF_LEVELS_DURATION_TIER2_H = float(os.getenv("HTF_LEVELS_DURATION_TIER2_H", "72"))
HTF_LEVELS_RUN_SOFT_PASS = os.getenv("HTF_LEVELS_RUN_SOFT_PASS", "1").strip().lower() not in (
    "0",
    "false",
    "no",
    "off",
)
HTF_LEVELS_DISABLE_SHEETS = os.getenv("HTF_LEVELS_DISABLE_SHEETS", "").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
HTF_LEVELS_SHEET_WORKSHEET = os.getenv("HTF_LEVELS_SHEET_WORKSHEET", "htf_volume_levels")
HTF_LEVELS_SHEET_ID = os.getenv("HTF_LEVELS_SHEET_ID", "").strip()
HTF_LEVELS_SHEET_URL = os.getenv("HTF_LEVELS_SHEET_URL", "").strip()
HTF_LEVELS_SHEET_TITLE = os.getenv("HTF_LEVELS_SHEET_TITLE", "").strip()

# -----------------------------------------------------------------------------
# Human levels (авто D1/W1 → price_levels human + Google Sheets)
# Практичные дефолты: шире кластер → min 2 фрактала в зоне → при необходимости strength / lookback в .env.
# -----------------------------------------------------------------------------
HUMAN_LEVELS_D1_LOOKBACK_DAYS = int(os.getenv("HUMAN_LEVELS_D1_LOOKBACK_DAYS", "300"))
HUMAN_LEVELS_W1_LOOKBACK_DAYS = int(os.getenv("HUMAN_LEVELS_W1_LOOKBACK_DAYS", "730"))
HUMAN_LEVELS_MIN_BARS_D1 = int(os.getenv("HUMAN_LEVELS_MIN_BARS_D1", "20"))
HUMAN_LEVELS_MIN_BARS_W1 = int(os.getenv("HUMAN_LEVELS_MIN_BARS_W1", "8"))
# Больше mult → шире зоны, меньше строк (склейка соседних фракталов).
HUMAN_LEVELS_CLUSTER_ATR_MULT = float(os.getenv("HUMAN_LEVELS_CLUSTER_ATR_MULT", "0.35"))
# Удалено: human levels берут ATR только из instruments.atr (Gerchik) или тот же Gerchik по хвосту D1.
# Отсев слабых зон перед записью в БД / Sheets (0 = не фильтровать по силе).
HUMAN_LEVELS_MIN_FRACTAL_COUNT = int(os.getenv("HUMAN_LEVELS_MIN_FRACTAL_COUNT", "2"))
_ms = os.getenv("HUMAN_LEVELS_MIN_STRENGTH", "0").strip()
HUMAN_LEVELS_MIN_STRENGTH = float(_ms) if _ms else 0.0
# Разрежение D1-зон по центрам после кластеризации (доли ATR_D1); 0 = выкл. W1 не затрагивается.
_zgap = os.getenv("HUMAN_LEVELS_ZONE_MIN_GAP_ATR", "0.5").strip()
HUMAN_LEVELS_ZONE_MIN_GAP_ATR = float(_zgap) if _zgap else 0.0
HUMAN_LEVELS_DISABLE_SHEETS = os.getenv("HUMAN_LEVELS_DISABLE_SHEETS", "").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
HUMAN_LEVELS_SHEET_WORKSHEET = os.getenv("HUMAN_LEVELS_SHEET_WORKSHEET", "human_levels")
HUMAN_LEVELS_SHEET_ID = os.getenv("HUMAN_LEVELS_SHEET_ID", "").strip()
HUMAN_LEVELS_SHEET_URL = os.getenv("HUMAN_LEVELS_SHEET_URL", "").strip()
HUMAN_LEVELS_SHEET_TITLE = os.getenv("HUMAN_LEVELS_SHEET_TITLE", "").strip()

# -----------------------------------------------------------------------------
# Локальный VP (level_type=vp_local в price_levels) → лист в MARKET_AUDIT_* таблице.
# Источник: только БД после планового rebuild (по умолчанию каждые VP_LOCAL_REBUILD_INTERVAL_HOURS ч;
# при 0 — раз в сутки 02:45 UTC). Ручной прогон: python -m trading_bot.scripts.rebuild_volume_profile_peaks_to_db
# -----------------------------------------------------------------------------
_vp_levels_sheet = (os.getenv("VOLUME_PEAK_LEVELS_WORKSHEET") or os.getenv("DBSCAN_ZONES_WORKSHEET") or "").strip()
VOLUME_PEAK_LEVELS_WORKSHEET = _vp_levels_sheet or "vp_local_levels"

# -----------------------------------------------------------------------------
# Cycle levels selection (DB-first detector input)
# -----------------------------------------------------------------------------
# Whitelist `price_levels.level_type` (через запятую). Известные типы в проекте:
#   vp_local              — локальный VP (1m rolling)
#   vp_global             — HTF VP (напр. 1d)
#   vp_global_4h_90d      — экспериментальный HTF (пока можно не включать)
#   human                 — уровни из human_levels (авто origin=auto, не «ручной ввод»)
#   manual_global_hvn     — ручные HVN из отдельной книги Sheets
# Старые алиасы в БД миграцией приводятся к vp_*.
def _parse_csv_types(raw: str, default: str) -> list[str]:
    s = (raw or "").strip()
    if not s:
        s = default
    return [x.strip() for x in s.split(",") if x.strip()]


# По умолчанию: локальный VP + ручные HVN из Sheets. Не включаем vp_global / vp_global_4h_90d / human, пока не решите иначе.
CYCLE_LEVELS_ALLOWED_LEVEL_TYPES: list[str] = _parse_csv_types(
    os.getenv("CYCLE_LEVELS_ALLOWED_LEVEL_TYPES", ""),
    "vp_local,manual_global_hvn",
)
# Мин. |price−ref|/ATR; дефолт 0 — порог задавайте в .env (напр. CYCLE_LEVELS_MIN_DIST_ATR=0.3)
CYCLE_LEVELS_MIN_DIST_ATR = float(os.getenv("CYCLE_LEVELS_MIN_DIST_ATR", "0.0"))
CYCLE_LEVELS_ZONE_HALF_WIDTH_ATR = float(os.getenv("CYCLE_LEVELS_ZONE_HALF_WIDTH_ATR", "0.3"))
CYCLE_LEVELS_ZONE_EXPAND_STEP_ATR = float(os.getenv("CYCLE_LEVELS_ZONE_EXPAND_STEP_ATR", "0.2"))
CYCLE_LEVELS_ZONE_EXPAND_MAX_STEPS = int(os.getenv("CYCLE_LEVELS_ZONE_EXPAND_MAX_STEPS", "1"))
CYCLE_LEVELS_FALLBACK_MAX_ATR = float(os.getenv("CYCLE_LEVELS_FALLBACK_MAX_ATR", "0.5"))
CYCLE_LEVELS_COOLDOWN_HOURS = int(os.getenv("CYCLE_LEVELS_COOLDOWN_HOURS", "24"))
CYCLE_LEVELS_WORKSHEET = os.getenv("CYCLE_LEVELS_WORKSHEET", "cycle_levels_v1").strip() or "cycle_levels_v1"
CYCLE_LEVELS_DIAG_WORKSHEET = (
    os.getenv("CYCLE_LEVELS_DIAG_WORKSHEET", "cycle_levels_diag_v1").strip()
    or "cycle_levels_diag_v1"
)
CYCLE_LEVELS_CANDIDATES_WORKSHEET = (
    os.getenv("CYCLE_LEVELS_CANDIDATES_WORKSHEET", "cycle_levels_candidates_v1").strip()
    or "cycle_levels_candidates_v1"
)
# Источник цены для freeze: позже = тот же, что у детектора (Bybit WS / last trade). Сейчас в коде — last 1m close из SQLite.
CYCLE_LEVELS_REFERENCE_PRICE_SOURCE = (
    os.getenv("CYCLE_LEVELS_REFERENCE_PRICE_SOURCE", "db_1m_close").strip() or "db_1m_close"
)
PRICE_FEED_WS_WARMUP_SEC = int(os.getenv("PRICE_FEED_WS_WARMUP_SEC", "8"))
PRICE_FEED_MAX_STALE_SEC = int(os.getenv("PRICE_FEED_MAX_STALE_SEC", "30"))
# 0 — не подключать Bybit WebSocket (только REST); если stream-demo/stream блокируется, без лишних ретраев pybit.
PRICE_FEED_WEBSOCKET_ENABLED = os.getenv("PRICE_FEED_WEBSOCKET_ENABLED", "1").strip().lower() not in (
    "0",
    "false",
    "no",
    "off",
)
# Гейт для legacy rebuild_cycle_levels (из price_levels -> cycle_levels).
# False: разрешён только явный вызов с force=True (например из orchestrator/ручного скрипта).
CYCLE_LEVELS_REBUILD_ENABLED = os.getenv("CYCLE_LEVELS_REBUILD_ENABLED", "0").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)

# Structural cycle: эталон W*=(U-L)/ATR (медиана по голосующим), люфт, mid-band → freeze в cycle_levels.
# Источник ref: `price_feed` (WS/REST); при отсутствии тика — fallback последний 1m close из БД (см. structural_cycle_db).
STRUCTURAL_REF_PRICE_SOURCE = (
    os.getenv("STRUCTURAL_REF_PRICE_SOURCE", "price_feed").strip().lower() or "price_feed"
)
STRUCTURAL_ALLOWED_LEVEL_TYPES: list[str] = _parse_csv_types(
    os.getenv("STRUCTURAL_ALLOWED_LEVEL_TYPES", ""),
    "vp_local,manual_global_hvn",
)
STRUCTURAL_MIN_CANDIDATES_PER_SIDE = int(os.getenv("STRUCTURAL_MIN_CANDIDATES_PER_SIDE", "1"))
STRUCTURAL_TOP_K = int(os.getenv("STRUCTURAL_TOP_K", "5"))
# Минимум монет с парой W∈[W_MIN,W_MAX] для медианы W* (эталон в один срез).
STRUCTURAL_N_ETALON = int(os.getenv("STRUCTURAL_N_ETALON", "3"))
STRUCTURAL_W_MIN = float(os.getenv("STRUCTURAL_W_MIN", "0.7"))
STRUCTURAL_W_MAX = float(os.getenv("STRUCTURAL_W_MAX", "2.5"))
# Доля от W*: effective_slack = max(STRUCTURAL_W_SLACK_ABS_MIN, W* * PCT/100).
STRUCTURAL_W_SLACK_PCT = float(os.getenv("STRUCTURAL_W_SLACK_PCT", "15"))
# Нижняя граница люфта в единицах W (ширина канала в ATR), независимо от W*.
STRUCTURAL_W_SLACK_ABS_MIN = float(os.getenv("STRUCTURAL_W_SLACK_ABS_MIN", "0.3"))
# Полоса «у линии L/U» для событий края: ± (PCT_ATR/100) * ATR в цене (по умолчанию 15% ATR).
_STRUCT_EDGE_PCT = os.getenv("STRUCTURAL_EDGE_ATR_PCT", "15").strip()
STRUCTURAL_EDGE_ATR_FRAC = float(_STRUCT_EDGE_PCT) / 100.0 if _STRUCT_EDGE_PCT else 0.15
# В отчётах z_w = |W-W*|/slack; ok при z_w <= threshold (дублируется в колонке pool_k).
STRUCTURAL_Z_W_OK_THRESHOLD = float(os.getenv("STRUCTURAL_Z_W_OK_THRESHOLD", "1"))
STRUCTURAL_STRENGTH_FIRST_ENABLED = os.getenv("STRUCTURAL_STRENGTH_FIRST_ENABLED", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
STRUCTURAL_MIN_POOL_SYMBOLS = int(os.getenv("STRUCTURAL_MIN_POOL_SYMBOLS", "3"))
STRUCTURAL_MID_BAND_PCT = float(os.getenv("STRUCTURAL_MID_BAND_PCT", "15"))
# Используется в cycle_levels_db при дозаполнении противоположной стороны (не в эталоне structural).
STRUCTURAL_MAD_K = float(os.getenv("STRUCTURAL_MAD_K", "3"))
STRUCTURAL_REFINE_MAX_ROUNDS = int(os.getenv("STRUCTURAL_REFINE_MAX_ROUNDS", "3"))
# Авто-отбор входного пула символов перед structural:
# 1) ликвидность avg_volume_24h, 2) корреляция к benchmark, 3) "скорость корреляции".
STRUCTURAL_POOL_SELECTOR_ENABLED = os.getenv("STRUCTURAL_POOL_SELECTOR_ENABLED", "0").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
STRUCTURAL_POOL_TARGET_SIZE = int(os.getenv("STRUCTURAL_POOL_TARGET_SIZE", "30"))
STRUCTURAL_POOL_MIN_SIZE = int(os.getenv("STRUCTURAL_POOL_MIN_SIZE", "15"))
STRUCTURAL_POOL_TIMEFRAME = os.getenv("STRUCTURAL_POOL_TIMEFRAME", "1h").strip() or "1h"
STRUCTURAL_POOL_CORR_LOOKBACK_BARS = int(os.getenv("STRUCTURAL_POOL_CORR_LOOKBACK_BARS", "168"))
STRUCTURAL_POOL_CORR_VELOCITY_WINDOW_BARS = int(os.getenv("STRUCTURAL_POOL_CORR_VELOCITY_WINDOW_BARS", "24"))
STRUCTURAL_POOL_BENCHMARK_SYMBOLS: list[str] = _parse_csv_types(
    os.getenv("STRUCTURAL_POOL_BENCHMARK_SYMBOLS", ""),
    "BTC/USDT,ETH/USDT",
)
STRUCTURAL_POOL_CORR_WEIGHT = float(os.getenv("STRUCTURAL_POOL_CORR_WEIGHT", "0.8"))
STRUCTURAL_POOL_VELOCITY_WEIGHT = float(os.getenv("STRUCTURAL_POOL_VELOCITY_WEIGHT", "0.2"))
STRUCTURAL_AUTO_FREEZE_ON_SCAN = os.getenv("STRUCTURAL_AUTO_FREEZE_ON_SCAN", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
# Групповой триггер mid-touch: минимум уникальных монет, коснувшихся своей mid-band.
STRUCTURAL_N_TOUCH = int(os.getenv("STRUCTURAL_N_TOUCH", "3"))
# Окно времени для набора N касаний (сек): если за это время N не набралось, триггера нет.
STRUCTURAL_TOUCH_WINDOW_SEC = int(os.getenv("STRUCTURAL_TOUCH_WINDOW_SEC", "43200"))
# После набора N: буфер-стабилизация перед входным этапом (сек).
# В это время structural еще контролирует отмену цикла при коллективном пробое.
STRUCTURAL_ENTRY_TIMER_SEC = int(os.getenv("STRUCTURAL_ENTRY_TIMER_SEC", "300"))
# Коллективная отмена: если в текущем срезе пробилось >= N символов.
STRUCTURAL_N_ABORT = int(os.getenv("STRUCTURAL_N_ABORT", "3"))
# Порог пробоя от границ канала в ATR (L-... или U+...).
STRUCTURAL_ABORT_DIST_ATR = float(os.getenv("STRUCTURAL_ABORT_DIST_ATR", "0.25"))
# Дебаунс повторного mid-touch по одному и тому же символу (сек).
STRUCTURAL_TOUCH_DEBOUNCE_SEC = int(os.getenv("STRUCTURAL_TOUCH_DEBOUNCE_SEC", "5"))
STRUCTURAL_POLL_SEC = float(os.getenv("STRUCTURAL_POLL_SEC", "1.0"))
STRUCTURAL_MAX_RUNTIME_SEC = int(os.getenv("STRUCTURAL_MAX_RUNTIME_SEC", "72000"))
# Историческое восстановление группового mid-touch события (например после рестарта):
# если за lookback уже было >= N уникальных касаний, сразу переходим к entry_timer.
STRUCTURAL_TOUCH_HISTORY_LOOKBACK_SEC = int(os.getenv("STRUCTURAL_TOUCH_HISTORY_LOOKBACK_SEC", "14400"))
STRUCTURAL_TOUCH_HISTORY_MIN_SYMBOLS = int(
    os.getenv("STRUCTURAL_TOUCH_HISTORY_MIN_SYMBOLS", str(STRUCTURAL_N_TOUCH))
)
# Дедуп группового события в одном цикле.
STRUCTURAL_GROUP_TOUCH_DEDUP_SEC = int(os.getenv("STRUCTURAL_GROUP_TOUCH_DEDUP_SEC", "300"))
# После группового триггера: не слать повторный сигнал N сек (0 = без блокировки повтора).
STRUCTURAL_TRIGGER_TIMEOUT_SEC = int(os.getenv("STRUCTURAL_TRIGGER_TIMEOUT_SEC", "300"))


@dataclass(frozen=True)
class StructuralSettings:
    """Снимок параметров structural v2 (эталон W*, зоны, групповые триггеры)."""

    N_ETALON_MIN: int
    W_GLOBAL_MIN: float
    W_GLOBAL_MAX: float
    W_SLACK_FRAC: float
    W_SLACK_ABS_MIN: float
    MID_BAND_PCT: float
    EDGE_TOLERANCE_ATR_FRAC: float
    N_TRIGGER: int
    N_BREAKOUT: int
    TRIGGER_TIMEOUT_SEC: int
    BREAKOUT_ATR_FRAC: float
    TOP_K_PER_SIDE: int
    ALLOWED_LEVEL_TYPES: tuple[str, ...]
    MIN_CANDIDATES_PER_SIDE: int


STRUCTURAL_SETTINGS = StructuralSettings(
    N_ETALON_MIN=STRUCTURAL_N_ETALON,
    W_GLOBAL_MIN=STRUCTURAL_W_MIN,
    W_GLOBAL_MAX=STRUCTURAL_W_MAX,
    W_SLACK_FRAC=STRUCTURAL_W_SLACK_PCT / 100.0,
    W_SLACK_ABS_MIN=STRUCTURAL_W_SLACK_ABS_MIN,
    MID_BAND_PCT=STRUCTURAL_MID_BAND_PCT,
    EDGE_TOLERANCE_ATR_FRAC=STRUCTURAL_EDGE_ATR_FRAC,
    N_TRIGGER=STRUCTURAL_N_TOUCH,
    N_BREAKOUT=STRUCTURAL_N_ABORT,
    TRIGGER_TIMEOUT_SEC=STRUCTURAL_TRIGGER_TIMEOUT_SEC,
    BREAKOUT_ATR_FRAC=STRUCTURAL_ABORT_DIST_ATR,
    TOP_K_PER_SIDE=STRUCTURAL_TOP_K,
    ALLOWED_LEVEL_TYPES=tuple(STRUCTURAL_ALLOWED_LEVEL_TYPES),
    MIN_CANDIDATES_PER_SIDE=STRUCTURAL_MIN_CANDIDATES_PER_SIDE,
)

# Контур слоя 4: дозаполнение недостающей противоположной стороны канала после входа/переворота.
STRUCTURAL_OPPOSITE_REBUILD_ENABLED = os.getenv("STRUCTURAL_OPPOSITE_REBUILD_ENABLED", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
STRUCTURAL_OPPOSITE_REBUILD_DEADLINE_SEC = int(
    os.getenv("STRUCTURAL_OPPOSITE_REBUILD_DEADLINE_SEC", "14400")
)
# Множитель диапазона ATR для rebuild противоположной стороны:
# fit_band = [STRUCTURAL_W_MIN * mult, STRUCTURAL_W_MAX * mult].
STRUCTURAL_OPPOSITE_REBUILD_BAND_MULT = float(
    os.getenv("STRUCTURAL_OPPOSITE_REBUILD_BAND_MULT", "2.0")
)

# Отчёт structural-scan → Google Sheets (лист по умолчанию; переименование через env).
STRUCTURAL_LEVELS_REPORT_WORKSHEET = (
    os.getenv("STRUCTURAL_LEVELS_REPORT_WORKSHEET", "structural_levels_report").strip()
    or "structural_levels_report"
)
STRUCTURAL_LEVELS_REPORT_V2_WORKSHEET = (
    os.getenv("STRUCTURAL_LEVELS_REPORT_V2_WORKSHEET", "structural_levels_report_v2").strip()
    or "structural_levels_report_v2"
)
STRUCTURAL_LEVELS_REPORT_V3_WORKSHEET = (
    os.getenv("STRUCTURAL_LEVELS_REPORT_V3_WORKSHEET", "structural_levels_report_v3").strip()
    or "structural_levels_report_v3"
)
STRUCTURAL_LEVELS_REPORT_V4_WORKSHEET = (
    os.getenv("STRUCTURAL_LEVELS_REPORT_V4_WORKSHEET", "structural_levels_report_v4").strip()
    or "structural_levels_report_v4"
)
# v4: полоса от ref в ATR для выбора сильнейшего уровня ниже/выше (vp_local + manual_global_hvn).
STRUCTURAL_V4_BAND_MIN_ATR = float(os.getenv("STRUCTURAL_V4_BAND_MIN_ATR", "1.5"))
STRUCTURAL_V4_BAND_MAX_ATR = float(os.getenv("STRUCTURAL_V4_BAND_MAX_ATR", "4.5"))
STRUCTURAL_V4_LEVELS_FETCH_LIMIT = int(os.getenv("STRUCTURAL_V4_LEVELS_FETCH_LIMIT", "2000"))
# Операционный контур: лог + Telegram + Google Sheets (без смешивания с сигналами входа).
STRUCTURAL_OPS_LOG = os.getenv("STRUCTURAL_OPS_LOG", "1").strip().lower() in ("1", "true", "yes", "on")
STRUCTURAL_OPS_TELEGRAM = os.getenv("STRUCTURAL_OPS_TELEGRAM", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
STRUCTURAL_OPS_SHEETS_LEVELS = os.getenv("STRUCTURAL_OPS_SHEETS_LEVELS", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
STRUCTURAL_OPS_SHEETS_LOG = os.getenv("STRUCTURAL_OPS_SHEETS_LOG", "0").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
# Каждое mid_touch в Telegram (шумно); по умолчанию только entry_timer (N_touch).
STRUCTURAL_OPS_TELEGRAM_EACH_MID_TOUCH = os.getenv(
    "STRUCTURAL_OPS_TELEGRAM_EACH_MID_TOUCH", "0"
).strip().lower() in ("1", "true", "yes", "on")
STRUCTURAL_OPS_LOG_WORKSHEET = (
    os.getenv("STRUCTURAL_OPS_LOG_WORKSHEET", "structural_ops_log").strip() or "structural_ops_log"
)
# Журнал каждого mid_touch в Sheets (очень шумно); по умолчанию только фазы и пробои.
STRUCTURAL_OPS_SHEETS_LOG_EACH_MID_TOUCH = os.getenv(
    "STRUCTURAL_OPS_SHEETS_LOG_EACH_MID_TOUCH", "0"
).strip().lower() in ("1", "true", "yes", "on")

# -----------------------------------------------------------------------------
# Ops stage telemetry (pipeline-level observability, low-noise)
# -----------------------------------------------------------------------------
OPS_STAGE_LOG = os.getenv("OPS_STAGE_LOG", "1").strip().lower() in ("1", "true", "yes", "on")
# Telegram по этапам supervisor — по умолчанию выкл. (минимум шума); логи этапов в БД при OPS_STAGE_LOG=1.
OPS_STAGE_TELEGRAM = os.getenv("OPS_STAGE_TELEGRAM", "1").strip().lower() in ("1", "true", "yes", "on")
# When enabled, Telegram only receives end-of-stage statuses and failures (no start spam).
OPS_STAGE_TELEGRAM_ONLY_FINAL = os.getenv("OPS_STAGE_TELEGRAM_ONLY_FINAL", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
OPS_STAGE_SHEETS = os.getenv("OPS_STAGE_SHEETS", "1").strip().lower() in ("1", "true", "yes", "on")
OPS_STAGE_WORKSHEET = os.getenv("OPS_STAGE_WORKSHEET", "ops_stages").strip() or "ops_stages"
# После rebuild vp_local: выгрузка листа vp_local_levels (та же книга MARKET_AUDIT_*), независимо от OPS_STAGE_SHEETS.
SUPERVISOR_EXPORT_VP_LOCAL_AFTER_LEVELS_REBUILD = os.getenv(
    "SUPERVISOR_EXPORT_VP_LOCAL_AFTER_LEVELS_REBUILD", "1"
).strip().lower() not in ("0", "false", "no", "off")
# Entry-тик: открытые позиции по cycle_id + append закрытых в Google Sheets.
SHEETS_TRADING_CYCLE_SYNC = os.getenv("SHEETS_TRADING_CYCLE_SYNC", "1").strip().lower() not in (
    "0",
    "false",
    "no",
    "off",
)
CYCLE_OPEN_POSITIONS_WORKSHEET = (
    os.getenv("CYCLE_OPEN_POSITIONS_WORKSHEET", "cycle_open_positions").strip() or "cycle_open_positions"
)
CYCLE_TRADING_STATS_WORKSHEET = (
    os.getenv("CYCLE_TRADING_STATS_WORKSHEET", "cycle_trading_stats").strip() or "cycle_trading_stats"
)

# -----------------------------------------------------------------------------
# Supervisor loop (единый авто-оркестратор на базе существующих модулей)
# -----------------------------------------------------------------------------
SUPERVISOR_LOOP_ENABLED = os.getenv("SUPERVISOR_LOOP_ENABLED", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
ALL_ACTIVE_LEVELS_WORKSHEET = "all_active_levels"
SUPERVISOR_POLL_SEC = float(os.getenv("SUPERVISOR_POLL_SEC", "2"))
# Как часто обновлять базовые данные (spot/macro/indices/oi/instruments), сек.
SUPERVISOR_DATA_REFRESH_SEC = int(os.getenv("SUPERVISOR_DATA_REFRESH_SEC", "900"))
# Как часто пересчитывать vp_local в БД и выгружать vp_local_levels, сек.
SUPERVISOR_LEVELS_REBUILD_SEC = int(os.getenv("SUPERVISOR_LEVELS_REBUILD_SEC", "3600"))
# Как часто запускать structural цикл (scan/realtime/freeze), сек.
SUPERVISOR_STRUCTURAL_SEC = int(os.getenv("SUPERVISOR_STRUCTURAL_SEC", "3600"))
# Не запускать плановый structural, пока freeze активен и цикл не закрыт (arming = ждём V3-вход; in_position = в сделке).
# Новый полный structural после cycle_phase=closed (и обычно levels_frozen=0). Противоположная сторона — maintenance в entry-тике.
SUPERVISOR_STRUCTURAL_SKIP_WHEN_CYCLE_ACTIVE = os.getenv(
    "SUPERVISOR_STRUCTURAL_SKIP_WHEN_CYCLE_ACTIVE", "1"
).strip().lower() in ("1", "true", "yes", "on")
# Если structural пропущен из‑за активного цикла — через сколько секунд снова проверить условие (не ждать полный STRUCTURAL_SEC).
SUPERVISOR_STRUCTURAL_RETRY_WHEN_BLOCKED_SEC = int(
    os.getenv("SUPERVISOR_STRUCTURAL_RETRY_WHEN_BLOCKED_SEC", "120")
)
# Как часто выполнять тик entry detector (monitor + gate + maintenance + reconcile), сек.
SUPERVISOR_ENTRY_TICK_SEC = int(os.getenv("SUPERVISOR_ENTRY_TICK_SEC", "10"))
# Перед запуском structural: выгрузка актуальных vp_local из БД в Google Sheet (лист как в export_volume_peaks_to_sheets_only).
SUPERVISOR_EXPORT_VP_LOCAL_BEFORE_STRUCTURAL = os.getenv(
    "SUPERVISOR_EXPORT_VP_LOCAL_BEFORE_STRUCTURAL", "1"
).strip().lower() in ("1", "true", "yes", "on")

# Какие шаги делать в supervisor `DATA_REFRESH` (и в load_all_data incremental — те же имена).
# 0 / false / no / off — пропустить; 1 / true / yes / on — выполнить.
# По умолчанию: spot по TRADING_SYMBOLS + instruments + ATR — вкл.; макро, TV, OI, crypto_context spot — выкл.
def _supervisor_data_refresh_on(name: str, *, default: str = "1") -> bool:
    return os.getenv(name, default).strip().lower() not in ("0", "false", "no", "off")


SUPERVISOR_DATA_REFRESH_SPOT_MAIN = _supervisor_data_refresh_on("SUPERVISOR_DATA_REFRESH_SPOT_MAIN")
SUPERVISOR_DATA_REFRESH_SPOT_CRYPTO_CONTEXT = _supervisor_data_refresh_on(
    "SUPERVISOR_DATA_REFRESH_SPOT_CRYPTO_CONTEXT",
    default="0",
)
SUPERVISOR_DATA_REFRESH_MACRO = _supervisor_data_refresh_on("SUPERVISOR_DATA_REFRESH_MACRO", default="0")
SUPERVISOR_DATA_REFRESH_INDICES_TV = _supervisor_data_refresh_on(
    "SUPERVISOR_DATA_REFRESH_INDICES_TV", default="0"
)
SUPERVISOR_DATA_REFRESH_OI_BYBIT = _supervisor_data_refresh_on("SUPERVISOR_DATA_REFRESH_OI_BYBIT", default="0")
SUPERVISOR_DATA_REFRESH_INSTRUMENTS = _supervisor_data_refresh_on("SUPERVISOR_DATA_REFRESH_INSTRUMENTS")
SUPERVISOR_DATA_REFRESH_INSTRUMENTS_ATR = _supervisor_data_refresh_on("SUPERVISOR_DATA_REFRESH_INSTRUMENTS_ATR")

# Лимит открытых позиций в одном направлении (инвариант из tutorial_v3/вашей политики).
MAX_POSITIONS_PER_SIDE = int(os.getenv("MAX_POSITIONS_PER_SIDE", "10"))

# -----------------------------------------------------------------------------
# Google: сервисный аккаунт (Sheets и др.)
# -----------------------------------------------------------------------------
_gcp = _env_strip_quotes(os.getenv("GOOGLE_CREDENTIALS_PATH", ""))
GOOGLE_CREDENTIALS_PATH = _gcp if _gcp else "credentials.json"

# -----------------------------------------------------------------------------
# Ручные глобальные уровни HVN (отдельная книга Sheets → price_levels manual_global_hvn)
# ID книги: MANUAL_GLOBAL_HVN_SPREADSHEET_ID или общий GOOGLE_SHEETS_ID (если одна книга для ручных уровней).
# -----------------------------------------------------------------------------
MANUAL_GLOBAL_HVN_SPREADSHEET_ID = _env_strip_quotes(
    os.getenv("MANUAL_GLOBAL_HVN_SPREADSHEET_ID", "")
) or _env_strip_quotes(os.getenv("GOOGLE_SHEETS_ID", ""))
MANUAL_GLOBAL_HVN_SPREADSHEET_URL = os.getenv("MANUAL_GLOBAL_HVN_SPREADSHEET_URL", "").strip()
MANUAL_GLOBAL_HVN_SPREADSHEET_TITLE = os.getenv("MANUAL_GLOBAL_HVN_SPREADSHEET_TITLE", "").strip()
MANUAL_GLOBAL_HVN_INSTRUCTION_SHEET = os.getenv("MANUAL_GLOBAL_HVN_INSTRUCTION_SHEET", "instruction").strip()

# -----------------------------------------------------------------------------
# Telegram (общий бот и джобы; см. trading_bot.tools.telegram_notify)
# -----------------------------------------------------------------------------
TELEGRAM_BOT_TOKEN = _env_strip_quotes(os.getenv("TELEGRAM_BOT_TOKEN", "")) or _env_strip_quotes(
    os.getenv("TELEGRAM_TOKEN", "")
)
TELEGRAM_CHAT_ID = _env_strip_quotes(os.getenv("TELEGRAM_CHAT_ID", "")) or _env_strip_quotes(
    os.getenv("CHAT_ID", "")
)

# ============================================================================
# TEST MODE - Тестовый контур для быстрой проверки (без ожидания реальных движений)
# ============================================================================

# Включить тестовый режим (генерация искусственных уровней)
TEST_MODE = os.getenv("TEST_MODE", "0").strip().lower() in ("1", "true", "yes", "on")

# Смещение тестовых уровней от текущей цены в ATR
# LONG уровень: current_price - TEST_LEVEL_OFFSET_ATR * atr
# SHORT уровень: current_price + TEST_LEVEL_OFFSET_ATR * atr
TEST_LEVEL_OFFSET_ATR = float(os.getenv("TEST_LEVEL_OFFSET_ATR", "0.2"))

# Смещение для rebuild противоположной стороны в ATR
# После входа в LONG: SHORT уровень = current_price + TEST_OPPOSITE_OFFSET_ATR * atr
TEST_OPPOSITE_OFFSET_ATR = float(os.getenv("TEST_OPPOSITE_OFFSET_ATR", "0.4"))

# Количество символов для тестового цикла
TEST_CYCLE_SYMBOLS_COUNT = int(os.getenv("TEST_CYCLE_SYMBOLS_COUNT", "10"))

# Интервал тестового цикла (сек) - как часто пересоздавать уровни
TEST_CYCLE_INTERVAL_SEC = int(os.getenv("TEST_CYCLE_INTERVAL_SEC", "60"))

# ============================================================================
# TEST MODE DATA OPTIMIZATION - Ускоренный режим без загрузки данных
# ============================================================================

# Автоматически отключаем загрузку данных в тестовом режиме (override вручную заданных значений)
if TEST_MODE:
    TEST_MODE_SKIP_DATA_REFRESH = True
    TEST_MODE_SKIP_LEVELS_REBUILD = True
    TEST_MODE_SKIP_VP_EXPORT = True
else:
    TEST_MODE_SKIP_DATA_REFRESH = False
    TEST_MODE_SKIP_LEVELS_REBUILD = False
    TEST_MODE_SKIP_VP_EXPORT = False
