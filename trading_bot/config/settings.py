"""Пути, API, источники данных и маппинги тикеров."""

from __future__ import annotations

import os

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

PRO_LEVELS_LOOKBACK_DAYS = 30  # 30 дней от последней 1m-свечи в БД
PRO_LEVELS_LOOKBACK_HOURS = None

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
# Гейт для legacy rebuild_cycle_levels (из price_levels -> cycle_levels).
# False: разрешён только явный вызов с force=True (например из orchestrator/ручного скрипта).
CYCLE_LEVELS_REBUILD_ENABLED = os.getenv("CYCLE_LEVELS_REBUILD_ENABLED", "0").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)

# Structural cycle (§3 cycle_structural_start_spec): пул (L,U), MAD, mid-полоса → freeze в cycle_levels.
# Источник ref для скана кандидатов из price_levels: `db_1m_close` — как vp_local (Binance spot в БД);
# `price_feed` — Bybit WS/REST (может расходиться со spot-уровнями).
STRUCTURAL_REF_PRICE_SOURCE = (
    os.getenv("STRUCTURAL_REF_PRICE_SOURCE", "db_1m_close").strip().lower() or "db_1m_close"
)
# Отдельно от CYCLE_LEVELS_ALLOWED_LEVEL_TYPES. Human (D1/W1) в structural по умолчанию не берём —
# только vp_local + ручные глобальные HVN. При экстремальной цене vp_local часто не закрывает обе
# стороны: это ограничение исходных уровней, не «узкое окно» N_touch (оно другое).
STRUCTURAL_ALLOWED_LEVEL_TYPES: list[str] = _parse_csv_types(
    os.getenv("STRUCTURAL_ALLOWED_LEVEL_TYPES", ""),
    "vp_local,manual_global_hvn",
)
# Минимум кандидатов с каждой стороны от ref (для входа в пул достаточно 1+1; top-K — отдельно).
STRUCTURAL_MIN_CANDIDATES_PER_SIDE = int(os.getenv("STRUCTURAL_MIN_CANDIDATES_PER_SIDE", "1"))
STRUCTURAL_TOP_K = int(os.getenv("STRUCTURAL_TOP_K", "5"))
STRUCTURAL_MAD_K = float(os.getenv("STRUCTURAL_MAD_K", "3"))
STRUCTURAL_CENTER_FILTER_ENABLED = os.getenv("STRUCTURAL_CENTER_FILTER_ENABLED", "0").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
STRUCTURAL_CENTER_MAD_K = float(os.getenv("STRUCTURAL_CENTER_MAD_K", "2.5"))
STRUCTURAL_TARGET_ALIGN_ENABLED = os.getenv("STRUCTURAL_TARGET_ALIGN_ENABLED", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
STRUCTURAL_ANCHOR_SYMBOLS: list[str] = _parse_csv_types(
    os.getenv("STRUCTURAL_ANCHOR_SYMBOLS", ""),
    "BTC/USDT,ETH/USDT,SOL/USDT,ADA/USDT",
)
STRUCTURAL_TARGET_W_BAND_K = float(os.getenv("STRUCTURAL_TARGET_W_BAND_K", "2.5"))
STRUCTURAL_TARGET_CENTER_WEIGHT = float(os.getenv("STRUCTURAL_TARGET_CENTER_WEIGHT", "1.0"))
STRUCTURAL_TARGET_WIDTH_WEIGHT = float(os.getenv("STRUCTURAL_TARGET_WIDTH_WEIGHT", "0.6"))
STRUCTURAL_MIN_POOL_SYMBOLS = int(os.getenv("STRUCTURAL_MIN_POOL_SYMBOLS", "5"))
STRUCTURAL_MID_BAND_PCT = float(os.getenv("STRUCTURAL_MID_BAND_PCT", "2"))
STRUCTURAL_REFINE_MAX_ROUNDS = int(os.getenv("STRUCTURAL_REFINE_MAX_ROUNDS", "5"))
STRUCTURAL_AUTO_FREEZE_ON_SCAN = os.getenv("STRUCTURAL_AUTO_FREEZE_ON_SCAN", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
STRUCTURAL_N_TOUCH = int(os.getenv("STRUCTURAL_N_TOUCH", "5"))
STRUCTURAL_TOUCH_WINDOW_SEC = int(os.getenv("STRUCTURAL_TOUCH_WINDOW_SEC", "900"))
STRUCTURAL_ENTRY_TIMER_SEC = int(os.getenv("STRUCTURAL_ENTRY_TIMER_SEC", "300"))
STRUCTURAL_N_ABORT = int(os.getenv("STRUCTURAL_N_ABORT", "5"))
STRUCTURAL_ABORT_DIST_ATR = float(os.getenv("STRUCTURAL_ABORT_DIST_ATR", "0.3"))
STRUCTURAL_TOUCH_DEBOUNCE_SEC = int(os.getenv("STRUCTURAL_TOUCH_DEBOUNCE_SEC", "5"))
STRUCTURAL_POLL_SEC = float(os.getenv("STRUCTURAL_POLL_SEC", "1.0"))
STRUCTURAL_MAX_RUNTIME_SEC = int(os.getenv("STRUCTURAL_MAX_RUNTIME_SEC", "7200"))

# Отчёт structural-scan → Google Sheets (лист по умолчанию; переименование через env).
STRUCTURAL_LEVELS_REPORT_WORKSHEET = (
    os.getenv("STRUCTURAL_LEVELS_REPORT_WORKSHEET", "structural_levels_report").strip()
    or "structural_levels_report"
)

def _env_strip_quotes(val: str) -> str:
    s = (val or "").strip()
    if len(s) >= 2 and s[0] == s[-1] and s[0] in ("'", '"'):
        s = s[1:-1].strip()
    return s


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
