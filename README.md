# Market Bot

Проект для сбора и хранения рыночных данных в SQLite с последующей проверкой качества данных через Google Sheets.

## Что уже реализовано

### 1) База данных и схема

- SQLite БД: `data/market_data.db`
- Таблицы:
  - `ohlcv`
  - `liquidations`
  - `open_interest`
  - `metadata`
  - `db_version`
- Индексы:
  - `idx_ohlcv_unique` на `(symbol, timeframe, timestamp)`
  - `idx_ohlcv_symbol_timeframe_timestamp`
  - `idx_ohlcv_timeframe_timestamp`
  - `idx_liquidations_symbol_timeframe_timestamp`
  - `idx_oi_symbol_timeframe_timestamp`
- Для `timeframe` добавлен `CHECK` (включая `1w`):
  - `('1m', '1h', '4h', '1d', '1w', '1W', '1M')`

> Важно: `CHECK` применяется для новых таблиц. Если БД была создана ранее, требуется пересоздание/миграция. В текущем проекте БД уже пересоздавалась.

---

### 2) Утилиты времени

Файл: `src/utils/time_utils.py`

Добавлены функции:

- `to_utc_timestamp(dt)` - перевод `datetime` в UTC timestamp (сек).
- `align_to_day_start(ts, tz='UTC')` - начало дня (`00:00`) в указанной TZ, результат в UTC timestamp.
- `align_to_week_start(ts)` - начало недели (понедельник `00:00 UTC`).
- `align_to_month_start(ts)` - начало месяца (`1-е число, 00:00 UTC`).
- `binance_to_utc(ms)` - Binance milliseconds -> UTC seconds (округление вниз).

---

### 3) Клиент Binance (spot OHLCV)

Файл: `src/provider/clients/binance.py`

Реализовано:

- `fetch_ohlcv(symbol, timeframe, since=None, limit=None)`
  - через `ccxt.binance()`
  - `since` (сек) -> ms для API
  - retry с экспоненциальной задержкой
  - обработка сетевых/лимитных ошибок
  - нормализация `1W -> 1w`
- `get_symbol_listing_ts(symbol)` для попытки определить дату листинга.

---

### 4) Клиент YFinance (macro)

Файл: `src/provider/clients/yfinance.py`

Реализовано:

- `YFinanceClient(ticker_map, timezone='US/Eastern')`
- `fetch_ohlcv(...)` для `1d`, `1w`, `1M`
- Приведение времени к UTC и выравнивание:
  - `1d`: `00:00 UTC`
  - `1w`: понедельник `00:00 UTC`
  - `1M`: первый день месяца `00:00 UTC`
- Retry (`tenacity`) для временных ошибок.
- `fetch_open_interest`/`fetch_liquidations` -> `NotImplementedError`.

Тесты:

- `tests/test_yfinance_client.py` (`5 passed`)
  - валидация symbol/timeframe
  - выравнивание 1d/1w/1M

---

### 5) Клиенты CoinGecko и CoinGlass (первичная реализация)

Файлы:

- `src/provider/clients/coingecko.py`
- `src/provider/clients/coinglass.py`

Статус:

- `CoinGeckoClient` реализован (global metrics, index builder TOTAL/TOTAL2/BTCD, агрегация 1w/1M).
- `CoinGlassClient` реализован, но в стартовом контуре **не используется** (см. ниже решение по источникам).

---

### 6) Фабрика клиентов

Файл: `src/provider/exchange_factory.py`

Поддержаны источники:

- `binance`
- `yfinance`
- `coingecko`
- `coinglass` (технически есть, но в текущем боевом потоке не используется)

---

### 7) Data collectors

Файл: `src/miner/collectors.py`

Реализовано:

- `update_binance_ohlcv(symbol, timeframe, days_back=None)`
  - первичная загрузка + инкремент
  - `1m` ограничен `MINUTE_DATA_RETENTION_DAYS`
- `update_yfinance_ohlcv(symbol, timeframe, days_back=None)`
- `update_yfinance_macro_all(...)`
- Fallback для `yfinance 1w`:
  - если weekly пустой, строится из `1d` (OHLC + sum(volume), якорь Monday 00:00 UTC)
- `update_indices(...)` (CoinGecko индексы, статус зависит от доступности endpoint)

Для Binance Futures:

- `update_futures_open_interest(symbol, period='4h', days_back=30)` - работает без ключей.
- `update_liquidation_history(symbol, timeframe='4h', days_back=30)` - требует API key/secret.
- `update_all_futures_data(days_back=30)` - общий запуск по всем `TRADING_SYMBOLS`.

---

### 8) Binance Futures вместо CoinGlass (принятое решение)

Принято в проекте:

- На старте **не используем CoinGlass** из-за стоимости.
- OI и ликвидации берем из Binance Futures API.
- Пока работаем **без ключей**:
  - OI собирается.
  - Ликвидации не собираются (и это нормально на текущем этапе).

Поддержка в коде:

- `fetch_open_interest_history` - REST futures, периоды `1h/4h/1d`.
- `fetch_liquidation_orders` - signed endpoint `/fapi/v1/forceOrders`, требует:
  - `BINANCE_API_KEY`
  - `BINANCE_API_SECRET`

---

### 9) Планировщик

Файл: `src/miner/scheduler.py`

Расписание:

- Spot OHLCV:
  - `1m` - каждые 15 минут
  - `1h` - каждый час
  - `4h` - каждые 4 часа
  - `1d` - ежедневно в `01:00 UTC`
  - `1w` - воскресенье `23:00 UTC`
  - `1M` - 1 число месяца `00:00 UTC`
- Futures данные:
  - `update_all_futures_data` - каждые 4 часа

Запуск:

- `python scripts/run_scheduler.py`

---

### 10) Экспорт в Google Sheets (аудит качества)

Файлы:

- `src/tools/sheets_exporter.py`
- `scripts/export_to_sheets.py`

Листы в таблице:

- `binance_ohlcv_sample`
- `macro_sample`
- `indices_sample`
- `coinglass_sample` *(в текущем потоке используется как sample futures OI/liq)*
- `audit_log`

Поддержка открытия таблицы:

- по `MARKET_AUDIT_SHEET_URL`
- по `MARKET_AUDIT_SHEET_ID`
- по `MARKET_AUDIT_SHEET_TITLE`

Запуск:

- `python scripts/export_to_sheets.py`

---

## Текущее состояние данных (по последнему прогону)

- `binance_ohlcv_sample`: заполняется
- `macro_sample`: заполняется (`yfinance`)
- `indices_sample`: может быть пустым (зависит от доступности исторических global-метрик CoinGecko)
- `coinglass_sample`: содержит данные из Binance Futures OI (и ликвидации, если будут ключи)

---

## Конфигурация (`config.py`)

Ключевые параметры:

- Символы:
  - `TRADING_SYMBOLS`
  - `ANALYTIC_SYMBOLS`
- Таймфреймы:
  - `TIMEFRAMES`
  - `TIMEFRAMES_BY_CATEGORY`
- Источники:
  - `SOURCE_BINANCE`
  - `SOURCE_YFINANCE`
  - `SOURCE_COINGECKO`
  - `SOURCE_COINGLASS`
- YFinance:
  - `YFINANCE_TICKERS`
  - `YFINANCE_TIMEZONE`
- Ретраи/лимиты:
  - `API_RETRY_ATTEMPTS`
  - `API_RETRY_DELAY`
  - `COINGECKO_DELAY`
  - `COINGLASS_DELAY`
- Прочее:
  - `FILL_MISSING_WEEKENDS`
  - `MINUTE_DATA_RETENTION_DAYS`
- Binance signed futures (опционально):
  - `BINANCE_API_KEY`
  - `BINANCE_API_SECRET`

---

## Быстрый старт

1. Установить зависимости (минимум):
   - `ccxt`, `yfinance`, `pandas`, `requests`, `tenacity`, `gspread`, `schedule`, `pytest`
2. Проверить/заполнить `config.py`.
3. Для Google Sheets:
   - положить `credentials.json` в корень
   - дать сервисному аккаунту доступ к таблице
4. Прогнать сбор:
   - вручную через функции коллекторов
   - или `python scripts/run_scheduler.py`
5. Экспорт:
   - `python scripts/export_to_sheets.py`

---

## Что можно сделать следующим шагом

- Переименовать лист `coinglass_sample` -> `futures_liquidity_sample` (чтобы отражал текущий источник).
- Добавить явный флаг `ENABLE_LIQUIDATIONS=false`, чтобы не вызывать signed endpoint без ключей.
- Добавить fallback для `indices_sample`, если CoinGecko historical endpoint недоступен.
