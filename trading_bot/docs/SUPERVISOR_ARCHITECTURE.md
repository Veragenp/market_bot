# 🧠 Supervisor — Единый авто-оркестратор торгового бота

## 📋 Обзор

**Supervisor** — центральный модуль, который координирует работу всего торгового бота. Он запускает все этапы в правильном порядке, управляет состоянием и обеспечивает непрерывную работу.

**Файл**: `trading_bot/scripts/run_supervisor.py`

---

## 🔄 Жизненный цикл

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        ЗАПУСК SUPERVISOR                                 │
│  python -m trading_bot.scripts.run_supervisor [--loop]                   │
└──────────────────────┬──────────────────────────────────────────────────┘
                       │
                       ▼
            ┌──────────────────────┐
            │  init_db()           │
            │  run_migrations()    │
            └──────────┬───────────┘
                       │
                       ▼
            ┌──────────────────────┐
            │ determine_start_mode │ ← State Manager
            │  - FRESH_START       │
            │  - RECOVERY_CONTINUE │
            │  - CLEAN_STALE_...   │
            └──────────┬───────────┘
                       │
                       ▼
            ┌──────────────────────┐
            │  _run_data_refresh() │
            │  - Spot данные       │
            │  - Instruments       │
            │  - ATR               │
            └──────────┬───────────┘
                       │
                       ▼
            ┌──────────────────────┐
            │ _run_levels_rebuild()│
            │  - VP local rebuild  │
            │  - Export to Sheets  │
            └──────────┬───────────┘
                       │
                       ▼
            ┌──────────────────────┐
            │  _run_structural()   │ ← Генерация уровней
            │  - TEST_MODE:        │   (реальный или тестовый)
            │    test_level_...    │
            │  - PROD:             │
            │    structural_...    │
            └──────────┬───────────┘
                       │
                       ▼
            ┌──────────────────────┐
            │   _run_entry_tick()  │ ← Entry Detector
            │   - Monitor levels   │   (каждые 10 сек)
            │   - Entry gate       │
            │   - Rebuild opposite │
            │   - Reconcile        │
            └──────────┬───────────┘
                       │
                       ▼
            ┌──────────────────────┐
            │   Цикл повторяется   │
            │   (если --loop)      │
            └──────────────────────┘
```

---

## 📊 Этапы (по порядку)

### 1. Инициализация

**Функции**: `init_db()`, `run_migrations()`

**Что делает**:
- Создаёт таблицы БД если не существуют
- Применяет миграции (v1, v2, ..., v23)
- Проверяет целостность схемы

**Модули**:
- `trading_bot/data/schema.py` — определение таблиц и миграций
- `trading_bot/data/db.py` — подключение к SQLite

---

### 2. Определение режима старта

**Функция**: `determine_start_mode()`

**Что делает**:
- Анализирует текущее состояние БД и биржи
- Определяет один из 5 режимов:
  - `FRESH_START` — новый цикл (нет позиций)
  - `RECOVERY_ADD_MISSING` — позиции на бирже, нет в БД
  - `CLEAN_STALE_POSITIONS` — позиции в БД, нет на бирже
  - `RECOVERY_CONTINUE` — всё синхронизировано
  - `RECOVERY_SYNC_MISMATCH` — критический рассинхрон (требует ручного сброса)

**Модули**:
- `trading_bot/data/state_manager.py` — логика определения режима
- `trading_bot/tools/bybit_trading.py` — запрос позиций к бирже

---

### 3. Обработка режима старта

**Функции**:
- `handle_fresh_start()`
- `handle_recovery_add_missing()`
- `handle_clean_stale_positions()`
- `handle_recovery_continue()`
- `handle_recovery_sync_mismatch()`

**Что делает**:
- Приводит систему к согласованному состоянию
- Синхронизирует БД с биржей
- Генерирует session_id для аудита

---

### 4. Обновление данных (DATA_REFRESH)

**Функция**: `_run_data_refresh()`

**Интервал**: `SUPERVISOR_DATA_REFRESH_SEC` (по умолчанию 900 сек = 15 мин)

**Что делает**:
1. **Spot данные** — OHLCV 1m для всех символов из `TRADING_SYMBOLS`
2. **Instruments** — метаданные инструментов (tick_size, min_qty, atr)
3. **ATR** — расчёт ATR для каждого символа

**Модули**:
- `trading_bot/data/data_loader.py` — загрузчик данных
- `trading_bot/config/symbols.py` — список символов
- `trading_bot/tools/price_feed.py` — получение цен

**Конфигурация** (можно включать/выключать шаги):
```env
SUPERVISOR_DATA_REFRESH_SPOT_MAIN=1
SUPERVISOR_DATA_REFRESH_INSTRUMENTS=1
SUPERVISOR_DATA_REFRESH_INSTRUMENTS_ATR=1
SUPERVISOR_DATA_REFRESH_MACRO=0
SUPERVISOR_DATA_REFRESH_INDICES_TV=0
SUPERVISOR_DATA_REFRESH_OI_BYBIT=0
```

---

### 5. Пересчёт уровней (LEVELS_REBUILD)

**Функция**: `_run_levels_rebuild()`

**Интервал**: `SUPERVISOR_LEVELS_REBUILD_SEC` (по умолчанию 1800 сек = 30 мин)

**Что делает**:
1. **VP Local rebuild** — Volume Profile peaks для 1m данных (rolling 30 дней)
2. **Export to Sheets** — выгрузка `vp_local_levels` в Google Sheets

**Модули**:
- `trading_bot/analytics/volume_profile_peaks.py` — поиск HVN/LVN
- `trading_bot/data/volume_profile_peaks_db.py` — запись в БД
- `trading_bot/entrypoints/export_volume_peaks_to_sheets_only.py` — экспорт

---

### 6. Structural Cycle (генерация уровней)

**Функция**: `_run_structural()`

**Интервал**: `SUPERVISOR_STRUCTURAL_SEC` (по умолчанию 1800 сек = 30 мин)

**Что делает**:

#### **Режим ПРОДАКШЕН** (`TEST_MODE=0`):
1. **Scan** — поиск кандидатов из `price_levels` (vp_local, manual_global_hvn)
2. **Filter** — отбор символов по ликвидности и корреляции
3. **Calculate W*** — медиана ширины каналов (L-U)/ATR
4. **Freeze** — фиксация уровней в `cycle_levels`
5. **Export** — выгрузка в Google Sheets

#### **Тестовый режим** (`TEST_MODE=1`):
1. **Generate** — создание уровней по формуле:
   - LONG: `current_price - 0.2 * ATR`
   - SHORT: `current_price + 0.2 * ATR`
2. **Save** — запись в те же таблицы

**Модули**:
- `trading_bot/analytics/structural_cycle.py` — реальный structural
- `trading_bot/analytics/test_level_generator.py` — тестовые уровни
- `trading_bot/data/structural_cycle_db.py` — работа с БД
- `trading_bot/data/structural_ops_notify.py` — экспорт и логирование

**Конфигурация**:
```env
# Реальный режим
TEST_MODE=0
STRUCTURAL_ALLOWED_LEVEL_TYPES=vp_local,manual_global_hvn
STRUCTURAL_MIN_POOL_SYMBOLS=15

# Тестовый режим
TEST_MODE=1
TEST_LEVEL_OFFSET_ATR=0.2
TEST_CYCLE_SYMBOLS_COUNT=10
```

---

### 7. Entry Detector (тик входа)

**Функция**: `_run_entry_tick()`

**Интервал**: `SUPERVISOR_ENTRY_TICK_SEC` (по умолчанию 10 сек)

**Что делает**:
1. **Monitor** — проверка пересечений уровней (level_cross_monitor)
2. **Gate** — проверка ATR-порога для входа (entry_gate)
3. **Flip** — закрытие противоположной стороны при смене сигнала
4. **Rebuild opposite** — перестройка уровней после входа
5. **Reconcile** — синхронизация позиций БД и биржи
6. ** sheets export** — обновление `cycle_open_positions`

**Модули**:
- `trading_bot/analytics/level_cross_monitor.py` — мониторинг пересечений
- `trading_bot/analytics/entry_gate.py` — логика входа (V3/V4)
- `trading_bot/tools/bybit_trading.py` — открытие/закрытие позиций
- `trading_bot/data/trading_cycle_sheets.py` — экспорт позиций

**Конфигурация**:
```env
# Частота
SUPERVISOR_ENTRY_TICK_SEC=10

# Entry Gate
ENTRY_GATE_LONG_ATR_THRESHOLD_PCT=2
ENTRY_GATE_SHORT_ATR_THRESHOLD_PCT=2

# Flip
ENTRY_CLOSE_OPPOSITE_ON_FLIP_SIGNAL=1

# Rebuild opposite
STRUCTURAL_OPPOSITE_REBUILD_ENABLED=1
STRUCTURAL_OPPOSITE_REBUILD_BAND_MULT=2.0

# Telegram
LEVEL_CROSS_TELEGRAM=1
ENTRY_DETECTOR_TELEGRAM_START=1
```

---

## 🔄 Режимы работы Supervisor

### 1. Однократный запуск (без `--loop`)

```powershell
python -m trading_bot.scripts.run_supervisor
```

**Поведение**:
- Выполняет один полный цикл: data → levels → structural → entry
- Завершается после выполнения

**Использование**:
- Ручной запуск по расписанию (scheduler)
- Тестирование отдельных прогонов
- Отладка

---

### 2. Постоянный цикл (`--loop`)

```powershell
python -m trading_bot.scripts.run_supervisor --loop
```

**Поведение**:
- Запускает бесконечный цикл с интервалами:
  - `DATA_REFRESH` каждые 900 сек
  - `LEVELS_REBUILD` каждые 1800 сек
  - `STRUCTURAL` каждые 1800 сек
  - `ENTRY_TICK` каждые 10 сек
- Работает до Ctrl+C

**Использование**:
- Продакшен запуск
- Тестирование в реальном времени

---

### 3. Ускоренный режим (FAST)

```powershell
# Для отладки с меньшими интервалами
python -m trading_bot.scripts.run_supervisor_fast --loop
```

**Интервалы**:
- `DATA_REFRESH=60` сек
- `LEVELS_REBUILD=300` сек
- `STRUCTURAL=300` сек
- `ENTRY_TICK=5` сек

---

## 🗂️ Модули, участвующие в контуре

### **Ядро**
| Файл | Описание |
|------|----------|
| `scripts/run_supervisor.py` | Главный оркестратор |
| `data/db.py` | Работа с SQLite |
| `data/schema.py` | Схема БД и миграции |
| `data/state_manager.py` | Определение режима старта |

### **Данные**
| Файл | Описание |
|------|----------|
| `data/data_loader.py` | Загрузка OHLCV данных |
| `data/collectors.py` | Сбор данных (indices, OI) |
| `tools/price_feed.py` | Текущие цены (WebSocket/REST) |
| `tools/bybit_trading.py` | API Bybit (позиции, ордера) |

### **Аналитика**
| Файл | Описание |
|------|----------|
| `analytics/volume_profile_peaks.py` | Поиск HVN/LVN |
| `analytics/structural_cycle.py` | Structural cycle (реальный) |
| `analytics/test_level_generator.py` | Тестовые уровни |
| `analytics/level_cross_monitor.py` | Мониторинг пересечений |
| `analytics/entry_gate.py` | Логика входа (V3/V4) |

### **Интеграция**
| Файл | Описание |
|------|----------|
| `entrypoints/export_volume_peaks_to_sheets_only.py` | Экспорт VP в Sheets |
| `data/structural_ops_notify.py` | Экспорт structural |
| `data/trading_cycle_sheets.py` | Экспорт позиций |
| `tools/telegram_notify.py` | Telegram уведомления |

### **Утилиты**
| Файл | Описание |
|------|----------|
| `scripts/full_reset.py` | Полный сброс |
| `scripts/analyze_test_run.py` | Анализатор отчётов |
| `scripts/test_state_manager.py` | Тесты State Manager |

---

## 📊 Состояния цикла

**Таблица**: `trading_state`

| Фаза | Описание |
|------|----------|
| `arming` | Уровни заморожены, ждём сигнал входа |
| `in_position` | Активные позиции открыты |
| `closed` | Цикл завершён, позиции закрыты |

**Поля**:
- `cycle_id` — ID текущего цикла
- `structural_cycle_id` — ID structural cycle
- `levels_frozen` — 1 = уровни заморожены
- `position_state` — 'none' | 'long' | 'short'
- `channel_mode` — 'two_sided' | 'single_sided'
- `known_side` — 'both' | 'long' | 'short'
- `need_rebuild_opposite` — 1 = нужна перестройка
- `last_start_mode` — режим старта (state manager)

---

## 🔧 Конфигурация

### Основные интервалы
```env
SUPERVISOR_LOOP_ENABLED=1
SUPERVISOR_POLL_SEC=2
SUPERVISOR_DATA_REFRESH_SEC=900
SUPERVISOR_LEVELS_REBUILD_SEC=1800
SUPERVISOR_STRUCTURAL_SEC=1800
SUPERVISOR_ENTRY_TICK_SEC=10
```

### Data Refresh (шаги)
```env
SUPERVISOR_DATA_REFRESH_SPOT_MAIN=1
SUPERVISOR_DATA_REFRESH_SPOT_CRYPTO_CONTEXT=0
SUPERVISOR_DATA_REFRESH_MACRO=0
SUPERVISOR_DATA_REFRESH_INDICES_TV=0
SUPERVISOR_DATA_REFRESH_OI_BYBIT=0
SUPERVISOR_DATA_REFRESH_INSTRUMENTS=1
SUPERVISOR_DATA_REFRESH_INSTRUMENTS_ATR=1
```

### Structural
```env
SUPERVISOR_STRUCTURAL_SKIP_WHEN_CYCLE_ACTIVE=1
SUPERVISOR_STRUCTURAL_RETRY_WHEN_BLOCKED_SEC=120
SUPERVISOR_EXPORT_VP_LOCAL_BEFORE_STRUCTURAL=1
```

### Entry Detector
```env
LEVEL_CROSS_POLL_SEC=10
LEVEL_CROSS_ALERT_TIMEOUT_MINUTES=5
LEVEL_CROSS_MIN_ALERTS_COUNT=2
ENTRY_GATE_LONG_ATR_THRESHOLD_PCT=2
ENTRY_GATE_SHORT_ATR_THRESHOLD_PCT=2
```

### Test Mode
```env
TEST_MODE=0
TEST_LEVEL_OFFSET_ATR=0.2
TEST_OPPOSITE_OFFSET_ATR=0.4
TEST_CYCLE_SYMBOLS_COUNT=10
```

---

## 📈 Мониторинг и отладка

### Логи
```powershell
# Последний лог
Get-Content trading_bot\logs\supervisor_*.log -Tail 100 -Wait

# Поиск по TEST_MODE
Get-Content trading_bot\logs\supervisor_*.log | Select-String "TEST_MODE"
```

### Анализатор
```powershell
# Авто-мониторинг (обновление каждые 5 мин)
python -m trading_bot.scripts.analyze_test_run --monitor 300

# Режим наблюдения (обновление каждые 60 сек)
python -m trading_bot.scripts.analyze_test_run --watch 60

# Однократный анализ
python -m trading_bot.scripts.analyze_test_run --output report.txt
```

### Проверка состояния
```sql
-- Текущее состояние
SELECT * FROM trading_state WHERE id=1;

-- Открытые позиции
SELECT * FROM position_records WHERE status IN ('open', 'pending');

-- Текущий цикл
SELECT * FROM structural_cycles ORDER BY created_at DESC LIMIT 1;
```

---

## ⚠️ Важные замечания

1. **State Manager** — определяет режим старта при каждом запуске
2. **Auto-reset** — залипшие циклы (>24ч без позиций) сбрасываются автоматически
3. **Rebuild opposite** — после входа перестраивается противоположная сторона
4. **Telegram** — уведомления о сигналах и событиях
5. **Sheets export** — автоматическая выгрузка уровней и позиций

---

## 📚 Ссылки

- `docs/STATE_MANAGER_DESIGN.md` — State Manager (детально)
- `docs/TEST_MODE_GUIDE.md` — Тестовый контур
- `scripts/README_STATE_MANAGER_WORKFLOW.md` — Workflow State Manager

---

**Версия**: 1.0  
**Дата**: 2026-04-16  
**Статус**: ✅ Актуально
