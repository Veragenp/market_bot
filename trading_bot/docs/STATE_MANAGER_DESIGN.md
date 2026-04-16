# State Manager — Детерминированное управление состоянием торгового бота

## 📋 Архитектура

### Цель
Устранить неопределённость при запуске supervisor и обеспечить **детерминированное**, **предсказуемое** поведение в любых ситуациях: первый запуск, сбой, ручной перезапуск, рассинхрон с биржей.

### Проблема до State Manager
- При запуске supervisor не было понятно, в каком состоянии находится система
- Ручная очистка данных требовалась каждый раз перед новым циклом
- Зависшие позиции в БД не обрабатывались
- Рассинхрон между БД и биржей требовал ручного вмешательства

### Решение
**State Manager** определяет текущее состояние системы и автоматически применяет подходящий сценарий восстановления или продолжения работы.

---

## 🔄 Режимы старта

### 1. FRESH_START
**Когда:** Нет активного цикла ИЛИ нет позиций ни в БД, ни на бирже

**Ситуации:**
- Первый запуск бота
- После полного ручного сброса (`full_reset.py --force`)
- Цикл завершён, позиции закрыты

**Действия:**
```python
# 1. Сброс trading_state
cycle_phase → 'arming'
levels_frozen → 0
cycle_id → NULL
structural_cycle_id → NULL
position_state → 'none'
last_start_mode → 'fresh'

# 2. Очистка старых данных
position_records: status → 'cancelled' (для старых позиций)
exec_orders: status → 'cancelled' (для старых ордеров)

# 3. Генерация session_id
session_id = uuid4()
last_session_id → session_id
last_start_ts → now()

# 4. Запуск нового structural цикла
run_structural_pipeline(auto_freeze=True)
```

**Лог:**
```
Supervisor: determining start mode...
Supervisor: start mode=FRESH_START session=a65bd3f7
Supervisor: fresh_start applied session=a65bd3f7
Structural: старт цикла, фаза scanning
Structural: пул готов, фаза armed
[STRUCTURAL_FREEZE] ok cycle=bb10765f Cycle levels frozen after scan
```

---

### 2. RECOVERY_ADD_MISSING
**Когда:** Позиции есть на бирже, но нет в БД

**Ситуации:**
- Сбой между отправкой ордера на биржу и записью в БД
- Перезапуск supervisor после падения во время открытия позиции

**Действия:**
```python
# 1. Получение позиций с биржи
exchange_positions = get_exchange_positions(symbols)

# 2. Добавление в БД
INSERT INTO position_records:
  - uuid = uuid4()
  - cycle_id = текущий cycle_id
  - structural_cycle_id = текущий structural_id
  - symbol, side, qty из exchange_positions
  - status → 'open'
  - exchange_position_id → ID позиции на бирже
  - last_sync_ts → now()
  - sync_status → 'synced'

# 3. Обновление состояния
cycle_phase → 'in_position'
levels_frozen → 1
position_state → 'in_position'
last_start_mode → 'recovery_add_missing'
```

**Лог:**
```
Supervisor: determining start mode...
Supervisor: start mode=RECOVERY_ADD_MISSING session=b4c7e2f1
Supervisor: recovery_add_missing applied positions=1
Entry detector: мониторинг позиций...
```

---

### 3. CLEAN_STALE_POSITIONS
**Когда:** Позиции есть в БД, но нет на бирже

**Ситуации:**
- Позиции были закрыты externally (через интерфейс биржи)
- Позиции "зависли" в БД после сбоя

**Действия:**
```python
# 1. Закрытие позиций в БД
UPDATE position_records:
  status → 'closed'
  close_reason → 'stale_position'
  closed_at → now()

# 2. Сброс цикла (аналог FRESH_START)
cycle_phase → 'arming'
levels_frozen → 0
cycle_id → NULL
structural_cycle_id → NULL
last_start_mode → 'recovery_clean_stale'

# 3. Автоматический переход к FRESH_START
```

**Лог:**
```
Supervisor: determining start mode...
Supervisor: start mode=CLEAN_STALE_POSITIONS session=c8d9f3a2
Supervisor: clean_stale_positions applied positions_closed=1
Supervisor: start mode=FRESH_START session=d1e2f4b5
```

---

### 4. RECOVERY_CONTINUE
**Когда:** Позиции совпадают на бирже и в БД

**Ситуации:**
- Нормальный перезапуск supervisor
- Позиции активно торгуются, всё синхронизировано

**Действия:**
```python
# 1. Синхронизация временных меток
UPDATE position_records:
  last_sync_ts → now()
  sync_status → 'synced'

# 2. Обновление session
last_session_id → session_id
last_start_ts → now()
last_start_mode → 'recovery_continue'

# 3. Продолжение работы без изменений
# structural пропускается (уровни уже заморожены)
# entry_detector продолжает мониторинг
```

**Лог:**
```
Supervisor: determining start mode...
Supervisor: start mode=RECOVERY_CONTINUE session=e3f5a6b7
Supervisor: recovery_continue applied
Supervisor: structural skipped (active_trading_cycle phase=in_position levels_frozen=1)
Entry detector: мониторинг...
```

---

### 5. RECOVERY_SYNC_MISMATCH
**Когда:** Позиции есть, но не совпадают (разный size/side)

**Ситуации:**
- Критический рассинхрон между БД и биржей
- Ручное изменение позиции на бирже без обновления БД

**Действия:**
```python
# 1. Логирование ошибки
logger.error("RECOVERY_SYNC_MISMATCH: exchange=%s db=%s", exchange_pos, db_pos)

# 2. Требование ручного вмешательства
return {
  "ok": False,
  "error": "positions_sync_mismatch_requires_manual_reset",
  "hint": "Run full_reset.py --force to resolve"
}
```

**Лог:**
```
Supervisor: determining start mode...
Supervisor: start mode=RECOVERY_SYNC_MISMATCH session=f6g7h8i9
❌ CRITICAL: Positions sync mismatch!
  Exchange: BTCUSDT size=0.001 Buy
  DB: BTCUSDT size=0.002 Sell
Supervisor: manual reset required - run full_reset.py --force
```

---

## 🗄️ Схема БД (Migration v23)

### position_records

```sql
ALTER TABLE position_records ADD COLUMN exchange_position_id TEXT;
-- ID позиции на бирже Bybit (для точной идентификации)

ALTER TABLE position_records ADD COLUMN last_sync_ts INTEGER;
-- Время последней успешной синхронизации с биржей

ALTER TABLE position_records ADD COLUMN sync_status TEXT DEFAULT 'pending';
-- Статус: 'pending' | 'synced' | 'failed'

ALTER TABLE position_records ADD COLUMN last_sync_error TEXT;
-- Текст последней ошибки синхронизации
```

### trading_state

```sql
ALTER TABLE trading_state ADD COLUMN last_session_id TEXT;
-- ID текущей сессии supervisor (uuid4)

ALTER TABLE trading_state ADD COLUMN last_start_ts INTEGER;
-- Время старта сессии (timestamp)

ALTER TABLE trading_state ADD COLUMN last_start_mode TEXT;
-- Режим старта: 'fresh' | 'recovery_add_missing' | 'recovery_continue' | 'manual_reset'

ALTER TABLE trading_state ADD COLUMN opposite_rebuild_in_progress INTEGER DEFAULT 0;
-- Флаг: пересбор противоположной стороны в процессе
```

### runtime_state (новая таблица)

```sql
CREATE TABLE IF NOT EXISTS runtime_state (
    key TEXT PRIMARY KEY,
    value TEXT,
    updated_at INTEGER
);
-- Временные флаги и состояния (например, supervisor_lock)
```

---

## 🔧 API State Manager

### Основные функции

```python
from trading_bot.data.state_manager import (
    get_trading_state,          # Получить текущее состояние
    update_trading_state,       # Обновить поля
    determine_start_mode,       # Определить режим старта
    handle_fresh_start,         # Обработать FRESH_START
    handle_recovery_add_missing,  # Добавить недостающие позиции
    handle_clean_stale_positions, # Очистить зависшие позиции
    handle_recovery_continue,   # Продолжить работу
    handle_recovery_sync_mismatch, # Обработать рассинхрон
)
```

### determine_start_mode()

**Вход:** Нет (читает БД)

**Выход:** `(mode: str, session_id: str, details: dict | None)`

**Алгоритм:**
```python
1. Получить trading_state
2. Если cycle_id = NULL → FRESH_START
3. Получить symbols из structural_cycle_symbols
4. Получить exchange_positions (Bybit API)
5. Получить db_positions (position_records)
6. Сравнить:
   - Нет нигде → FRESH_START
   - Только exchange → RECOVERY_ADD_MISSING
   - Только db → CLEAN_STALE_POSITIONS
   - Оба есть и совпадают → RECOVERY_CONTINUE
   - Оба есть, не совпадают → RECOVERY_SYNC_MISMATCH
```

---

## 📊 Интеграция в Supervisor

### Изменения в `run_supervisor_once()`

```python
def run_supervisor_once() -> Dict[str, object]:
    init_db()
    run_migrations()
    
    # НОВЫЙ ШАГ: определить режим старта
    logger.info("Supervisor: determining start mode...")
    mode, session_id, details = determine_start_mode()
    logger.info("Supervisor: start mode=%s session=%s", mode, session_id[:8])
    
    # Выполняем обработчик режима
    if mode == "FRESH_START":
        result = handle_fresh_start()
        logger.info("Supervisor: fresh_start applied session=%s", session_id[:8])
    elif mode == "RECOVERY_ADD_MISSING":
        result = handle_recovery_add_missing(details)
        logger.info("Supervisor: recovery_add_missing applied positions=%s", 
                    len(details.get("exchange_positions", {})) if details else 0)
    elif mode == "CLEAN_STALE_POSITIONS":
        result = handle_clean_stale_positions(details)
        logger.info("Supervisor: clean_stale_positions applied positions_closed=%s",
                    len(details.get("db_positions", [])) if details else 0)
    elif mode == "RECOVERY_CONTINUE":
        result = handle_recovery_continue(details)
        logger.info("Supervisor: recovery_continue applied")
    elif mode == "RECOVERY_SYNC_MISMATCH":
        result = handle_recovery_sync_mismatch(details)
        logger.error("Supervisor: RECOVERY_SYNC_MISMATCH - manual reset required!")
    
    # Остальной код (data refresh → levels rebuild → structural → entry)
    ...
```

---

## 🔄 Жизненный цикл

```
┌─────────────────────────────────────────────────────────────┐
│                    Запуск supervisor                        │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
            ┌──────────────────────┐
            │ determine_start_mode │
            └──────────┬───────────┘
                       │
         ┌─────────────┼─────────────┬──────────────┐
         │             │             │              │
         ▼             ▼             ▼              ▼
   ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────┐
   │ FRESH_   │  │ RECOVERY │  │ CLEAN_   │  │ RECOVERY_    │
   │ START    │  │ _ADD_    │  │ STALE_   │  │ SYNC_        │
   │          │  │ MISSING  │  │ POSITIONS│  │ MISMATCH     │
   └────┬─────┘  └────┬─────┘  └────┬─────┘  └──────┬───────┘
        │             │             │               │
        │             │             │               ▼
        │             │             │      ┌─────────────────┐
        │             │             │      │ ❌ MANUAL RESET │
        │             │             │      └─────────────────┘
        ▼             ▼             ▼
   ┌──────────────────────────────────────┐
   │     Обработка режима (handle_*)      │
   └──────────────┬───────────────────────┘
                  │
                  ▼
   ┌──────────────────────────────────────┐
   │    Data Refresh → Levels Rebuild     │
   │    → Structural → Entry Detector     │
   └──────────────┬───────────────────────┘
                  │
                  ▼
   ┌──────────────────────────────────────┐
   │      Нормальная работа бота         │
   │   (Entry detector каждые 10 сек)    │
   └──────────────────────────────────────┘
```

---

## 📝 Примеры использования

### 1. Первый запуск

```bash
cd market_bot
$env:PYTHONPATH="."
python -m trading_bot.scripts.run_supervisor --loop
```

**Лог:**
```
Supervisor: determining start mode...
Supervisor: start mode=FRESH_START session=a65bd3f7
Supervisor: fresh_start applied session=a65bd3f7
Supervisor: data refresh started
Supervisor: levels rebuild started
Supervisor: structural cycle started
Structural [bb10765f]: старт цикла, фаза scanning
Structural [bb10765f]: пул готов, фаза armed
[STRUCTURAL_FREEZE] ok cycle=bb10765f Cycle levels frozen after scan
```

### 2. Перезапуск после останова

```bash
# Supervisor был остановлен, позиции активны
python -m trading_bot.scripts.run_supervisor --loop
```

**Лог:**
```
Supervisor: determining start mode...
Supervisor: start mode=RECOVERY_CONTINUE session=b4c7e2f1
Supervisor: recovery_continue applied
Supervisor: structural skipped (active_trading_cycle phase=in_position)
Entry detector: мониторинг...
```

### 3. Ручной сброс

```bash
python -m trading_bot.scripts.full_reset --force
```

**Лог:**
```
================================================================================
ТЕКУЩЕЕ СОТОЯНИЕ
================================================================================
cycle_id: bb10765f
cycle_phase: armed
Открытые позиции: 0

================================================================================
СБРОС trading_state
================================================================================
✅ trading_state сброшен

✅ ПОЛНЫЙ СБРОЗ ЗАВЕРШЁН
Теперь можно запускать supervisor с чистого состояния
```

---

## 🧪 Тестирование

### Запуск тестов

```bash
python -m trading_bot.scripts.test_state_manager
```

**Вывод:**
```
================================================================================
ТЕСТИРОВАНИЕ STATE MANAGER
================================================================================

================================================================================
ТЕСТ: Migration v23
================================================================================
✅ Миграция v23 применена
✅ Поля position_records: exchange_position_id, last_sync_ts, sync_status, last_sync_error
✅ Поля trading_state: last_session_id, last_start_ts, last_start_mode, opposite_rebuild_in_progress
✅ Таблица runtime_state создана

================================================================================
ТЕСТ: get_trading_state
================================================================================
✅ trading_state: {показывает текущее состояние}

================================================================================
ТЕСТ: determine_start_mode
================================================================================
✅ Результат: Mode=FRESH_START Session=06f6571a

================================================================================
ТЕСТ: handle_fresh_start
================================================================================
✅ Результат: Mode=FRESH_START Session=14d3c1db
✅ trading_state обновлён: phase=arming, mode=fresh

================================================================================
РЕЗУЛЬТАТЫ
================================================================================
✅ PASS: Migration v23
✅ PASS: get_trading_state
✅ PASS: determine_start_mode
✅ PASS: handle_fresh_start

✅ ВСЕ ТЕСТЫ ПРОЙДЕНЫ
```

---

## ⚠️ Важные замечания

1. **Session ID** генерируется при каждом старте supervisor и сохраняется в БД
2. **last_start_mode** используется для аудита и отладки
3. **RECOVERY_SYNC_MISMATCH** требует ручного сброса — это защита от критических ошибок
4. **Все операции транзакционные** — нет риска частичных обновлений
5. **Миграция v23** применяется автоматически через `run_migrations()`

---

## 📞 Диагностика

### Проверка версии БД
```python
from trading_bot.data.db import get_connection
conn = get_connection()
version = conn.execute("SELECT MAX(version) FROM db_version").fetchone()[0]
print(f"DB version: {version}")  # Должно быть >= 23
conn.close()
```

### Проверка текущего режима
```python
from trading_bot.data.state_manager import get_trading_state
state = get_trading_state()
print(f"Mode: {state.get('last_start_mode')}")
print(f"Session: {state.get('last_session_id', '')[:8]}")
print(f"Phase: {state.get('cycle_phase')}")
```

### Проверка полей
```sql
PRAGMA table_info(position_records);
PRAGMA table_info(trading_state);
SELECT name FROM sqlite_master WHERE type='table' AND name='runtime_state';
```

---

## 📚 Референсы

- **Файлы:**
  - `trading_bot/data/state_manager.py` — основной модуль
  - `trading_bot/data/schema.py` — миграция v23
  - `trading_bot/scripts/run_supervisor.py` — интеграция
  - `trading_bot/scripts/full_reset.py` — ручной сброс
  - `trading_bot/scripts/test_state_manager.py` — тесты

- **Документация:**
  - `docs/state_manager.md` — API reference
  - `scripts/README_STATE_MANAGER_WORKFLOW.md` — workflow guide

---

**Версия:** 1.0  
**Дата:** 2026-04-16  
**Статус:** ✅ Готово к продакшену
