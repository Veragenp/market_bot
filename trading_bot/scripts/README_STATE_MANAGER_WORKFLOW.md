# State Manager Workflow

## 📋 Что было сделано

### 1. Миграция БД v23

Добавлены новые поля для session tracking и position sync:

**position_records:**
- `exchange_position_id TEXT` - ID позиции на бирже Bybit
- `last_sync_ts INTEGER` - Время последней синхронизации
- `sync_status TEXT DEFAULT 'pending'` - Статус: 'pending', 'synced', 'failed'
- `last_sync_error TEXT` - Последняя ошибка синхронизации

**trading_state:**
- `last_session_id TEXT` - ID текущей сессии supervisor
- `last_start_ts INTEGER` - Время старта сессии
- `last_start_mode TEXT` - Режим старта ('fresh', 'recovery_add_missing', 'recovery_continue', 'manual_reset')
- `opposite_rebuild_in_progress INTEGER DEFAULT 0` - Флаг пересбора противоположной стороны

**runtime_state (новая таблица):**
```sql
CREATE TABLE runtime_state (
    key TEXT PRIMARY KEY,
    value TEXT,
    updated_at INTEGER
)
```

### 2. State Manager модуль

**Файл:** `trading_bot/data/state_manager.py`

**Функции:**
- `get_trading_state()` - Получить текущее состояние
- `update_trading_state(**kwargs)` - Обновить поля
- `determine_start_mode()` - Определить режим старта
- `handle_fresh_start()` - Обработать FRESH_START
- `handle_recovery_add_missing(details)` - Добавить недостающие позиции
- `handle_clean_stale_positions(details)` - Очистить зависшие позиции
- `handle_recovery_continue(details)` - Продолжить работу
- `handle_recovery_sync_mismatch(details)` - Обработать рассинхрон

### 3. Интеграция в Supervisor

**Файл:** `trading_bot/scripts/run_supervisor.py`

Изменения в `run_supervisor_once()`:
```python
# НОВЫЙ ШАГ: определить режим старта
mode, session_id, details = determine_start_mode()
logger.info("Supervisor: start mode=%s session=%s", mode, session_id[:8])

# Выполняем обработчик режима
if mode == "FRESH_START":
    result = handle_fresh_start()
elif mode == "RECOVERY_ADD_MISSING":
    result = handle_recovery_add_missing(details)
# ... и т.д.
```

### 4. Обновление full_reset.py

**Файл:** `trading_bot/scripts/full_reset.py`

Новые возможности:
- Флаг `--close-exchange` - закрыть позиции на бирже (placeholder)
- Установлен `last_start_mode='manual_reset'`
- Добавлено поле `opposite_rebuild_in_progress = 0`

### 5. Тесты

**Файл:** `trading_bot/scripts/test_state_manager.py`

Тесты:
- ✅ Migration v23
- ✅ get_trading_state
- ✅ determine_start_mode
- ✅ handle_fresh_start

## 🚀 Режимы старта

### FRESH_START
**Когда:** Нет цикла или нет позиций нигде

**Действия:**
- Сброс trading_state в 'arming'
- Закрытие старых позиций
- Закрытие старых ордеров
- Установка last_start_mode='fresh'

### RECOVERY_ADD_MISSING
**Когда:** Позиции на бирже, нет в БД

**Действия:**
- Добавление позиций в БД
- Установка phase='in_position'
- last_start_mode='recovery_add_missing'

### CLEAN_STALE_POSITIONS
**Когда:** Позиции в БД, нет на бирже

**Действия:**
- Закрытие позиций в БД (status='closed')
- Сброс цикла
- Переход к FRESH_START

### RECOVERY_CONTINUE
**Когда:** Позиции совпадают

**Действия:**
- Синхронизация last_sync_ts
- last_start_mode='recovery_continue'

### RECOVERY_SYNC_MISMATCH
**Когда:** Позиции не совпадают

**Действия:**
- Логирование ошибки
- Требует ручного сброса

## 📊 Примеры использования

### Запуск supervisor

```powershell
cd market_bot
$env:PYTHONPATH="."
python -m trading_bot.scripts.run_supervisor --loop
```

**Лог:**
```
Supervisor: determining start mode...
Supervisor: start mode=FRESH_START session=a65bd3f7
Supervisor: fresh_start applied session=a65bd3f7
```

### Ручной сброс

```powershell
# Обычный сброс
python -m trading_bot.scripts.full_reset --force

# С закрытием позиций на бирже (пока не реализовано)
python -m trading_bot.scripts.full_reset --close-exchange --force
```

### Тестирование

```powershell
python -m trading_bot.scripts.test_state_manager
```

**Вывод:**
```
✅ PASS: Migration v23
✅ PASS: get_trading_state
✅ PASS: determine_start_mode
✅ PASS: handle_fresh_start
✅ ВСЕ ТЕСТЫ ПРОЙДЕНЫ
```

## 🔍 Диагностика

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
```

### Проверка полей

```sql
PRAGMA table_info(position_records);
PRAGMA table_info(trading_state);
SELECT name FROM sqlite_master WHERE type='table' AND name='runtime_state';
```

## 📝 Следующие шаги

### Приоритет 1 (критично)
- ✅ Миграция v23 - DONE
- ✅ State Manager - DONE
- ✅ Интеграция в supervisor - DONE
- ✅ Тесты - DONE

### Приоритет 2 (важно)
- [ ] position_sync.py - модульная синхронизация
- [ ] Rebuild opposite handler
- [ ] Защита от двойного запуска (file lock)

### Приоритет 3 (опционально)
- [ ] order_executor.py - вынос логики
- [ ] Health check API
- [ ] Интеграционные тесты

## ⚠️ Важные замечания

1. **Session ID** генерируется при каждом запуске supervisor
2. **last_start_mode** сохраняется для аудита
3. **RECOVERY_SYNC_MISMATCH** требует ручного сброса через `full_reset.py --force`
4. **Migration v23** применена автоматически через `run_migrations()`

## 📞 Если что-то не работает

1. **Проверьте миграцию:**
   ```powershell
   python -m trading_bot.scripts.run_migrations
   ```

2. **Проверьте тесты:**
   ```powershell
   python -m trading_bot.scripts.test_state_manager
   ```

3. **Проверьте логи supervisor:**
   ```powershell
   Get-Content trading_bot\logs\supervisor_*.log -Tail 100
   ```

---

**Дата:** 2026-04-16  
**Версия:** 1.0  
**Статус:** ✅ Готово к продакшену
