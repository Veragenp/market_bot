# State Manager - Управление состоянием бота

## 📋 Обзор

Модуль `state_manager.py` отвечает за:
- Детерминированное определение режима старта при запуске supervisor
- Управление trading_state
- Синхронизацию позиций между БД и биржей
- Session tracking

## 🚀 Режимы старта

### FRESH_START
**Когда:** Нет цикла или нет позиций нигде (ни в БД, ни на бирже)

**Действия:**
- Сброс trading_state в 'arming'
- Закрытие старых позиций (статус → 'cancelled')
- Закрытие старых ордеров
- Начало нового structural цикла

### RECOVERY_ADD_MISSING
**Когда:** Позиции есть на бирже, но нет в БД

**Причина:** Сбой между открытием позиции и записью в БД

**Действия:**
- Добавление недостающих позиций в БД
- Установка фазы 'in_position'
- Синхронизация exchange_position_id

### CLEAN_STALE_POSITIONS
**Когда:** Позиции есть в БД, но нет на бирже

**Причина:** Позиции закрыты externally или "зависли"

**Действия:**
- Закрытие "зависших" позиций в БД (статус → 'closed')
- Сброс цикла
- Автоматический переход к FRESH_START

### RECOVERY_CONTINUE
**Когда:** Позиции совпадают на бирже и в БД

**Действия:**
- Синхронизация last_sync_ts
- Продолжение работы без изменений

### RECOVERY_SYNC_MISMATCH
**Когда:** Позиции есть, но не совпадают (разные size/side)

**Причина:** Критический рассинхрон

**Действия:**
- Логирование ошибки
- Требует ручного сброса через `full_reset.py --force`

## 📊 Алгоритм определения режима

```python
mode, session_id, details = determine_start_mode()
```

**Шаги:**
1. Проверка cycle_id в trading_state
2. Получение списка символов из structural_cycle_symbols
3. Запрос позиций с биржи (Bybit API)
4. Запрос позиций из БД (position_records)
5. Сравнение и принятие решения

## 🔧 API

### Основные функции

```python
from trading_bot.data.state_manager import (
    get_trading_state,
    update_trading_state,
    determine_start_mode,
    handle_fresh_start,
    handle_recovery_add_missing,
    handle_clean_stale_positions,
    handle_recovery_continue,
    handle_recovery_sync_mismatch,
)
```

### get_trading_state()
Возвращает текущее состояние trading_state как dict.

### update_trading_state(**kwargs)
Обновляет поля trading_state.

Пример:
```python
update_trading_state(
    cycle_phase='in_position',
    levels_frozen=1,
    last_start_mode='recovery_continue'
)
```

### determine_start_mode()
Определяет режим старта.

Возвращает: `(mode: str, session_id: str, details: dict | None)`

### Обработчики режимов

Каждый обработчик возвращает dict с результатом:
```python
{
    "ok": True,
    "mode": "FRESH_START",
    "session_id": "abc123...",
    # Дополнительные поля...
}
```

## 🗄️ Миграция v23

Добавлены поля:

### position_records
- `exchange_position_id TEXT` - ID позиции на бирже
- `last_sync_ts INTEGER` - Время последней синхронизации
- `sync_status TEXT` - Статус синхронизации ('pending', 'synced', 'failed')
- `last_sync_error TEXT` - Ошибка синхронизации

### trading_state
- `last_session_id TEXT` - ID текущей сессии
- `last_start_ts INTEGER` - Время старта сессии
- `last_start_mode TEXT` - Режим старта ('fresh', 'recovery_add_missing', 'recovery_continue', 'manual_reset')
- `opposite_rebuild_in_progress INTEGER` - Флаг пересбора противоположной стороны

### runtime_state (новая таблица)
Временные флаги и состояния:
```sql
CREATE TABLE runtime_state (
    key TEXT PRIMARY KEY,
    value TEXT,
    updated_at INTEGER
)
```

## 📝 Примеры использования

### В supervisor

```python
from trading_bot.data.state_manager import determine_start_mode, handle_fresh_start

# Определить режим
mode, session_id, details = determine_start_mode()
logger.info("Start mode: %s session: %s", mode, session_id[:8])

# Обработать
if mode == "FRESH_START":
    result = handle_fresh_start()
elif mode == "RECOVERY_CONTINUE":
    result = handle_recovery_continue(details)
# ... и т.д.
```

### Ручной сброс

```bash
# Обычный сброс
python -m trading_bot.scripts.full_reset --force

# С закрытием позиций на бирже
python -m trading_bot.scripts.full_reset --close-exchange --force
```

## 🔍 Диагностика

### Проверка текущего режима

```python
from trading_bot.data.state_manager import get_trading_state

state = get_trading_state()
print(f"Mode: {state.get('last_start_mode')}")
print(f"Session: {state.get('last_session_id', '')[:8]}")
print(f"Phase: {state.get('cycle_phase')}")
```

### Проверка синхронизации

```python
from trading_bot.data.state_manager import (
    get_exchange_positions,
    get_db_positions,
)

symbols = ['BTC/USDT', 'ETH/USDT']
exchange_pos = get_exchange_positions(symbols)
db_pos = get_db_positions('cycle-uuid', ['open', 'pending'])

print(f"Exchange: {exchange_pos}")
print(f"DB: {len(db_pos)} positions")
```

## ⚠️ Важные замечания

1. **Session ID** генерируется при каждом старте supervisor
2. **last_start_mode** сохраняется в БД для аудита
3. **RECOVERY_SYNC_MISMATCH** требует ручного вмешательства
4. **Безопасность:** Все операции транзакционные (BEGIN/COMMIT)

## 📞 Если что-то не работает

1. Проверьте миграцию v23:
   ```python
   from trading_bot.data.db import get_connection
   conn = get_connection()
   version = conn.execute("SELECT MAX(version) FROM db_version").fetchone()[0]
   print(f"DB version: {version}")  # Должно быть >= 23
   conn.close()
   ```

2. Проверьте поля в position_records:
   ```sql
   PRAGMA table_info(position_records);
   ```

3. Проверьте логи supervisor на ошибки:
   ```powershell
   Get-Content trading_bot\logs\supervisor_*.log -Tail 100
   ```

---

**Версия:** 1.0  
**Миграция:** v23  
**Статус:** Активно используется в supervisor
