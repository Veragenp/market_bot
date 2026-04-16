# State Manager — Управление состоянием

## Обзор

**Файл**: `trading_bot/data/state_manager.py`

**Назначение**: Детерминированное определение и обработка режима старта supervisor при каждом запуске.

---

## Режимы старта

### 1. FRESH_START
**Когда**: Нет активного цикла ИЛИ нет позиций ни в БД, ни на бирже

**Действия**:
- Сброс trading_state
- Очистка старых позиций (status → 'cancelled')
- Генерация session_id
- Запуск нового structural цикла

---

### 2. RECOVERY_ADD_MISSING
**Когда**: Позиции есть на бирже, но нет в БД

**Действия**:
- Получение позиций с биржи
- Добавление в position_records
- Синхронизация cycle_id

---

### 3. CLEAN_STALE_POSITIONS
**Когда**: Позиции есть в БД, но нет на бирже

**Действия**:
- Закрытие позиций в БД (status → 'closed')
- Сброс цикла (аналог FRESH_START)

---

### 4. RECOVERY_CONTINUE
**Когда**: Позиции совпадают на бирже и в БД

**Действия**:
- Обновление last_sync_ts
- Продолжение работы без изменений

---

### 5. RECOVERY_SYNC_MISMATCH
**Когда**: Позиции есть, но не совпадают (разный size/side)

**Действия**:
- Логирование ошибки
- Требование ручного сброса (`full_reset.py --force`)

---

## API

### determine_start_mode()
```python
mode, session_id, details = determine_start_mode()
# mode: 'FRESH_START' | 'RECOVERY_ADD_MISSING' | ...
# session_id: uuid4
# details: dict с дополнительной информацией
```

### handle_fresh_start()
```python
result = handle_fresh_start()
# Возвращает: {'ok': True, 'mode': 'FRESH_START', ...}
```

### handle_recovery_add_missing(details)
```python
result = handle_recovery_add_missing(details)
# Добавляет недостающие позиции из биржи в БД
```

### handle_clean_stale_positions(details)
```python
result = handle_clean_stale_positions(details)
# Закрывает зависшие позиции в БД
```

### handle_recovery_continue(details)
```python
result = handle_recovery_continue(details)
# Синхронизирует временные метки
```

---

## Схема БД

### position_records (новые поля)
- `exchange_position_id` — ID на бирже
- `last_sync_ts` — время последней синхронизации
- `sync_status` — 'pending' | 'synced' | 'failed'
- `last_sync_error` — текст ошибки

### trading_state (новые поля)
- `last_session_id` — UUID текущей сессии
- `last_start_ts` — время старта
- `last_start_mode` — режим старта
- `opposite_rebuild_in_progress` — флаг rebuild

---

## Тесты

```powershell
python -m trading_bot.scripts.test_state_manager
```

---

**См. также**: `STATE_MANAGER_DESIGN.md`, `SUPERVISOR_ARCHITECTURE.md`
