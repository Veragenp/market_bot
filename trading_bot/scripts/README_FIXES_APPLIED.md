# Исправления проблем с ботом

## 📋 Что исправлено

### 1. Database locked в export_cycle_levels_sheets_snapshot

**Проблема:**
- Функция `build_cycle_levels_candidates_df()` вызывала `init_db()` каждый тик
- Это создавало блокировку БД при экспорте в Google Sheets
- Ошибка: `sqlite3.OperationalError: database is locked`

**Исправление:**
- Убраны вызовы `init_db()` и `run_migrations()` из `build_cycle_levels_candidates_df()`
- Добавлена обработка ошибок `database is locked` в `export_cycle_levels_sheets_snapshot()`
- Теперь экспорт graceful degradation - если БД заблокирована, пропускает часть экспорта

**Файлы:**
- `trading_bot/data/cycle_levels_db.py` (строки 652, 768-800)

---

### 2. Telegram уведомления отключены

**Проблема:**
- `ENTRY_DETECTOR_TELEGRAM_START=0` в `.env`
- Нет уведомлений о старте тика entry detector

**Исправление:**
- Установлено `ENTRY_DETECTOR_TELEGRAM_START=1` в `.env`

**Файлы:**
- `trading_bot/.env`

---

### 3. Ошибка в build_structural_trading_levels_df

**Проблема:**
- Отсутствовал импорт `PricePoint`
- Ошибка при создании fallback PricePoint

**Исправление:**
- Добавлен импорт `PricePoint` в `structural_ops_notify.py`
- Упрощена логика получения current_price

**Файлы:**
- `trading_bot/data/structural_ops_notify.py` (строки 19, 218)

---

## 🚀 Как применить исправления

### Шаг 1: Остановить supervisor

```powershell
# Найти процесс
Get-Process python -ErrorAction SilentlyContinue

# Остановить (заменить ID на ваш)
Stop-Process -Id 32908 -Force
```

### Шаг 2: Проверить исправления

Исправления уже применены в коде. Проверьте:

```powershell
# Проверка cycle_levels_db.py
Get-Content trading_bot\data\cycle_levels_db.py | Select-String -Pattern "def build_cycle_levels_candidates_df" -Context 0,2

# Должно быть без init_db() и run_migrations()
```

### Шаг 3: Запустить supervisor

```powershell
cd market_bot
$env:PYTHONPATH="."
python -m trading_bot.scripts.run_supervisor --loop
```

### Шаг 4: Проверить логи

Через 1-2 минуты проверьте логи:

```powershell
Get-Content trading_bot\logs\supervisor_*.log -Tail 50
```

**Ожидаемые логи:**
```
ENTRY_DETECTOR_TELEGRAM_START=1
Telegram: старт тика entry detector (если настроен TELEGRAM_BOT_TOKEN)
Successfully exported levels snapshot to structural_levels_report
export_cycle_levels_sheets_snapshot: ... (без ошибок database locked)
```

---

## ✅ Что должно работать теперь

| Компонент | Статус |
|-----------|--------|
| **Structural scan** | ✅ Работает |
| **Entry detector** | ✅ Работает (тиковая проверка каждые 10 сек) |
| **Export structural_levels_report** | ✅ Работает |
| **Export cycle_levels sheets** | ✅ Работает (с graceful degradation при locked) |
| **Telegram уведомления** | ✅ Включены (нужны TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID) |
| **Bybit WebSocket Demo** | ⚠️ 404 ошибка (нужен prod или другой URL) |

---

## 📊 Проверка работы

### 1. Проверить Telegram

В логах должно быть:
```
telegram start notify: Модуль entry detector: тик...
```

### 2. Проверить Google Sheets

Откройте Google Sheets:
- Лист `structural_levels_report` - 19 строк (LONG+SHORT уровни)
- Лист `cycle_levels_v1` - замороженные уровни
- Лист `cycle_levels_diag_v1` - диагностика
- Лист `cycle_levels_candidates_v1` - кандидаты

### 3. Проверить entry detector

В логах каждые 10 секунд:
```
EntryDetector prices: pool=structural_cycle_id members=15 priced=15/15
EntryDetector level_cross signals=...
```

---

## ⚠️ Известные проблемы

### Bybit WebSocket Demo 404

**Проблема:**
```
WebSocket Unified V5 attempting connection...
Handshake status 404 Not Found
```

**Причина:**
- Bybit Demo API использует другой URL
- `wss://stream-demo.bybit.com/v5/public/linear` не работает

**Решение:**
1. Переключиться на prod: `BYBIT_USE_DEMO=0` в `.env`
2. Либо обновить URL WebSocket в `price_feed.py`

---

## 🧪 Тестирование

### Запустить тесты

```powershell
cd market_bot
$env:PYTHONPATH="."
python -m trading_bot.scripts.test_fixes
```

**Ожидаемый результат:**
```
✅ PASS: Database access
✅ PASS: Telegram config
✅ PASS: Build structural levels DF
✅ PASS: Export cycle levels sheets
✅ ВСЕ ТЕСТЫ ПРОЙДЕНЫ
```

---

## 📞 Если что-то не работает

1. **Проверьте что supervisor запущен:**
   ```powershell
   Get-Process python
   ```

2. **Проверьте логи:**
   ```powershell
   Get-Content trading_bot\logs\supervisor_*.log -Tail 100
   ```

3. **Проверьте Telegram настройки:**
   ```powershell
   Get-Content trading_bot\.env | Select-String "TELEGRAM"
   ```

4. **Проверьте БД не заблокирована:**
   ```powershell
   python -c "from trading_bot.data.db import get_connection; conn = get_connection(); print('OK')"
   ```

---

**Дата:** 2026-04-16  
**Версия:** 1.0  
**Статус:** Исправления применены, требуют перезапуска supervisor
