# Ручные глобальные HVN — роадмап и чеклист

Внутренний ориентир: что уже есть и что дописать (планировщик, Telegram, логика отбора).

## Уже сделано

- Синк Google Sheets → `price_levels` (`level_type=manual_global_hvn`, `origin=manual`, `layer=manual_global_hvn_sheet`).
- Скрипт: `trading_bot/entrypoints/sync_manual_global_hvn_from_sheets.py` (обёртка `scripts/sync_manual_global_hvn_from_sheets.py`).
- Автосоздание вкладок по `TRADING_SYMBOLS` (имя листа = slug символа, напр. `BTC_USDT`).
- Универсальная отправка Telegram: `trading_bot/tools/telegram_notify.py` (токен/чат из `.env`).
- Чтение Sheets: `trading_bot/tools/sheets_reader.py`.
- Инструкция по колонкам: `manual_global_levels_sheet.md`.

## Запланировать в `scheduler.py` (не реализовано)
BTC
1. **Периодический синк**  
   - Вызов того же пайплайна, что entrypoint (импорт `sync_manual_global_hvn_from_sheets` из `manual_global_hvn_sheet_sync`).  
   - Рекомендация: 1× в сутки вне пиковых минут (например после human levels / export), либо реже.  
   - Флаг отключения: например `SCHEDULER_DISABLE_MANUAL_GLOBAL_HVN_SYNC=1` (добавить при внедрении).

2. **Telegram после синка (опционально)**  
   - Краткий отчёт: `inserted/updated/errors` из `stats` — только если `errors` не пусто или по env «всегда краткий лог».

3. **Еженедельное напоминание**  
   - Раз в неделю: список торгуемых символов, у которых нет ни одной **активной** строки `manual_global_hvn` в БД, или пустой лист (сложнее без отдельного флага — можно начать с «нет активных уровней в БД»).  
   - Через `send_telegram_message`.

4. **Новый символ в `TRADING_SYMBOLS`**  
   - Одноразовое уведомление: появился символ, для которого нет вкладки / нет данных — хранить «уже уведомили» в маленькой таблице SQLite или файле состояния в `DATA_DIR` (добавить при реализации).

## Логика отбора уровней (отдельный модуль, позже)

- Единый диспетчер: `human` (strength + TF), `vp_local` (tier + при необходимости volume), `manual_global_hvn` (tier: меньше = важнее).  
- Не смешивать смысл `strength` между типами.

## Проверка после правок в планировщике

- Локально: `python trading_bot/entrypoints/sync_manual_global_hvn_from_sheets.py --dry-run --json`.  
- Убедиться, что `GOOGLE_CREDENTIALS_PATH` и ID книги читаются из `.env` при запуске из корня репо.

## Ссылки

- `trading_bot/data/scheduler.py` — TODO в docstring `run_scheduler_forever`.  
- `trading_bot/data/manual_global_hvn_sheet_sync.py` — `sync_manual_global_hvn_from_sheets()`.
