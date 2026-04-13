# Ручные глобальные уровни HVN (Google Sheets → `price_levels`)

Отдельная книга Google Таблиц: вы ведёте уровни вручную, скрипт заливает их в SQLite (`price_levels`).

## Константы в базе

| Поле | Значение |
|------|----------|
| `level_type` | `manual_global_hvn` |
| `layer` | `manual_global_hvn_sheet` |
| `origin` | `manual` |

`strength` для этих строк всегда **0**, `volume_peak` и окна времени — **NULL** (ручной ввод без авторасчёта).

## Шкала `tier`

- **1** — самый сильный уровень, **2** слабее, **3** ещё слабее (допускаются **4…999** при необходимости).
- В модуле отбора уровней позже: **меньше число = выше приоритет** (не путать с человеческими уровнями, где `strength` — сумма весов фракталов).

## Структура книги

1. Лист **`instruction`** (имя задаётся в `MANUAL_GLOBAL_HVN_INSTRUCTION_SHEET`, по умолчанию `instruction`) — **только для пояснений**, скрипт его **не создаёт и не перезаписывает**. Создайте и оформите вручную.
2. По одному листу на **каждый символ из объединения**
   `TRADING_SYMBOLS` + `ANALYTIC_SYMBOLS["crypto_context"]` +
   `ANALYTIC_SYMBOLS["macro"]` + `ANALYTIC_SYMBOLS["indices"]`:
   - Имя листа = символ без слэша: `BTC/USDT` → **`BTC_USDT`**.
   - При первом запуске синка с `--ensure-tabs` (по умолчанию) отсутствующий лист **создаётся** с шапкой.

## Шапка данных (первая строка листа символа)

Столбцы (порядок может быть любым, имена — как ниже, без учёта регистра и с пробелами вместо `_`):

| Колонка | Обязательно | Описание |
|---------|-------------|----------|
| `stable_level_id` | да | Уникальный id уровня **глобально** по всей БД (например `BTC_USDT_001`). Не переиспользовать id для другой цены. |
| `price` | да | Цена уровня (число, запятая или точка). |
| `tier` | да | Целое **1…999** (ранг силы). |
| `is_active` | да | **1** / **0** (или yes/no, true/false). **0** — уровень снят с учёта (`status` → archived, `is_active` = 0). Строки из листа **не удаляем** — только меняем активность. |

Пустые строки и строки без `stable_level_id` пропускаются.

## Переменные окружения

В `.env` только строки вида `KEY=value` (без Python-кода). Путь к `credentials.json` задаётся так: `GOOGLE_CREDENTIALS_PATH=credentials.json` — файл ищется от **корня репозитория** (`REPO_ROOT`).

См. `trading_bot/config/settings.py` и корневой `.env.example`:

- `GOOGLE_CREDENTIALS_PATH` — JSON сервисного аккаунта (по умолчанию `credentials.json` в корне репо).
- Книгу **создаёте вы** в [Google Sheets](https://sheets.google.com): новая таблица → поделиться доступом с **email сервисного аккаунта** из `credentials.json` (роль «Редактор»). ID скопируйте из адреса:  
  `https://docs.google.com/spreadsheets/d/ВОТ_ЭТОТ_ФРАГМЕНТ/edit`
- Указать ID книги можно так:
  - **`GOOGLE_SHEETS_ID`** — если одна книга именно под ручные глобальные уровни; или
  - **`MANUAL_GLOBAL_HVN_SPREADSHEET_ID`** — то же назначение, приоритетнее `GOOGLE_SHEETS_ID`, если заданы оба.
- Либо вместо ID: `MANUAL_GLOBAL_HVN_SPREADSHEET_URL` или `MANUAL_GLOBAL_HVN_SPREADSHEET_TITLE`.
- `MANUAL_GLOBAL_HVN_INSTRUCTION_SHEET` — имя листа-инструкции (через запятую — несколько имён для проверки конфликтов).
- Для Telegram (общий бот): `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID` (или `TELEGRAM_TOKEN` — см. `telegram_notify.py`).

## Запуск

Из корня репозитория:

```bash
python trading_bot/entrypoints/sync_manual_global_hvn_from_sheets.py
python trading_bot/entrypoints/sync_manual_global_hvn_from_sheets.py --dry-run
python trading_bot/entrypoints/sync_manual_global_hvn_from_sheets.py --symbol BTC/USDT --json
```

Флаг `--no-ensure-tabs` — не создавать вкладки (ошибка, если листа нет).

## Зависимости

Нужны `gspread` и `requests` (см. `trading_bot/requirements.txt`).

## Roadmap (не забыть)

Подробный чеклист (планировщик, Telegram, модуль отбора): **`manual_global_hvn_roadmap.md`**.

Кратко: периодический синк в `scheduler.py`, опциональные отчёты и напоминания в Telegram через `telegram_notify`.

См. комментарий в `trading_bot/data/scheduler.py` у блока расписания.
