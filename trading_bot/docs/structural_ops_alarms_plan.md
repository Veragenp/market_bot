# П.1: операционные алармы structural-контура

Цель: **наблюдаемость и предсказуемость** (`structural_cycle` + `cycle_levels` + `trading_state`), **без** торговых сигналов входа.

Чёткие алгоритмы скана и realtime зафиксированы в **`structural_cycle_module.md`** (части A и B) и в коде `structural_cycle_db.py` / `structural_cycle.py`.

## Что уже есть в БД (`structural_events`)

- Смены фаз: `scanning` → `armed` / `cancelled` / `touch_window` / `entry_timer`, действие `freeze` с `cycle_levels_rows`.
- Причины `cancelled`: `insufficient_pool_after_mad`, `no_ok_rows_for_touch_window`, `collective_breakout`, `touch_window_timeout` (в `meta` + `structural_cycles.cancel_reason`).
- Realtime: `mid_touch`, `breakout_lower`, `breakout_upper` по символам.

## Пробелы относительно целевого п.1

1. **Нет внешних алармов** — события пишутся в SQLite, но **нет** дублирования в лог (структурированно) / **Telegram** (`trading_bot.tools.telegram_notify`). Нужен единый слой «ops notify» по выбранным `event_type` / фазам.
2. **Ранний выход без цикла** — при `no_valid_ref_prices` pipeline возвращает ошибку **без** строки `structural_cycles` и **без** `structural_events` → снаружи выглядит как «тишина». Нужно как минимум событие/аларм (опционально с `cycle_id = NULL` или отдельная таблица `ops_alerts`).
3. **Частичная деградация ref** — символы с ref ≤ 0 **тихо отбрасываются** из списка перед сканом; нет агрегата «сколько символов без цены / источник».
4. **Сводка по пулу до `ok`** — счётчики `incomplete_structure`, `outlier`, нет ATR и т.д. есть в `structural_cycle_symbols`, но **нет** одного digest-события после скана (по желанию — не блокер).
5. **VP-refine** — в текущем коде пары подгоняются под MAD до `refine_max_roundов`; отдельного статуса «unresolved после refine» в событиях нет (при неуспехе символ уходит в `outlier` / не в пуле `ok`). Если нужен явный флаг в digest — добавить в `meta` скана.

## Правило канала

- Операционные уведомления: **лог + Telegram** и **строка в `structural_events`** (расширить `event_type` при необходимости, напр. `ops_alert`) **или** отдельная таблица **`ops_alerts`**, чтобы не смешивать с будущими рыночными сигналами входа.

## Настройки (черновик)

- Флаги/env: включение Telegram для structural, уровень шума (только phase_change + cancel + freeze vs все mid_touch), `TELEGRAM_*` уже в `settings.py` для общего бота.
