# Level Events State Machine (v1)

Цель: однозначно фиксировать, что произошло с уровнем после касания:

- событие ещё в работе (`open`);
- подтверждённый пробой (`confirmed_breakout_*`);
- подтверждённый отбой (`confirmed_rebound_*`);
- возврат через уровень без подтверждения исхода (`false_break`);
- долго без развязки (`stale_open`).

## Базовые параметры

- `confirm_atr_pct` (`LEVEL_EVENTS_CONFIRM_ATR_PCT`): порог **подтверждения исхода** в долях ATR.  
  Текущий рабочий дефолт: `0.30` (30% ATR от цены уровня).
- `confirm_delta = confirm_atr_pct * ATR`.

- `return_eps_atr` (`LEVEL_EVENTS_RETURN_EPS_ATR`): порог **возврата через уровень** в долях ATR (отдельная величина, не путать с `confirm_atr_pct`).  
  Если цена после касания пересекает уровень обратно на величину `eps = return_eps_atr * ATR`, фиксируется момент `return_time`.

Параметры настраиваются через `.env` / `settings.py`.

## Статусы

- `open` — касание зафиксировано, ни подтверждения (`confirm_time`), ни возврата через уровень (`return_time`) по правилам ниже ещё не было, либо сценарий не попал в `stale_open` / `false_break`.
- `confirmed_breakout_up` / `confirmed_breakout_down` — подтверждённый пробой (экстремум свечи пересёк `confirm_delta` с нужной стороны).
- `confirmed_rebound_up` / `confirmed_rebound_down` — подтверждённый отбой (зеркально пробою).
- `false_break` — зафиксирован **возврат через уровень** (`return_time` задан), при этом **нет** подтверждённого исхода (`confirm_time` отсутствует).  
  Перекрывает `stale_open`, если возврат уже произошёл: итог «вернулись через уровень, confirm не случился».
- `stale_open` — между касанием и концом окна прошло не меньше `LEVEL_EVENTS_STALE_OPEN_MINUTES`, подтверждения нет; при этом **не** было сценария `false_break` (иначе статус `false_break`).

## Создание события

Событие создаётся при касании уровня:

- `low <= level_price <= high`, либо
- мягкий допуск около уровня (`~0.01 * ATR` к ближайшему из low/high) — как в реализации `level_events.py`.

На старте: `touch_time`, `pre_side` (по `prev_close` относительно уровня), далее наращиваются `penetration`, `rebound_pure` и т.д.

## Переходы (подтверждение исхода)

Экстремумы свечи (`high` / `low`) — как в коде.

### Подход сверху (`pre_side=from_above`)

- `confirmed_breakout_down`: `low <= level_price - confirm_delta`.
- `confirmed_rebound_up`: `high >= level_price + confirm_delta`.

### Подход снизу (`pre_side=from_below`)

- `confirmed_breakout_up`: `high >= level_price + confirm_delta`.
- `confirmed_breakout_down`: `low <= level_price - confirm_delta`.

При срабатывании подтверждения выставляются `confirm_time` и соответствующий `event_status`; дальнейшая классификация `false_break` не применяется.

## Возврат через уровень (`return_time`)

Независимо от подтверждения, внутри того же прохода по свечам отслеживается пересечение обратно через уровень на `eps`:

- сверху: `high >= level_price + eps`;
- снизу: `low <= level_price - eps`.

Это задаёт `return_time` и используется для `false_break`, если `confirm_time` так и не появился.

## Отбор событий в БД (отбраковка «шумовых» касаний)

Событие **не сохраняется**, если глубина проникновения в ATR меньше порога:

- `penetration_atr < LEVEL_EVENTS_MIN_PENETRATION_ATR`.

Отдельного отсечения по `rebound_pure` для записи события **нет** (слабый отбой всё равно попадает в данные). Порог `LEVEL_EVENTS_MIN_REBOUND_PURE_ATR` в настройках остаётся для совместимости/документации, не как gate записи.

## Метрики на событие

- `event_status`;
- `touch_time`, `return_time`, `confirm_time` (nullable);
- `confirm_time_sec`, `touch_count_before_confirm`;
- `penetration_atr` / `penetration_pct`;
- `rebound_pure_atr` / `rebound_pure_pct` — максимальный «чистый» отбой до возврата/подтверждения в долях ATR;
- `rebound_after_return_atr` (если применимо);
- `cluster_size` и пр. — как в схеме `level_events`.

## Экспорт в Sheets: порог «сильного отбоя»

Для листов `level_strength_report` и `level_stop_profile` отбираются **все** события с итоговым статусом из множества  
`confirmed_*` и `false_break`, у которых **`rebound_pure_atr >= LEVEL_STRENGTH_REPORT_MIN_REBOUND_PURE_ATR`** (дефолт `0.30`, т.е. **не менее 30% ATR**).  
Это **не** то же самое, что `return_eps_atr` для фиксации возврата через уровень.

## Правила для торговли (ориентир)

- Торговый контур ориентируется на события со статусом `confirmed_*` и, при необходимости, на аналитику по `false_break`.
- `open` и `stale_open` — мониторинг без завершённого сценария в смысле confirm/return.
- Агрегаты (`broken_flag`, `trade_allowed`, `composite_score`) в экспорте считаются по **всем** касаниям уровня в окне, а строки strength/stop — по **каждому** отобранному событию (не одна строка на уровень).

## Пояснение по `stale_open`

Рынок может долго идти в сторону без подтверждения — это нормально для `open`.  
`stale_open` подсвечивает «долго без развязки» в мониторинге. Если при этом произошёл возврат через уровень без confirm, итоговый статус — **`false_break`**, а не `stale_open`.
