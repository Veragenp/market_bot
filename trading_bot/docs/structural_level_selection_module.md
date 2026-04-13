# Модуль выбора уровней Structural (полное описание)

Документ описывает **как в коде** устроен выбор канала (L/U), пул монет, mid-band, freeze в `cycle_levels` и realtime-ветку (touch / entry_timer / abort). Нужен для отладки ситуаций вида «уровни странные», «цикл отменился», «supervisor пишет failed».

---

## 1. Где живёт логика (файлы)

| Компонент | Файл | Роль |
|-----------|------|------|
| Эталон ширины \(W^*=(U-L)/ATR\), подгонка пар, статусы `ok` / `incomplete_structure` | `trading_bot/analytics/structural_cycle.py` | Ядро выбора уровней из `price_levels` |
| Оркестрация: цикл в БД, freeze, realtime, события, ops_stage | `trading_bot/data/structural_cycle_db.py` | Запись `structural_cycles`, `structural_cycle_symbols`, `structural_events`, вызов freeze |
| Дозаполнение недостающей стороны в `cycle_levels` (тот же эталон, что и в scan) | `trading_bot/data/cycle_levels_db.py` → `backfill_missing_cycle_side` | Вызов из entry/maintenance при одностороннем freeze |
| Типы уровней (vp_local + manual_global_hvn и т.д.) | `trading_bot/config/settings.py` → `STRUCTURAL_ALLOWED_LEVEL_TYPES` (часто из env) | Фильтр по `price_levels.level_type` |
| Список символов для обхода | `trading_bot/config/symbols.py` → `TRADING_SYMBOLS` | Символы в формате `BTC/USDT` |
| ATR для нормировки ширины канала | `trading_bot/data/repositories.py` → `get_instruments_atr_bybit_futures_cur` | Чтение `instruments.atr` (Bybit futures, Gerchik из daily job) |
| Экспорт снимков / Telegram / Sheets (опционально) | `trading_bot/data/structural_ops_notify.py` | Наблюдаемость, не ядро выбора |
| Supervisor (порядок этапов) | `trading_bot/scripts/run_supervisor.py` | Вызывает `run_structural_realtime_cycle` |

---

## 2. Входные данные

### 2.1. Таблица `price_levels`

Для каждого символа structural берёт **активные** строки:

- `is_active = 1`
- `status = 'active'`
- `level_type IN (STRUCTURAL_ALLOWED_LEVEL_TYPES)` — типично `vp_local`, `manual_global_hvn`

Уровни должны быть **заранее** пересчитаны (VP rebuild, sync manual HVN). Structural **не** строит VP сам — только читает БД.

### 2.2. Опорная цена `ref_price` (на символ)

Источник задаётся `STRUCTURAL_REF_PRICE_SOURCE`:

- **`price_feed`** (по умолчанию): Bybit WS/REST через `get_price_feed().get_prices()`; при отсутствии символа — fallback на последний `ohlcv` 1m `close`.
- **`db_1m_close`**: только последний 1m close из `ohlcv`.

Символы с `ref_price <= 0` **отбрасываются** до расчёта.

### 2.3. ATR

Берётся из `instruments` для `exchange = 'bybit_futures'`. Символ ищется как `BTCUSDT` и как `BTC/USDT`.

Если ATR нет или ≤ 0 — символ **не попадает** в рабочий пул (см. ниже `incomplete_structure`).

---

## 3. Параметры `StructuralParams` (из `settings`)

Кратко, что влияет на выбор:

| Параметр | Смысл |
|----------|--------|
| `STRUCTURAL_MIN_CANDIDATES_PER_SIDE` | Минимум кандидатов снизу и сверху от `ref`; иначе символ не строит сетку пар |
| `STRUCTURAL_TOP_K` | Сколько кандидатов с каждой стороны участвует в переборе пар |
| `STRUCTURAL_N_ETALON` | Минимум **голосов** за ширину: символов, у которых есть хотя бы одна пара с \(W \in [W_{min}, W_{max}]\) в порядке силы пары; иначе весь scan → `insufficient_etalon` |
| `STRUCTURAL_W_MIN`, `STRUCTURAL_W_MAX` | Допустимый диапазон нормированной ширины \(W=(U-L)/ATR\) для голосования и подгонки |
| `STRUCTURAL_W_SLACK_PCT` | Люфт вокруг \(W^*\) в процентах: полоса подгонки \([W^* \pm slack] \cap [W_{min}, W_{max}]\) (`slack = W_SLACK_PCT/100`) |
| `STRUCTURAL_MIN_POOL_SYMBOLS` | После подгонки нужно минимум столько строк `status == ok`, иначе **cancelled** (`insufficient_pool_after_fit`) |
| `STRUCTURAL_MID_BAND_PCT` | Полоса mid: процент от \((U-L)\): `half = (mid_band_pct/100) * (U-L) / 2` |
| `STRUCTURAL_EDGE_ATR_FRAC` | (из env `STRUCTURAL_EDGE_ATR_PCT`) доля ATR для полос у границ L/U в спеках realtime |
| `STRUCTURAL_Z_W_OK_THRESHOLD` | Пишется в БД как `structural_cycles.pool_k` (порог z по W для отчётов/экспорта; не путать с `TOP_K`) |
| `STRUCTURAL_STRENGTH_FIRST_ENABLED` | Порядок перебора пар: сначала сила пары (`volume_peak`/`strength`), затем \(|W|\) |

Устаревшие в env, **не используются** ядром structural scan: `STRUCTURAL_MAD_K`, `STRUCTURAL_REFINE_MAX_ROUNDS`, center/target/anchor — см. только исторические конфиги.

Полный список defaults — в `trading_bot/config/settings.py` и в `STRUCTURAL_AND_TELEGRAM.env.sample`.

---

## 4. Алгоритм выбора пары (L, U) на символ

Реализация: `compute_structural_symbol_results()` в `structural_cycle.py`.

### 4.1. Кандидаты «ниже» и «выше» ref

- **Ниже ref:** `price < ref` (направление запроса `"long"`).
- **Выше ref:** `price > ref` (направление `"short"`).

Сортировка кандидатов: `volume_peak` DESC, `strength` DESC, `updated_at/created_at` DESC. Берётся до `STRUCTURAL_TOP_K` с каждой стороны.

Если с любой стороны кандидатов **меньше** `STRUCTURAL_MIN_CANDIDATES_PER_SIDE`, или нет ATR — символ в итоге получает `incomplete_structure` (сетка пар для него не строится).

### 4.2. Порядок пар и сила

Из декартова произведения top-K×top-K остаются только пары с \(L < ref < U\). Сортировка пар:

- при `STRUCTURAL_STRENGTH_FIRST_ENABLED`: ключ \((-\min(\text{peak}), -(\text{peak}_L+\text{peak}_U), |W|)\);
- иначе: по возрастанию \(|W|\).

### 4.3. Голосование за \(W^*\)

Для каждого символа, у которого построена сетка, берётся **первая** пара в этом порядке, у которой \(W \in [W_{min}, W_{max}]\). Её \(W\) — один голос.  
Если число голосов **строго меньше** `STRUCTURAL_N_ETALON`, scan проваливается целиком: **все** символы получают `incomplete_structure`, `cancel_reason = insufficient_etalon`.

### 4.4. Подгонка и статусы

\(W^* = \mathrm{median}(\text{голоса})\). Полоса подгонки: \([W^* - slack, W^* + slack] \cap [W_{min}, W_{max}]\) с `slack = STRUCTURAL_W_SLACK_PCT/100`.

Для каждого символа с валидной сеткой: **первая** пара в том же порядке силы, чей \(W\) попадает в полосу → `status = ok` с этой парой; иначе → `incomplete_structure`.

Полоса подгонки после \(W^* \pm slack\) **расширяется** до отрезка, содержащего все голоса \(\min(w_{votes}) \ldots \max(w_{votes})\) (в пределах \([W_{min}, W_{max}]\)), чтобы символ, давший голос для медианы, не отваливался только из‑за сдвига медианы при узком slack.

Статуса **`outlier`** в этой схеме **нет**: либо `ok`, либо `incomplete_structure`.

### 4.5. Агрегаты пула (совместимость полей)

В словаре статистики: `w_star` и `pool_median_w` = \(W^*\); `pool_mad` заполняется значением **slack** (имя колонки историческое). `pool_median_r` / `pool_mad_r` = 0.

---

## 5. Запись в БД и freeze (`structural_cycle_db.py`)

### 5.1. `run_structural_pipeline` (скан без realtime)

1. Создаётся строка `structural_cycles` с `phase = scanning`; в `pool_k` сразу пишется `STRUCTURAL_Z_W_OK_THRESHOLD` (см. §3).
2. Вызывается `compute_structural_symbol_results`.
3. Результаты пишутся в `structural_cycle_symbols` (`ok` или `incomplete_structure`).
4. Если не набралось голосов для эталона → `phase = cancelled`, `cancel_reason = insufficient_etalon`, freeze **нет**.
5. Если `len(ok) < STRUCTURAL_MIN_POOL_SYMBOLS` → `phase = cancelled`, `cancel_reason = insufficient_pool_after_fit`, freeze **нет**.
6. Иначе → `phase = armed`.
7. Если `STRUCTURAL_AUTO_FREEZE_ON_SCAN` (или аргумент) — `_freeze_cycle_levels`: очистка `cycle_levels`, по каждой `ok` — long на `L_price`, short на `U_price`; в `trading_state` — `levels_frozen`, `cycle_phase`, `cycle_id` / `structural_cycle_id`.

### 5.2. Поля при freeze

- `distance_atr` для long/short — расстояние от `ref` до уровня в единицах ATR.
- `ref_price`, `ref_price_source` — откуда взята опора при freeze.

### 5.3. Дозаполнение противоположной стороны в `cycle_levels`

`backfill_missing_cycle_side` (`cycle_levels_db.py`) вызывается, когда у символа уже заморожена одна сторона (long или short), а вторая отсутствует (entry gate / maintenance).

Логика **согласована со scan**: те же `STRUCTURAL_W_MIN` / `STRUCTURAL_W_MAX` / `STRUCTURAL_W_SLACK_PCT` / `STRUCTURAL_N_ETALON` / `STRUCTURAL_TOP_K` / `STRUCTURAL_MIN_POOL_SYMBOLS`.

- Для символов с **обеими** сторонами в `cycle_levels` считается их \(W\) относительно уже зафиксированной опорной стороны; если \(W \in [W_{min}, W_{max}]\), значение идёт в голоса.
- Для символов «только одна сторона» загружаются кандидаты противоположной стороны (как в rebuild), в порядке силы БД; **первый** кандидат с \(W\) в \([W_{min}, W_{max}]\) даёт один голос.
- \(W^* = \mathrm{median}(\text{голоса})\); подгонка — первая пара/кандидат с \(W \in [W^* \pm slack] \cap [W_{min}, W_{max}]\).
- Если голосов `< max(1, min(N_ETALON, len(symbols)))` → в ответе `reason = insufficient_etalon_rebuild`, вставок нет.
- Если после подгонки число символов с найденным кандидатом `< max(1, min(MIN_POOL_SYMBOLS, |todo|))` → `insufficient_pool_after_fit_rebuild`.

`entry_gate` не дублирует математику: только вызывает эту функцию.

---

## 6. Realtime-ветка: `run_structural_realtime_cycle`

Схема:

1. **`run_structural_pipeline(..., auto_freeze=False)`** — только скан и `structural_cycle_symbols`, **без** записи в `cycle_levels` на этом шаге.
2. Если фаза не `armed` (например cancelled из-за пула) — realtime **не стартует**, возвращается результат скана.
3. Иначе `phase` в БД → `touch_window`, пишется стадия `MID_TOUCH_MONITOR` **started**.
4. Загружаются только строки `structural_cycle_symbols` с `status = 'ok'` и валидными полосами mid.

Далее цикл `while True`:

### 6.1. Жёсткий лимит времени: `STRUCTURAL_MAX_RUNTIME_SEC`

Если `(now - started_at) > STRUCTURAL_MAX_RUNTIME_SEC` → выход из цикла с **`timed_out = true`**, затем:

- `structural_cycles.phase = cancelled`
- `cancel_reason = touch_window_timeout`
- `MID_TOUCH_MONITOR` → **failed**, сообщение `Touch window timeout`
- **Freeze в realtime не выполняется** (`frozen = false` в возвращаемом dict)

Это **не** то же самое, что `STRUCTURAL_TOUCH_WINDOW_SEC`.  
`TOUCH_WINDOW_SEC` — сколько секунд «живёт» засчитанное касание символа в множестве для N_TOUCH.  
`MAX_RUNTIME_SEC` — **максимальная длительность всего realtime-цикла ожидания**. Если он меньше, чем время, за которое вы ожидаете набрать N касаний (например при окне 12 ч), поведение будет выглядеть как «дичь»: в логах постоянные `mid_touch`, а итог — **timeout**.

### 6.2. Цены

- Без override: `get_price_feed().get_prices(syms)` + при старте `feed.start_ws(syms)`.
- Если по символу из фида цены нет — подставляется последний `ohlcv` 1m `close` (тот же fallback, что и при сборе ref в scan при `price_feed`).

### 6.3. Пока `touch_started_at is None` (фаза набора группы)

- Для каждого символа: если цена в `[mid_band_low, mid_band_high]` и прошёл `STRUCTURAL_TOUCH_DEBOUNCE_SEC` с прошлого emit — пишется событие `mid_touch` в `structural_events` и обновляется `touch_times[s] = now`.
- Множество для триггера: только символы, у которых `now - touch_times[s] <= STRUCTURAL_TOUCH_WINDOW_SEC`.
- Если `len(touch_times) >= STRUCTURAL_N_TOUCH` → переход в **`entry_timer`**:  
  `touch_started_at = now`, `entry_timer_until = now + STRUCTURAL_ENTRY_TIMER_SEC`, фиксируется группа в `trading_state` (dedup полей `last_group_touch_*`).

**Важно:** считаются **уникальные символы**, не число тиков. Два символа (ADA, ENA), сколько бы ни логировалось `mid_touch`, **никогда не дадут** `STRUCTURAL_N_TOUCH = 3`.

**Recovery:** если в `structural_events` за `STRUCTURAL_TOUCH_HISTORY_LOOKBACK_SEC` уже есть ≥ `STRUCTURAL_TOUCH_HISTORY_MIN_SYMBOLS` различных `mid_touch`, можно сразу перейти в `entry_timer` без нового набора (после рестарта и т.д.).

**Bootstrap:** после первого опроса цен символы, у которых цена уже в mid-band, получают «касание» на время старта цикла — чтобы не терять группу при рестарте, если рынок уже в полосе.

### 6.4. Коллективный пробой (на каждом тике)

Перед логикой mid-touch / entry_timer на **каждой** итерации: если число символов с ценой за пределами `L ± STRUCTURAL_ABORT_DIST_ATR * ATR` или `U ± ...` в **этом** тике ≥ `STRUCTURAL_N_ABORT` → `cancelled`, `collective_breakout` (и в фазе набора N касаний, и в `entry_timer`).

### 6.5. Завершение entry_timer

Когда `now >= entry_timer_until` (и не collective breakout) — выход из `while`, затем:

- `phase = armed`
- если `force_freeze` (у supervisor обычно True) — `_freeze_cycle_levels` с ref из снимка строк (`ref_price_ws` или mid)
- `MID_TOUCH_MONITOR` → **ok**, `STRUCTURAL_FREEZE` при успешном freeze

---

## 7. Почему supervisor пишет `STRUCTURAL_RUN` = **failed**

В `run_supervisor._run_structural` успех помечается только если `out.get("phase") in ("armed", "completed")`.

Фазы **`cancelled`** (timeout, collective breakout, insufficient pool и т.д.) дают **`failed`** на уровне supervisor — это **ожидаемая** маркировка оркестратора, не обязательно «краш Python».

---

## 8. Что именно могло «сломаться» в вашем недавнем прогоне (типичная диагностика)

По симптомам из логов/БД:

1. **Постоянные `mid_touch` по 2 символам (ADA, ENA), затем `Touch window timeout`**  
   - При **`STRUCTURAL_N_TOUCH = 3`** третья уникальная монета в окне так и не попала, либо не держалась в полосе в пределах `STRUCTURAL_TOUCH_WINDOW_SEC`.  
   - Итог: логика отработала **корректно**, но параметры N и окно не согласованы с рынком.

2. **`STRUCTURAL_MAX_RUNTIME_SEC` срезал ожидание раньше, чем вы набрали N при окне 12 ч**  
   - Визуально «крутилось 9 часов и сдохло» — это как раз глобальный таймаут realtime-цикла, а не «12 часов на касания».  
   - Нужно явно согласовать: `MAX_RUNTIME_SEC >= TOUCH_WINDOW_SEC` (и запас на entry_timer), либо осознанно держать короткий max runtime.

3. **Странные L/U у отдельных монет**  
   - Проверить: достаточно ли уровней в `price_levels` с обеих сторон от ref; не ушёл ли ref из-за стейла WS.  
   - Порядок пар задаётся `strength_first`: при одинаковой силе выигрывает более узкий \(W\); голос и подгонка берут **первую** подходящую пару в этом порядке, а не «глобально лучшую» по цене.

4. **Мало голосов для \(W^*\) или мало `ok` после подгонки**  
   - Ослабить `STRUCTURAL_W_MIN`/`STRUCTURAL_W_MAX`, увеличить `STRUCTURAL_W_SLACK_PCT` или уменьшить `STRUCTURAL_N_ETALON`; проверить, что у большинства символов есть пара с \(W\) в полосе.  
   - Для `insufficient_pool_after_fit` — снизить `STRUCTURAL_MIN_POOL_SYMBOLS` или улучшить наполнение `price_levels`.

5. **ATR из `instruments` нулевой/старый**  
   - Символ выпадет в `incomplete_structure`; ширина канала в W_atr будет некорректна для остальных.

---

## 9. Таблицы БД для разбора руками

- `structural_cycles` — фаза, `cancel_reason`, тайминги touch/entry_timer.
- `structural_cycle_symbols` — по каждому символу: `L_price`, `U_price`, `status`, `mid_*`, `ref_price_ws`.
- `structural_events` — `mid_touch`, `breakout_*`, `phase_change`.
- `cycle_levels` — что реально заморожено для entry-модуля.
- `trading_state` — `levels_frozen`, `cycle_phase`, `structural_cycle_id`.

---

## 10. Остановка автоматического цикла

Для отладки выбора уровней имеет смысл остановить supervisor:

```text
taskkill /PID <pid> /T /F
```

или не запускать `--loop`, а вызывать точечно:

```text
PYTHONPATH=. python -m trading_bot.scripts.run_structural_cycle --scan-only
```

(`--scan-only` = только `run_structural_pipeline` без realtime; см. `run_structural_cycle.py`.)

---

## 11. Краткий чеклист «почему плохой пул / плохой freeze»

1. В `price_levels` для символа есть активные уровни из `STRUCTURAL_ALLOWED_LEVEL_TYPES` **и с обеих сторон** от текущей цены?  
2. `STRUCTURAL_MIN_CANDIDATES_PER_SIDE` и `STRUCTURAL_TOP_K` не слишком жёсткие?  
3. Достаточно ли символов дают голос в \([W_{min}, W_{max}]\) для `STRUCTURAL_N_ETALON`?  
4. `STRUCTURAL_MIN_POOL_SYMBOLS` достижим после подгонки вокруг \(W^*\)?  
5. `STRUCTURAL_N_TOUCH` ≤ реального числа монет, которые могут одновременно быть в mid-band?  
6. `STRUCTURAL_MAX_RUNTIME_SEC` ≥ времени набора N + `STRUCTURAL_ENTRY_TIMER_SEC` + запас?  
7. `instruments.atr` актуален для всех `TRADING_SYMBOLS`?  
8. `STRUCTURAL_REF_PRICE_SOURCE`, fallback 1m и качество price feed (stale)?  
9. При одностороннем `cycle_levels`: срабатывает ли `backfill_missing_cycle_side` (см. §5.3) или падает на `insufficient_etalon_rebuild`?

---

## 12. Заметки по реализации (кратко)

- **Эталон:** один проход — голоса → медиана \(W^*\) → подгонка в полосе; нет итеративного refine и нет `outlier`.
- **`pool_k` в БД** = `STRUCTURAL_Z_W_OK_THRESHOLD`, не `TOP_K`; имя колонки историческое.
- **Realtime:** коллективный пробой проверяется на каждом тике; при старте — bootstrap mid-touch для символов уже в полосе; цена из фида с fallback на 1m close.
- **Два таймера:** `STRUCTURAL_TOUCH_WINDOW_SEC` (свежесть касаний) и `STRUCTURAL_MAX_RUNTIME_SEC` (жёсткий предел всего realtime) — по-прежнему независимы; типичный инцидент — короткий `MAX_RUNTIME` при длинном окне касаний.
- **`run_structural_realtime_cycle`** с `auto_freeze=False` на первом шаге не обновляет `cycle_levels` до успешного завершения realtime (или scan с авто-freeze).

---

*Документ для разбора инцидентов structural; при изменении кода сверяйте с `structural_cycle.py`, `structural_cycle_db.py`, `cycle_levels_db.py`.*
