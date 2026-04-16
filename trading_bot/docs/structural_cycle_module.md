# Модуль structural cycle: пошаговое описание

Документ описывает реализацию в коде (`trading_bot/analytics/structural_cycle.py`, `trading_bot/data/structural_cycle_db.py`, скрипты). Базовая спека: `cycle_structural_start_spec.md`.

---

## Текущий runtime-контракт (фиксируем)

- Перед стартом нового торгового цикла supervisor выполняет `levels_rebuild` (обновляет `vp_local_levels`).
- Затем выполняется structural (`run_structural_pipeline(auto_freeze=True)`): выбираются уровни из актуального `price_levels` и фиксируются в `cycle_levels`.
- Для торговли внутри цикла источником истины является `cycle_levels` (freeze-снимок), а не "текущий" `vp_local_levels` после последующих rebuild.
- Монета может попасть только в одну сторону или в обе стороны; симметрия long/short по тикеру не обязательна.
- При поиске противоположной стороны (flip/rebuild) используется актуальный пул уровней и текущая цена как fallback-якорь.

---

## Зачем нужен модуль

- Построить по каждой монете торговые уровни из `price_levels`: **L** (опора снизу, зона long) и/или **U** (сопротивление сверху, зона short).
- Согласовать пул монет по **ширине коридора в ATR**: \(W_i = (U_i - L_i) / \mathrm{ATR}_i\).
- Зафиксировать снимок в **`cycle_levels`** и обновить **`trading_state`** (единый writer freeze).

Исполнение сделок (**tutorial_v3**) этим модулем не занимается — он только готовит контракт `cycle_levels` + фазы в БД.

---

## Входные данные

| Источник | Назначение |
|----------|------------|
| `price_levels` | Кандидаты уровней; типы задаёт **`STRUCTURAL_ALLOWED_LEVEL_TYPES`** (по умолчанию `vp_local`, `manual_global_hvn`). |
| `ohlcv` (1m) | **Ref-цена** для скана при **`STRUCTURAL_REF_PRICE_SOURCE=db_1m_close`** (согласованность с spot-уровнями). |
| `instruments.atr` (Bybit futures) | ATR в знаменателе \(W_i\), полосы mid, дистанции abort. |
| Настройки `settings.py` / `.env` | Пороги пула, N_touch, окна, freeze и т.д. |

---

## Часть A — батч-скан: `run_structural_pipeline`

Вызывается из скриптов и тестов. Один проход, без ожидания WebSocket (если не включён realtime поверх).

### Шаг A1. Подготовка

1. `init_db()` / `run_migrations()`.
2. Список символов: `TRADING_SYMBOLS` или переданный `symbols=`.
3. Сбор **ref** по символам:
   - при **`ref_prices_override`** — из него;
   - иначе при **`STRUCTURAL_REF_PRICE_SOURCE=db_1m_close`** — последний **close** 1m из `ohlcv`;
   - иначе — `get_price_feed()` (Bybit WS/REST), с fallback на 1m из БД.
4. Символы с ref ≤ 0 отбрасываются.

### Шаг A2. Запись цикла в БД

5. Создаётся строка в **`structural_cycles`**: фаза **`scanning`**, `params_json` (снимок параметров), событие в **`structural_events`** (`phase_change` → scanning).

### Шаг A3. Расчёт пула (`compute_structural_symbol_results`)

6. Для каждого символа из **разрешённых `level_type`** выбираются кандидаты **ниже** и **выше** ref (сортировка: `volume_peak`, `strength`, `updated_at`), не более **`STRUCTURAL_TOP_K`** с каждой стороны.

7. Если с какой-то стороны кандидатов **меньше** `STRUCTURAL_MIN_CANDIDATES_PER_SIDE` (по умолчанию **1**) или нет ATR — символ получает статус **`incomplete_structure`**; при этом в freeze допускаются односторонние символы (только long или только short).

8. Иначе выполняется v4-подобный выбор сильных уровней с ATR-band приоритетом (с fallback на более широкий side-фильтр), затем итерации подгонки под **MAD** пула (выбросы по \(|W_i - m| > k \cdot \mathrm{MAD}\)\).

9. Финально по пулу считаются **медиана \(W\)** и **MAD** по строкам со статусом **`ok`**; выбросы помечаются **`outlier`**.

10. Результаты пишутся в **`structural_cycle_symbols`** (L, U, id уровней, mid, mid-полоса % от \((U-L)\), ref, tier, volume_peak и т.д.).

### Шаг A4. Решение по пулу

11. Если число **`ok`** \< **`STRUCTURAL_MIN_POOL_SYMBOLS`** → фаза **`cancelled`**, `cancel_reason` (например `insufficient_pool_after_mad`), freeze **не** делается.

12. Иначе фаза **`armed`** (в смысле «пул прошёл», готовность к realtime/freeze по настройкам).

### Шаг A5. Freeze (опционально)

13. Если **`auto_freeze=True`**:
    - `DELETE FROM cycle_levels`, вставка по каждому **`ok`** независимо по сторонам: если есть **L** — строка `long`, если есть **U** — строка `short`;
    - **`trading_state`**: `cycle_id` = `structural_cycle_id`, `levels_frozen=1`, `cycle_phase='arming'`, инкремент `cycle_version`;
    - события в **`structural_events`**.

---

## Таблицы SQLite

| Таблица | Роль |
|---------|------|
| `structural_cycles` | Фаза, время, `params_json`, агрегаты пула, таймеры, `cancel_reason`. |
| `structural_cycle_symbols` | Снимок по символу: статус, L/U, id уровней, ATR, W. |
| `structural_events` | Аудит: `phase_change`, `mid_touch`, `breakout_*`. |
| `cycle_levels` | Результат freeze (контракт для исполнителя). |
| `trading_state` | `structural_cycle_id`, `cycle_id`, `levels_frozen`, `cycle_phase`. |

---

## Скрипты и проверки

| Команда | Назначение |
|---------|------------|
| `python -m trading_bot.scripts.run_structural_cycle` | Scan + immediate freeze (рабочий режим). |
| `python -m trading_bot.scripts.run_structural_cycle --no-freeze` | Только scan, без freeze. |
| `python -m trading_bot.scripts.export_structural_scan_to_sheets` | Скан + лист **`structural_levels_report`**; по умолчанию **с freeze**. |
| `python -m trading_bot.scripts.verify_structural` | Сухой скан + проверка инвариантов (L \< ref \< U, W_atr, mid) на текущей БД. |
| `pytest tests/test_structural_cycle.py` | Автотесты scan/freeze-контракта structural. |

---

## Связь с legacy `rebuild_cycle_levels`

- **`CYCLE_LEVELS_REBUILD_ENABLED`** по умолчанию выкл.: перетирать **`cycle_levels`** таймером без согласования не следует.
- Актуальный снимок уровней для цикла задаёт **structural** при freeze.

---

## Где лежит файл

Репозиторий (от корня `market_bot`):

**`trading_bot/docs/structural_cycle_module.md`**

Абсолютный путь на диске (типичная установка):

`d:\Program\tradebot\IITradebot\market_bot_v1\market_bot\trading_bot\docs\structural_cycle_module.md`

В Cursor: откройте файл из дерева проекта по пути выше или через поиск по имени `structural_cycle_module.md`.
