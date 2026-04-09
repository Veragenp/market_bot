# Модуль structural cycle: пошаговое описание

Документ описывает реализацию в коде (`trading_bot/analytics/structural_cycle.py`, `trading_bot/data/structural_cycle_db.py`, скрипты). Базовая спека: `cycle_structural_start_spec.md`.

---

## Зачем нужен модуль

- Построить по каждой монете **пару уровней**: **L** (опора снизу, зона long) и **U** (сопротивление сверху, зона short) из `price_levels`.
- Согласовать пул монет по **ширине коридора в ATR**: \(W_i = (U_i - L_i) / \mathrm{ATR}_i\), отфильтровать выбросы (**MAD**).
- После правил **касаний / таймера / abort** (realtime-ветка) зафиксировать снимок в **`cycle_levels`** и обновить **`trading_state`** (единый writer freeze).

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

7. Если с какой-то стороны кандидатов **меньше** `STRUCTURAL_MIN_CANDIDATES_PER_SIDE` (по умолчанию **1**) или нет ATR — символ получает статус **`incomplete_structure`**, в пул пар не входит.

8. Иначе стартует пара **режим A**: индексы (0,0) в сетке top-K×top-K; при необходимости **до `STRUCTURAL_REFINE_MAX_ROUNDS`** итераций подгоняется пара под **MAD** пула (выбросы по \(|W_i - m| > k \cdot \mathrm{MAD}\)\).

9. Финально по пулу считаются **медиана \(W\)** и **MAD** по строкам со статусом **`ok`**; выбросы помечаются **`outlier`**.

10. Результаты пишутся в **`structural_cycle_symbols`** (L, U, id уровней, mid, mid-полоса % от \((U-L)\), ref, tier, volume_peak и т.д.).

### Шаг A4. Решение по пулу

11. Если число **`ok`** \< **`STRUCTURAL_MIN_POOL_SYMBOLS`** → фаза **`cancelled`**, `cancel_reason` (например `insufficient_pool_after_mad`), freeze **не** делается.

12. Иначе фаза **`armed`** (в смысле «пул прошёл», готовность к realtime/freeze по настройкам).

### Шаг A5. Freeze (опционально)

13. Если **`auto_freeze=True`** (и не передан `--no-freeze` в экспорте):
    - `DELETE FROM cycle_levels`, вставка по каждому **`ok`**: **long** на **L**, **short** на **U**;
    - **`trading_state`**: `cycle_id` = `structural_cycle_id`, `levels_frozen=1`, `cycle_phase='arming'`, инкремент `cycle_version`;
    - события в **`structural_events`**.

---

## Часть B — realtime: `run_structural_realtime_cycle`

Полный контур с ожиданием цен (блокирующий цикл в процессе).

### Шаг B1. Скан без freeze

14. Вызывается **`run_structural_pipeline(auto_freeze=False)`**. Если фаза не **`armed`** (отмена на скане) — realtime **не** стартует, возврат.

### Шаг B2. Touch window

15. Фаза **`touch_window`**. Для каждого **`ok`** символа из БД читаются L, U, mid-полоса, ATR.

16. В цикле (poll или синтетические **`price_ticks_override`** в тестах):
    - если **last** попал в **[mid_band_low, mid_band_high]** — учёт **касания** с **дебаунсом** `STRUCTURAL_TOUCH_DEBOUNCE_SEC`, событие **`mid_touch`**;
    - если цена **≤ L − D_abort·ATR** или **≥ U + D_abort·ATR** — символ в множество abort, события **`breakout_lower` / `breakout_upper`**.

17. Пока не набрано **`STRUCTURAL_N_TOUCH`** уникальных (по времени окна **`STRUCTURAL_TOUCH_WINDOW_SEC`**) касаний — остаёмся в touch_window. Иначе переход к шагу B3.

### Шаг B3. Entry timer

18. Фаза **`entry_timer`**, выставляются `touch_started_at`, `entry_timer_until` (+ `STRUCTURAL_ENTRY_TIMER_SEC`).

19. В этом окне: если **число символов с abort** ≥ **`STRUCTURAL_N_ABORT`** → **`cancelled`**, `collective_breakout`, выход.

20. По истечении таймера — выход из цикла ожидания.

### Шаг B4. Freeze

21. Фаза снова **`armed`**, **`_freeze_cycle_levels`** из снимка **`structural_cycle_symbols`** (как в батче), ref в строках — из снимка скана.

Таймаут всего ожидания: **`STRUCTURAL_MAX_RUNTIME_SEC`** → **`cancelled`**, `touch_window_timeout`.

---

## Таблицы SQLite

| Таблица | Роль |
|---------|------|
| `structural_cycles` | Фаза, время, `params_json`, агрегаты пула, таймеры, `cancel_reason`. |
| `structural_cycle_symbols` | Снимок по символу: статус, L/U, id уровней, ATR, W, mid-полоса. |
| `structural_events` | Аудит: `phase_change`, `mid_touch`, `breakout_*`. |
| `cycle_levels` | Результат freeze (контракт для исполнителя). |
| `trading_state` | `structural_cycle_id`, `cycle_id`, `levels_frozen`, `cycle_phase`. |

---

## Скрипты и проверки

| Команда | Назначение |
|---------|------------|
| `python -m trading_bot.scripts.run_structural_cycle` | Полный realtime (долго, нужна сеть). |
| `python -m trading_bot.scripts.run_structural_cycle --scan-only` | Только часть A (+ опциональный freeze по `STRUCTURAL_AUTO_FREEZE_ON_SCAN`). |
| `python -m trading_bot.scripts.export_structural_scan_to_sheets` | Скан + лист **`structural_levels_report`**; по умолчанию **с freeze**. |
| `python -m trading_bot.scripts.verify_structural` | Сухой скан + проверка инвариантов (L \< ref \< U, W_atr, mid) на текущей БД. |
| `pytest tests/test_structural_cycle.py` | Автотесты (в т.ч. realtime на синтетических тиках). |

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
