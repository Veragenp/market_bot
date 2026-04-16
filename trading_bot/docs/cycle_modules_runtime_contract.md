# Cycle Modules Runtime Contract

Документ фиксирует текущий runtime-контракт между `structural_cycle`, `entry_detector` и `entry_gate` после перехода на независимые LONG/SHORT наборы.

## 0) Важно: текущий активный режим

- В `run_supervisor.py` structural запускается как `run_structural_pipeline(auto_freeze=True)`.
- Рабочий контур: **scan -> armed -> freeze** (без `touch_window` / `entry_timer` / `collective_breakout`).

## 1) Контур модулей цикла

- `trading_bot/scripts/run_supervisor.py`
  - Оркестратор тиков: data refresh -> structural -> entry detector -> maintenance/export.
- `trading_bot/analytics/structural_cycle.py`
  - Скан уровней и формирование `StructuralSymbolResult` для freeze.
  - Выбор кандидатов с ATR-band приоритетом и fallback.
- `trading_bot/data/structural_cycle_db.py`
  - Freeze в `cycle_levels`, обновление `trading_state`, события `STRUCTURAL_RUN` и `phase_change`.
  - Формирует и пишет side-set агрегаты: `long_symbols`, `short_symbols`, `long_count`, `short_count`.
- `trading_bot/analytics/level_cross_monitor.py`
  - Отслеживает пересечения уровней по замороженному `cycle_id`, генерирует entry/cancel сигналы.
  - В summary возвращает `symbols`, `long_count`, `short_count`, `signals`, `skipped`.
- `trading_bot/analytics/entry_detector.py`
  - Единый тик входного контура: level_cross -> entry_gate -> reconcile -> maintenance.
  - В stage `ENTRY_SIGNAL` пишет `details.level_cross_summary` и `details.gate_results`.
- `trading_bot/analytics/entry_gate.py`
  - Обрабатывает сигналы `ENTER_*` / `CANCEL_*`, открывает/закрывает leg, делает flip/maintenance.
  - В результате `process_v3_signal` отдает `cycle_level_side_counts` (явно для дебага контракта).
- `trading_bot/data/cycle_levels_db.py`
  - Дозаполнение отсутствующей стороны (`backfill_missing_cycle_side`) без требования симметричной пары по тому же тикеру.

## 2) Актуальный data contract (independent LONG/SHORT)

- Символ может участвовать только в LONG, только в SHORT, или в обеих сторонах.
- Freeze больше не требует симметричной пары `long+short` на одном символе.
- Side-наборы считаются по факту `cycle_levels(level_step=1, is_active=1)`:
  - `long_symbols` / `short_symbols`
  - `long_count` / `short_count`
- Эти агрегаты должны быть согласованы во всех местах:
  - `STRUCTURAL_RUN` output/meta
  - `phase_change` meta на freeze
  - `ENTRY_SIGNAL.details.level_cross_summary`
  - `ENTRY_SIGNAL.details.gate_results[*].cycle_level_side_counts`

## 3) Что считается корректной логикой цикла

- Structural часть корректна, если:
  - `armed` достигается без ложного `closed` в фазе `arming`;
  - freeze сохраняет односторонние символы;
  - side counts в structural событиях соответствуют `cycle_levels`.
- Entry detector корректен, если:
  - берет уровни только текущего `cycle_id`;
  - не требует парности сторон для symbol в `level_cross_monitor`;
  - фиксирует side counts в `ENTRY_SIGNAL`.
- Entry gate корректен, если:
  - обрабатывает сигнал независимо от симметрии уровней по тикеру;
  - в каждом результате сигнала содержит текущие `cycle_level_side_counts`;
  - rebuild opposite side использует общий пул символов цикла + fallback к ref цене.

## 4) Минимальный runtime checklist

- После freeze:
  - `cycle_phase='arming'`, `levels_frozen=1`, `cycle_id` заполнен.
  - В `cycle_levels` есть строки хотя бы одной стороны.
- На тике entry detector:
  - В `ENTRY_SIGNAL.details.level_cross_summary` есть `long_count`/`short_count`.
  - В `gate_results` (если были сигналы) есть `cycle_level_side_counts`.
- При расхождении counts:
  - Проверить фильтры `level_step=1` и `is_active=1`.
  - Проверить, не сменился ли `cycle_id` между freeze и тиком.

## 5) Смежные документы

- `trading_bot/docs/structural_cycle_module.md` — пошаговая логика structural цикла.
- `trading_bot/docs/structural_level_selection_module.md` — детально про выбор уровней и отладку.
- `trading_bot/docs/cycle_structural_start_spec.md` — исходная спецификация и инварианты.
- `trading_bot/docs/level_events_state_machine_spec.md` — state machine уровня событий.
- `trading_bot/docs/level_events_runtime_spec.md` — runtime метрики level events (отдельный контур).
