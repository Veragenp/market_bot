# Level Events Runtime Spec

> Сейчас это отдельный аналитический контур; в active structural -> entry pipeline не является обязательным этапом.

Цель: получать из `price_levels` практическую аналитику по касаниям уровней:

- глубина проникновения (`penetration_atr`);
- сила отскока (`rebound_pure_atr`, `rebound_after_return_atr`);
- агрегированная сила уровня (`composite_score`, `broken_flag`);
- профиль стопа для торговли.

## Изменённые модули

- `trading_bot/analytics/level_events.py`
- `trading_bot/config/settings.py`
- `trading_bot/entrypoints/export_to_sheets.py`

## Настройки (`.env` / `settings.py`)

- `LEVEL_EVENTS_MODE=runtime` (зарезервировано под режимы)
- `LEVEL_EVENTS_LOOKBACK_HOURS=24`
- `LEVEL_EVENTS_WINDOW_HOURS=4`
- `LEVEL_EVENTS_MIN_PENETRATION_ATR=0.05` — единственный gate при записи события (минимальная глубина в ATR)
- `LEVEL_EVENTS_MIN_REBOUND_PURE_ATR=0.03` — в коде записи событий **не используется** (оставлено для совместимости/описания)
- `LEVEL_EVENTS_RETURN_EPS_ATR` — допуск **возврата через уровень** (не путать с порогом подтверждения)
- `LEVEL_EVENTS_CONFIRM_ATR_PCT=0.30` — порог **подтверждения** пробоя/отбоя в долях ATR
- `LEVEL_EVENTS_REBOUND_HORIZON_BARS=240`
- `LEVEL_EVENTS_STALE_OPEN_MINUTES=180`
- `LEVEL_STRENGTH_REPORT_MIN_REBOUND_PURE_ATR=0.30` — минимум `rebound_pure` **в долях ATR** для попадания строки в `level_strength_report` и `level_stop_profile` (≥ 30% ATR)
- `LEVEL_EVENTS_WORKSHEET=level_events`
- `LEVEL_STRENGTH_WORKSHEET=level_strength_report`
- `LEVEL_STOP_PROFILE_WORKSHEET=level_stop_profile`

## Листы Google Sheets

### `level_events`

События касаний (компактный набор колонок для аудита):

- `symbol`, `stable_level_id`, `event_status`, `pre_side`, `level_price`
- `touch_time_utc`, `confirm_time_sec`, `touch_count_before_confirm`
- `dist_start_atr_pct`, `penetration_atr_pct`, `rebound_pure_atr_pct`, `rebound_after_return_atr_pct`
- `cluster_size`

Полная модель в SQLite включает также `event_id`, `return_time`, `atr_daily`, окно и др.

### `level_strength_report`

**Не агрегат «одна строка на уровень»**: выгружаются **все события**, у которых:

- `event_status` ∈ {`confirmed_rebound_up`, `confirmed_rebound_down`, `confirmed_breakout_up`, `confirmed_breakout_down`, `false_break`};
- `rebound_pure_atr >= LEVEL_STRENGTH_REPORT_MIN_REBOUND_PURE_ATR` (по умолчанию 0.30 ATR).

К каждой строке события добавляются **агрегаты по уровню** (одинаковые для всех событий того же `stable_level_id` в окне lookback):

- идентификация: `symbol`, `stable_level_id`, `event_id`, `pre_side`, `level_price`, `touch_time_utc`
- метрики события: `penetration_atr_pct`, `rebound_pure_atr_pct`, `rebound_after_return_atr_pct`, `confirm_time_sec`, `touch_count_before_confirm`, `cluster_size`
- контекст уровня: `touches_n`, `return_rate`, `break_rate`, `broken_flag`, `composite_score`, `strength_bucket`, `p80_penetration_atr_pct`, `median_rebound_after_atr_pct`
- справочно по уровню (p80 по returned + пол): `lvl_recommended_stop_pct_base`, `lvl_recommended_stop_pct_conservative`
- `exported_at_utc`

### `level_stop_profile`

Те же отобранные **события**, что и для strength: по **каждому** событию считается стоп от **его** `penetration_atr_pct` (`max(penetration_atr_pct + 10, floor confirm)` в шкале «процентов ATR», как в коде экспорта).

Колонки:

- `symbol`, `stable_level_id`, `event_id`, `event_status`, `level_price`
- `broken_flag`, `trade_allowed`, `deny_reason`
- `stop_formula_atr_pct`, `recommended_stop_pct_base`, `recommended_stop_pct_conservative`, `break_boundary_price`
- `valid_from_utc`

`trade_allowed` учитывает агрегированный `broken_flag` уровня и `touches_n >= 5`.

### Запуск выгрузки

Из корня репозитория (где доступен пакет `trading_bot`):

```bash
python -m trading_bot.entrypoints.export_to_sheets
```

Нужны `GOOGLE_CREDENTIALS_PATH` и идентификатор таблицы (`MARKET_AUDIT_SHEET_ID` / URL), см. `export_to_sheets.py`.
