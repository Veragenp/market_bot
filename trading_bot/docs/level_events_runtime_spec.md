# Level Events Runtime Spec

Цель: получать из `price_levels` практическую аналитику по касаниям уровней:

- глубина проникновения (`penetration_atr`);
- сила отскока (`rebound_pure_atr`, `rebound_after_return_atr`);
- агрегированная сила уровня (`composite_score`);
- статистический профиль стопа для торговли.

## Изменённые модули

- `trading_bot/analytics/level_events.py`
- `trading_bot/config/settings.py`
- `trading_bot/entrypoints/export_to_sheets.py`

## Новые/обновлённые настройки (`.env`)

- `LEVEL_EVENTS_MODE=runtime` (зарезервировано под режимы)
- `LEVEL_EVENTS_LOOKBACK_HOURS=24`
- `LEVEL_EVENTS_WINDOW_HOURS=4`
- `LEVEL_EVENTS_MIN_PENETRATION_ATR=0.05`
- `LEVEL_EVENTS_MIN_REBOUND_PURE_ATR=0.03`
- `LEVEL_EVENTS_RETURN_EPS_ATR=0.05`
- `LEVEL_EVENTS_REBOUND_HORIZON_BARS=240`
- `LEVEL_EVENTS_WORKSHEET=level_events`
- `LEVEL_STRENGTH_WORKSHEET=level_strength_report`
- `LEVEL_STOP_PROFILE_WORKSHEET=level_stop_profile`

## Листы Google Sheets

### `level_events`

Сырые события касаний:

- `event_id`
- `stable_level_id`
- `symbol`
- `month_utc`
- `tier`
- `layer`
- `level_price`
- `volume_peak`
- `duration_hours`
- `atr_daily`
- `dist_start_atr`
- `touch_time_utc`
- `return_time_utc`
- `penetration_atr`
- `rebound_pure_atr`
- `rebound_after_return_atr`
- `cluster_size`
- `window_start_utc`
- `window_end_utc`

### `level_strength_report`

Агрегат по уровню (`stable_level_id`):

- `symbol`
- `stable_level_id`
- `tier`
- `layer`
- `level_price`
- `touches_n`
- `return_rate`
- `p50_penetration_atr`
- `p80_penetration_atr`
- `p95_penetration_atr`
- `median_rebound_after_atr`
- `median_rebound_pure_atr`
- `median_cluster_size`
- `composite_score`
- `strength_bucket` (`strong` / `medium` / `weak`)
- `recommended_stop_atr_base`
- `recommended_stop_atr_conservative`
- `exported_at_utc`

### `level_stop_profile`

Готовый профиль стопов:

- `symbol`
- `stable_level_id`
- `tier`
- `layer`
- `level_price`
- `atr_daily`
- `recommended_stop_atr_base`
- `recommended_stop_atr_conservative`
- `stop_price_long_base`
- `stop_price_short_base`
- `strength_bucket`
- `composite_score`
- `valid_from_utc`
- `valid_to_utc`

