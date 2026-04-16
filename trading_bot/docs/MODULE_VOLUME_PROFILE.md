# Volume Profile — Поиск HVN/LVN

## Обзор

**Файлы**:
- `trading_bot/analytics/volume_profile_peaks.py` — алгоритм
- `trading_bot/data/volume_profile_peaks_db.py` — запись в БД

---

## Алгоритм

1. **Load OHLCV** — 1m данные за последние 30 дней
2. **Build histogram** — распределение объёма по ценам
3. **Find peaks** — поиск локальных максимумов (HVN) и минимумов (LVN)
4. **Merge zones** — склейка соседних уровней
5. **Save to DB** — запись в `price_levels` (level_type='vp_local')

---

## Параметры

```env
PRO_LEVELS_LOOKBACK_DAYS=30
PRO_LEVELS_HEIGHT_MULT=None  # adaptive
PRO_LEVELS_DISTANCE_PCT=None  # adaptive
PRO_LEVELS_VALLEY_THRESHOLD=None  # adaptive
PRO_LEVELS_MAX_LEVELS=18
```

---

## Интервал обновления

```env
VP_LOCAL_REBUILD_INTERVAL_HOURS=4
SUPERVISOR_LEVELS_REBUILD_SEC=1800
```

---

**См. также**: `SUPERVISOR_ARCHITECTURE.md`
