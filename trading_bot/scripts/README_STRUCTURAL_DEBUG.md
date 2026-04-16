# Отладка и контроль работы Structural Cycle

## 🚀 Быстрый старт

### 1. Проверка текущего состояния цикла

```bash
PYTHONPATH=. python -m trading_bot.scripts.reset_trading_state
```

**Что показывает:**
- Активный cycle_id и structural_cycle_id
- Фазу цикла (arming/in_position/closed)
- Статус frozen (1/0)
- Количество открытых позиций
- Количество символов в цикле
- Количество уровней LONG/SHORT

### 2. Принудительный сброс цикла

**ВНИМАНИЕ:** Используйте только если уверены!

```bash
PYTHONPATH=. python -m trading_bot.scripts.reset_trading_state --force
```

**Что делает:**
- Устанавливает `cycle_phase = 'closed'`
- Сбрасывает `levels_frozen = 0`
- Очищает `cycle_id` и `structural_cycle_id`
- Ставит `close_reason = 'manual_reset'`

### 3. Диагностика выбора уровней (v4)

```bash
PYTHONPATH=. python -m trading_bot.scripts.debug_structural_v4
```

**Что показывает:**
- Текущие цены и ATR для всех 21 символа
- LONG уровни в полосе [ref - d_max*ATR, ref - d_min*ATR]
- SHORT уровни в полосе [ref + d_min*ATR, ref + d_max*ATR]
- Сколько символов имеют LONG/SHORT/оба уровня
- ТОП-15 сильнейших LONG и ТОП-16 сильнейших SHORT
- Сохраняет полный отчёт в `structural_v4_debug_report.csv`

**Пример вывода:**
```
================================================================================
ТОП LONG-уровней (сильнейшие по volume_peak)
================================================================================
Символ       Цена       VolumePeak   DistATR  Tier     Тип
BTC/USDT     62345.67   5678.90      1.98     Tier_2   vp_local
ETH/USDT     3456.78    2345.67      2.34     Tier_1   vp_local
...

================================================================================
ТОП SHORT-уровней (сильнейшие по volume_peak)
================================================================================
Символ       Цена       VolumePeak   DistATR  Tier     Тип
AAVE/USDT    105.6780   1456.78      3.13     Tier_1   vp_local
...

================================================================================
Символы, попавшие в оба топа: 12 – ['AAVE/USDT', 'BTC/USDT', ...]
```

### 4. Проверка VP уровней в БД

```sql
-- Сколько активных уровней каждого типа
SELECT level_type, COUNT(*) as c 
FROM price_levels 
WHERE is_active = 1 AND status = 'active'
GROUP BY level_type;

-- Пример уровней для BTC/USDT
SELECT id, price, level_type, tier, volume_peak, created_at
FROM price_levels
WHERE symbol = 'BTC/USDT' AND is_active = 1
ORDER BY COALESCE(volume_peak, strength, 0) DESC
LIMIT 20;

-- Старые уровни (не пересчитывались > 7 дней)
SELECT symbol, level_type, MAX(created_at) as last_update
FROM price_levels
WHERE is_active = 1
GROUP BY symbol, level_type
HAVING last_update < datetime('now', '-7 days');
```

### 5. Проверка structural_cycle_symbols (сырые результаты скана)

```sql
-- Уровни для конкретного цикла
SELECT symbol, direction, level_price, tier, volume_peak, w_atr, z_w, ok_w
FROM structural_cycle_symbols
WHERE cycle_id = 'xxx-xxx-xxx'
ORDER BY direction, volume_peak DESC;

-- Статистика по циклу
SELECT 
    direction,
    COUNT(*) as levels_n,
    AVG(w_atr) as avg_width_atr,
    AVG(z_w) as avg_z_score
FROM structural_cycle_symbols
WHERE cycle_id = 'xxx-xxx-xxx'
GROUP BY direction;
```

### 6. Проверка cycle_levels (финальные уровни для торговли)

```sql
-- Все уровни текущего цикла
SELECT symbol, direction, level_price, tier, distance_atr, created_at
FROM cycle_levels
WHERE cycle_id = 'xxx-xxx-xxx'
ORDER BY direction, distance_atr;

-- Уровни с большим расстоянием (возможная проблема)
SELECT symbol, direction, level_price, distance_atr
FROM cycle_levels
WHERE cycle_id = 'xxx-xxx-xxx' AND distance_atr > 3.0;
```

---

## 🔍 Чек-лист диагностики

### Проблема: Мало уровней в цикле (< 15 символов)

**Шаги:**

1. **Проверить VP:**
   ```bash
   PYTHONPATH=. python -m trading_bot.scripts.debug_structural_v4
   ```
   - Если `has_lower` или `has_upper` < 15 → проблема в VP

2. **Проверить VP в БД:**
   ```sql
   SELECT COUNT(DISTINCT symbol) FROM price_levels 
   WHERE is_active = 1 AND level_type = 'vp_local';
   ```
   - Должно быть ~21 символ

3. **Если VP мало:**
   - Пересчитать VP вручную:
     ```bash
     PYTHONPATH=. python -m trading_bot.scripts.rebuild_volume_profile
     ```
   - Увеличить `PRO_LEVELS_LOOKBACK_DAYS` в .env
   - Уменьшить `PRO_LEVELS_MIN_DURATION_HOURS`

4. **Если VP есть, но не попадают в полосу:**
   - Расширить полосу в .env:
     ```env
     STRUCTURAL_V4_BAND_MIN_ATR=0.5
     STRUCTURAL_V4_BAND_MAX_ATR=4.0
     ```

### Проблема: Старые уровни в cycle_levels

**Шаги:**

1. **Проверить расстояние:**
   ```sql
   SELECT symbol, direction, level_price, distance_atr, created_at
   FROM cycle_levels
   WHERE cycle_id = 'xxx-xxx-xxx'
   ORDER BY distance_atr DESC;
   ```

2. **Если distance_atr > 5:**
   - Проверить `CYCLE_LEVELS_FALLBACK_MAX_ATR` в .env
   - Уменьшить до 2.0-3.0
   - Пересоздать цикл

3. **Проверить, что VP свежий:**
   ```sql
   SELECT symbol, MAX(created_at) as last_vp
   FROM price_levels
   WHERE level_type = 'vp_local' AND symbol = 'BTC/USDT'
   GROUP BY symbol;
   ```

### Проблема: Цикл "залип" после закрытия терминала

**Шаги:**

1. **Проверить состояние:**
   ```bash
   PYTHONPATH=. python -m trading_bot.scripts.reset_trading_state
   ```

2. **Если `cycle_phase = 'in_position'` но позиций нет:**
   ```bash
   # Проверить позиции в БД
   SELECT * FROM position_records WHERE cycle_id = 'xxx' AND status IN ('pending', 'open');
   
   # Если позиций нет — сбросить цикл
   PYTHONPATH=. python -m trading_bot.scripts.reset_trading_state --force
   ```

3. **Авто-сброс включён в supervisor:**
   - Если нет позиций > 24 часа → цикл сбросится автоматически

### Проблема: Уровни не соответствуют v4 диагностике

**Шаги:**

1. **Сравнить v4 diagnostic и actual cycle:**
   ```bash
   # v4 diagnostic
   PYTHONPATH=. python -m trading_bot.scripts.debug_structural_v4
   
   # Посмотреть что выбрано
   SELECT symbol, direction, level_price, tier, volume_peak
   FROM structural_cycle_symbols
   WHERE cycle_id = 'xxx'
   ORDER BY direction, volume_peak DESC;
   ```

2. **Если расхождения:**
   - Проверить `STRUCTURAL_ALLOWED_LEVEL_TYPES` в .env
   - Проверить `STRUCTURAL_STRENGTH_FIRST_ENABLED`
   - Посмотреть логи `structural_cycle_db.py`

---

## 📊 Параметры для настройки

### Основные параметры (в .env)

```env
# Полоса поиска уровней (в ATR от текущей цены)
STRUCTURAL_V4_BAND_MIN_ATR=0.8
STRUCTURAL_V4_BAND_MAX_ATR=2.0

# Сколько уровней проверять на каждом символе
STRUCTURAL_V4_LEVELS_FETCH_LIMIT=50
STRUCTURAL_TOP_K=5

# Минимальное количество символов для запуска
STRUCTURAL_POOL_MIN_SIZE=15
STRUCTURAL_MIN_CANDIDATES_PER_SIDE=1

# Типы уровней для отбора
STRUCTURAL_ALLOWED_LEVEL_TYPES="vp_local,manual_global_hvn"

# Фильтрация по силе уровня
STRUCTURAL_STRENGTH_FIRST_ENABLED=1

# Групповой триггер (сколько монет должны коснуться mid-band)
LEVEL_CROSS_MIN_ALERTS_COUNT=2

# Расстояние до fallback-уровней
CYCLE_LEVELS_FALLBACK_MAX_ATR=3.0
CYCLE_LEVELS_MIN_DIST_ATR=0.5
```

### Параметры VP (исходные уровни)

```env
# Как часто пересчитывать VP
VP_LOCAL_REBUILD_INTERVAL_HOURS=4

# Сколько дней истории для VP
PRO_LEVELS_LOOKBACK_DAYS=30

# Минимальная длительность уровня (часы)
PRO_LEVELS_MIN_DURATION_HOURS=4
```

---

## 🧪 Тестирование

### Локальный прогон supervisor

```bash
# Запустить supervisor в фоновом режиме
PYTHONPATH=. python -m trading_bot.scripts.run_supervisor &

# Следить за логами
tail -f trading_bot/logs/supervisor_*.log

# Проверить что structural запустился
grep "STRUCTURAL CYCLE" trading_bot/logs/supervisor_*.log
```

### Проверка выгрузки в Google Sheets

**Листы для контроля:**
1. `structural_trading_levels` - основные торговые уровни (новый)
2. `structural_levels_report_v4` - полная диагностика
3. `cycle_levels_v1` - финальные уровни для торговли
4. `cycle_levels_candidates_v1` - все кандидаты
5. `vp_local_levels` - исходные VP уровни

**Сравнивать:**
- `structural_trading_levels` ↔ `debug_structural_v4` вывод
- `cycle_levels_v1` ↔ SQL запрос `SELECT * FROM cycle_levels WHERE cycle_id = 'xxx'`

---

## 📞 Контакты

При проблемах собирать:
1. Вывод `reset_trading_state`
2. Вывод `debug_structural_v4`
3. Последние 100 строк из `supervisor_*.log`
4. SQL dump `structural_cycle_symbols` и `cycle_levels` для последнего цикла

---

**Версия:** 1.0  
**Дата:** 2026-04-15
