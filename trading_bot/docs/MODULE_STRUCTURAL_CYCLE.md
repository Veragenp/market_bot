# Structural Cycle — Генерация уровней

## Обзор

**Файлы**:
- `trading_bot/analytics/structural_cycle.py` — реальный алгоритм
- `trading_bot/analytics/test_level_generator.py` — тестовый режим
- `trading_bot/data/structural_cycle_db.py` — работа с БД

---

## Реальный режим (TEST_MODE=0)

### Алгоритм

1. **Scan candidates** — поиск уровней из `price_levels` (vp_local, manual_global_hvn)
2. **Filter symbols** — отбор по ликвидности (avg_volume_24h) и корреляции
3. **Calculate W** — для каждого символа: W = (U - L) / ATR
4. **Calculate W*** — медиана W по голосующим символам
5. **Select levels** — отбор уровней с W ∈ [W_MIN, W_MAX]
6. **Freeze** — фиксация в `cycle_levels`
7. **Export** — выгрузка в Google Sheets

### Параметры

```env
STRUCTURAL_W_MIN=0.7
STRUCTURAL_W_MAX=2.5
STRUCTURAL_N_ETALON=3
STRUCTURAL_TOP_K=5
STRUCTURAL_MID_BAND_PCT=15
STRUCTURAL_ALLOWED_LEVEL_TYPES=vp_local,manual_global_hvn
```

---

## Тестовый режим (TEST_MODE=1)

### Алгоритм

1. **Get symbols** — первые N символов из TRADING_SYMBOLS
2. **Get prices** — текущая цена из price_feed
3. **Get ATR** — из instruments
4. **Generate levels**:
   - LONG: `current_price - TEST_LEVEL_OFFSET_ATR * ATR`
   - SHORT: `current_price + TEST_LEVEL_OFFSET_ATR * ATR`
5. **Save** — запись в те же таблицы

### Параметры

```env
TEST_MODE=1
TEST_LEVEL_OFFSET_ATR=0.2
TEST_OPPOSITE_OFFSET_ATR=0.4
TEST_CYCLE_SYMBOLS_COUNT=10
```

---

## Rebuild противоположной стороны

**Когда**: После входа в LONG или SHORT

**Реальный режим**:
- Поиск уровней с подходящим W*
- Использование price_levels для кандидатов

**Тестовый режим**:
- LONG: `current_price - TEST_OPPOSITE_OFFSET_ATR * ATR`
- SHORT: `current_price + TEST_OPPOSITE_OFFSET_ATR * ATR`

---

## API

### generate_test_levels()
```python
result = generate_test_levels()
# {'ok': True, 'cycle_id': '...', 'levels_created': 20}
```

### rebuild_opposite_test_levels(cycle_id, known_side)
```python
result = rebuild_opposite_test_levels(cycle_id, 'long')
# Создает SHORT уровни на +0.4 ATR
```

---

**См. также**: `TEST_MODE_GUIDE.md`, `SUPERVISOR_ARCHITECTURE.md`
