# Entry Detector — Детектор входа

## Обзор

**Файлы**:
- `trading_bot/analytics/level_cross_monitor.py` — мониторинг пересечений
- `trading_bot/analytics/entry_gate.py` — логика входа (V3/V4)

**Интервал**: SUPERVISOR_ENTRY_TICK_SEC (10 сек по умолчанию)

---

## Компоненты

### 1. Level Cross Monitor

**Что делает**:
- Мониторит пересечения уровней из `cycle_levels`
- Отслеживает алерты по каждому символу
- Формирует групповые сигналы (N символов одновременно)

**Параметры**:
```env
LEVEL_CROSS_POLL_SEC=10
LEVEL_CROSS_ALERT_TIMEOUT_MINUTES=5
LEVEL_CROSS_MIN_ALERTS_COUNT=2
```

---

### 2. Entry Gate

**Что делает**:
- Проверяет ATR-порог для входа
- Выставляет лимитные ордера
- Обрабатывает flip (закрытие противоположной стороны)

**Параметры**:
```env
ENTRY_GATE_LONG_ATR_THRESHOLD_PCT=2
ENTRY_GATE_SHORT_ATR_THRESHOLD_PCT=2
```

---

### 3. Flip (смена сигнала)

**Когда**: Групповой сигнал LONG при позиции SHORT (и наоборот)

**Действия**:
1. Закрытие позиций противоположной стороны (market order)
2. Отмена pending ордеров
3. Rebuild уровней для новой стороны
4. Открытие новых позиций

**Параметры**:
```env
ENTRY_CLOSE_OPPOSITE_ON_FLIP_SIGNAL=1
```

---

### 4. Rebuild Opposite

**Когда**: После входа в позицию

**Что делает**:
- Пересчитывает противоположные уровни
- Использует текущую цену + offset * ATR

**Параметры**:
```env
STRUCTURAL_OPPOSITE_REBUILD_ENABLED=1
STRUCTURAL_OPPOSITE_REBUILD_BAND_MULT=2.0
```

---

### 5. Reconcile

**Что делает**:
- Синхронизирует position_records с биржей
- Обновляет status, filled_qty, close_reason
- Закрывает позиции по TP/SL

---

## API

### run_entry_detector_tick()
```python
result = run_entry_detector_tick()
# {'monitor': {...}, 'gate': [...], 'flip': {...}}
```

### process_v4_signal()
```python
result = process_v4_signal(
    cur,
    signal_type='LONG',
    monitor=monitor,
    prices={'BTC/USDT': 68500, ...}
)
# {'ok': True, 'entered': ['BTC/USDT', ...], 'rejected': [...]}
```

---

**См. также**: `SUPERVISOR_ARCHITECTURE.md`
