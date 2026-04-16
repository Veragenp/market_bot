# Проблема с auto-reset цикла и её исправление

## 📋 Проблема

После включения Bybit API ключей и переключения на production:

```
[11:48] [LEVELS_REBUILD] ok cycle=bd4b86ce Supervisor levels rebuild completed
[11:49] [SAFE_AUTO_RESET] ok cycle=bd4b86ce Safe auto-reset before structural
[11:49] Structural [bb10765f]: старт цикла, фаза scanning
[11:49] Structural [bb10765f]: пул готов, фаза armed
[11:49] [STRUCTURAL_FREEZE] ok cycle=bb10765f Cycle levels frozen after scan
```

**Цикл `bd4b86ce` автоматически сбросился через SAFE_AUTO_RESET**, хотя:
- ✅ Входа в сделку не было
- ✅ Нет позиций в БД
- ✅ Уровни были зафиксированы (`levels_frozen=1`)
- ✅ Фаза была `armed` (готов к входу)

## 🔍 Причина

Функция `_should_skip_scheduled_structural()` вызывала `_safe_auto_reset_cycle()` **ДВАЖДЫ**:

1. **Первый вызов** (строка 235): проверка залипания >24h - ✅ OK
2. **Второй вызов** (строка 248): **немедленный auto-reset** для любого active frozen cycle - ❌ БАГ

Второй вызов сбрасывал **любой** цикл в фазе `arming/in_position` с `levels_frozen=1`, даже если он активен всего несколько минут!

### Логика _safe_auto_reset_cycle()

```python
# Проверяет:
# 1. levels_frozen=1 и phase in ("arming", "in_position") ✅
# 2. Нет открытых позиций в БД ✅
# 3. Нет pending ордеров в БД ✅
# 4. Flat на Bybit (через API) ✅
# → Если всё OK: сбрасывает цикл!
```

**Проблема:** Auto-reset был предназначен для **залипших** циклов (>24h без позиций), но из-за двойного вызова срабатывал **немедленно**.

## ✅ Исправление

**Файл:** `trading_bot/scripts/run_supervisor.py`

Убран второй вызов `_safe_auto_reset_cycle()`. Теперь auto-reset применяется **ТОЛЬКО** если:
1. Цикл активен более **24 часов**
2. Нет позиций в БД
3. Flat на Bybit

```python
if frozen and phase in ("arming", "in_position"):
    cycle_id = str(row["cycle_id"]) if row["cycle_id"] else ""
    last_transition = int(row["last_transition_at"] or 0)
    now = int(time.time())
    
    # Auto-reset только если цикл залип >24h
    if last_transition and now - last_transition > 86400:
        open_pos = cur.execute(
            "SELECT COUNT(*) AS c FROM position_records WHERE cycle_id = ? AND status IN ('pending', 'open')",
            (cycle_id,)
        ).fetchone()
        n_open = int(open_pos["c"] if open_pos else 0)
        
        if n_open == 0:
            reset_done, reset_reason = _safe_auto_reset_cycle()
            if reset_done:
                logger.warning("auto-reset STUCK cycle (no positions for >24h)")
    
    # Выходим - structural будет пропущен
    # Auto-reset не применяется для нормальных активных циклов
    return True, f"active_trading_cycle phase={phase} levels_frozen=1"
```

## 🚀 Как применить

### 1. Остановить supervisor

```powershell
Get-Process python
Stop-Process -Id <ID> -Force
```

### 2. Проверить текущие позиции на Bybit

```powershell
cd market_bot
$env:PYTHONPATH="."
python -m trading_bot.scripts.check_bybit_positions
```

**Ожидаемый вывод:**
```
Открытые позиции на Bybit:
  Нет открытых позиций (FLAT)
  или
  BTCUSDT          size=0.00100000 (LONG)
```

### 3. Запустить supervisor

```powershell
cd market_bot
$env:PYTHONPATH="."
python -m trading_bot.scripts.run_supervisor --loop
```

### 4. Проверить логи

Через 1-2 минуты:

```powershell
Get-Content trading_bot\logs\supervisor_*.log -Tail 50
```

**Ожидаемые логи:**
```
[STRUCTURAL_FREEZE] ok cycle=bb10765f Cycle levels frozen after scan
Supervisor: safe auto-reset blocked, keep structural skip (phase=armed frozen=1)
```

**НЕ должно быть:**
```
[SAFE_AUTO_RESET] ok cycle=...
```

## 📊 Что изменилось

| Сценарий | До исправления | После исправления |
|----------|----------------|-------------------|
| Цикл в фазе `armed` (несколько минут) | ❌ Auto-reset | ✅ Skip structural |
| Цикл в фазе `armed` (>24h, нет позиций) | ✅ Auto-reset | ✅ Auto-reset |
| Цикл в фазе `in_position` (есть позиция) | ✅ Skip | ✅ Skip |
| Цикл в фазе `in_position` (>24h, нет позиций) | ✅ Auto-reset | ✅ Auto-reset |

## ⚠️ Важные замечания

### 1. Auto-reset теперь ТОЛЬКО для залипших циклов

- ✅ Работает для циклов >24h без позиций
- ❌ НЕ работает для нормальных активных циклов

### 2. Если цикл всё ещё сбрасывается

Проверьте логи:
```
Supervisor: safe auto-reset applied (cycle=... reason=...)
```

Причины auto-reset:
- `stale_active_cycle_without_positions_or_orders` - цикл >24h без позиций
- `exchange_has_open_positions` - есть позиции на Bybit

### 3. Ручной сброс

Если цикл залип и нужно сбросить вручную:

```powershell
python -m trading_bot.scripts.full_reset.py --force
```

**Внимание:** Это закроет все позиции и сбросит состояние!

## 🧪 Проверка

### 1. Проверить Bybit API

```powershell
python -m trading_bot.scripts.check_bybit_positions
```

### 2. Проверить текущий цикл

```powershell
python -c "
from trading_bot.data.db import get_connection
conn = get_connection()
row = conn.execute('SELECT * FROM trading_state WHERE id=1').fetchone()
print(f'cycle_id: {row[\"cycle_id\"][:8] if row[\"cycle_id\"] else None}')
print(f'phase: {row[\"cycle_phase\"]}')
print(f'levels_frozen: {row[\"levels_frozen\"]}')
print(f'last_transition_at: {row[\"last_transition_at\"]}')
conn.close()
"
```

### 3. Проверить логи supervisor

```powershell
Get-Content trading_bot\logs\supervisor_*.log -Tail 100 | Select-String "SAFE_AUTO_RESET|active_trading_cycle"
```

**Ожидаемый результат:**
```
Supervisor: safe auto-reset blocked, keep structural skip (phase=armed frozen=1)
```

## 📞 Если что-то не работает

1. **Проверьте что supervisor запущен:**
   ```powershell
   Get-Process python
   ```

2. **Проверьте логи на ошибки:**
   ```powershell
   Get-Content trading_bot\logs\supervisor_*.log -Tail 200
   ```

3. **Проверьте Bybit API ключи:**
   ```powershell
   Get-Content trading_bot\.env | Select-String "BYBIT_API"
   ```

4. **Проверьте есть ли позиции на Bybit:**
   ```powershell
   python -m trading_bot.scripts.check_bybit_positions
   ```

---

**Дата:** 2026-04-16  
**Версия:** 1.1  
**Статус:** Исправление применено, требует перезапуска supervisor
