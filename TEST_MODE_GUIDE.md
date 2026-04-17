# TEST_MODE - Тестовый контур для торгового бота

## Обзор

TEST_MODE позволяет тестировать торговый контур без подключения к бирже, используя кэшированные данные из базы (ATR, цены).

## Включение тестового режима

В файле `market_bot/trading_bot/.env` установите:

```env
# Включить тестовый режим
TEST_MODE=1

# Отключить загрузку данных с биржи (использовать только ATR из БД)
TEST_MODE_SKIP_DATA_REFRESH=1

# Отключить rebuild vp_local
TEST_MODE_SKIP_LEVELS_REBUILD=1

# Отключить экспорт vp_local к Sheets
TEST_MODE_SKIP_VP_EXPORT=1
```

## Что меняется в тестовом режиме

### 1. DATA_REFRESH пропускается
- Не загружаются данные с биржи (spot, macro, indices, OI)
- Не обновляются instruments
- Не обновляется ATR
- Используется кэш из базы данных

### 2. LEVELS_REBUILD пропускается
- Не пересчитываются vp_local уровни
- Не экспортируются к Google Sheets
- Используются кэшированные уровни из БД

### 3. Structural pipeline заменяется
- Вместо реального scan создаются искусственные уровни:
  - LONG: `current_price - TEST_LEVEL_OFFSET_ATR * ATR`
  - SHORT: `current_price + TEST_LEVEL_OFFSET_ATR * ATR`
- Цены берутся из БД (последний close 1m), не с WebSocket

### 4. Entry detector работает нормально
- Проверяет пересечения уровней
- Открывает/закрывает позиции через Bybit Demo (если BYBIT_USE_DEMO=1)

## Требования для работы

1. **ATR в базе данных** - должны быть предварительно загружены:
   ```bash
   python -m trading_bot.scripts.run_supervisor --loop  # В режиме production до первого запуска
   ```

2. **Bybit Demo API** - для открытия ордеров (опционально):
   ```env
   BYBIT_USE_DEMO=1
   BYBIT_API_KEY_TEST=ваш_ключ
   BYBIT_API_SECRET_TEST=ваш_секрет
   ```

3. **Цены в базе** - последние 1m бары для trading symbols

## Отключение тестового режима

В `.env` установите:
```env
TEST_MODE=0
```

Или полностью удалите/закомментируйте эти строки.

## Преимущества тестового режима

- **Быстрее** - нет задержек на загрузку данных с биржи
- **Независим от сети** - работает без подключения к Binance/Bybit
- **Предсказуем** - искусственные уровни позволяют тестировать конкретные сценарии
- **Безопасен** - используется Bybit Demo для ордеров (если включён)

## Ограничения

- **Только для тестирования** - не использовать в production!
- **Требует предварительных данных** - ATR и цены должны быть в БД
- **Искусственные уровни** - не отражают реальные рыночные условия

## Мониторинг

В логах supervisor вы увидите:

```
Supervisor config: ... TEST_MODE=1 skip_data=1 skip_levels=1 skip_vp_export=1 ...
TEST_MODE: Skipping all data refresh steps (using cached data from DB)
TEST_MODE: Skipping vp_local rebuild (using cached levels from DB)
TEST_MODE: Generating test levels
TEST_MODE: BTCUSDT - current=64523.50 LONG=64480.20 SHORT=64566.80 ATR=43.30
```

## Пример запуска

```powershell
# В режиме тестирования
cd market_bot
$env:PYTHONPATH="."
python -m trading_bot.scripts.run_supervisor

# В режиме loop
python -m trading_bot.scripts.run_supervisor --loop
```

## Сброс тестового цикла

Если тестовый цикл "залип", можно вручную сбросить его через БД:

```sql
UPDATE trading_state 
SET cycle_phase = 'closed', 
    levels_frozen = 0, 
    cycle_id = NULL, 
    structural_cycle_id = NULL
WHERE id = 1;

DELETE FROM structural_cycles WHERE ref_price_source = 'test';
DELETE FROM cycle_levels WHERE cycle_id IS NULL;
```

## См. также

- `market_bot/trading_bot/analytics/test_level_generator.py` - генератор тестовых уровней
- `market_bot/trading_bot/.env` - примеры настроек
- `market_bot/trading_bot/config/settings.py` - TEST_MODE параметры
