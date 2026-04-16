# Test Analyzer — Анализатор и мониторинг

## Обзор

**Файл**: `trading_bot/scripts/analyze_test_run.py`

**Назначение**: Анализ тестовых прогонов и постоянный мониторинг торгового контура.

---

## Режимы работы

### 1. Однократный анализ
```powershell
python -m trading_bot.scripts.analyze_test_run
```

Выводит отчёт в консоль.

---

### 2. Сохранение в файл
```powershell
python -m trading_bot.scripts.analyze_test_run --output report.txt
```

Сохраняет отчёт в файл (UTF-8).

---

### 3. Режим наблюдения
```powershell
python -m trading_bot.scripts.analyze_test_run --watch 60
```

Обновляет отчёт каждые 60 секунд (вывод в консоль + опционально в файл).

---

### 4. Автоматический мониторинг (фоновый процесс)
```powershell
python -m trading_bot.scripts.analyze_test_run --monitor 300
```

**Что делает**:
- Запускается один раз и работает постоянно
- Каждый 300 сек (5 мин) анализирует БД
- Автоматически перезаписывает файл отчёта
- Выводит краткую сводку в консоль

**Использование**:
```powershell
# Запустить в фоне
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd market_bot; python -m trading_bot.scripts.analyze_test_run --monitor 300"

# Или в отдельном терминале
python -m trading_bot.scripts.analyze_test_run --monitor 300 --output trading_bot_report.txt
```

---

## Отчёт включает

1. **Общая информация** — режим, время, символы
2. **Структурные циклы** — фазы, уровни
3. **Позиции** — открытые/закрытые, PnL, причины
4. **Ордера** — статусы, типы
5. **Ошибки** — из БД и логов
6. **Метрики** — время этапов, winning rate
7. **Рекомендации** — автоматические советы

---

## API

### auto_monitor_mode(interval_sec, output_file)
```python
auto_monitor_mode(interval_sec=300, output_file='trading_bot_report.txt')
# Запускает бесконечный цикл анализа
```

### watch_mode(interval_sec, output_file)
```python
watch_mode(interval_sec=60, output_file=None)
# Режим наблюдения
```

---

**См. также**: `TEST_MODE_GUIDE.md`, `SUPERVISOR_ARCHITECTURE.md`
