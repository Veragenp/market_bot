# 📚 Документация торгового бота

## 🧠 Архитектура

### Основной контур
- **SUPERVISOR_ARCHITECTURE.md** — Единый авто-оркестратор (главный документ)
- **STATE_MANAGER_DESIGN.md** — Детальное описание State Manager
- **TEST_MODE_GUIDE.md** — Тестовый контур и быстрые проверки

---

## 📦 Модули

### State Manager
- **MODULE_STATE_MANAGER.md** — Краткое описание (API, режимы)
- **STATE_MANAGER_DESIGN.md** — Полное описание (500+ строк)
- **scripts/README_STATE_MANAGER_WORKFLOW.md** — Workflow и примеры

### Structural Cycle
- **MODULE_STRUCTURAL_CYCLE.md** — Краткое описание
- **analytics/structural_cycle.py** — Реальный алгоритм
- **analytics/test_level_generator.py** — Тестовый режим

### Entry Detector
- **MODULE_ENTRY_DETECTOR.md** — Описание entry detector, gate, flip

### Volume Profile
- **MODULE_VOLUME_PROFILE.md** — Поиск HVN/LVN

### Test Analyzer
- **MODULE_TEST_ANALYZER.md** — Анализатор и мониторинг

---

## 🧪 Тестирование

- **TEST_MODE_GUIDE.md** — Полный гайд по тестовому контуру
- **scripts/analyze_test_run.py** — Анализатор отчётов
- **scripts/test_state_manager.py** — Тесты State Manager

---

## 🔧 Утилиты

- **scripts/full_reset.py** — Полный сброс
- **scripts/run_supervisor.py** — Главный оркестратор
- **scripts/run_supervisor_fast.py** — Ускоренный режим (отладка)

---

## 📊 Конфигурация

- **config/settings.py** — Все настройки
- **.env** — Переменные окружения
- **config/STRUCTURAL_AND_TELEGRAM.env.sample** — Пример конфига

---

## 🗑️ Устаревшие файлы

Эти файлы больше не актуальны и будут удалены:

- **cycle_modules_runtime_contract.md** — Устаревшая спецификация
- **cycle_structural_start_spec.md** — Устаревшая спецификация
- **level_events_runtime_spec.md** — Устаревшая спецификация
- **level_events_state_machine_spec.md** — Устаревшая спецификация
- **manual_global_hvn_roadmap.md** — Roadmap (устарел)
- **manual_global_levels_sheet.md** — Устаревшее описание
- **state_manager.md** — Дубликат (заменён на STATE_MANAGER_DESIGN.md)
- **structural_cycle_module.md** — Устаревшее описание
- **structural_cycle_v2_get_zone_for_price.md** — Устаревшая версия
- **structural_level_selection_module.md** — Устаревшее описание
- **structural_ops_alarms_plan.md** — План (не реализован)
- **tutorial_v3_long_short_formula_map.md** — Старая документация V3

---

## 📞 Быстрый старт

### Запуск supervisor
```powershell
# Однократный запуск
python -m trading_bot.scripts.run_supervisor

# Постоянный цикл
python -m trading_bot.scripts.run_supervisor --loop
```

### Тестовый режим
```powershell
# Включить тестовый режим
$env:TEST_MODE=1

# Запустить
python -m trading_bot.scripts.run_supervisor --loop

# Анализировать
python -m trading_bot.scripts.analyze_test_run --monitor 300
```

### Мониторинг
```powershell
# Авто-мониторинг (обновление каждые 5 мин)
python -m trading_bot.scripts.analyze_test_run --monitor 300 --output report.txt

# Режим наблюдения (обновление каждые 60 сек)
python -m trading_bot.scripts.analyze_test_run --watch 60
```

---

**Версия**: 1.0  
**Дата**: 2026-04-16  
**Статус**: Актуально
