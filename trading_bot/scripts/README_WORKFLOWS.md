# Работа с состояниями торгового бота

## 🎯 Два сценария запуска

### Сценарий 1: Новый старт после ручного останова

**Когда:** Вы остановили бот руками и запускаете новую сессию

**Что делать:**
```bash
# Полный сброс перед новым запуском
PYTHONPATH=. python -m trading_bot.scripts.full_reset --force

# Запуск supervisor
PYTHONPATH=. python -m trading_bot.scripts.run_supervisor --loop
```

**Что произойдёт:**
- Все открытые позиции → `cancelled`
- Все pending ордера → `cancelled`
- `cycle_id` → `NULL`
- `structural_cycle_id` → `NULL`
- `cycle_phase` → `arming`
- `levels_frozen` → `0`
- Бот начинает с чистого состояния

---

### Сценарий 2: Аварийное восстановление после сбоя

**Когда:** Терминал упал, потеря связи, системная ошибка

**Что делать:**
```bash
# Просто перезапустите supervisor
PYTHONPATH=. python -m trading_bot.scripts.run_supervisor --loop
```

**Что произойдёт:**
- Supervisor проверит позиции в БД
- Если есть открытые позиции → продолжит работу с ними
- Если позиций нет → автоматически сбросит цикл и начнёт новый

---

## 🔍 Диагностика состояний

### Проверить текущее состояние

```bash
PYTHONPATH=. python -m trading_bot.scripts.check_open_positions
```

Показывает:
- Открытые позиции в БД
- Текущий cycle_id
- Статусы позиций

### Проверить залипший цикл

```bash
PYTHONPATH=. python -m trading_bot.scripts.reset_trading_state
```

Показывает:
- Активный cycle_id
- Фаза цикла
- Позиции в цикле
- Уровни в cycle_levels

---

## 🚨 Когда использовать full_reset

### ✅ Используйте full_reset когда:

1. **Заканчиваете торговую сессию** и хотите начать новую
2. **Меняете стратегию/параметры** и хотите чистый старт
3. **Переходите с демо на продакшен** или наоборот
4. **Планируете долгого не запускать** бота

### ❌ НЕ используйте full_reset когда:

1. **Терминал упал сам** - пусть supervisor восстановит состояние
2. **Потеряли соединение** - бот подхватит позиции сам
3. **Есть реальные открытые позиции на бирже** - они будут отменены!

---

## 📋 Типовые сценарии

### Сценарий A: Утренняя сессия

```bash
# Утро - новый день, новый цикл
PYTHONPATH=. python -m trading_bot.scripts.full_reset --force
PYTHONPATH=. python -m trading_bot.scripts.run_supervisor --loop
```

### Сценарий B: Аварийный перезапуск

```bash
# Бот упал - просто перезапускаем
PYTHONPATH=. python -m trading_bot.scripts.run_supervisor --loop
```

### Сценарий C: Вечерний останов

```bash
# Просто закройте терминал
# supervisor завершится
# На следующий день используйте full_reset
```

### Сценарий D: Отладка/Тестирование

```bash
# Полная очистка для теста
PYTHONPATH=. python -m trading_bot.scripts.full_reset --dry-run  # посмотреть что будет
PYTHONPATH=. python -m trading_bot.scripts.full_reset --force    # выполнить
```

---

## ⚠️ Важные предупреждения

### Опасность full_reset

**full_reset ОТСУТСТВУЕТ подтверждениями!**

```bash
# Сначала посмотрите что будет
PYTHONPATH=. python -m trading_bot.scripts.full_reset --dry-run

# Только потом выполняйте
PYTHONPATH=. python -m trading_bot.scripts.full_reset --force
```

### Риск потери позиций

Если у вас **реальные открытые позиции на бирже** и вы выполните `full_reset`:
- Позиции в БД будут помечены как `cancelled`
- **На бирже позиции останутся открытыми!**
- Бот не будет их контролировать
- **Можете получить убытки!**

**Решение:** Перед full_reset проверьте позиции на бирже и закройте их вручную, если нужно.

---

## 🔧 Параметры full_reset

```bash
# Показать что будет, но не выполнять
PYTHONPATH=. python -m trading_bot.scripts.full_reset --dry-run

# Выполнить без подтверждения
PYTHONPATH=. python -m trading_bot.scripts.full_reset --force

# Не закрывать позиции (только сброс состояния)
PYTHONPATH=. python -m trading_bot.scripts.full_reset --no-close-pos --force
```

---

## 📊 Логирование

Все действия full_reset логируются:
- `trading_bot/logs/supervisor_*.log` - если запущен supervisor
- `position_records.close_reason` - причина закрытия позиций
- `trading_state.close_reason` - причина сброса состояния

---

## 🧪 Тестирование

### Проверить что full_reset работает

```bash
# 1. Запустить supervisor
PYTHONPATH=. python -m trading_bot.scripts.run_supervisor --loop

# 2. В другом терминале проверить состояние
PYTHONPATH=. python -m trading_bot.scripts.check_open_positions

# 3. Остановить supervisor (Ctrl+C)

# 4. Выполнить полный сброс
PYTHONPATH=. python -m trading_bot.scripts.full_reset --force

# 5. Проверить что всё сброшено
PYTHONPATH=. python -m trading_bot.scripts.check_open_positions
# Должно показать: "Нет открытых позиций"

# 6. Запустить заново
PYTHONPATH=. python -m trading_bot.scripts.run_supervisor --loop

# 7. Проверить логи - должен быть новый cycle_id
grep "Supervisor config" trading_bot/logs/supervisor_*.log | tail -1
```

---

## 🆘 Частые проблемы

### Проблема: "Не могу запустить новый цикл, supervisor говорит что цикл активен"

**Причина:** Есть открытые позиции в БД

**Решение:**
```bash
# Проверить позиции
PYTHONPATH=. python -m trading_bot.scripts.check_open_positions

# Если позиции "зависшие" (на бирже их нет)
PYTHONPATH=. python -m trading_bot.scripts.full_reset --force

# Если позиции реальные - дождаться их закрытия или закрыть вручную
```

### Проблема: "Бот не подхватывает позиции после сбоя"

**Причина:** Supervisor не видит позиций или цикл был сброшен

**Решение:**
```bash
# Проверить позиции в БД
PYTHONPATH=. python -m trading_bot.scripts.check_open_positions

# Проверить trading_state
PYTHONPATH=. python -m trading_bot.scripts.reset_trading_state

# Если positions=open но cycle_phase=closed - нужно восстановить связь
```

---

## 📞 Контакты

При проблемах собирать:
1. Вывод `check_open_positions`
2. Вывод `reset_trading_state`
3. Последние 100 строк supervisor лога
4. SQL dump `position_records` где status IN ('pending', 'open')

---

**Версия:** 1.0  
**Дата:** 2026-04-16  
**Автор:** Koda AI Assistant
