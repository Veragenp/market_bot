# Совместимость

Рабочие скрипты перенесены в **`trading_bot/entrypoints/`** (пакет приложения).

Файлы в этой папке — тонкие обёртки: старые команды вида `python scripts/load_all_data.py` по-прежнему работают.

Рекомендуемый запуск (из корня репозитория, рядом с `config.py`):

- `python trading_bot/entrypoints/load_all_data.py`
- `python trading_bot/entrypoints/run_scheduler.py`
- `python trading_bot/entrypoints/healthcheck_data.py`

См. также `python -m trading_bot.scripts.data_foundation_status`.
