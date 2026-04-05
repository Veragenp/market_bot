import sys
import os

print("=== Диагностика создания БД ===\n")
print("Текущая директория:", os.getcwd())

# Добавляем корень проекта в sys.path
root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, root)
print("Корень проекта добавлен в sys.path:", root)

# Проверяем наличие необходимых файлов
print("\nПроверка файлов:")
required_files = ["config.py", "trading_bot/data/db.py", "trading_bot/data/schema.py"]
for f in required_files:
    exists = os.path.exists(os.path.join(root, f))
    print(f"  {f}: {'OK' if exists else 'НЕ НАЙДЕН'}")

# Пробуем импортировать config
print("\nИмпорт config...")
try:
    import config
    print("  config импортирован успешно")
    print("  DATA_DIR =", config.DATA_DIR)
    print("  DB_PATH =", config.DB_PATH)
except Exception as e:
    print("  Ошибка импорта config:", e)
    import traceback
    traceback.print_exc()

# Пробуем импортировать connection
print("\nИмпорт trading_bot.data.db...")
try:
    from trading_bot.data.db import get_connection
    print("  get_connection импортирована")
except Exception as e:
    print("  Ошибка импорта connection:", e)
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Пробуем импортировать schema и вызвать init_db
print("\nИмпорт trading_bot.data.schema...")
try:
    from trading_bot.data.schema import init_db, run_migrations
    print("  init_db / run_migrations импортированы")
except Exception as e:
    print("  Ошибка импорта schema:", e)
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Вызываем init_db
print("\nВызов init_db...")
try:
    init_db()
    run_migrations()
    print("  init_db + run_migrations выполнены успешно")
except Exception as e:
    print("  Ошибка при вызове init_db/run_migrations:", e)
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Проверяем наличие файла БД
print("\nПроверка файла БД:")
if os.path.exists(config.DB_PATH):
    size = os.path.getsize(config.DB_PATH)
    print(f"  БД создана: {config.DB_PATH} (размер: {size} байт)")
else:
    print(f"  БД НЕ НАЙДЕНА по пути: {config.DB_PATH}")
    # Проверим, создалась ли папка data
    if os.path.exists(config.DATA_DIR):
        print(f"  Папка data существует: {config.DATA_DIR}")
        print("  Содержимое папки data:", os.listdir(config.DATA_DIR))
    else:
        print(f"  Папка data НЕ создана: {config.DATA_DIR}")

print("\n=== Диагностика завершена ===")