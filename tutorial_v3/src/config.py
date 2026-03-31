import os
from dotenv import load_dotenv

# Загружаем переменные из .env
load_dotenv()

# Google Sheets
GOOGLE_SHEETS_CREDENTIALS = os.getenv(
    "GOOGLE_SHEETS_CREDENTIALS", "credentials.json"
)  # Путь к JSON-файлу сервисного аккаунта
GOOGLE_SHEETS_ID = os.getenv("GOOGLE_SHEETS_ID")  # ID твоей таблицы
# Telegram
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
# Bybit API (пока пустые, для будущего)
BYBIT_API_KEY = os.getenv("BYBIT_API_KEY")
BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET")
ALERT_TIMEOUT_MINUTES = int(
    os.getenv("ALERT_TIMEOUT_MINUTES", 60)
)  # Для тестов 1 минута, в продакшне можно установить 60
MIN_ALERTS_COUNT = int(
    os.getenv("MIN_ALERTS_COUNT", 4)
)  # минимальное количество оповещений для входа
MAX_ADDITIONAL_ALERTS = int(
    os.getenv("MAX_ADDITIONAL_ALERTS", 10)
)  # максимальное количество дополнительных оповещений для отмены
VOLUME_THRESHOLD = int(
    os.getenv("VOLUME_THRESHOLD", 20000000)
)  # Порог объема для фильтрации символов
# Максимальное количество одновременно открытых LONG и SHORT сделок
MAX_LONG_TRADES = int(os.getenv("MAX_LONG_TRADES", 10))
MAX_SHORT_TRADES = int(os.getenv("MAX_SHORT_TRADES", 10))
LOG_DIR_NAME = "Logs"

# Отладочный вывод для проверки
print("GOOGLE_SHEETS_CREDENTIALS:", repr(GOOGLE_SHEETS_CREDENTIALS))
print("GOOGLE_SHEETS_ID:", repr(GOOGLE_SHEETS_ID))

# Новые параметры
LONG_ATR_THRESHOLD_PERCENT = float(os.getenv("LONG_ATR_THRESHOLD_PERCENT", 5))
SHORT_ATR_THRESHOLD_PERCENT = float(os.getenv("SHORT_ATR_THRESHOLD_PERCENT", 5))
STATIC_DATA_UPDATE_DAYS = int(os.getenv("STATIC_DATA_UPDATE_DAYS", 30))
HISTORICAL_DATA_UPDATE_DAYS = int(os.getenv("HISTORICAL_DATA_UPDATE_DAYS", 2))
