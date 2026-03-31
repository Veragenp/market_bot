import logging
import os
from datetime import datetime, timedelta
import glob
from logging.handlers import RotatingFileHandler


class LoggerSetup:
    """Класс для настройки логирования с использованием синглтона."""

    _loggers = {}  # Хранилище для логгеров по имени модуля
    _telegram_client = None  # Экземпляр TelegramClient для уведомлений

    @staticmethod
    def get_telegram_client():
        """Инициализация TelegramClient (синглтон) с ленивым импортом."""
        if LoggerSetup._telegram_client is None:
            try:
                from telegram_client import TelegramClient  # Импорт внутри метода

                LoggerSetup._telegram_client = TelegramClient()
                print("TelegramClient инициализирован для LoggerSetup")
            except Exception as e:
                print(f"Ошибка инициализации TelegramClient: {e}")
        return LoggerSetup._telegram_client

    @staticmethod
    def check_log_directory_size(
        log_dir, module_name, max_size_bytes=1 * 1024 * 1024 * 1024
    ):
        """Проверяет размер папки Logs и отправляет уведомление, если превышен лимит (1 ГБ)."""
        logger = logging.getLogger(module_name)
        try:
            total_size = 0
            for dirpath, _, filenames in os.walk(log_dir):
                for f in filenames:
                    fp = os.path.join(dirpath, f)
                    total_size += os.path.getsize(fp)
            if total_size > max_size_bytes:
                message = f"⚠️ Размер папки логов ({log_dir}) превысил 1 ГБ: {total_size / (1024*1024):.2f} МБ"
                logger.warning(message)
                # Отправка в Telegram
                telegram_client = LoggerSetup.get_telegram_client()
                if telegram_client:
                    telegram_client.send_message(message)
                return True
            return False
        except Exception as e:
            logger.error(f"Ошибка при проверке размера папки логов: {e}")
            return False

    @staticmethod
    def setup_logging(module_name, retention_days=1):
        """Настройка логирования для указанного модуля (один логгер на модуль)."""
        if module_name in LoggerSetup._loggers:
            print(
                f"Логгер для модуля {module_name} уже существует, возвращаем существующий"
            )
            return LoggerSetup._loggers[module_name]

        # Создаем директорию для логов
        current_dir = os.path.dirname(__file__)
        parent_dir = os.path.dirname(current_dir)
        log_dir = os.path.join(parent_dir, "Logs")
        try:
            os.makedirs(log_dir, exist_ok=True)
            print(f"Log directory created/exists: {log_dir}")
        except Exception as e:
            print(f"Error creating log directory: {e}")

        # Проверяем размер папки логов
        LoggerSetup.check_log_directory_size(log_dir, module_name)

        # Используем один лог-файл для сессии
        session_start_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        log_file = os.path.join(log_dir, f"{module_name}_{session_start_time}.log")
        print(f"Log file: {log_file}")

        # Настраиваем логгер
        logger = logging.getLogger(module_name)
        env = os.getenv("ENV", "development")
        log_level = logging.DEBUG if env == "development" else logging.INFO
        logger.setLevel(log_level)

        if not logger.handlers:
            # Обработчик для файла с ротацией (10 МБ, до 5 файлов)
            try:
                file_handler = RotatingFileHandler(
                    log_file, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
                )
                file_handler.setLevel(log_level)
                file_handler.setFormatter(
                    logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
                )
                logger.addHandler(file_handler)
                print(f"RotatingFileHandler added for: {log_file}")
            except Exception as e:
                print(f"Error setting up file handler: {e}")

            # Обработчик для консоли
            console_handler = logging.StreamHandler()
            console_handler.setLevel(log_level)
            console_handler.setFormatter(
                logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
            )
            logger.addHandler(console_handler)
            print("Console handler added")

        # Очищаем старые логи (старше 1 дня)
        LoggerSetup.cleanup_old_logs(log_dir, module_name, retention_days)

        # Сохраняем логгер
        LoggerSetup._loggers[module_name] = logger
        print(f"Создан новый логгер для модуля {module_name}, лог-файл: {log_file}")
        return logger

    @staticmethod
    def cleanup_old_logs(log_dir, module_name, retention_days=1):
        """Удаляет лог-файлы старше заданного количества дней (по умолчанию 1 день)."""
        cutoff_date = datetime.now() - timedelta(days=retention_days)
        pattern = os.path.join(log_dir, f"{module_name}_*.log")
        for log_file in glob.glob(pattern):
            try:
                file_time_str = log_file.split(f"{module_name}_")[1].replace(".log", "")
                file_time = datetime.strptime(file_time_str, "%Y-%m-%d_%H-%M-%S")
                if file_time < cutoff_date:
                    os.remove(log_file)
                    print(f"Удален старый лог: {log_file}")
            except (IndexError, ValueError, OSError) as e:
                print(f"Ошибка при удалении {log_file}: {e}")
