import os
import time
import threading
import sys
from bybit_api import BybitAPI
from datetime import datetime, timedelta
from traiding_monitor import TradingMonitor
from trade_signal_processor import TradeSignalProcessor
from trade_manager import TradeManager
from populate_static_data import populate_static_data
from populate_historical_data import populate_historical_data
from google_sheets import GoogleSheetsClient
from telegram_client import TelegramClient
from statistics_collector import run_statistics_collection  # Добавляем импорт
from config import (
    STATIC_DATA_UPDATE_DAYS,
    HISTORICAL_DATA_UPDATE_DAYS,
    GOOGLE_SHEETS_CREDENTIALS,
    GOOGLE_SHEETS_ID,
    BYBIT_API_KEY,
    BYBIT_API_SECRET,
    TELEGRAM_TOKEN,
    CHAT_ID,
)
from utils import LoggerSetup


class Main:
    def __init__(self):
        sys.path.append(os.path.abspath(os.path.dirname(__file__)))
        self.logger = LoggerSetup.setup_logging("main")
        self.running = True
        self.is_running = False
        self.allow_long_signals = True
        self.allow_short_signals = True

        # Создаем BybitAPI с ключами
        self.bybit_api = BybitAPI(BYBIT_API_KEY, BYBIT_API_SECRET)

        self.sheets_client = GoogleSheetsClient(
            GOOGLE_SHEETS_CREDENTIALS, GOOGLE_SHEETS_ID
        )
        self.telegram_client = TelegramClient(TELEGRAM_TOKEN, CHAT_ID)

        self.trading_monitor = TradingMonitor(self)
        self.trade_signal_processor = TradeSignalProcessor(
            self, self.trading_monitor, self.trading_monitor.price_fetcher
        )
        self.trading_monitor.set_signal_processor(self.trade_signal_processor)
        self.trade_manager = TradeManager(
            self,
            bybit_api=self.bybit_api,
            telegram_token=TELEGRAM_TOKEN,
            chat_id=CHAT_ID,
        )

    def reset_column_f(self):
        """Очищает столбец F в листах long и short при запуске программы."""
        self.logger.info("Сброс столбца F в листах long и short")
        print("Сброс столбца F в листах long и short")

        for sheet_name in ["long", "short"]:
            sheet = self.sheets_client.get_sheet(sheet_name)
            if not sheet:
                self.logger.error(
                    f"Не удалось получить лист {sheet_name} для сброса столбца F"
                )
                print(f"Не удалось получить лист {sheet_name} для сброса столбца F")
                continue

            try:
                # Получаем все значения столбца F (индекс 6)
                column_f = sheet.col_values(6)
                updates = []

                # Проходим по всем строкам, начиная со второй (пропускаем заголовок)
                for row_idx in range(2, len(column_f) + 1):
                    updates.append(
                        {
                            "range": f"{sheet_name}!F{row_idx}",
                            "values": [[""]],  # Устанавливаем пустое значение
                        }
                    )

                if updates:
                    self.sheets_client.batch_update(updates)
                    self.logger.info(
                        f"Столбец F очищен в листе {sheet_name}: {len(updates)} строк"
                    )
                    print(
                        f"Столбец F очищен в листе {sheet_name}: {len(updates)} строк"
                    )
                else:
                    self.logger.info(
                        f"Столбец F в листе {sheet_name} пустой, изменений не требуется"
                    )
                    print(
                        f"Столбец F в листе {sheet_name} пустой, изменений не требуется"
                    )
            except Exception as e:
                self.logger.error(
                    f"Ошибка при сбросе столбца F в листе {sheet_name}: {e}"
                )
                print(f"Ошибка при сбросе столбца F в листе {sheet_name}: {e}")

    def update_static_callback(self, action):
        """Обработчик для обновления статических данных."""
        if action == "yes":
            self.telegram_client.send_message("Запущено обновление статических данных")
            populate_static_data()
            self.telegram_client.send_message("Статические данные обновлены")
        else:
            self.telegram_client.send_message("Обновление статических данных пропущено")

    def update_historical_callback(self, action):
        """Обработчик для обновления исторических данных."""
        if action == "yes":
            self.telegram_client.send_message("Запущено обновление исторических данных")
            populate_historical_data()
            self.telegram_client.send_message("Исторические данные обновлены")
        else:
            self.telegram_client.send_message(
                "Обновление исторических данных пропущено"
            )

    def start_program(self):
        """Запускает основную логику программы."""
        self.logger.info("Запуск программы...")
        print("Запуск программы...")

        # Сбрасываем столбец F перед запуском
        self.reset_column_f()

        # Запускаем сбор статистики
        self.logger.info("Запуск сбора статистики закрытых сделок")
        print("Запуск сбора статистики закрытых сделок")
        run_statistics_collection(
            self.bybit_api, self.sheets_client, self.telegram_client
        )

        # Устанавливаем флаг, что программа запущена
        self.is_running = True
        self.running = True

        # Регистрируем callback-функции для обработки нажатий, используя register_general_callback
        self.telegram_client.register_general_callback(
            "update_static", self.update_static_callback
        )
        self.telegram_client.register_general_callback(
            "update_historical", self.update_historical_callback
        )

        # Отправляем сообщения в Telegram
        self.telegram_client.send_message(
            "Обновить статические данные?",
            with_buttons=True,
            callback_id="update_static",
        )
        self.telegram_client.send_message(
            "Обновить исторические данные?",
            with_buttons=True,
            callback_id="update_historical",
        )

        # Поток для TradingMonitor
        monitor_thread = threading.Thread(target=self.trading_monitor.run, daemon=True)
        monitor_thread.start()

        # Поток для TradeSignalProcessor
        signal_processor_thread = threading.Thread(
            target=self.trade_signal_processor.run, daemon=True
        )
        signal_processor_thread.start()

        # Поток для TradeManager
        manager_thread = threading.Thread(target=self.trade_manager.run, daemon=True)
        manager_thread.start()

        self.logger.info("Все модули запущены. Система работает...")
        print("Все модули запущены. Система работает...")

        # Запускаем polling в главном потоке
        self.telegram_client.start_polling()

        # Ожидаем завершения потоков
        monitor_thread.join()
        signal_processor_thread.join()
        manager_thread.join()

        # После завершения сбрасываем флаг
        self.is_running = False
        self.logger.info("Программа остановлена")
        print("Программа остановлена")

    def stop(self):
        """Останавливает программу."""
        self.logger.info("Остановка программы...")
        self.running = False
        self.is_running = False
        self.telegram_client.stop()

    def run(self):
        """ОсновнойMETHOD для запуска программы через командную строку."""
        self.start_program()


if __name__ == "__main__":
    main = Main()
    main.run()
