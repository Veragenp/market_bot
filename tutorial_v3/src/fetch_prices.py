import logging
import time
import os
from datetime import datetime
from bybit_api import BybitAPI
import requests
from google_sheets import GoogleSheetsClient
from config import GOOGLE_SHEETS_CREDENTIALS, GOOGLE_SHEETS_ID
from utils import LoggerSetup


class PriceFetcher:
    def __init__(self, bybit_api):
        self.logger = LoggerSetup.setup_logging("fetch_prices", retention_days=3)
        self.logger.info("Инициализация PriceFetcher")
        print("Инициализация PriceFetcher...")

        self.google_sheets = GoogleSheetsClient(
            GOOGLE_SHEETS_CREDENTIALS, GOOGLE_SHEETS_ID
        )
        self.trading_coins = self.google_sheets.get_trading_coins()
        self.symbols = [coin["coin"] for coin in self.trading_coins]

        self.logger.info(
            f"Получено {len(self.trading_coins)} монет для мониторинга: {self.symbols}"
        )
        print(
            f"Получено {len(self.trading_coins)} монет для мониторинга: {self.symbols}"
        )

        self.bybit_api = bybit_api  # Используем переданный экземпляр
        self.current_prices = {symbol: 0.0 for symbol in self.symbols}
        self.valid_symbols = []
        self.running = True

    def reconnect(self):
        self.logger.warning("Попытка переподключения WebSocket...")
        print("Попытка переподключения WebSocket...")
        self.valid_symbols = []
        self.subscribe_to_valid_symbols()
        # Не создаем новый BybitAPI, используем существующий

    def validate_symbol(self, symbol):
        """Проверяет валидность символа через REST API."""
        self.logger.debug(f"Проверка валидности символа {symbol}")
        try:
            base_url = "https://api.bybit.com"
            endpoint = "/v5/market/instruments-info"
            params = {"category": "linear", "symbol": symbol}
            response = requests.get(base_url + endpoint, params=params)
            data = response.json()
            if data["retCode"] == 0 and data["result"]["list"]:
                self.logger.info(f"Символ {symbol} валиден")
                print(f"Символ {symbol} валиден")
                return True
            self.logger.warning(
                f"Символ {symbol} не валиден: {data.get('retMsg', 'Нет данных')}"
            )
            print(f"Символ {symbol} не валиден: {data.get('retMsg', 'Нет данных')}")
            return False
        except Exception as e:
            self.logger.error(f"Ошибка проверки символа {symbol}: {e}")
            print(f"Ошибка проверки символа {symbol}: {e}")
            return False

    def handle_price_update(self, symbol, last_price):
        """Обработка обновления цены через WebSocket."""
        if symbol in self.valid_symbols:
            self.current_prices[symbol] = last_price
            self.logger.debug(f"Обновлена цена для {symbol}: {last_price}")
            print(f"Обновлена цена для {symbol}: {last_price}")

    def reconnect(self):
        """Переподключение WebSocket при разрыве."""
        self.logger.warning("Попытка переподключения WebSocket...")
        print("Попытка переподключения WebSocket...")
        self.bybit_api = BybitAPI()  # Синглтон, повторная инициализация не требуется
        self.valid_symbols = []
        self.subscribe_to_valid_symbols()

    def subscribe_to_valid_symbols(self):
        """Подписка только на валидные символы через WebSocket."""
        self.valid_symbols = [s for s in self.symbols if self.validate_symbol(s)]
        if not self.valid_symbols:
            self.logger.error("Не удалось найти валидные символы. Завершаем работу.")
            print("Не удалось найти валидные символы. Завершаем работу.")
            self.running = False
            return

        for symbol in self.valid_symbols:
            try:
                self.bybit_api.subscribe_to_ticker([symbol], self.handle_price_update)
                self.logger.info(f"Успешно подписались на {symbol}")
                print(f"Успешно подписались на {symbol}")
            except Exception as e:
                self.logger.error(f"Ошибка подписки на {symbol}: {e}")
                print(f"Ошибка подписки на {symbol}: {e}")

    def run(self):
        """Запуск получения цен в бесконечном цикле."""
        self.logger.info(
            f"Запуск получения цен для {len(self.symbols)} инструментов: {self.symbols}"
        )
        print(
            f"Запуск получения цен для {len(self.symbols)} инструментов: {self.symbols}"
        )

        # Инициализируем подписку
        self.subscribe_to_valid_symbols()

        if not self.valid_symbols:
            return

        # Бесконечный цикл с логированием цен
        try:
            self.logger.info("Начало мониторинга цен...")
            print("Начало мониторинга цен...")
            while self.running:
                prices_str = ", ".join(
                    f"{symbol}: {price}"
                    for symbol, price in self.current_prices.items()
                    if symbol in self.valid_symbols
                )
                self.logger.info(f"Текущие цены: {prices_str}")
                print(f"Текущие цены: {prices_str}")
                time.sleep(10)
        except KeyboardInterrupt:
            self.logger.info("Остановлено пользователем")
            print("Остановлено пользователем")
        except Exception as e:
            self.logger.error(f"Неожиданная ошибка в цикле: {e}")
            print(f"Неожиданная ошибка в цикле: {e}")
            self.reconnect()
            if self.running:
                time.sleep(5)

    def get_current_prices(self):
        """Метод для получения текущих цен."""
        return self.current_prices


if __name__ == "__main__":
    fetcher = PriceFetcher()
    fetcher.run()
