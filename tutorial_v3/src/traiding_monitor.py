import logging
import time
import os
from datetime import datetime, timedelta
from google_sheets import GoogleSheetsClient
from fetch_prices import PriceFetcher
from telegram_client import TelegramClient
from config import (
    GOOGLE_SHEETS_CREDENTIALS,
    GOOGLE_SHEETS_ID,
    ALERT_TIMEOUT_MINUTES,
    MIN_ALERTS_COUNT,
    MAX_ADDITIONAL_ALERTS,
    TELEGRAM_TOKEN,
    CHAT_ID,
)
import threading
from utils import LoggerSetup


class TradingMonitor:
    def __init__(self, main):
        self.main = main
        self.logger = LoggerSetup.setup_logging("trading_monitor", retention_days=3)
        self.logger.info("Инициализация TradingMonitor")
        self.google_sheets = GoogleSheetsClient(
            GOOGLE_SHEETS_CREDENTIALS, GOOGLE_SHEETS_ID
        )
        try:
            self.trading_coins = self.google_sheets.get_trading_coins()
        except Exception as e:
            self.logger.error(f"Ошибка при получении trading_coins: {e}")
            self.trading_coins = []
        self.levels = {
            coin["coin"]: {
                "long_level": coin["long_level"],
                "short_level": coin["short_level"],
            }
            for coin in self.trading_coins
        }
        self.logger.info(
            f"Получено {len(self.trading_coins)} монет для мониторинга: {[coin['coin'] for coin in self.trading_coins]}"
        )
        self.price_fetcher = PriceFetcher(main.bybit_api)
        self.telegram_client = TelegramClient(TELEGRAM_TOKEN, CHAT_ID)
        self.alerts_history = []
        self.prev_prices = {coin["coin"]: None for coin in self.trading_coins}
        self.alerted = {
            coin["coin"]: {"long": False, "short": False} for coin in self.trading_coins
        }
        self.last_alert_count = 0
        self.long_alerts = {}
        self.short_alerts = {}
        self.long_window_start = None
        self.short_window_start = None
        self.trade_signal_processor = None
        self.logger.info(f"ALERT_TIMEOUT_MINUTES: {ALERT_TIMEOUT_MINUTES}")
        self.logger.info(f"MIN_ALERTS_COUNT: {MIN_ALERTS_COUNT}")
        self.logger.info(f"MAX_ADDITIONAL_ALERTS: {MAX_ADDITIONAL_ALERTS}")

    def set_signal_processor(self, trade_signal_processor):
        self.trade_signal_processor = trade_signal_processor
        self.logger.info("TradeSignalProcessor установлен в TradingMonitor")

    def get_alerted_status(self, symbol, signal_type):
        return self.alerted[symbol][signal_type.lower()]

    def check_levels(self, symbol, current_price):
        levels = self.levels.get(symbol, {})
        long_level = levels.get("long_level")
        short_level = levels.get("short_level")
        env = os.getenv("ENV", "development")

        # Логируем детальные проверки только в режиме разработки
        if env == "development":
            self.logger.debug(
                f"Проверка уровней для {symbol}: current_price={current_price}, long_level={long_level}, short_level={short_level}, prev_price={self.prev_prices[symbol]}"
            )

        if self.prev_prices[symbol] is None:
            self.prev_prices[symbol] = current_price
            if env == "development":
                self.logger.debug(
                    f"Установлена начальная цена для {symbol}: {current_price}"
                )
            return

        timestamp = datetime.now()
        if (
            self.main.allow_long_signals
            and long_level is not None
            and not self.alerted[symbol]["long"]
        ):
            if self.prev_prices[symbol] > long_level and current_price <= long_level:
                alert_msg = f"Пересечение уровня LONG для {symbol}: цена {current_price} <= {long_level}"
                self.logger.info(alert_msg)
                self.alerts_history.append(
                    {
                        "symbol": symbol,
                        "type": "LONG",
                        "price": current_price,
                        "level": long_level,
                        "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                    }
                )
                self.alerted[symbol]["long"] = True
                self.telegram_client.send_message(alert_msg)
            # Убрано логирование "Условия для LONG не выполнены"
        elif (
            not self.main.allow_long_signals
            and long_level is not None
            and self.prev_prices[symbol] > long_level
            and current_price <= long_level
        ):
            self.logger.info(
                f"Пересечение уровня LONG для {symbol} игнорируется: allow_long_signals=False"
            )

        if (
            self.main.allow_short_signals
            and short_level is not None
            and not self.alerted[symbol]["short"]
        ):
            if self.prev_prices[symbol] < short_level and current_price >= short_level:
                alert_msg = f"Пересечение уровня SHORT для {symbol}: цена {current_price} >= {short_level}"
                self.logger.info(alert_msg)
                self.alerts_history.append(
                    {
                        "symbol": symbol,
                        "type": "SHORT",
                        "price": current_price,
                        "level": short_level,
                        "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                    }
                )
                self.alerted[symbol]["short"] = True
                self.telegram_client.send_message(alert_msg)
            # Убрано логирование "Условия для SHORT не выполнены"
        elif (
            not self.main.allow_short_signals
            and short_level is not None
            and self.prev_prices[symbol] < short_level
            and current_price >= short_level
        ):
            self.logger.info(
                f"Пересечение уровня SHORT для {symbol} игнорируется: allow_short_signals=False"
            )

        self.prev_prices[symbol] = current_price

    def process_alerts(self):
        # Убрано логирование "Начало process_alerts"
        current_alert_count = len(self.alerts_history)
        if current_alert_count > self.last_alert_count:
            new_alerts = self.alerts_history[
                self.last_alert_count : current_alert_count
            ]
            self.logger.info(f"Обнаружено {len(new_alerts)} новых алертов")
            for alert in new_alerts:
                symbol = alert["symbol"]
                alert_type = alert["type"]
                timestamp = datetime.strptime(alert["timestamp"], "%Y-%m-%d %H:%M:%S")
                if alert_type == "LONG":
                    if symbol not in self.long_alerts:
                        self.long_alerts[symbol] = []
                    self.long_alerts[symbol].append(timestamp)
                    if (
                        len(self.long_alerts) == MIN_ALERTS_COUNT
                        and self.long_window_start is None
                    ):
                        self.long_window_start = timestamp
                        # Убрано логирование начала окна
                elif alert_type == "SHORT":
                    if symbol not in self.short_alerts:
                        self.short_alerts[symbol] = []
                    self.short_alerts[symbol].append(timestamp)
                    if (
                        len(self.short_alerts) == MIN_ALERTS_COUNT
                        and self.short_window_start is None
                    ):
                        self.short_window_start = timestamp
                        # Убрано логирование начала окна
            self.last_alert_count = current_alert_count

    def check_entry_conditions(self):
        # Убрано логирование "Начало check_entry_conditions"
        current_time = datetime.now()
        env = os.getenv("ENV", "development")

        if self.long_window_start:
            long_count = len(self.long_alerts)
            time_diff = (current_time - self.long_window_start).total_seconds() / 60
            if long_count >= MIN_ALERTS_COUNT and time_diff >= ALERT_TIMEOUT_MINUTES:
                self.telegram_client.send_message("Сигнал на вход в сделку LONG")
                self.logger.info("Отправлено оповещение: Сигнал на вход в сделку LONG")
                if self.trade_signal_processor:
                    self.trade_signal_processor.process_signal("LONG")
                self.long_alerts.clear()
                self.long_window_start = None
            elif env == "development":
                # Логируем условия входа только в разработке
                self.logger.debug(
                    f"Условия для входа LONG не выполнены: count={long_count}/{MIN_ALERTS_COUNT}, time_diff={time_diff:.2f}/{ALERT_TIMEOUT_MINUTES}"
                )

        if self.short_window_start:
            short_count = len(self.short_alerts)
            time_diff = (current_time - self.short_window_start).total_seconds() / 60
            if short_count >= MIN_ALERTS_COUNT and time_diff >= ALERT_TIMEOUT_MINUTES:
                self.telegram_client.send_message("Сигнал на вход в сделку SHORT")
                self.logger.info("Отправлено оповещение: Сигнал на вход в сделку SHORT")
                if self.trade_signal_processor:
                    self.trade_signal_processor.process_signal("SHORT")
                self.short_alerts.clear()
                self.short_window_start = None
            elif env == "development":
                # Логируем условия входа только в разработке
                self.logger.debug(
                    f"Условия для входа SHORT не выполнены: count={short_count}/{MIN_ALERTS_COUNT}, time_diff={time_diff:.2f}/{ALERT_TIMEOUT_MINUTES}"
                )

    def check_cancellation_conditions(self):
        # Убрано логирование "Начало check_cancellation_conditions"
        current_time = datetime.now()
        if self.long_window_start:
            time_diff = (current_time - self.long_window_start).total_seconds() / 60
            long_count = len(self.long_alerts)
            if time_diff <= ALERT_TIMEOUT_MINUTES and long_count >= MIN_ALERTS_COUNT:
                other_long_count = long_count - MIN_ALERTS_COUNT
                if other_long_count >= MAX_ADDITIONAL_ALERTS:
                    self.telegram_client.send_message("Отмена сценария LONG")
                    self.logger.info("Отправлено оповещение: Отмена сценария LONG")
                    if self.trade_signal_processor:
                        self.trade_signal_processor.process_signal("CANCEL_LONG")
                    self.long_alerts.clear()
                    self.long_window_start = None

        if self.short_window_start:
            time_diff = (current_time - self.short_window_start).total_seconds() / 60
            short_count = len(self.short_alerts)
            if time_diff <= ALERT_TIMEOUT_MINUTES and short_count >= MIN_ALERTS_COUNT:
                other_short_count = short_count - MIN_ALERTS_COUNT
                if other_short_count >= MAX_ADDITIONAL_ALERTS:
                    self.telegram_client.send_message("Отмена сценария SHORT")
                    self.logger.info("Отправлено оповещение: Отмена сценария SHORT")
                    if self.trade_signal_processor:
                        self.trade_signal_processor.process_signal("CANCEL_SHORT")
                    self.short_alerts.clear()
                    self.short_window_start = None

    def run(self):
        self.logger.info("Запуск TradingMonitor...")
        fetcher_thread = threading.Thread(target=self.price_fetcher.run)
        fetcher_thread.daemon = True
        fetcher_thread.start()
        try:
            while self.main.running:
                current_prices = self.price_fetcher.get_current_prices()
                for symbol, price in current_prices.items():
                    if price > 0:
                        self.check_levels(symbol, price)
                self.process_alerts()
                self.check_entry_conditions()
                self.check_cancellation_conditions()
                time.sleep(10)  # Увеличен интервал до 10 секунд
        except KeyboardInterrupt:
            self.logger.info("Остановлено пользователем")
            self.main.running = False
            self.price_fetcher.running = False
        except Exception as e:
            self.logger.error(f"Неожиданная ошибка в цикле: {e}")
            self.main.running = False
            self.price_fetcher.running = False


if __name__ == "__main__":

    class MainStub:
        def __init__(self):
            self.running = True
            self.allow_long_signals = True
            self.allow_short_signals = True
            self.bybit_api = BybitAPI()

    monitor = TradingMonitor(MainStub())
    monitor.run()
