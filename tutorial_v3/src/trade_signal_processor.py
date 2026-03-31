import time
from datetime import datetime
from fetch_prices import PriceFetcher
from google_sheets import GoogleSheetsClient
from telegram_client import TelegramClient
from config import (
    GOOGLE_SHEETS_CREDENTIALS,
    GOOGLE_SHEETS_ID,
    LONG_ATR_THRESHOLD_PERCENT,
    SHORT_ATR_THRESHOLD_PERCENT,
    TELEGRAM_TOKEN,
    CHAT_ID,
)
from utils import LoggerSetup


class TradeSignalProcessor:
    def __init__(self, main, trading_monitor, price_fetcher):
        """Инициализация TradeSignalProcessor."""
        self.main = main
        self.logger = LoggerSetup.setup_logging("trade_signal_processor")
        self.logger.info("Инициализация TradeSignalProcessor")
        self.trading_monitor = trading_monitor
        self.sheets_client = GoogleSheetsClient(
            GOOGLE_SHEETS_CREDENTIALS, GOOGLE_SHEETS_ID
        )
        self.price_fetcher = price_fetcher
        self.telegram_client = TelegramClient(TELEGRAM_TOKEN, CHAT_ID)
        self.symbols_cache = {}  # Локальный кэш для данных монет
        self.cache_timeout = 10  # Таймаут кэша в секундах

    def get_symbol_data(self, sheet_name):
        """Получение данных о монетах из листа long или short с кэшированием."""
        cache_key = f"symbols_{sheet_name}"
        if cache_key in self.symbols_cache:
            data, timestamp = self.symbols_cache[cache_key]
            if (datetime.now() - timestamp).total_seconds() < self.cache_timeout:
                self.logger.debug(f"Использован кэш для символов листа {sheet_name}")
                return data

        sheet = self.sheets_client.get_sheet(sheet_name)
        if not sheet:
            self.logger.error(f"Не удалось получить лист {sheet_name}")
            return {}

        rows = self.sheets_client.get_all_data(sheet, sheet_name)
        if not rows:
            self.logger.error(f"Не удалось получить данные из листа {sheet_name}")
            return {}

        symbols_data = {}
        symbol_col, trade_enabled_col, level_col = 7, 4, 10  # H, E, K
        for row_idx, row in enumerate(rows[1:], start=2):
            if len(row) <= max(symbol_col, trade_enabled_col, level_col):
                self.logger.debug(f"Строка {row_idx} пропущена: недостаточно столбцов")
                continue

            symbol = row[symbol_col]
            trade_enabled = (
                row[trade_enabled_col].upper() == "TRUE"
                if row[trade_enabled_col]
                else False
            )
            try:
                level = (
                    float(row[level_col])
                    if row[level_col] and row[level_col] != "#N/A"
                    else None
                )
            except (ValueError, TypeError) as e:
                self.logger.warning(
                    f"Не удалось преобразовать уровень для {symbol} в строке {row_idx}: {e}"
                )
                level = None

            self.logger.debug(
                f"Строка {row_idx}: symbol={symbol}, trade_enabled={trade_enabled}, level={level}"
            )
            if symbol and trade_enabled and level is not None:
                symbols_data[symbol] = {"row_idx": row_idx, "level": level}

        self.symbols_cache[cache_key] = (symbols_data, datetime.now())
        self.logger.info(f"Данные символов для листа {sheet_name} закэшированы")
        return symbols_data

    def reset_trade_enabled(self, sheet_name):
        """Сброс столбца E (Торговля) в листе long или short для всех строк с E=TRUE."""
        sheet = self.sheets_client.get_sheet(sheet_name)
        if not sheet:
            self.logger.error(
                f"Не удалось получить лист {sheet_name} для сброса столбца E"
            )
            return

        rows = self.sheets_client.get_all_data(sheet, sheet_name)
        if not rows:
            self.logger.error(
                f"Не удалось получить данные из листа {sheet_name} для сброса столбца E"
            )
            return

        trade_enabled_col = 4  # E
        updates = []
        for row_idx, row in enumerate(rows[1:], start=2):
            if len(row) <= trade_enabled_col:
                continue
            trade_enabled = (
                row[trade_enabled_col].upper() == "TRUE"
                if row[trade_enabled_col]
                else False
            )
            if trade_enabled:
                updates.append(
                    {"sheet": sheet_name, "range": f"E{row_idx}", "values": [[""]]}
                )

        if updates:
            self.logger.info(
                f"Сброс столбца E для листа {sheet_name}: {len(updates)} строк"
            )
            try:
                self.sheets_client.batch_update(updates)
            except Exception as e:
                self.logger.error(
                    f"Ошибка при сбросе столбца E в листе {sheet_name}: {e}"
                )
        else:
            self.logger.info(f"Нет строк с E=TRUE для сброса в листе {sheet_name}")

    def get_atr(self, symbol):
        """Получение значения ATR для монеты из листа database."""
        sheet = self.sheets_client.get_sheet("database")
        if not sheet:
            self.logger.error(f"Не удалось получить лист database")
            return None

        rows = self.sheets_client.get_all_data(sheet, "database")
        if not rows:
            self.logger.error(f"Не удалось получить данные из листа database")
            return None

        atr_col, symbol_col = 15, 0  # P, A
        for row in rows[1:]:
            if len(row) <= max(symbol_col, atr_col):
                continue
            if row[symbol_col] == symbol:
                try:
                    atr = (
                        float(row[atr_col])
                        if row[atr_col] and row[atr_col] != "#N/A"
                        else None
                    )
                    self.logger.debug(f"ATR для {symbol}: {atr}")
                    return atr
                except (ValueError, TypeError) as e:
                    self.logger.warning(
                        f"Не удалось преобразовать ATR для {symbol}: {row[atr_col]}, ошибка: {e}"
                    )
                    return None
        self.logger.warning(f"ATR для {symbol} не найден в листе database")
        return None

    def process_signal(self, signal_type):
        """Обработка сигнала от TradingMonitor для входа в сделки или отмены сценария."""
        print(f"[{datetime.now()}] Получен сигнал: {signal_type}")
        self.logger.info(f"Получен сигнал: {signal_type}")

        if signal_type in ["CANCEL_LONG", "CANCEL_SHORT"]:
            sheet_name = "long" if signal_type == "CANCEL_LONG" else "short"
            self.logger.info(
                f"Обработка отмены сценария: {signal_type}, лист {sheet_name}"
            )
            self.reset_trade_enabled(sheet_name)
            self.main.stop()
            return

        sheet_name = "long" if signal_type == "LONG" else "short"
        symbols_data = self.get_symbol_data(sheet_name)
        symbols = list(symbols_data.keys())

        if not symbols:
            self.logger.info(f"Нет монет с E=TRUE на листе {sheet_name}")
            return

        self.logger.info(f"Монеты с E=TRUE на листе {sheet_name}: {symbols}")
        all_prices = self.price_fetcher.get_current_prices()
        current_prices = {symbol: all_prices.get(symbol, None) for symbol in symbols}
        self.logger.debug(f"Текущие цены: {current_prices}")

        updates = []
        entered_symbols = []
        for symbol in symbols_data:
            data = symbols_data[symbol]
            row_idx = data["row_idx"]
            level = data["level"]
            current_price = current_prices.get(symbol)
            if current_price is None:
                self.logger.warning(
                    f"Не удалось получить цену для {symbol}, пропускаем."
                )
                continue

            atr = self.get_atr(symbol)
            if atr is None:
                self.logger.warning(
                    f"Не удалось получить ATR для {symbol}, пропускаем."
                )
                continue

            criteria_met = False
            if signal_type == "LONG":
                threshold = LONG_ATR_THRESHOLD_PERCENT / 100 * atr
                criteria_met = current_price >= level - threshold
                self.logger.debug(
                    f"Проверка LONG для {symbol}: current_price={current_price}, level={level}, atr={atr}, threshold={threshold}, criteria_met={criteria_met}"
                )
            else:  # SHORT
                alerted = self.trading_monitor.get_alerted_status(symbol, "short")
                if not alerted:
                    threshold = SHORT_ATR_THRESHOLD_PERCENT / 100 * atr
                    criteria_met = current_price < level - threshold
                    self.logger.debug(
                        f"Проверка SHORT для {symbol}: current_price={current_price}, level={level}, atr={atr}, threshold={threshold}, criteria_met={criteria_met}, alerted={alerted}"
                    )
                else:
                    self.logger.debug(
                        f"Сброс criteria_met для {symbol}, так как alerted=True"
                    )

            if criteria_met:
                self.logger.info(
                    f"Монета {symbol} соответствует критериям для {signal_type}, проставляем вход."
                )
                updates.append(
                    {"sheet": sheet_name, "range": f"F{row_idx}", "values": [["TRUE"]]}
                )
                updates.append(
                    {"sheet": sheet_name, "range": f"E{row_idx}", "values": [[""]]}
                )
                entered_symbols.append(symbol)
            else:
                self.logger.info(
                    f"Монета {symbol} НЕ соответствует критериям для {signal_type}, сбрасываем торговлю."
                )
                updates.append(
                    {"sheet": sheet_name, "range": f"E{row_idx}", "values": [[""]]}
                )

        if updates:
            try:
                self.sheets_client.batch_update(updates)
            except Exception as e:
                self.logger.error(
                    f"Ошибка при пакетном обновлении в листе {sheet_name}: {e}"
                )

        if entered_symbols:
            self.telegram_client.send_message(
                f"Вход в сделку {signal_type}: {entered_symbols}"
            )
            self.logger.info(
                f"Отправлено оповещение: Вход в сделку {signal_type}: {entered_symbols}"
            )
            if signal_type == "LONG":
                self.main.allow_long_signals = False
                self.logger.info(
                    "Установлен allow_long_signals=False после входа в сделку LONG"
                )
            elif signal_type == "SHORT":
                self.main.allow_short_signals = False
                self.logger.info(
                    "Установлен allow_short_signals=False после входа в сделку SHORT"
                )
        else:
            self.logger.info(
                f"Нет монет, соответствующих критериям для входа в сделку {signal_type}"
            )

    def run(self):
        """Основной цикл ожидания сигналов."""
        print("TradeSignalProcessor запущен, ожидание сигналов...")
        while self.main.running:
            time.sleep(5)  # Увеличен интервал
