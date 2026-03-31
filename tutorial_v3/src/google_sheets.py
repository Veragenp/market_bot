import gspread
from googleapiclient.errors import HttpError
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
import logging
import os
import time
from datetime import datetime, timedelta
from utils import LoggerSetup

try:
    from config import GOOGLE_SHEETS_CREDENTIALS, GOOGLE_SHEETS_ID
except ImportError as e:
    print(f"Ошибка импорта из config.py: {str(e)}")
    raise


class GoogleSheetsClient:
    """Синглтон-класс для работы с Google Sheets с оптимизацией запросов."""

    _instance = None

    def __new__(cls, credentials_file, spreadsheet_id):
        """Реализация паттерна Singleton и инициализация."""
        if cls._instance is None:
            cls._instance = super(GoogleSheetsClient, cls).__new__(cls)
            cls._instance.logger = LoggerSetup.setup_logging("google_sheets")
            cls._instance.logger.info("Инициализация GoogleSheetsClient (синглтон)")

            scope = [
                "https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/drive",
            ]
            try:
                creds = Credentials.from_service_account_file(
                    credentials_file, scopes=scope
                )
                cls._instance.client = gspread.authorize(creds)
                cls._instance.spreadsheet = cls._instance.client.open_by_key(
                    spreadsheet_id
                )
                cls._instance.logger.info(
                    f"Подключение к таблице с ID: {spreadsheet_id}"
                )
                # Инициализация кэша и счетчика запросов
                cls._instance.cache = {}
                cls._instance.cache_timeout = timedelta(seconds=30)  # Кэш на 30 секунд
                cls._instance.request_count = 0
                cls._instance.last_reset = datetime.now()
            except Exception as e:
                cls._instance.logger.error(
                    f"Ошибка при инициализации клиента Google Sheets: {e}"
                )
                raise
        return cls._instance

    def _log_request_count(self):
        """Логирует количество запросов за последнюю минуту."""
        if datetime.now() - self.last_reset > timedelta(minutes=1):
            self.logger.info(
                f"Google Sheets API requests last minute: {self.request_count}"
            )
            self.request_count = 0
            self.last_reset = datetime.now()

    def get_sheet(self, sheet_name, retries=5, backoff_factor=1):
        """Получает указанный лист из таблицы с кэшированием и повторными попытками."""
        self._log_request_count()
        cache_key = f"worksheet_{sheet_name}"
        if cache_key in self.cache:
            data, timestamp = self.cache[cache_key]
            if datetime.now() - timestamp < self.cache_timeout:
                return data

        for attempt in range(retries):
            try:
                self.request_count += 1
                worksheet = self.spreadsheet.worksheet(sheet_name)
                self.cache[cache_key] = (worksheet, datetime.now())
                self.logger.info(f"Лист {sheet_name} получен и закэширован")
                return worksheet
            except gspread.exceptions.APIError as e:
                if e.response.status_code == 429:
                    sleep_time = backoff_factor * (2**attempt)
                    self.logger.warning(
                        f"Quota exceeded for {sheet_name}, retrying in {sleep_time}s..."
                    )
                    time.sleep(sleep_time)
                else:
                    self.logger.error(f"Ошибка при получении листа {sheet_name}: {e}")
                    return None
            except gspread.exceptions.WorksheetNotFound:
                self.logger.error(f"Лист {sheet_name} не найден")
                return None
            except Exception as e:
                self.logger.error(f"Ошибка при получении листа {sheet_name}: {e}")
                return None
        self.logger.error(
            f"Не удалось получить лист {sheet_name} после {retries} попыток"
        )
        return None

    def add_worksheet(self, sheet_name, rows, cols):
        """Создает новый лист в таблице."""
        try:
            self.request_count += 1
            worksheet = self.spreadsheet.add_worksheet(
                title=sheet_name, rows=rows, cols=cols
            )
            self.logger.info(f"Лист {sheet_name} создан с размерами {rows}x{cols}")
            return worksheet
        except Exception as e:
            self.logger.error(f"Ошибка при создании листа {sheet_name}: {e}")
            raise

    def append_rows(self, sheet_name, values, retries=5, backoff_factor=1):
        """Добавляет несколько строк в конец листа."""
        self._log_request_count()
        sheet = self.get_sheet(sheet_name)
        if not sheet:
            self.logger.error(
                f"Не удалось получить лист {sheet_name} для добавления строк"
            )
            raise Exception(f"Не удалось получить лист {sheet_name}")

        for attempt in range(retries):
            try:
                self.request_count += 1
                sheet.append_rows(values)
                self.logger.info(f"Добавлено {len(values)} строк в лист {sheet_name}")
                return
            except gspread.exceptions.APIError as e:
                if e.response.status_code == 429:
                    sleep_time = backoff_factor * (2**attempt)
                    self.logger.warning(
                        f"Quota exceeded for append_rows in {sheet_name}, retrying in {sleep_time}s..."
                    )
                    time.sleep(sleep_time)
                else:
                    self.logger.error(
                        f"Ошибка при добавлении строк в лист {sheet_name}: {e}"
                    )
                    raise
            except Exception as e:
                self.logger.error(
                    f"Ошибка при добавлении строк в лист {sheet_name}: {e}"
                )
                raise
        self.logger.error(
            f"Не удалось добавить строки в лист {sheet_name} после {retries} попыток"
        )
        raise Exception(f"Не удалось добавить строки в лист {sheet_name}")

    def get_row_count(self, sheet_name):
        """Возвращает общее количество строк в листе."""
        sheet = self.get_sheet(sheet_name)
        if not sheet:
            self.logger.error(
                f"Не удалось получить лист {sheet_name} для подсчета строк"
            )
            return 0

        try:
            self.request_count += 1
            all_values = sheet.get_all_values()
            row_count = len(all_values)
            self.logger.info(f"В листе {sheet_name} {row_count} строк")
            return row_count
        except Exception as e:
            self.logger.error(f"Ошибка при подсчете строк в листе {sheet_name}: {e}")
            return 0

    def clear_data_except_headers(self, sheet_name, retries=5, backoff_factor=1):
        """Очищает данные в листе, оставляя только заголовки (первую строку)."""
        self._log_request_count()
        sheet = self.get_sheet(sheet_name)
        if not sheet:
            self.logger.error(f"Не удалось получить лист {sheet_name} для очистки")
            raise Exception(f"Не удалось получить лист {sheet_name}")

        try:
            # Получаем все данные
            all_values = sheet.get_all_values()
            if not all_values:
                self.logger.warning(f"Лист {sheet_name} пуст, очистка не требуется")
                return

            # Сохраняем заголовки (первая строка)
            headers = all_values[0]
            for attempt in range(retries):
                try:
                    self.request_count += 1
                    # Обновляем лист, оставляя только заголовки
                    sheet.update("A1:L1", [headers])
                    # Очищаем остальные строки (предполагаем максимум 1000 строк для очистки)
                    sheet.update("A2:L1000", [[""] * 12] * 999)
                    self.logger.info(
                        f"Данные в листе {sheet_name} очищены, заголовки сохранены"
                    )
                    return
                except gspread.exceptions.APIError as e:
                    if e.response.status_code == 429:
                        sleep_time = backoff_factor * (2**attempt)
                        self.logger.warning(
                            f"Quota exceeded for clear_data_except_headers in {sheet_name}, retrying in {sleep_time}s..."
                        )
                        time.sleep(sleep_time)
                    else:
                        self.logger.error(f"Ошибка при очистке листа {sheet_name}: {e}")
                        raise
                except Exception as e:
                    self.logger.error(f"Ошибка при очистке листа {sheet_name}: {e}")
                    raise
            self.logger.error(
                f"Не удалось очистить лист {sheet_name} после {retries} попыток"
            )
            raise Exception(f"Не удалось очистить лист {sheet_name}")
        except Exception as e:
            self.logger.error(f"Ошибка при очистке листа {sheet_name}: {e}")
            raise

    def list_sheets(self):
        """Возвращает список всех листов в таблице."""
        cache_key = "list_sheets"
        if cache_key in self.cache:
            data, timestamp = self.cache[cache_key]
            if datetime.now() - timestamp < self.cache_timeout:
                return data

        try:
            self.request_count += 1
            sheets = [worksheet.title for worksheet in self.spreadsheet.worksheets()]
            self.cache[cache_key] = (sheets, datetime.now())
            self.logger.info(f"Доступные листы: {sheets}")
            return sheets
        except Exception as e:
            self.logger.error(f"Ошибка при получении списка листов: {e}")
            return []

    def update_cell(self, sheet, row, col, value):
        """Обновляет значение в указанной ячейке."""
        try:
            self.request_count += 1
            sheet.update_cell(row, col, value)
            self.logger.info(
                f"Обновлена ячейка: строка {row}, столбец {col}, значение {value}"
            )
        except gspread.exceptions.APIError as e:
            if e.response.status_code == 429:
                self.logger.warning(f"Quota exceeded during update_cell, retrying...")
                time.sleep(2)  # Простая задержка, можно заменить на экспоненциальную
                self.update_cell(sheet, row, col, value)
            else:
                self.logger.error(
                    f"Ошибка при обновлении ячейки: строка {row}, столбец {col}, значение {value}: {e}"
                )
                raise
        except Exception as e:
            self.logger.error(
                f"Ошибка при обновлении ячейки: строка {row}, столбец {col}, значение {value}: {e}"
            )
            raise

    def get_all_data(self, sheet, sheet_name):
        """Получает все данные из указанного листа с кэшированием."""
        cache_key = f"all_data_{sheet_name}"
        if cache_key in self.cache:
            data, timestamp = self.cache[cache_key]
            if datetime.now() - timestamp < self.cache_timeout:
                return data

        try:
            self.request_count += 1
            data = sheet.get_all_values()
            self.cache[cache_key] = (data, datetime.now())
            self.logger.info(
                f"Все данные из листа {sheet_name} получены и закэшированы"
            )
            return data
        except gspread.exceptions.APIError as e:
            if e.response.status_code == 429:
                self.logger.warning(f"Quota exceeded during get_all_data, retrying...")
                time.sleep(2)
                return self.get_all_data(sheet, sheet_name)
            else:
                self.logger.error(
                    f"Ошибка при чтении данных из листа {sheet_name}: {e}"
                )
                return []
        except Exception as e:
            self.logger.error(f"Ошибка при чтении данных из листа {sheet_name}: {e}")
            return []

    def batch_update(self, updates, retries=5, backoff_factor=1):
        """Пакетное обновление ячеек с повторными попытками."""
        self._log_request_count()
        self.logger.info(f"Пакетное обновление: {len(updates)} изменений")
        attempt = 0
        while attempt < retries:
            try:
                self.request_count += 1
                batch_requests = []
                for update in updates:
                    sheet_name = update["sheet"]
                    sheet = self.get_sheet(sheet_name)
                    if not sheet:
                        self.logger.error(
                            f"Не удалось получить лист {sheet_name} для обновления"
                        )
                        continue
                    range_name = f"{sheet_name}!{update['range']}"
                    batch_requests.append(
                        {"range": range_name, "values": update["values"]}
                    )
                if batch_requests:
                    self.spreadsheet.values_batch_update(
                        {"valueInputOption": "RAW", "data": batch_requests}
                    )
                self.logger.info(
                    f"Пакетное обновление выполнено: {len(batch_requests)} изменений"
                )
                return
            except gspread.exceptions.APIError as e:
                if e.response.status_code in [429, 503]:
                    attempt += 1
                    if attempt == retries:
                        self.logger.error(
                            f"Не удалось выполнить пакетное обновление после {retries} попыток: {e}"
                        )
                        raise
                    delay = backoff_factor * (2**attempt)
                    self.logger.warning(
                        f"APIError {e.response.status_code}: Повторная попытка {attempt}/{retries} через {delay} секунд"
                    )
                    time.sleep(delay)
                else:
                    self.logger.error(f"Ошибка при пакетном обновлении: {e}")
                    raise
            except Exception as e:
                self.logger.error(f"Ошибка при пакетном обновлении: {e}")
                raise

    def get_trading_coins(self):
        """Получает список монет для торговли из листа 'analitics'."""
        cache_key = "trading_coins"
        if cache_key in self.cache:
            data, timestamp = self.cache[cache_key]
            if datetime.now() - timestamp < self.cache_timeout:
                return data

        self.logger.info("Начало выполнения get_trading_coins")
        sheet = self.get_sheet("analitics")
        if not sheet:
            self.logger.error("Не удалось получить лист analitics")
            return []

        try:
            data = self.get_all_data(sheet, "analitics")
            if not data:
                self.logger.warning("Лист analitics пуст")
                return []

            valid_rows = [
                i + 2
                for i, row in enumerate(data[1:])
                if len(row) > 3 and row[3].strip().upper() in ["TRUE", "TRU"]
            ]
            self.logger.info(
                f"Найдено строк с TRUE: {len(valid_rows)} на индексах: {valid_rows}"
            )

            trading_coins = []
            for row_idx in valid_rows:
                row_data = data[row_idx - 1] if row_idx - 1 < len(data) else []
                if len(row_data) > 12:
                    coin = row_data[6]
                    long_level = row_data[9]
                    short_level = row_data[12]
                    try:
                        long_level = (
                            float(long_level)
                            if long_level and long_level != "#N/A"
                            else None
                        )
                        short_level = (
                            float(short_level)
                            if short_level and short_level != "#N/A"
                            else None
                        )
                    except ValueError:
                        self.logger.warning(
                            f"Невозможно преобразовать уровни для монеты {coin} в числа"
                        )
                        continue
                    trading_coins.append(
                        {
                            "coin": coin,
                            "long_level": long_level,
                            "short_level": short_level,
                        }
                    )
                    self.logger.info(
                        f"Добавлена монета: {coin}, long_level: {long_level}, short_level: {short_level}"
                    )
            self.cache[cache_key] = (trading_coins, datetime.now())
            self.logger.info(f"Найдено {len(trading_coins)} монет для мониторинга")
            return trading_coins
        except Exception as e:
            self.logger.error(f"Ошибка при получении списка монет: {e}")
            return []

    def get_pending_trades(self):
        """Получает список сделок для входа с вкладок long и short."""
        cache_key = "pending_trades"
        if cache_key in self.cache:
            data, timestamp = self.cache[cache_key]
            if datetime.now() - timestamp < self.cache_timeout:
                return data

        self.logger.info("Начало выполнения get_pending_trades")
        pending_trades = []

        for sheet_name in ["long", "short"]:
            sheet = self.get_sheet(sheet_name)
            if not sheet:
                self.logger.error(f"Не удалось получить лист {sheet_name}")
                continue

            try:
                data = self.get_all_data(sheet, sheet_name)
                if not data:
                    self.logger.warning(f"Лист {sheet_name} пуст")
                    continue

                for idx, row in enumerate(data[1:], start=2):
                    if len(row) < 28:
                        continue
                    trade_entry = row[5].strip().upper() if len(row) > 5 else ""
                    if trade_entry != "TRUE":
                        continue

                    status = row[6] if len(row) > 6 else ""
                    if status.strip() in ["вход, ожидание", "отменено: лимит сделок"]:
                        self.logger.debug(f"Строка {idx} пропущена: статус '{status}'")
                        continue

                    coin = row[7] if len(row) > 7 else None
                    entry_price = row[24] if len(row) > 24 else None
                    qty = row[25] if len(row) > 25 else None
                    take_profit = row[26] if len(row) > 26 else None
                    stop_loss = row[27] if len(row) > 27 else None

                    self.logger.debug(
                        f"Строка {idx} данные: coin={coin}, entry_price={entry_price}, qty={qty}, take_profit={take_profit}, stop_loss={stop_loss}"
                    )

                    try:
                        entry_price = (
                            float(entry_price)
                            if entry_price and entry_price != "#N/A"
                            else None
                        )
                        qty = float(qty) if qty and qty != "#N/A" else None
                        take_profit = (
                            float(take_profit)
                            if take_profit and take_profit != "#N/A"
                            else None
                        )
                        stop_loss = (
                            float(stop_loss)
                            if stop_loss and stop_loss != "#N/A"
                            else None
                        )
                    except ValueError as e:
                        self.logger.error(
                            f"Ошибка преобразования данных в строке {idx}: {e}"
                        )
                        continue

                    if not all([coin, entry_price, qty, stop_loss]):
                        self.logger.warning(
                            f"Пропущены обязательные параметры в строке {idx} листа {sheet_name}"
                        )
                        continue

                    pending_trades.append(
                        {
                            "sheet": sheet_name,
                            "row": idx,
                            "coin": coin,
                            "entry_price": entry_price,
                            "qty": qty,
                            "take_profit": take_profit,
                            "stop_loss": stop_loss,
                            "side": "Buy" if sheet_name.lower() == "long" else "Sell",
                        }
                    )
                    self.logger.info(
                        f"Добавлена сделка для обработки: {sheet_name}, строка {idx}, монета {coin}"
                    )
            except Exception as e:
                self.logger.error(f"Ошибка при обработке листа {sheet_name}: {e}")
                continue

        self.cache[cache_key] = (pending_trades, datetime.now())
        self.logger.info(f"Найдено {len(pending_trades)} сделок для входа")
        return pending_trades

    def update_trade_status(self, sheet_name, row, status):
        """Обновляет статус сделки в столбце G."""
        sheet = self.get_sheet(sheet_name)
        if not sheet:
            self.logger.error(
                f"Не удалось получить лист {sheet_name} для обновления статуса"
            )
            raise Exception(f"Не удалось получить лист {sheet_name}")

        try:
            self.request_count += 1
            sheet.update_cell(row, 7, status)
            self.logger.info(
                f"Статус сделки обновлен: лист {sheet_name}, строка {row}, статус {status}"
            )
            # Инвалидировать кэш pending_trades после обновления
            self.cache.pop("pending_trades", None)
        except gspread.exceptions.APIError as e:
            if e.response.status_code == 429:
                self.logger.warning(
                    f"Quota exceeded during update_trade_status, retrying..."
                )
                time.sleep(2)
                self.update_trade_status(sheet_name, row, status)
            else:
                self.logger.error(
                    f"Ошибка при обновлении статуса в листе {sheet_name}, строка {row}: {e}"
                )
                raise
        except Exception as e:
            self.logger.error(
                f"Ошибка при обновлении статуса в листе {sheet_name}, строка {row}: {e}"
            )
            raise

    def cancel_trade(self, sheet_name, row):
        """Отменяет сделку (сбрасывает F)."""
        sheet = self.get_sheet(sheet_name)
        if not sheet:
            self.logger.error(
                f"Не удалось получить лист {sheet_name} для отмены сделки"
            )
            raise Exception(f"Не удалось получить лист {sheet_name}")

        try:
            self.request_count += 1
            sheet.update_cell(row, 6, "")
            self.logger.info(f"Сделка отменена: лист {sheet_name}, строка {row}")
            # Инвалидировать кэш pending_trades после обновления
            self.cache.pop("pending_trades", None)
        except gspread.exceptions.APIError as e:
            if e.response.status_code == 429:
                self.logger.warning(f"Quota exceeded during cancel_trade, retrying...")
                time.sleep(2)
                self.cancel_trade(sheet_name, row)
            else:
                self.logger.error(
                    f"Ошибка при отмене сделки в листе {sheet_name}, строка {row}: {e}"
                )
                raise
        except Exception as e:
            self.logger.error(
                f"Ошибка при отмене сделки в листе {sheet_name}, строка {row}: {e}"
            )
            raise


if __name__ == "__main__":
    print("Запуск тестового скрипта...")
    try:
        client = GoogleSheetsClient(GOOGLE_SHEETS_CREDENTIALS, GOOGLE_SHEETS_ID)
        print("Клиент инициализирован, получение данных...")

        sheets = client.list_sheets()
        print(f"Доступные листы в таблице: {sheets}")

        coins = client.get_trading_coins()
        print(f"Найдено монет: {len(coins)}")
        for coin in coins:
            print(coin)

        trades = client.get_pending_trades()
        print(f"Ожидающие сделки: {trades}")

    except Exception as e:
        print(f"Произошла ошибка: {e}")
        logging.error(f"Ошибка при выполнении: {e}")
