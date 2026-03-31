import time
from datetime import datetime, timedelta
from googleapiclient.errors import HttpError
from bybit_api import BybitAPI
from google_sheets import GoogleSheetsClient
from telegram_client import TelegramClient
from utils import LoggerSetup


class StatisticsCollector:
    def __init__(
        self,
        bybit_api: BybitAPI,
        sheets_client: GoogleSheetsClient,
        telegram_client: TelegramClient,
    ):
        """Инициализация сборщика статистики."""
        self.logger = LoggerSetup.setup_logging("statistics_collector")
        self.bybit = bybit_api
        self.sheets = sheets_client
        self.telegram = telegram_client
        self.logger.info("Инициализация StatisticsCollector")

    def fetch_all_closed_pnl(self, start_timestamp, end_timestamp):
        """Извлекает все закрытые P&L за период с пагинацией."""
        all_positions = []
        cursor = None
        max_attempts = 3

        while True:
            for attempt in range(max_attempts):
                try:
                    response = self.bybit.session.get_closed_pnl(
                        category="linear",
                        start=start_timestamp,
                        end=end_timestamp,
                        limit=200,
                        cursor=cursor,
                    )
                    if response["retCode"] == 0:
                        result = response["result"]
                        positions = result.get("list", [])
                        all_positions.extend(positions)
                        cursor = result.get("nextPageCursor")
                        self.logger.info(
                            f"Получено {len(positions)} позиций, cursor: {cursor}"
                        )
                        # Логируем наличие execId для отладки
                        if positions:
                            sample_position = positions[0]
                            self.logger.debug(f"Пример ответа API: {sample_position}")
                            self.logger.debug(
                                f"Поле execId доступно: {'execId' in sample_position}"
                            )
                            self.logger.debug(
                                f"Поле orderId доступно: {'orderId' in sample_position}"
                            )
                        break
                    elif response["retCode"] == 10001:
                        self.logger.warning(
                            f"Попытка {attempt + 1}/{max_attempts}: Превышен лимит запросов API, пауза 10 секунд"
                        )
                        time.sleep(10)
                        continue
                    else:
                        self.logger.error(f"Ошибка API: {response['retMsg']}")
                        return []
                except Exception as e:
                    self.logger.error(
                        f"Попытка {attempt + 1}/{max_attempts}: Ошибка при запросе закрытых P&L: {e}"
                    )
                    if attempt < max_attempts - 1:
                        time.sleep(10)
                    else:
                        self.logger.critical(
                            "Не удалось получить закрытые P&L после всех попыток"
                        )
                        return []

            if not cursor:  # Если нет следующей страницы, выходим
                break

        self.logger.info(f"Всего получено {len(all_positions)} закрытых позиций")
        return all_positions

    def get_last_update_time(self, sheet_name="Statistic"):
        """Получает дату и время последнего обновления из листа Statistic, ячейка M1."""
        try:
            sheet = self.sheets.get_sheet(sheet_name)
            if not sheet:
                self.logger.error(f"Лист {sheet_name} не найден")
                return None

            last_update = sheet.get("M1")
            if last_update and last_update[0][0]:
                try:
                    last_update_time = datetime.strptime(
                        last_update[0][0], "%Y-%m-%d %H:%M:%S"
                    )
                    self.logger.info(
                        f"Последнее обновление: {last_update_time.strftime('%Y-%m-%d %H:%M:%S')}"
                    )
                    return last_update_time
                except ValueError:
                    self.logger.warning(
                        f"Неверный формат даты в ячейке M1 листа {sheet_name}: {last_update[0][0]}"
                    )
                    return None
            else:
                self.logger.info(f"Дата последнего обновления в ячейке M1 не найдена")
                return None
        except Exception as e:
            self.logger.error(
                f"Ошибка при получении даты последнего обновления из листа {sheet_name}: {e}"
            )
            return None

    def set_last_update_time(self, sheet_name="Statistic", update_time=None):
        """Записывает текущую дату и время как последнее обновление в ячейку M1."""
        if update_time is None:
            update_time = datetime.now()
        try:
            sheet = self.sheets.get_sheet(sheet_name)
            if not sheet:
                self.logger.error(f"Лист {sheet_name} не найден")
                return False

            update_time_str = update_time.strftime("%Y-%m-%d %H:%M:%S")
            sheet.update("M1", [[update_time_str]])
            self.logger.info(
                f"Обновлена дата последнего обновления в ячейке M1: {update_time_str}"
            )
            return True
        except Exception as e:
            self.logger.error(
                f"Ошибка при записи даты последнего обновления в лист {sheet_name}: {e}"
            )
            self.telegram.send_message(
                f"Ошибка при записи даты последнего обновления в лист {sheet_name}: {e}"
            )
            return False

    def clean_duplicates(self, sheet_name="Statistic"):
        """Удаляет дубликаты из листа Statistic по временным меткам."""
        try:
            sheet = self.sheets.get_sheet(sheet_name)
            if not sheet:
                self.logger.error(f"Лист {sheet_name} не найден")
                return False

            # Очищаем кэш перед получением данных
            self.sheets.cache.pop(f"worksheet_{sheet_name}", None)
            self.sheets.cache.pop(f"all_data_{sheet_name}", None)

            data = sheet.get_all_values()[1:]  # Пропускаем заголовки
            unique_rows = []
            seen_timestamps = set()

            for row in data:
                timestamp = row[11].strip() if len(row) > 11 and row[11] else ""
                if timestamp and timestamp not in seen_timestamps:
                    unique_rows.append(row)
                    seen_timestamps.add(timestamp)
                else:
                    self.logger.debug(f"Обнаружен дубликат: {timestamp}")

            # Перезаписываем лист уникальными строками
            headers = sheet.get_all_values()[0]
            sheet.clear()
            sheet.update("A1", [headers] + unique_rows)
            self.logger.info(
                f"Удалено {len(data) - len(unique_rows)} дубликатов из листа {sheet_name}"
            )
            self.telegram.send_message(
                f"Удалено {len(data) - len(unique_rows)} дубликатов из листа {sheet_name}"
            )
            return True
        except Exception as e:
            self.logger.error(
                f"Ошибка при очистке дубликатов в листе {sheet_name}: {e}"
            )
            self.telegram.send_message(
                f"Ошибка при очистке дубликатов в листе {sheet_name}: {e}"
            )
            return False

    def extend_sheet_if_needed(self, sheet_name, required_rows):
        """Расширяет лист до необходимого количества строк, если текущий размер недостаточен."""
        try:
            sheet = self.sheets.get_sheet(sheet_name)
            if not sheet:
                self.logger.error(f"Лист {sheet_name} не найден")
                return None

            # Получаем актуальное количество строк в листе
            current_rows = len(sheet.get_all_values())
            self.logger.info(f"Текущий размер листа {sheet_name}: {current_rows} строк")

            if current_rows < required_rows:
                additional_rows = required_rows - current_rows + 100  # Добавляем запас
                sheet.add_rows(additional_rows)
                self.logger.info(
                    f"Лист {sheet_name} расширен на {additional_rows} строк, новый размер: {current_rows + additional_rows}"
                )
                # Обновляем объект sheet после расширения
                sheet = self.sheets.get_sheet(sheet_name)
            return sheet
        except Exception as e:
            self.logger.error(f"Ошибка при расширении листа {sheet_name}: {e}")
            self.telegram.send_message(f"Ошибка при расширении листа {sheet_name}: {e}")
            return None

    def process_from_last_update(self):
        """Обрабатывает данные с момента последнего обновления."""
        sheet_name = "Statistic"
        end_time = datetime.now()
        last_update_time = self.get_last_update_time(sheet_name)

        if last_update_time is None:
            start_time = end_time - timedelta(days=7)
            self.logger.info(
                "Дата последнего обновления не найдена, использую период 7 дней"
            )
        else:
            start_time = last_update_time + timedelta(seconds=1)

        start_timestamp = int(start_time.timestamp() * 1000)
        end_timestamp = int(end_time.timestamp() * 1000)

        self.logger.info(f"Обработка периода с {start_time} по {end_time}")

        # Извлечь все закрытые P&L
        closed_positions = self.fetch_all_closed_pnl(start_timestamp, end_timestamp)

        if not closed_positions:
            self.logger.info("Новых сделок с последнего обновления не найдено")
            self.telegram.send_message(
                "Новых сделок с последнего обновления не найдено"
            )
            self.set_last_update_time(sheet_name, end_time)
            return True

        # Получаем существующие временные метки из Google Sheets
        sheet = self.sheets.get_sheet(sheet_name)
        if not sheet:
            self.logger.error(f"Лист {sheet_name} не найден")
            return False

        try:
            # Очищаем кэш перед получением данных
            self.sheets.cache.pop(f"worksheet_{sheet_name}", None)
            self.sheets.cache.pop(f"all_data_{sheet_name}", None)
            existing_data = sheet.get_all_values()[1:]  # Пропускаем заголовки
            existing_timestamps = {
                row[11].strip() for row in existing_data if len(row) > 11 and row[11]
            }
            self.logger.info(
                f"Найдено {len(existing_timestamps)} существующих временных меток"
            )
            self.logger.debug(f"Существующие временные метки: {existing_timestamps}")
        except Exception as e:
            self.logger.error(f"Ошибка при получении существующих данных: {e}")
            return False

        table_rows = []
        closed_positions.sort(key=lambda x: int(x.get("updatedTime", "0")))

        for position in closed_positions:
            updated_time_ms = int(position.get("updatedTime", "0"))
            updated_time = datetime.fromtimestamp(updated_time_ms / 1000).replace(
                microsecond=0
            )
            updated_time_str = updated_time.strftime("%Y-%m-%d %H:%M:%S")
            exec_id = position.get("execId", position.get("orderId", ""))

            # Проверяем дубликаты по временной метке (для совместимости с текущей таблицей)
            if updated_time_str in existing_timestamps:
                self.logger.debug(
                    f"Пропущена дублирующаяся запись: time={updated_time_str}, execId={exec_id}"
                )
                continue

            symbol = position.get("symbol", "")
            exec_type = position.get("execType", "")
            stop_order_type = position.get("stopOrderType", "")
            if exec_type == "Trade":
                exec_reason = "Торговать"
            elif exec_type == "StopLoss" or stop_order_type == "StopLoss":
                exec_reason = "Стоп-лосс"
            elif exec_type == "TakeProfit" or stop_order_type == "TakeProfit":
                exec_reason = "Тейк-профит"
            else:
                exec_reason = "Неизвестно"

            row = [
                "",  # Идентификатор недели
                symbol,  # Контракт
                "LONG" if position.get("side", "") == "Buy" else "SHORT",  # Направление
                position.get("qty", ""),  # К-во
                position.get("avgEntryPrice", ""),  # Цена входа
                position.get("avgExitPrice", ""),  # Цена выхода
                position.get("closedPnl", ""),  # Закрытие P&L
                "0",  # Комиссия за открытие
                "0",  # Комиссия за закрытие
                "0",  # Комиссия за финансирование
                exec_reason,  # Исполненный тик
                updated_time_str,  # Время
            ]
            table_rows.append(row)
            self.logger.debug(f"Добавлена строка: {row}, execId={exec_id}")

        if not table_rows:
            self.logger.info("Новых уникальных сделок для добавления не найдено")
            self.telegram.send_message(
                "Новых уникальных сделок для добавления не найдено"
            )
            self.set_last_update_time(sheet_name, end_time)
            return True

        # Инициализация листа, если он не существует
        if not self.initialize_sheet(sheet_name):
            return False

        # Проверяем и расширяем лист, если нужно
        current_rows = len(sheet.get_all_values())  # Текущее количество строк
        required_rows = current_rows + len(table_rows)
        sheet = self.extend_sheet_if_needed(sheet_name, required_rows)
        if not sheet:
            return False

        # Записываем данные и обновляем M1 в одной транзакции
        try:
            updates = [
                {
                    "sheet": sheet_name,
                    "range": f"A{current_rows + 1}",  # Начинаем с первой пустой строки
                    "values": table_rows,
                },
                {
                    "sheet": sheet_name,
                    "range": "M1",
                    "values": [[end_time.strftime("%Y-%m-%d %H:%M:%S")]],
                },
            ]
            self.sheets.batch_update(updates)
            self.logger.info(f"Добавлено {len(table_rows)} строк в лист {sheet_name}")
            total_rows = self.sheets.get_row_count(sheet_name)
            self.logger.info(
                f"Общее количество строк в листе {sheet_name} после добавления: {total_rows}"
            )
        except HttpError as e:
            self.logger.error(f"Ошибка при пакетном обновлении Google Sheets: {e}")
            self.telegram.send_message(
                f"Ошибка при добавлении строк в листе {sheet_name}: {e}"
            )
            return False

        return True

    def initialize_sheet(self, sheet_name="Statistic"):
        """Инициализирует лист Statistic, если он не существует."""
        sheet = self.sheets.get_sheet(sheet_name)
        if not sheet:
            self.logger.info(f"Лист {sheet_name} не найден, создаем новый")
            try:
                self.sheets.add_worksheet(
                    sheet_name, rows=5000, cols=13
                )  # 13 столбцов для текущей структуры
                headers = [
                    "Идентификатор недели",
                    "Контракт",
                    "Направление закрытия",
                    "К-во",
                    "Цена входа",
                    "Цена выхода",
                    "Закрытие P&L",
                    "Комиссия за открытие",
                    "Комиссия за закрытие",
                    "Комиссия за финансирование",
                    "Исполненный тик",
                    "Время",
                    "Последнее обновление",  # Для M1
                ]
                sheet = self.sheets.get_sheet(sheet_name)
                sheet.update("A1:M1", [headers])
                self.logger.info(f"Лист {sheet_name} создан с заголовками")
                self.telegram.send_message(
                    f"Лист {sheet_name} создан в Google Sheets с заголовками"
                )
            except Exception as e:
                self.logger.error(f"Ошибка при создании листа {sheet_name}: {e}")
                self.telegram.send_message(
                    f"Ошибка при создании листа {sheet_name}: {e}"
                )
                return False

        # Проверяем и исправляем заголовки
        try:
            headers = sheet.get_all_values()[0]
            expected_headers = [
                "Идентификатор недели",
                "Контракт",
                "Направление закрытия",
                "К-во",
                "Цена входа",
                "Цена выхода",
                "Закрытие P&L",
                "Комиссия за открытие",
                "Комиссия за закрытие",
                "Комиссия за финансирование",
                "Исполненный тик",
                "Время",
                "Последнее обновление",
            ]
            if len(headers) != len(expected_headers) or headers != expected_headers:
                self.logger.warning(
                    f"Заголовки в листе {sheet_name} отличаются: {headers}"
                )
                self.logger.info(f"Исправляем заголовки на правильные")
                sheet.update("A1:M1", [expected_headers])
            else:
                self.logger.info(f"Заголовки в листе {sheet_name} корректны")
        except Exception as e:
            self.logger.error(f"Ошибка при проверке или обновлении заголовков: {e}")
            self.telegram.send_message(
                f"Ошибка при обновлении заголовков в листе {sheet_name}: {e}"
            )
            return False
        return True

    def update_statistics(self):
        """Обновляет статистику закрытых сделок с последнего обновления."""
        self.logger.info("Запуск обновления статистики закрытых сделок")

        # Обрабатываем данные с последнего обновления
        success = self.process_from_last_update()
        if not success:
            self.logger.error("Не удалось обработать данные с последнего обновления")
            return

        # Отправить уведомление
        try:
            self.telegram.send_message(
                f"Обновлена статистика с последнего обновления до {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )
        except Exception as e:
            self.logger.error(f"Ошибка при отправке уведомления в Telegram: {e}")


def run_statistics_collection(bybit_api, sheets_client, telegram_client):
    """Запускает сбор статистики."""
    collector = StatisticsCollector(bybit_api, sheets_client, telegram_client)
    collector.update_statistics()

