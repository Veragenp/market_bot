import logging
import time
import os
from datetime import datetime
from bybit_api import BybitAPI
from google_sheets import GoogleSheetsClient
from config import GOOGLE_SHEETS_CREDENTIALS, GOOGLE_SHEETS_ID, VOLUME_THRESHOLD
from utils import LoggerSetup


def calculate_atr(high_low_data):
    """Расчет ATR (Average True Range) как среднего диапазона (high - low) за 7 дней."""
    if not high_low_data:
        return 0
    tr_values = [high - low for high, low in high_low_data]
    if len(tr_values) > 7:
        tr_values = tr_values[:7]
    elif len(tr_values) < 7:
        tr_values.extend([0] * (7 - len(tr_values)))
    return sum(tr_values) / 7 if tr_values else 0


def populate_historical_data():
    """Основная функция для заполнения исторических данных в Google Sheets."""
    logger = LoggerSetup.setup_logging("populate_historical_data")
    logger.info(
        "Запуск populate_historical_data: обновление исторических данных, объёма и ATR"
    )

    # Инициализация синглтонов
    sheets_client = GoogleSheetsClient(GOOGLE_SHEETS_CREDENTIALS, GOOGLE_SHEETS_ID)
    bybit_api = BybitAPI()

    # Получаем лист "database"
    logger.info("Попытка получить лист database")
    sheet = sheets_client.get_sheet("database")
    if not sheet:
        logger.error("Не удалось получить лист database")
        return

    # Читаем все данные из листа
    logger.info("Получение существующих данных из листа")
    all_data = sheets_client.get_all_data(
        sheet, "database"
    )  # Обновлено для совместимости
    if len(all_data) < 2:
        logger.error(
            "В листе database отсутствуют монеты, сначала запустите populate_static_data.py"
        )
        return

    # Собираем уникальные символы из столбца A
    symbols = []
    seen = set()
    for row in all_data[1:]:
        if row and row[0] and isinstance(row[0], str) and row[0] not in seen:
            symbols.append(row[0])
            seen.add(row[0])
    logger.info(f"Найдено {len(symbols)} уникальных символов в таблице")

    # Подготавливаем список строк для записи
    updated_rows = [all_data[0]]
    volume_threshold = VOLUME_THRESHOLD
    filtered_count = 0

    logger.info("Обновление исторических данных, объёма и ATR")
    for idx, symbol in enumerate(symbols, start=1):
        logger.info(f"Обработка символа {symbol} (строка {idx + 1})")

        # Находим текущую строку или создаем пустую
        current_row = next((row for row in all_data[1:] if row[0] == symbol), [""] * 22)
        if len(current_row) < 22:
            current_row.extend([""] * (22 - len(current_row)))
        static_cols = [current_row[i] for i in [0, 16, 17, 19, 20, 21]]

        # Получаем объем торгов за 24 часа
        volume_usdt = bybit_api.get_24h_volume(symbol)
        logger.info(f"Средний объём за 24 часа для {symbol}: {volume_usdt} USDT")

        # Инициализируем значения
        low_day1 = ""
        historical_values = [""] * 13
        atr = ""

        if volume_usdt >= volume_threshold:
            high_low_data = bybit_api.get_last_7_days_high_low(symbol, days=7)
            logger.debug(f"Сырые данные high/low для {symbol}: {high_low_data}")

            if high_low_data:
                low_day1 = high_low_data[0][1]
                historical_values = []
                historical_values.append(high_low_data[0][0])
                for i, (high, low) in enumerate(high_low_data[1:], start=2):
                    historical_values.append(low)
                    historical_values.append(high)
                historical_values = historical_values[:13]
                while len(historical_values) < 13:
                    historical_values.append(0)
                logger.info(
                    f"Исторические данные для {symbol} (C–O): {historical_values}"
                )

                atr = calculate_atr(high_low_data)
                logger.info(f"ATR для {symbol}: {atr}")
        else:
            logger.info(
                f"Символ {symbol} пропущен: объём {volume_usdt} < {volume_threshold:,} USDT"
            )
            filtered_count += 1

        # Формируем новую строку
        new_row = (
            [static_cols[0]]
            + [low_day1]
            + historical_values
            + [atr]
            + [static_cols[1]]
            + [static_cols[2]]
            + [volume_usdt]
            + [static_cols[3]]
            + [static_cols[4]]
            + [static_cols[5]]
        )
        logger.info(f"Подготовлена строка для {symbol} (строка {idx + 1}): {new_row}")
        updated_rows.append(new_row)

        time.sleep(0.1)

    # Логируем статистику
    logger.info(
        f"Отфильтровано {filtered_count} символов с объёмом менее {volume_threshold:,} USDT"
    )
    logger.info(f"Подготовлено {len(updated_rows) - 1} строк для записи")

    # Определяем диапазон для записи
    range_name = f"A1:V{len(updated_rows)}"
    logger.info(f"Диапазон для записи: {range_name}")

    # Очищаем лист и записываем данные
    try:
        sheet.clear()
        logger.info("Лист database очищен перед записью")
        sheet.update(values=updated_rows, range_name=range_name)
        logger.info(f"Успешно обновлены данные в диапазоне {range_name}")

        # Проверяем запись
        updated_data = sheets_client.get_all_data(
            sheet, "database"
        )  # Обновлено для совместимости
        if updated_data == updated_rows:
            logger.info("Данные в таблице совпадают с подготовленными")
        else:
            logger.error("Данные в таблице не совпадают с подготовленными")
    except Exception as e:
        logger.error(f"Ошибка при записи данных: {e}", exc_info=True)
        return

    logger.info("Исторические данные, объём и ATR успешно обновлены")


if __name__ == "__main__":
    populate_historical_data()
