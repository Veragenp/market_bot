import logging
import time
import os
from datetime import datetime
from bybit_api import BybitAPI
from google_sheets import GoogleSheetsClient
from config import (
    GOOGLE_SHEETS_CREDENTIALS,
    GOOGLE_SHEETS_ID,
    BYBIT_API_KEY,
    BYBIT_API_SECRET,
)
from utils import LoggerSetup


def populate_static_data():
    """Основная функция для заполнения статичных данных в Google Sheets."""
    logger = LoggerSetup.setup_logging("populate_static_data")
    logger.info("Запуск populate_static_data: обновление статичных данных")

    sheets_client = GoogleSheetsClient(GOOGLE_SHEETS_CREDENTIALS, GOOGLE_SHEETS_ID)
    bybit_api = BybitAPI(BYBIT_API_KEY, BYBIT_API_SECRET)

    logger.info("Попытка получить лист database")
    sheet = sheets_client.get_sheet("database")
    if not sheet:
        logger.error("Не удалось получить лист database")
        return

    logger.info("Получение списка фьючерсов")
    symbols = bybit_api.get_futures_instruments()
    logger.info(f"Получено {len(symbols)} символов")
    if not symbols:
        logger.error("Не удалось получить список инструментов")
        return

    headers = sheet.row_values(1)
    expected_headers = [
        "Монета",
        "ДЕНЬ 1",
        "ДЕНЬ 1",
        "ДЕНЬ 2",
        "ДЕНЬ 2",
        "ДЕНЬ 3",
        "ДЕНЬ 3",
        "ДЕНЬ 4",
        "ДЕНЬ 4",
        "ДЕНЬ 5",
        "ДЕНЬ 5",
        "ДЕНЬ 6",
        "ДЕНЬ 6",
        "ДЕНЬ 7",
        "ДЕНЬ 7",
        "ATR",
        "Размер тика",
        "Мин шаг покупки",
        "Средний объём",
        "Текущая цена",
        "Комиссия открытие",
        "Комиссия закрытие",
    ]
    if headers != expected_headers:
        logger.info("Обновление заголовков")
        updates = [
            {"sheet": "database", "range": "A1:V1", "values": [expected_headers]}
        ]
        try:
            sheets_client.batch_update(updates)
        except Exception as e:
            logger.error(f"Ошибка при обновлении заголовков: {e}")
            return
    else:
        logger.info("Заголовки корректны")

    logger.info("Получение размера тика, минимального шага покупки и комиссий")
    filtered_symbols = []
    for symbol in symbols:
        instrument_info = bybit_api.get_instrument_info(symbol)
        tick_size = float(instrument_info.get("priceFilter", {}).get("tickSize", 0))
        min_order_qty = float(
            instrument_info.get("lotSizeFilter", {}).get("minOrderQty", 0)
        )
        maker_fee, taker_fee = bybit_api.get_fee_rates(symbol)
        logger.info(
            f"Комиссии для {symbol}: taker_fee={taker_fee}, maker_fee={maker_fee}"
        )

        if tick_size > 0 and min_order_qty > 0:
            filtered_symbols.append(
                (symbol, tick_size, min_order_qty, taker_fee, maker_fee)
            )
        else:
            logger.warning(
                f"Пропущен символ {symbol}: tick_size={tick_size}, min_order_qty={min_order_qty}"
            )

    logger.info(f"Запись {len(filtered_symbols)} отфильтрованных символов в таблицу")
    values = []
    for symbol, tick_size, min_order_qty, taker_fee, maker_fee in filtered_symbols:
        row = (
            [symbol]
            + [""] * 14
            + ["", tick_size, min_order_qty, "", "", taker_fee, maker_fee]
        )
        values.append(row)

    range_name = f"A2:V{len(filtered_symbols) + 1}"
    try:
        updates = [{"sheet": "database", "range": range_name, "values": values}]
        sheets_client.batch_update(updates)
        logger.info(f"Успешно записаны данные в диапазон {range_name}")
    except Exception as e:
        logger.error(f"Ошибка при записи данных: {e}")
        return

    logger.info("Статичные данные успешно обновлены")


if __name__ == "__main__":
    populate_static_data()
