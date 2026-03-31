import logging
import os
import time
import threading
import json  # Добавлено для возможного сохранения processed_closed_positions
from datetime import datetime
from bybit_api import BybitAPI
from google_sheets import GoogleSheetsClient
from telegram_client import TelegramClient
from config import (
    GOOGLE_SHEETS_CREDENTIALS,
    GOOGLE_SHEETS_ID,
    BYBIT_API_KEY,
    BYBIT_API_SECRET,
    TELEGRAM_TOKEN,
    CHAT_ID,
    MAX_LONG_TRADES,
    MAX_SHORT_TRADES,
)
from utils import LoggerSetup


class TradeCallback:
    def __init__(self, manager, key):
        self.manager = manager
        self.key = key

    def __call__(self, action):
        self.manager.handle_response(self.key, action)


class TradeManager:
    def __init__(self, main, bybit_api, telegram_token, chat_id):
        """Инициализация TradeManager."""
        self.main = main
        self.logger = LoggerSetup.setup_logging("trade_manager")
        self.logger.info("Инициализация TradeManager")
        self.bybit = bybit_api
        self.sheets = GoogleSheetsClient(GOOGLE_SHEETS_CREDENTIALS, GOOGLE_SHEETS_ID)
        self.telegram_client = TelegramClient(telegram_token, chat_id)
        self.chat_id = chat_id
        self.pending_confirmation = {}  # Ожидающие подтверждения сделки
        self.active_orders = {}  # Активные ордера
        self.executed_long_trades = {}  # Исполненные LONG позиции
        self.executed_short_trades = {}  # Исполненные SHORT позиции
        self.max_long_trades = MAX_LONG_TRADES
        self.max_short_trades = MAX_SHORT_TRADES
        self.processed_keys = set()  # Обработанные ключи для подтверждений
        self.pnl_cache = {}  # Кэш для результатов get_closed_pnl
        self.processed_closed_positions = (
            set()
        )  # Множество для отслеживания закрытых позиций
        self.logger.info(
            "Инициализация словарей: active_orders, executed_long_trades, executed_short_trades пусты"
        )
        self.logger.info(
            "Инициализация processed_closed_positions для отслеживания закрытых позиций"
        )

    def process_pending_trades(self, trades):
        """Обрабатывает ожидающие сделки."""
        self.logger.info(f"Обработка {len(trades)} ожидающих сделок")
        for trade in trades:
            sheet_name = trade["sheet"]
            sheet = self.sheets.get_sheet(sheet_name)
            if not sheet:
                self.logger.error(f"Не удалось получить лист {sheet_name}")
                continue

            if not trade["stop_loss"]:
                row_idx = trade["row"]
                self.sheets.cancel_trade(sheet_name, row_idx)
                self.sheets.update_trade_status(
                    sheet_name, row_idx, "отменено: стоп-лосс не установлен"
                )
                self.telegram_client.send_message(
                    f"Сделка для {trade['coin']} отменена: стоп-лосс не установлен."
                )
                continue

            trade_key = (trade["sheet"], trade["row"])
            already_pending = any(
                data["trade"]["sheet"] == trade["sheet"]
                and data["trade"]["row"] == trade["row"]
                for data in self.pending_confirmation.values()
            )
            if already_pending:
                self.logger.debug(
                    f"Сделка {trade['coin']} уже ожидает подтверждения, пропускаем"
                )
                continue

            row_idx = trade["row"]
            self.sheets.update_trade_status(sheet_name, row_idx, "вход, ожидание")

            message = (
                f"Подтвердите вход в сделку для {trade['coin']}:\n"
                f"Тип: {trade['side']}\n"
                f"Цена: {trade['entry_price']}\n"
                f"Количество: {trade['qty']}\n"
                f"Тейк-профит: {trade['take_profit'] if trade['take_profit'] else 'не установлен'}\n"
                f"Стоп-лосс: {trade['stop_loss']}\n"
            )
            self.logger.info(f"Отправка запроса на подтверждение: {trade['coin']}")
            callback_id = f"trade_{trade['sheet']}_{trade['row']}_{int(time.time())}"
            message_id = self.telegram_client.send_message(
                message, with_buttons=True, callback_id=callback_id
            )

            if message_id:
                key = (int(self.chat_id), message_id)
                self.pending_confirmation[key] = {
                    "trade": trade,
                    "sheet": sheet,
                    "sheet_name": sheet_name,
                    "callback_id": callback_id,
                }
                self.logger.debug(
                    f"Регистрация callback для callback_id={callback_id}, key={key}"
                )
                callback = TradeCallback(self, key)
                self.telegram_client.register_trade_callback(callback_id, callback)

    def handle_response(self, key, action):
        """Обработка ответа пользователя (текстового или через кнопку)."""
        self.logger.debug(f"Вызов handle_response: key={key}, action={action}")
        if key not in self.pending_confirmation:
            self.logger.warning(f"Не найдена сделка для ключа {key}")
            return

        if key in self.processed_keys:
            self.logger.warning(f"Повторная обработка ключа {key}, пропускаем")
            return
        self.processed_keys.add(key)

        trade_data = self.pending_confirmation[key]
        coin = trade_data["trade"]["coin"]

        if action in ["yes", "да"]:
            self.logger.info(f"Сделка подтверждена пользователем: {coin}")
            self.execute_trade(trade_data)
            self.telegram_client.send_message(f"Сделка для {coin} подтверждена.")
        else:
            self.logger.info(f"Сделка отменена пользователем: {coin}")
            self.cancel_trade(trade_data, "отменаП")
            self.telegram_client.send_message(
                f"Сделка для {coin} отменена пользователем."
            )

        del self.pending_confirmation[key]

    def execute_trade(self, trade_data):
        """Выполняет сделку на бирже."""
        trade = trade_data["trade"]
        sheet_name = trade_data["sheet_name"]
        coin = trade["coin"]
        row_idx = trade["row"]
        side = trade["side"]

        # Проверка лимитов перед размещением ордера
        if side == "Buy" and len(self.executed_long_trades) >= self.max_long_trades:
            self.logger.info(
                f"Сделка для {coin} отменена: достигнут лимит LONG позиций"
            )
            self.sheets.cancel_trade(sheet_name, row_idx)
            self.sheets.update_trade_status(sheet_name, row_idx, "отменено: лимит LONG")
            self.telegram_client.send_message(
                f"Сделка для {coin} отменена: достигнут лимит LONG позиций ({self.max_long_trades})"
            )
            return
        if side == "Sell" and len(self.executed_short_trades) >= self.max_short_trades:
            self.logger.info(
                f"Сделка для {coin} отменена: достигнут лимит SHORT позиций"
            )
            self.sheets.cancel_trade(sheet_name, row_idx)
            self.sheets.update_trade_status(
                sheet_name, row_idx, "отменено: лимит SHORT"
            )
            self.telegram_client.send_message(
                f"Сделка для {coin} отменена: достигнут лимит SHORT позиций ({self.max_short_trades})"
            )
            return

        order_id = self.bybit.place_limit_order(
            symbol=coin,
            side=side,
            qty=trade["qty"],
            price=trade["entry_price"],
            take_profit=trade["take_profit"],
            stop_loss=trade["stop_loss"],
        )

        if order_id:
            self.logger.debug(f"Обновление статуса для {coin}: ордер, row={row_idx}")
            self.sheets.update_trade_status(sheet_name, row_idx, "ордер")
            self.sheets.cancel_trade(sheet_name, row_idx)  # Сбрасываем F
            self.telegram_client.send_message(
                f"Ордер для {coin} размещен, ожидает выполнения. Order ID: {order_id}"
            )
            self.active_orders[order_id] = {
                "trade_data": trade_data,
                "row_idx": row_idx,
            }
        else:
            self.logger.error(f"Ошибка размещения ордера для {coin}")
            self.sheets.update_trade_status(sheet_name, row_idx, "ошибка входа")
            self.sheets.cancel_trade(sheet_name, row_idx)
            self.telegram_client.send_message(f"Ошибка размещения ордера для {coin}.")

    def cancel_trade(self, trade_data, reason):
        """Отменяет сделку и обновляет статус."""
        trade = trade_data["trade"]
        sheet_name = trade_data["sheet_name"]
        row_idx = trade["row"]
        self.sheets.cancel_trade(sheet_name, row_idx)
        self.sheets.update_trade_status(sheet_name, row_idx, reason)

    def monitor_orders(self):
        """Периодически проверяет статус активных ордеров и исполненных позиций."""
        while self.main.running:
            try:
                if (
                    not self.active_orders
                    and not self.executed_long_trades
                    and not self.executed_short_trades
                ):
                    time.sleep(5)  # Пауза, если нет ордеров или позиций
                    continue

                self.logger.info(
                    f"Проверка статуса {len(self.active_orders)} активных ордеров"
                )
                self.logger.info(
                    f"Исполненных LONG позиций: {len(self.executed_long_trades)}, "
                    f"SHORT позиций: {len(self.executed_short_trades)}"
                )

                # Проверка лимита для LONG и SHORT позиций
                long_limit_reached = (
                    len(self.executed_long_trades) >= self.max_long_trades
                )
                short_limit_reached = (
                    len(self.executed_short_trades) >= self.max_short_trades
                )

                if long_limit_reached or short_limit_reached:
                    self.logger.warning(
                        f"Достигнут лимит позиций: LONG ({len(self.executed_long_trades)}/{self.max_long_trades}), "
                        f"SHORT ({len(self.executed_short_trades)}/{self.max_short_trades})"
                    )
                    for order_id in list(self.active_orders.keys()):
                        trade_data = self.active_orders[order_id]["trade_data"]
                        coin = trade_data["trade"]["coin"]
                        side = trade_data["trade"]["side"]
                        if (side == "Buy" and long_limit_reached) or (
                            side == "Sell" and short_limit_reached
                        ):
                            order_status = self.bybit.get_order_status(
                                order_id=order_id
                            )
                            if order_status["status"] == "New":
                                row_idx = self.active_orders[order_id]["row_idx"]
                                sheet_name = trade_data["sheet_name"]
                                max_attempts = 3
                                for attempt in range(max_attempts):
                                    if self.bybit.cancel_order(
                                        order_id=order_id, symbol=coin
                                    ):
                                        self.sheets.cancel_trade(sheet_name, row_idx)
                                        self.sheets.update_trade_status(
                                            sheet_name,
                                            row_idx,
                                            "отменено: лимит позиций",
                                        )
                                        self.telegram_client.send_message(
                                            f"Ордер для {coin} отменен: достигнут лимит {side} позиций "
                                            f"({'LONG' if side == 'Buy' else 'SHORT'}, {self.max_long_trades if side == 'Buy' else self.max_short_trades}). "
                                            f"Order ID: {order_id}"
                                        )
                                        self.logger.info(
                                            f"Ордер {order_id} для {coin} отменен из-за лимита {side} позиций"
                                        )
                                        del self.active_orders[order_id]
                                        break
                                    else:
                                        self.logger.error(
                                            f"Попытка {attempt + 1}/{max_attempts}: Не удалось отменить ордер {order_id} для {coin}"
                                        )
                                        if attempt < max_attempts - 1:
                                            time.sleep(2)
                                        else:
                                            self.logger.critical(
                                                f"Ордер {order_id} для {coin} не отменен после {max_attempts} попыток"
                                            )
                                    time.sleep(0.1)  # Задержка между вызовами API

                # Проверка статуса активных ордеров
                for order_id in list(self.active_orders.keys()):
                    trade_data = self.active_orders[order_id]["trade_data"]
                    row_idx = self.active_orders[order_id]["row_idx"]
                    coin = trade_data["trade"]["coin"]
                    sheet_name = trade_data["sheet_name"]
                    side = trade_data["trade"]["side"]

                    order_status = self.bybit.get_order_status(order_id=order_id)
                    self.logger.debug(
                        f"Статус ордера {order_id} для {coin}: {order_status}"
                    )
                    if order_status["status"] == "Filled":
                        self.sheets.update_trade_status(sheet_name, row_idx, "вход")
                        self.telegram_client.send_message(
                            f"Ордер для {coin} исполнен, сделка активна. Order ID: {order_id}"
                        )
                        self.logger.info(f"Ордер {order_id} для {coin} исполнен")
                        executed_trades = (
                            self.executed_long_trades
                            if side == "Buy"
                            else self.executed_short_trades
                        )
                        executed_trades[order_id] = {
                            "trade_data": trade_data,
                            "row_idx": row_idx,
                            "timestamp": time.time(),
                        }
                        self.logger.info(
                            f"Добавлен ордер {order_id} в {'LONG' if side == 'Buy' else 'SHORT'} позиции"
                        )
                        del self.active_orders[order_id]
                    elif order_status["status"] in ["Cancelled", "Rejected"]:
                        reason = order_status.get("rejectReason", "причина неизвестна")
                        self.sheets.update_trade_status(sheet_name, row_idx, "отменаБ")
                        self.telegram_client.send_message(
                            f"Ордер для {coin} отклонен биржей: {reason}. Order ID: {order_id}"
                        )
                        self.logger.warning(
                            f"Ордер {order_id} для {coin} отклонен: {reason}"
                        )
                        del self.active_orders[order_id]
                    elif order_status["status"] == "New":
                        self.logger.debug(
                            f"Ордер {order_id} для {coin} ожидает исполнения"
                        )
                        continue
                    else:
                        self.logger.warning(
                            f"Неизвестный статус ордера {order_id}: {order_status['status']}"
                        )
                        self.sheets.update_trade_status(
                            sheet_name, row_idx, "ошибка статуса"
                        )
                        self.telegram_client.send_message(
                            f"Ордер для {coin} имеет неизвестный статус: {order_status['status']}. Order ID: {order_id}"
                        )
                        del self.active_orders[order_id]
                    time.sleep(0.1)  # Задержка между вызовами API

                # Проверка статуса исполненных позиций для выявления закрытия
                for executed_trades, side_name in [
                    (self.executed_long_trades, "LONG"),
                    (self.executed_short_trades, "SHORT"),
                ]:
                    for order_id, data in list(executed_trades.items()):
                        trade_data = data["trade_data"]
                        coin = trade_data["trade"]["coin"]
                        row_idx = data["row_idx"]
                        sheet_name = trade_data["sheet_name"]

                        # Пропускаем позицию, если она уже обработана как закрытая
                        if order_id in self.processed_closed_positions:
                            self.logger.debug(
                                f"Позиция {order_id} для {coin} ({side_name}) уже обработана как закрытая, пропуск"
                            )
                            continue

                        try:
                            # Проверка текущего состояния позиции
                            response = self.bybit.get_positions(
                                category="linear", symbol=coin
                            )
                            if response["retCode"] == 0:
                                positions = response["result"]["list"]
                                position = next(
                                    (
                                        p
                                        for p in positions
                                        if p["side"]
                                        == ("Buy" if side_name == "LONG" else "Sell")
                                    ),
                                    None,
                                )
                                if not position or float(position.get("size", 0)) == 0:
                                    # Позиция закрыта, обрабатываем один раз
                                    cache_key = f"{coin}_{side_name}"
                                    if (
                                        cache_key in self.pnl_cache
                                        and (
                                            time.time()
                                            - self.pnl_cache[cache_key]["timestamp"]
                                        )
                                        < 30
                                    ):
                                        closed_pnl = self.pnl_cache[cache_key]["data"]
                                        self.logger.debug(
                                            f"Использован кэш get_closed_pnl для {coin}"
                                        )
                                    else:
                                        closed_pnl = self.bybit.get_closed_pnl(
                                            category="linear", symbol=coin
                                        )
                                        self.pnl_cache[cache_key] = {
                                            "data": closed_pnl,
                                            "timestamp": time.time(),
                                        }
                                        self.logger.debug(
                                            f"Получены данные get_closed_pnl для {coin}"
                                        )

                                    # Определение причины закрытия
                                    status = "закрыто"
                                    reason = "неизвестно"
                                    for record in closed_pnl:
                                        exec_type = record.get("execType", "")
                                        stop_order_type = record.get(
                                            "stopOrderType", ""
                                        )
                                        if (
                                            exec_type == "StopLoss"
                                            or stop_order_type == "StopLoss"
                                        ):
                                            reason = "стоп-лосс"
                                        elif (
                                            exec_type == "TakeProfit"
                                            or stop_order_type == "TakeProfit"
                                        ):
                                            reason = "тейк-профит"
                                        break  # Берем первую подходящую запись

                                    # Обновляем статус в Google Sheets
                                    self.sheets.update_trade_status(
                                        sheet_name, row_idx, status
                                    )
                                    # Отправляем сообщение в Telegram
                                    self.telegram_client.send_message(
                                        f"Позиция для {coin} ({side_name}) закрыта по {reason}. Order ID: {order_id}"
                                    )
                                    self.logger.info(
                                        f"Позиция для {coin} ({side_name}) закрыта по {reason}, Order ID: {order_id}"
                                    )

                                    # Отмечаем позицию как обработанную
                                    self.processed_closed_positions.add(order_id)
                                    self.logger.debug(
                                        f"Позиция {order_id} для {coin} добавлена в processed_closed_positions"
                                    )

                                    # Очищаем кэш для предотвращения накопления данных
                                    if cache_key in self.pnl_cache:
                                        del self.pnl_cache[cache_key]
                                        self.logger.debug(
                                            f"Очищен кэш pnl_cache для {cache_key}"
                                        )
                            elif response["retCode"] == 10001:
                                self.logger.warning(
                                    "Превышен лимит запросов API, пауза 10 секунд"
                                )
                                time.sleep(10)
                                continue
                            else:
                                self.logger.error(
                                    f"Ошибка get_positions для {coin}: {response['retMsg']}"
                                )
                        except Exception as e:
                            self.logger.error(
                                f"Исключение при проверке позиции для {coin}: {e}"
                            )
                        time.sleep(0.1)  # Задержка между вызовами API

                time.sleep(5)  # Пауза между циклами проверки
            except Exception as e:
                self.logger.error(f"Ошибка при проверке статуса ордеров: {e}")
                time.sleep(5)

    def check_trades(self):
        """Проверяет наличие ожидающих сделок."""
        try:
            while self.main.running:
                self.logger.info("Начало цикла проверки сделок")
                try:
                    trades = self.sheets.get_pending_trades()
                    if trades:
                        self.logger.info(f"Найдено {len(trades)} ожидающих сделок")
                        self.process_pending_trades(trades)
                    time.sleep(15)
                except Exception as e:
                    self.logger.error(f"Ошибка при получении ожидающих сделок: {e}")
                self.logger.info("Конец цикла проверки сделок")
        except KeyboardInterrupt:
            self.logger.info("Остановлено пользователем")
            self.main.running = False

    def run(self):
        """Запускает TradeManager."""
        self.logger.info("Запуск TradeManager...")
        check_thread = threading.Thread(target=self.check_trades, daemon=True)
        check_thread.start()
        monitor_thread = threading.Thread(target=self.monitor_orders, daemon=True)
        monitor_thread.start()
        try:
            while self.main.running:
                time.sleep(60)
        except KeyboardInterrupt:
            self.logger.info("Остановка TradeManager")
            # Логирование состояния словарей при завершении
            with open("trade_manager_state.log", "a") as f:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                f.write(f"{timestamp} - TradeManager shutdown:\n")
                f.write(f"active_orders: {self.active_orders}\n")
                f.write(f"executed_long_trades: {self.executed_long_trades}\n")
                f.write(f"executed_short_trades: {self.executed_short_trades}\n")
                f.write(
                    f"processed_closed_positions: {self.processed_closed_positions}\n"
                )
            self.main.running = False
