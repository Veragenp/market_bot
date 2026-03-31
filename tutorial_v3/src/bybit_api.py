from pybit.unified_trading import HTTP, WebSocket
import logging
import time
import requests
import os
from datetime import datetime
from utils import LoggerSetup


class BybitAPI:
    """Синглтон-класс для работы с Bybit API."""

    _instance = None

    def __new__(cls, api_key=None, api_secret=None):
        if cls._instance is None:
            cls._instance = super(BybitAPI, cls).__new__(cls)
            cls._instance.logger = LoggerSetup.setup_logging("bybit_api")
            cls._instance.logger.info("Инициализация BybitAPI (синглтон)")
            cls._instance.api_key = api_key
            cls._instance.api_secret = api_secret
            cls._instance.session = HTTP(
                api_key=api_key, api_secret=api_secret, testnet=False
            )
            cls._instance.ws = WebSocket(testnet=False, channel_type="linear")
            cls._instance.logger.info("HTTP и WebSocket клиенты инициализированы")
        return cls._instance

    def get_last_7_days_high_low(self, symbol, days=7):
        self.logger.debug(f"Запрос high/low для {symbol}, период: {days} дней")
        try:
            end_time = int(time.time() * 1000) - (86400 * 1000)
            start_time = end_time - (days * 86400 * 1000)
            response = self.session.get_kline(
                category="linear",
                symbol=symbol,
                interval="D",
                start=start_time,
                end=end_time,
                limit=days,
            )
            if response["retCode"] == 0 and response["result"]["list"]:
                candles = response["result"]["list"]
                candles.sort(key=lambda x: int(x[0]), reverse=True)
                result = [(float(candle[2]), float(candle[3])) for candle in candles]
                self.logger.info(f"Успешно получены high/low для {symbol}: {result}")
                return result
            self.logger.error(
                f"Ошибка получения исторических данных для {symbol}: {response['retMsg']}"
            )
            return []
        except Exception as e:
            self.logger.error(
                f"Исключение при запросе исторических данных для {symbol}: {e}"
            )
            return []

    def get_24h_volume(self, symbol):
        self.logger.debug(f"Запрос объема торгов за 24 часа для {symbol}")
        try:
            base_url = "https://api.bybit.com"
            endpoint = "/v5/market/tickers"
            params = {"category": "linear", "symbol": symbol}
            response = requests.get(base_url + endpoint, params=params)
            data = response.json()
            if data["retCode"] == 0 and data["result"]["list"]:
                volume = float(data["result"]["list"][0]["turnover24h"])
                self.logger.info(f"Объем торгов за 24 часа для {symbol}: {volume} USDT")
                return volume
            self.logger.error(
                f"Не удалось получить объем для {symbol}: {data.get('retMsg', 'Нет данных')}"
            )
            return 0
        except Exception as e:
            self.logger.error(f"Исключение при запросе объема для {symbol}: {e}")
            return 0

    def get_positions(self, category, symbol):
        self.logger.debug(f"Запрос позиций для {symbol}")
        try:
            response = self.session.get_positions(category=category, symbol=symbol)
            if response["retCode"] == 0:
                self.logger.info(f"Позиции для {symbol}: {response['result']['list']}")
                return response
            self.logger.error(
                f"Ошибка получения позиций для {symbol}: {response['retMsg']}"
            )
            return response
        except Exception as e:
            self.logger.error(f"Исключение при получении позиций для {symbol}: {e}")
            return {"retCode": -1, "retMsg": str(e), "result": {"list": []}}

    def get_closed_pnl(self, category, symbol):
        self.logger.debug(f"Запрос закрытых позиций для {symbol}")
        try:
            response = self.session.get_closed_pnl(category=category, symbol=symbol)
            if response["retCode"] == 0:
                self.logger.info(f"Получены данные закрытых позиций для {symbol}")
                return response["result"]["list"]
            self.logger.error(
                f"Ошибка получения закрытых позиций: {response['retMsg']}"
            )
            return []
        except Exception as e:
            self.logger.error(f"Исключение при запросе закрытых позиций: {e}")
            return []

    def get_fee_rates(self, symbol):
        self.logger.debug(f"Запрос комиссий для {symbol}")
        try:
            response = self.session.get_fee_rates(category="linear", symbol=symbol)
            if response["retCode"] == 0 and response["result"]["list"]:
                fee_data = response["result"]["list"][0]
                maker_fee = float(fee_data.get("makerFeeRate", 0))
                taker_fee = float(fee_data.get("takerFeeRate", 0))
                self.logger.info(
                    f"Комиссии для {symbol}: maker_fee={maker_fee}, taker_fee={taker_fee}"
                )
                return maker_fee, taker_fee
            self.logger.error(
                f"Не удалось получить комиссии для {symbol}: {response['retMsg']}"
            )
            return 0, 0
        except Exception as e:
            self.logger.error(f"Исключение при запросе комиссий для {symbol}: {e}")
            return 0, 0

    def get_futures_instruments(self, limit=500):
        self.logger.debug(f"Запрос списка фьючерсных инструментов, limit={limit}")
        try:
            all_symbols = []
            cursor = None
            while True:
                response = self.session.get_instruments_info(
                    category="linear", limit=limit, cursor=cursor
                )
                if response["retCode"] != 0:
                    self.logger.error(
                        f"Ошибка получения списка фьючерсов: {response['retMsg']}"
                    )
                    return all_symbols
                instruments = response["result"]["list"]
                symbols = [item["symbol"] for item in instruments]
                all_symbols.extend(symbols)
                cursor = response["result"].get("nextPageCursor")
                self.logger.info(
                    f"Получено {len(instruments)} символов, всего: {len(all_symbols)}"
                )
                if not cursor or len(instruments) < limit:
                    break
                time.sleep(0.1)
            self.logger.info(
                f"Всего получено {len(all_symbols)} фьючерсных инструментов"
            )
            return all_symbols
        except Exception as e:
            self.logger.error(f"Исключение при запросе списка фьючерсов: {e}")
            return []

    def subscribe_to_ticker(self, symbols, callback):
        def handle_message(message):
            if "topic" in message and "data" in message:
                symbol = message["topic"].split(".")[1]
                last_price = float(message["data"]["lastPrice"])
                callback(symbol, last_price)

        for symbol in symbols:
            self.ws.ticker_stream(symbol=symbol, callback=handle_message)

    def get_open_positions(self):
        self.logger.debug("Запрос количества открытых позиций")
        try:
            response = self.session.get_positions(category="linear", settleCoin="USDT")
            if response["retCode"] == 0:
                positions = [
                    pos for pos in response["result"]["list"] if float(pos["size"]) > 0
                ]
                self.logger.info(f"Открытых позиций: {len(positions)}")
                return len(positions)
            self.logger.error(f"Ошибка получения позиций: {response['retMsg']}")
            return 0
        except Exception as e:
            self.logger.error(f"Исключение при получении позиций: {e}")
            return 0

    def place_limit_order(
        self, symbol, side, qty, price, take_profit=None, stop_loss=None
    ):
        self.logger.debug(
            f"Размещение ордера: symbol={symbol}, side={side}, qty={qty}, price={price}, "
            f"take_profit={take_profit}, stop_loss={stop_loss}"
        )
        try:
            response = self.session.place_order(
                category="linear",
                symbol=symbol,
                side=side,
                orderType="Limit",
                qty=str(qty),
                price=str(price),
                timeInForce="GTC",
                takeProfit=str(take_profit) if take_profit else None,
                stopLoss=str(stop_loss) if stop_loss else None,
            )
            if response["retCode"] == 0:
                order_id = response["result"]["orderId"]
                self.logger.info(f"Ордер размещен: {order_id}")
                return order_id
            self.logger.error(f"Ошибка размещения ордера: {response['retMsg']}")
            return None
        except Exception as e:
            self.logger.error(f"Исключение при размещении ордера: {e}")
            return None

    def get_order_status(self, order_id):
        self.logger.debug(f"Запрос статуса ордера: order_id={order_id}")
        try:
            # Сначала проверяем активные ордера
            response = self.session.get_open_orders(category="linear", orderId=order_id)
            self.logger.debug(f"Ответ от get_open_orders: {response}")
            if response["retCode"] == 0 and response["result"]["list"]:
                order_info = response["result"]["list"][0]
                status = order_info["orderStatus"]
                reject_reason = order_info.get("rejectReason", "")
                self.logger.info(
                    f"Статус ордера {order_id}: {status}, причина отклонения: {reject_reason}"
                )
                return {"status": status, "rejectReason": reject_reason}

            # Если ордер не найден среди активных, проверяем историю
            response = self.session.get_order_history(
                category="linear", orderId=order_id
            )
            self.logger.debug(f"Ответ от get_order_history: {response}")
            if response["retCode"] == 0 and response["result"]["list"]:
                order_info = response["result"]["list"][0]
                status = order_info["orderStatus"]
                reject_reason = order_info.get("rejectReason", "")
                self.logger.info(
                    f"Статус ордера {order_id}: {status}, причина отклонения: {reject_reason}"
                )
                return {"status": status, "rejectReason": reject_reason}

            self.logger.error(f"Ордер {order_id} не найден ни в активных, ни в истории")
            return {"status": "Unknown", "rejectReason": "Order not found"}
        except Exception as e:
            self.logger.error(f"Исключение при запросе статуса ордера {order_id}: {e}")
            return {"status": "Unknown", "rejectReason": str(e)}

    def cancel_order(self, order_id, symbol):
        """Отменяет ордер по его ID и символу."""
        self.logger.debug(
            f"Запрос на отмену ордера: order_id={order_id}, symbol={symbol}"
        )
        try:
            response = self.session.cancel_order(
                category="linear", orderId=order_id, symbol=symbol
            )
            if response["retCode"] == 0:
                self.logger.info(f"Ордер {order_id} успешно отменен")
                return True
            self.logger.error(f"Ошибка отмены ордера {order_id}: {response['retMsg']}")
            self.logger.debug(f"Ответ cancel_order: {response}")
            return False
        except Exception as e:
            self.logger.error(f"Исключение при отмене ордера {order_id}: {e}")
            self.logger.debug(f"Ответ cancel_order: исключение {e}")
            return False

    def cancel_all_orders(self):
        self.logger.debug("Запрос на отмену всех открытых ордеров")
        try:
            response = self.session.cancel_all_orders(category="linear")
            if response["retCode"] == 0:
                self.logger.info(
                    f"Все ордеры отменены: {len(response['result']['list'])} ордеров"
                )
                return True
            self.logger.error(f"Ошибка отмены ордеров: {response['retMsg']}")
            return False
        except Exception as e:
            self.logger.error(f"Исключение при отмене ордеров: {e}")
            return False
