import logging
import os
import requests
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, MessageHandler, CallbackQueryHandler, filters
from config import TELEGRAM_TOKEN, CHAT_ID
from utils import LoggerSetup


class TelegramClient:
    """Синглтон-класс для работы с Telegram: отправка сообщений и обработка ответов."""

    _instance = None

    def __new__(cls, telegram_token=TELEGRAM_TOKEN, chat_id=CHAT_ID):
        if cls._instance is None:
            cls._instance = super(TelegramClient, cls).__new__(cls)
            cls._instance.logger = LoggerSetup.setup_logging("telegram_client")
            cls._instance.logger.info("Инициализация TelegramClient (синглтон)")
            print("Инициализация TelegramClient")
            cls._instance.telegram_token = telegram_token
            cls._instance.chat_id = chat_id
            cls._instance.base_url = (
                f"https://api.telegram.org/bot{telegram_token}/sendMessage"
            )
            try:
                cls._instance.app = Application.builder().token(telegram_token).build()
                cls._instance.logger.info("Telegram Application инициализирован")
                print("Telegram Application инициализирован")
            except Exception as e:
                cls._instance.logger.error(
                    f"Ошибка при инициализации Telegram Application: {e}"
                )
                print(f"Ошибка при инициализации Telegram Application: {e}")
                raise
            cls._instance.trade_callbacks = {}
            cls._instance.general_callbacks = {}
            cls._instance.message_callbacks = {}
            cls._instance.processed_messages = (
                set()
            )  # Для отслеживания обработанных message_id
            cls._instance.app.add_handler(
                MessageHandler(
                    filters.TEXT & ~filters.COMMAND, cls._instance.handle_confirmation
                )
            )
            cls._instance.app.add_handler(
                CallbackQueryHandler(cls._instance.handle_button)
            )
            cls._instance.running = True
        return cls._instance

    def start_polling(self):
        """Запуск Telegram-бота в главном потоке."""
        try:
            self.logger.info("Starting Telegram polling...")
            print("Starting Telegram polling...")
            self.app.run_polling(allowed_updates=[])
        except Exception as e:
            self.logger.error(f"Ошибка в Telegram polling: {e}")
            print(f"Ошибка в Telegram polling: {e}")
            self.running = False

    def escape_html(self, text):
        """Экранирование HTML-символов в тексте."""
        text = text.replace("&", "&").replace("<", "<").replace(">", ">")
        text = text.replace("<=", "≤").replace(">=", "≥")
        return text

    def send_message(self, text, with_buttons=False, callback_id=None):
        """Отправляет сообщение в Telegram с опциональными кнопками."""
        text = self.escape_html(text)
        payload = {"chat_id": self.chat_id, "text": text, "parse_mode": "HTML"}

        if with_buttons:
            keyboard = [
                [
                    InlineKeyboardButton(
                        "Да",
                        callback_data=f"{callback_id}_yes" if callback_id else "yes",
                    ),
                    InlineKeyboardButton(
                        "Нет",
                        callback_data=f"{callback_id}_no" if callback_id else "no",
                    ),
                ]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            payload["reply_markup"] = reply_markup.to_dict()
            self.logger.debug(f"Кнопки добавлены: {keyboard}")
            print(f"Кнопки добавлены: {keyboard}")

        self.logger.debug(f"Отправка сообщения в Telegram: Payload={payload}")
        print(f"Отправка сообщения в Telegram: Payload={payload}")
        try:
            response = requests.post(self.base_url, json=payload)
            response.raise_for_status()
            self.logger.info(f"Сообщение отправлено в Telegram: {text}")
            print(f"Сообщение отправлено в Telegram: {text}")
            return response.json().get("result", {}).get("message_id")
        except Exception as e:
            self.logger.error(f"Ошибка при отправке сообщения в Telegram: {e}")
            print(f"Ошибка при отправке сообщения в Telegram: {e}")
            return None

    def register_trade_callback(self, callback_id, callback_func):
        """Регистрация callback-функции для торговых операций."""
        self.trade_callbacks[callback_id] = callback_func
        self.logger.info(f"Зарегистрирован trade callback для {callback_id}")
        print(f"Зарегистрирован trade callback для {callback_id}")

    def register_general_callback(self, callback_id, callback_func):
        """Регистрация callback-функции для общих операций."""
        self.general_callbacks[callback_id] = callback_func
        self.logger.info(f"Зарегистрирован general callback для {callback_id}")
        print(f"Зарегистрирован general callback для {callback_id}")

    def register_message_callback(self, message_id, callback_func):
        """Регистрация callback-функции для текстовых ответов."""
        self.message_callbacks[message_id] = callback_func
        self.logger.info(
            f"Зарегистрирован message callback для message_id={message_id}"
        )
        print(f"Зарегистрирован message callback для message_id={message_id}")

    async def handle_confirmation(self, update, context):
        """Обработка текстового подтверждения ('да'/'нет')."""
        self.logger.debug("Получен ответ от пользователя в Telegram")
        print("Получен ответ от пользователя в Telegram")
        message = update.message.text.lower()
        chat_id = update.effective_chat.id
        message_id = update.message.message_id

        self.logger.info(
            f"Получено сообщение: '{message}' от chat_id: {chat_id}, message_id: {message_id}"
        )
        print(
            f"Получено сообщение: '{message}' от chat_id: {chat_id}, message_id: {message_id}"
        )

        if chat_id != int(self.chat_id):
            self.logger.warning(
                f"Получен ответ от неизвестного chat_id: {chat_id}, ожидаемый chat_id: {self.chat_id}"
            )
            print(
                f"Получен ответ от неизвестного chat_id: {chat_id}, ожидаемый chat_id: {self.chat_id}"
            )
            await update.message.reply_text(
                "Вы не авторизованы для управления программой."
            )
            return

        if message not in ["да", "нет"]:
            self.logger.debug(f"Некорректный ответ: {message}")
            print(f"Некорректный ответ: {message}")
            await update.message.reply_text("Пожалуйста, ответьте 'да' или 'нет'.")
            return

        key = (chat_id, message_id - 1)
        callback_func = self.message_callbacks.get(key)
        if callback_func:
            callback_func(message)
            self.logger.info(
                f"Обработан текстовый ответ: {message} для message_id={message_id}"
            )
            print(f"Обработан текстовый ответ: {message} для message_id={message_id}")
            await update.message.reply_text("Ответ принят.")
            del self.message_callbacks[key]
        else:
            self.logger.warning(
                f"Не найден callback для текстового ответа: message_id={message_id}"
            )
            print(f"Не найден callback для текстового ответа: message_id={message_id}")

    async def handle_button(self, update, context):
        """Обработка нажатий кнопок ('Да'/'Нет') в Telegram."""
        query = update.callback_query
        await query.answer()

        chat_id = query.message.chat_id
        message_id = query.message.message_id
        data = query.data

        callback_id = data.rsplit("_", 1)[0] if "_" in data else data
        action = data.rsplit("_", 1)[-1] if "_" in data else data

        self.logger.debug(
            f"Обработка кнопки: data={data}, callback_id={callback_id}, action={action}"
        )

        # Проверяем, не обработан ли уже message_id
        if (chat_id, message_id) in self.processed_messages:
            self.logger.warning(
                f"Повторная обработка message_id={message_id}, игнорируем"
            )
            return

        # Удаляем кнопки сразу
        await query.message.edit_reply_markup(reply_markup=None)

        # Проверяем торговые callback'и
        callback_func = self.trade_callbacks.get(callback_id)
        if callback_func:
            self.logger.debug(f"Найден trade callback для {callback_id}")
            try:
                callback_func(action)
                self.logger.debug(
                    f"Вызван callback для {callback_id} с action={action}"
                )
                self.processed_messages.add(
                    (chat_id, message_id)
                )  # Помечаем как обработанный
            except Exception as e:
                self.logger.error(
                    f"Ошибка при вызове trade callback {callback_id}: {e}"
                )
            if callback_id in self.trade_callbacks:
                del self.trade_callbacks[callback_id]
            return

        # Проверяем общие callback'и
        callback_func = self.general_callbacks.get(callback_id)
        if callback_func:
            self.logger.debug(f"Найден general callback для {callback_id}")
            try:
                callback_func(action)
                self.logger.debug(
                    f"Вызван callback для {callback_id} с action={action}"
                )
                self.processed_messages.add(
                    (chat_id, message_id)
                )  # Помечаем как обработанный
            except Exception as e:
                self.logger.error(
                    f"Ошибка при вызове general callback {callback_id}: {e}"
                )
            return

        self.logger.warning(
            f"Не найден callback для {callback_id}: message_id={message_id}"
        )

    def stop(self):
        """Остановка Telegram-бота."""
        self.running = False
        self.app.stop()
        self.logger.info("TelegramClient остановлен")
        print("TelegramClient остановлен")


if __name__ == "__main__":
    client = TelegramClient()
    message_id = client.send_message(
        "Тестовое сообщение с <= и >", with_buttons=True, callback_id="test"
    )
    print(f"Message ID: {message_id}")
